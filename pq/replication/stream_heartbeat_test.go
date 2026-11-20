package replication

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var heartbeatTable = publication.Table{Schema: "public", Name: "cdc_heartbeat"}

func heartbeatConfig() config.Config {
	return config.Config{Heartbeat: config.HeartbeatConfig{Table: heartbeatTable}}
}

// heartbeatEnd is where the heartbeat transaction in these tests ends.
const heartbeatEnd = 200

// heartbeatRow is the library's own heartbeat UPDATE, at its transaction-end LSN.
func heartbeatRow() *Message {
	return &Message{
		message:  &format.Update{TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name},
		walStart: heartbeatEnd,
	}
}

// bookRow is the last row of a business transaction ending at walStart; the
// listener tells rows apart by Insert.LSN.
func bookRow(walStart int64) *Message {
	return &Message{message: &format.Insert{TableName: "books", LSN: pq.LSN(walStart)}, walStart: walStart}
}

// runHeartbeatStream sends msgs the way the sink does, so they count as sent,
// and runs processLoop over them to completion. listener sees every message
// the heartbeat path does not take.
func runHeartbeatStream(t *testing.T, listener ListenerFunc, msgs ...*Message) (*stream, *messageBuffer) {
	t.Helper()
	s := NewStream("", heartbeatConfig(), metric.NewMetric("test_slot"), listener).(*stream)
	buf := &messageBuffer{outCh: s.messageCH}
	for _, m := range msgs {
		require.True(t, buf.send(m))
	}
	close(s.messageCH)
	require.NoError(t, s.processLoop(context.Background()))
	return s, buf
}

// A listener that buffers a row (a bulk connector waiting for its flush) holds
// it un-acked. Confirming the heartbeat past it would make the server skip the
// row on restart if the process dies before the flush.
func TestHeartbeatDoesNotConfirmPastUnackedMessage(t *testing.T) {
	held := make(chan *ListenerContext, 1)
	h := newKeepaliveHarness(t, func(l *ListenerContext) { held <- l })
	h.s.config = heartbeatConfig()
	h.startProcess()

	h.dispatch(&format.Begin{Xid: 5, FinalLSN: 110}, 10)
	h.dispatch(&format.Insert{XID: 5, TableName: "books"}, 20)
	h.dispatch(&format.Commit{CommitLSN: 110, TransactionEndLSN: 120}, 110)
	row := receiveListener(t, held) // delivered at 120, held un-acked

	h.dispatch(&format.Begin{Xid: 6, FinalLSN: 190}, 130)
	h.dispatch(&format.Update{XID: 6, TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name}, 180)
	h.dispatch(&format.Commit{CommitLSN: 190, TransactionEndLSN: 200}, 190)
	require.Eventually(t, func() bool { return h.s.acked.Load() == 1 }, 2*time.Second, time.Millisecond,
		"the heartbeat was not taken")
	assert.Zero(t, h.s.LoadConfirmedXLogPos(), "the heartbeat must not confirm past the un-acked row")
	assert.Zero(t, h.keepalive(300), "the row is still in flight")

	require.NoError(t, row.Ack())
	assert.Equal(t, pq.LSN(120), h.s.LoadConfirmedXLogPos())
	assert.Equal(t, h.buf.sent, h.s.acked.Load(), "the heartbeat counts as acked")

	// The stall has cleared: the next heartbeat confirms its own position.
	h.dispatch(&format.Begin{Xid: 8, FinalLSN: 240}, 210)
	h.dispatch(&format.Update{XID: 8, TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name}, 230)
	h.dispatch(&format.Commit{CommitLSN: 240, TransactionEndLSN: 250}, 240)
	require.Eventually(t, func() bool { return h.s.acked.Load() == 3 }, 2*time.Second, time.Millisecond,
		"the second heartbeat was not taken")
	assert.Equal(t, pq.LSN(250), h.s.LoadConfirmedXLogPos())
	assert.Equal(t, pq.LSN(300), h.keepalive(300), "idle: the keepalive confirms the WAL end")
}

// What the heartbeat is for: with everything handed to the listener acked, it
// confirms its own position.
func TestHeartbeatConfirmsWhenNothingIsInFlight(t *testing.T) {
	t.Run("nothing delivered", func(t *testing.T) {
		s, _ := runHeartbeatStream(t, nil, heartbeatRow())
		assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())
	})
	t.Run("everything delivered acked", func(t *testing.T) {
		s, buf := runHeartbeatStream(t, func(l *ListenerContext) { _ = l.Ack() },
			bookRow(100), bookRow(150), heartbeatRow())
		assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())
		assert.Equal(t, buf.sent, s.acked.Load())
	})
}

func TestHeartbeatRepeatedAckDoesNotCoverLaterMessage(t *testing.T) {
	s, _ := runHeartbeatStream(t, func(l *ListenerContext) {
		if l.Message.(*format.Insert).LSN == 100 {
			_ = l.Ack()
			_ = l.Ack()
		}
	}, bookRow(100), bookRow(150), heartbeatRow())
	assert.Equal(t, pq.LSN(100), s.LoadConfirmedXLogPos(), "150 is still un-acked")
}

// A batching listener acks only the last message it flushed. The confirmed
// position is a running maximum, so that ack already covers the batch, and the
// heartbeat keeps WAL bounded for such listeners (README).
func TestHeartbeatConfirmsAfterBatchAckOfLastMessage(t *testing.T) {
	var batch []*ListenerContext
	s, _ := runHeartbeatStream(t, func(l *ListenerContext) {
		batch = append(batch, l)
		if len(batch) == 2 {
			_ = batch[1].Ack() // the flush acks the last message only
		}
	}, bookRow(100), bookRow(150), heartbeatRow())
	assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())
}

// Connectors commonly return without acking Relation messages, the heartbeat
// table's own included, which arrives with the first heartbeat of every
// session. A Relation carries no rows and is sent again by every new walsender
// session, so it must not hold the heartbeat back.
func TestHeartbeatIgnoresUnackedRelation(t *testing.T) {
	relation := &Message{message: &format.Relation{Namespace: heartbeatTable.Schema, Name: heartbeatTable.Name}, walStart: 180}
	s, _ := runHeartbeatStream(t, func(*ListenerContext) {}, relation, heartbeatRow())
	assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())
}

// A non-transactional message delivered while a streamed transaction holds rows
// carries walStart 0, but the server still skips it on restart once the
// confirmed position passes its own LSN.
func TestHeartbeatWaitsForDeferredLogicalMessage(t *testing.T) {
	deferred := &Message{message: nonTxLogicalMessage(150)} // walStart 0, as dispatchMessage sends it
	s, _ := runHeartbeatStream(t, func(*ListenerContext) {}, deferred, heartbeatRow())
	assert.Zero(t, s.LoadConfirmedXLogPos())
}

// The heartbeat table written inside a business transaction puts the heartbeat
// row last: it carries the transaction's end, while the delivered row before it
// carries only its own record. Confirming the heartbeat would make the server
// skip the whole transaction, the held row included, even though the confirmed
// position already passes the row's record.
func TestHeartbeatInBusinessTransactionDoesNotConfirmPastItsRows(t *testing.T) {
	// Transaction 7: a row at 100 and the heartbeat row last; commit record at 390, end 400.
	held := &Message{message: &format.Insert{TableName: "books", LSN: 100}, walStart: 100, xid: 7, commitLSN: 390}
	lastHeartbeat := &Message{
		message:  &format.Update{TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name},
		walStart: 400, xid: 7, commitLSN: 390,
	}
	ackOnly200 := func(l *ListenerContext) {
		if l.Message.(*format.Insert).LSN == 200 {
			_ = l.Ack()
		}
	}

	t.Run("a separate heartbeat confirmed 200", func(t *testing.T) {
		s, _ := runHeartbeatStream(t, ackOnly200, heartbeatRow(), held, lastHeartbeat)
		assert.Equal(t, pq.LSN(200), s.LoadConfirmedXLogPos())
	})
	t.Run("the listener acked a transaction ending at 200", func(t *testing.T) {
		s, _ := runHeartbeatStream(t, ackOnly200, bookRow(200), held, lastHeartbeat)
		assert.Equal(t, pq.LSN(200), s.LoadConfirmedXLogPos())
	})
}

// A deferred message the listener acked must release the heartbeat even when no
// later ack passes its position: the streamed transaction it was deferred for
// aborted, and a listener that skips Relation acks keeps the idle keepalive
// from confirming.
func TestHeartbeatReleasedByAckOfDeferredLogicalMessage(t *testing.T) {
	relation := &Message{message: &format.Relation{Namespace: "public", Name: "books"}, walStart: 90}
	deferred := &Message{message: nonTxLogicalMessage(150)} // walStart 0, as dispatchMessage sends it
	s, _ := runHeartbeatStream(t, func(l *ListenerContext) {
		if _, ok := l.Message.(*format.LogicalDecodingMessage); ok {
			_ = l.Ack()
		}
	}, relation, deferred, heartbeatRow())
	assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())
}

// newHeartbeatHarness runs the real dispatch and processLoop with heartbeat
// enabled; listener acks synchronously, so processing is observable through acked.
func newHeartbeatHarness(t *testing.T, listener ListenerFunc) *keepaliveHarness {
	t.Helper()
	h := newKeepaliveHarness(t, listener)
	h.s.config = heartbeatConfig()
	h.startProcess()
	return h
}

// heartbeatTx dispatches one standalone heartbeat transaction ending at end.
func (h *keepaliveHarness) heartbeatTx(xid uint32, end pq.LSN) {
	h.dispatch(&format.Begin{Xid: xid, FinalLSN: end - 10}, end-30)
	h.dispatch(&format.Update{XID: xid, TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name}, end-20)
	h.dispatch(&format.Commit{CommitLSN: end - 10, TransactionEndLSN: end}, end-10)
}

// confirmedAfter waits until processLoop has counted n acks, then returns the confirmed position.
func (h *keepaliveHarness) confirmedAfter(n uint64) pq.LSN {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.s.acked.Load() == n }, 2*time.Second, time.Millisecond)
	return h.s.LoadConfirmedXLogPos()
}

// A non-transactional message is delivered at its record's start, and the
// server skips it on restart once the confirmed position passes that start.
// An idle keepalive can already have confirmed exactly that position.
func TestHeartbeatWaitsForUnackedMessageAtConfirmedPosition(t *testing.T) {
	h := newHeartbeatHarness(t, func(*ListenerContext) {}) // holds every message
	require.Equal(t, pq.LSN(150), h.keepalive(150))
	h.dispatch(nonTxLogicalMessage(150), 150)
	h.heartbeatTx(6, 200)
	assert.Equal(t, pq.LSN(150), h.confirmedAfter(1))
}

// A listener that skips Relation acks keeps the keepalive from confirming. When
// it acks the row of a transaction whose last message is a heartbeat row, the
// transaction is fully handled, and heartbeats must keep confirming.
func TestHeartbeatConfirmsAfterAckedRowOfMixedTransaction(t *testing.T) {
	h := newHeartbeatHarness(t, func(l *ListenerContext) {
		if _, ok := l.Message.(*format.Relation); !ok {
			_ = l.Ack()
		}
	})
	h.dispatch(&format.Begin{Xid: 7, FinalLSN: 390}, 50)
	h.dispatch(&format.Relation{Namespace: "public", Name: "books"}, 100)
	h.dispatch(&format.Insert{XID: 7, TableName: "books"}, 100)
	h.dispatch(&format.Update{XID: 7, TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name}, 380)
	h.dispatch(&format.Commit{CommitLSN: 390, TransactionEndLSN: 400}, 390)
	assert.Equal(t, pq.LSN(400), h.confirmedAfter(2), "the row is acked: skipping the transaction loses nothing")

	h.heartbeatTx(8, 450)
	assert.Equal(t, pq.LSN(450), h.confirmedAfter(3))
}

// Two messages delivered between the chunks of a streamed transaction that then
// aborts; a batching listener acks only the second, which covers the first.
func TestHeartbeatConfirmsAfterBatchAckOfDeferredMessages(t *testing.T) {
	h := newHeartbeatHarness(t, func(l *ListenerContext) {
		if l.Message.(*format.LogicalDecodingMessage).LSN == 170 {
			_ = l.Ack()
		}
	})
	h.dispatch(&format.StreamStart{Xid: 5}, 100)
	h.dispatch(&format.Insert{XID: 5, TableName: "books"}, 110)
	h.dispatch(&format.StreamStop{}, 120)
	h.dispatch(nonTxLogicalMessage(150), 150) // delivered with walStart 0
	h.dispatch(nonTxLogicalMessage(170), 170)
	h.dispatch(&format.StreamAbort{Xid: 5, SubXid: 5}, 180)
	h.heartbeatTx(6, 200)
	assert.Equal(t, pq.LSN(200), h.confirmedAfter(2))
}

// A Relation needs no ack of its own, but a batching listener whose batch ends
// with one, here the heartbeat table's Relation of the session's first
// heartbeat, acks it as the batch's last message, covering everything before.
func TestHeartbeatAckOfRelationCoversEarlierMessages(t *testing.T) {
	h := newHeartbeatHarness(t, func(l *ListenerContext) {
		if _, ok := l.Message.(*format.Relation); ok {
			_ = l.Ack() // flushes the batch [row, Relation]
		}
	})
	h.dispatch(&format.Begin{Xid: 5, FinalLSN: 110}, 10)
	h.dispatch(&format.Insert{XID: 5, TableName: "books"}, 20)
	h.dispatch(&format.Commit{CommitLSN: 110, TransactionEndLSN: 120}, 110)
	h.dispatch(&format.Begin{Xid: 6, FinalLSN: 190}, 130)
	h.dispatch(&format.Relation{Namespace: heartbeatTable.Schema, Name: heartbeatTable.Name}, 180)
	h.dispatch(&format.Update{XID: 6, TableNamespace: heartbeatTable.Schema, TableName: heartbeatTable.Name}, 180)
	h.dispatch(&format.Commit{CommitLSN: 190, TransactionEndLSN: 200}, 190)
	assert.Equal(t, pq.LSN(200), h.confirmedAfter(2))

	h.heartbeatTx(7, 250)
	assert.Equal(t, pq.LSN(250), h.confirmedAfter(3))
}

// An Ack of an older message after a newer one, repeated, must not uncover the
// newer one. The Relation is never acked, so the keepalive cannot confirm.
func TestHeartbeatLateAckOfEarlierMessageKeepsCoverage(t *testing.T) {
	relation := &Message{message: &format.Relation{Namespace: "public", Name: "books"}, walStart: 90}
	var first *ListenerContext
	s, buf := runHeartbeatStream(t, func(l *ListenerContext) {
		row, ok := l.Message.(*format.Insert)
		switch {
		case !ok: // the Relation, never acked
		case row.LSN == 100:
			first = l
		default:
			_ = l.Ack()
			_ = first.Ack()
			_ = first.Ack()
		}
	}, relation, bookRow(100), bookRow(150), heartbeatRow())
	assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos())

	require.NoError(t, s.handleKeepalive(context.Background(), keepaliveData(300, false), buf, &streamTxBuffer{}))
	assert.Equal(t, pq.LSN(heartbeatEnd), s.LoadConfirmedXLogPos(), "the un-acked Relation keeps the keepalive closed")
}

// Once the run context is cancelled a sink send can drop a message without a
// trace (TestKeepaliveStopsConfirmingOnceRunContextIsCancelled), and Close's
// final status update would make the server skip it, so no heartbeat confirms.
func TestHeartbeatStopsConfirmingOnceRunContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("counted, not confirmed", func(t *testing.T) {
		s := NewStream("", heartbeatConfig(), metric.NewMetric("test_slot"), nil).(*stream)
		s.ackHeartbeat(ctx, heartbeatRow(), true)
		assert.Zero(t, s.LoadConfirmedXLogPos())
		assert.Equal(t, uint64(1), s.acked.Load(), "still counted, so in-flight accounting stays balanced")
	})
	t.Run("through processLoop", func(t *testing.T) {
		// processLoop takes the heartbeat or the cancellation at random; neither may confirm.
		for range 50 {
			s := NewStream("", heartbeatConfig(), metric.NewMetric("test_slot"), nil).(*stream)
			s.messageCH <- heartbeatRow()
			require.NoError(t, s.processLoop(ctx))
			require.Zero(t, s.LoadConfirmedXLogPos())
		}
	})
}
