package replication

import (
	"context"
	"encoding/binary"
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keepaliveHarness plays the sink goroutine: dispatch and keepalive run on the
// test goroutine against the buffers the sink owns.
type keepaliveHarness struct {
	t         *testing.T
	s         *stream
	buf       *messageBuffer
	streamBuf *streamTxBuffer
}

func newKeepaliveHarness(t *testing.T, listener ListenerFunc) *keepaliveHarness {
	t.Helper()
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), listener).(*stream)
	return &keepaliveHarness{t: t, s: s, buf: &messageBuffer{outCh: s.messageCH}, streamBuf: &streamTxBuffer{}}
}

func (h *keepaliveHarness) dispatch(msg any, lsn pq.LSN) {
	h.s.dispatchMessage(msg, XLogData{WALStart: lsn}, h.buf, h.streamBuf)
}

// keepalive feeds a primary keepalive carrying walEnd and returns the confirmed position after it.
func (h *keepaliveHarness) keepalive(walEnd pq.LSN) pq.LSN {
	h.t.Helper()
	require.NoError(h.t, h.s.handleKeepalive(context.Background(), keepaliveData(walEnd, false), h.buf, h.streamBuf))
	return h.s.LoadConfirmedXLogPos()
}

// startProcess runs the real processLoop, so the listener gets the real Ack.
func (h *keepaliveHarness) startProcess() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.s.processLoop(ctx)
	}()
	h.t.Cleanup(func() {
		cancel()
		<-done
	})
}

// keepaliveData is a primary keepalive payload as handleKeepalive receives it (after the leading 'k').
func keepaliveData(walEnd pq.LSN, replyRequested bool) []byte {
	data := make([]byte, 17)
	binary.BigEndian.PutUint64(data, uint64(walEnd))
	if replyRequested {
		data[16] = 1
	}
	return data
}

func receiveListener(t *testing.T, ch <-chan *ListenerContext) *ListenerContext {
	t.Helper()
	select {
	case l := <-ch:
		return l
	case <-time.After(2 * time.Second):
		t.Fatal("listener was not called")
		return nil
	}
}

func TestKeepaliveConfirmsWALEndWhenIdle(t *testing.T) {
	h := newKeepaliveHarness(t, nil)
	assert.Equal(t, pq.LSN(100), h.keepalive(100))
}

// BEGIN is never delivered to the listener, so between BEGIN and the first row
// only the open-transaction flag holds the position.
func TestKeepaliveWaitsForOpenTransaction(t *testing.T) {
	held := make(chan *ListenerContext, 1)
	h := newKeepaliveHarness(t, func(l *ListenerContext) { held <- l })
	h.startProcess()

	h.dispatch(&format.Begin{Xid: 5, FinalLSN: 110}, 10)
	assert.Zero(t, h.keepalive(100), "the transaction is open")

	h.dispatch(&format.Insert{XID: 5, TableName: "books"}, 20)
	h.dispatch(&format.Commit{CommitLSN: 110, TransactionEndLSN: 120}, 110)
	require.NoError(t, receiveListener(t, held).Ack())
	assert.Equal(t, pq.LSN(130), h.keepalive(130))

	// A filtered (empty) transaction still closes on its COMMIT.
	h.dispatch(&format.Begin{Xid: 6, FinalLSN: 140}, 135)
	assert.Equal(t, pq.LSN(130), h.keepalive(140), "the empty transaction is open")
	h.dispatch(&format.Commit{CommitLSN: 140, TransactionEndLSN: 145}, 140)
	assert.Equal(t, pq.LSN(150), h.keepalive(150))
}

// A streamed transaction is open from its STREAM START to its STREAM COMMIT or
// ABORT, rows or not, and its rows exist only in this process.
func TestKeepaliveWaitsForOpenStreamedTransaction(t *testing.T) {
	h := newKeepaliveHarness(t, nil)

	h.dispatch(&format.StreamStart{Xid: 7}, 10)
	assert.Zero(t, h.keepalive(100), "inside a chunk")
	h.dispatch(&format.StreamStop{}, 15)
	assert.Zero(t, h.keepalive(100), "between chunks, none with rows yet")

	h.dispatch(&format.StreamStart{Xid: 7}, 18)
	h.dispatch(&format.Insert{XID: 8, TableName: "books"}, 20) // sub-transaction 8 of 7
	h.dispatch(&format.StreamStop{}, 30)
	assert.Zero(t, h.keepalive(100), "between chunks, rows held")

	h.dispatch(&format.StreamAbort{Xid: 7, SubXid: 8}, 40)
	assert.Zero(t, h.keepalive(100), "a sub-transaction abort leaves the transaction open")

	h.dispatch(&format.StreamAbort{Xid: 7, SubXid: 7}, 50)
	assert.Equal(t, pq.LSN(100), h.keepalive(100))

	// STREAM COMMIT hands the rows to messageCH: in flight until acked.
	h.dispatch(&format.StreamStart{Xid: 9}, 110)
	h.dispatch(&format.Insert{XID: 9, TableName: "books"}, 120)
	h.dispatch(&format.StreamStop{}, 130)
	h.dispatch(&format.StreamCommit{Xid: 9, CommitLSN: 140, TransactionEndLSN: 150}, 140)
	assert.Equal(t, pq.LSN(100), h.keepalive(200), "committed streamed rows are queued, not acked")
}

// A message is in flight from the channel send to its first Ack, and a repeated
// Ack must not stand in for the next message's.
func TestKeepaliveWaitsForEveryAck(t *testing.T) {
	held := make(chan *ListenerContext, 1)
	h := newKeepaliveHarness(t, func(l *ListenerContext) { held <- l })

	h.dispatch(nonTxLogicalMessage(10), 10)
	assert.Zero(t, h.keepalive(100), "queued in messageCH")

	h.startProcess()
	first := receiveListener(t, held)
	assert.Zero(t, h.keepalive(100), "delivered, not acked")

	require.NoError(t, first.Ack())
	assert.Equal(t, pq.LSN(100), h.keepalive(100))

	require.NoError(t, first.Ack())
	assert.Equal(t, h.buf.sent, h.s.acked.Load(), "a repeated Ack must not count twice")
	assert.Equal(t, pq.LSN(120), h.keepalive(120), "still idle after the repeat")

	h.dispatch(nonTxLogicalMessage(150), 150)
	second := receiveListener(t, held)
	assert.Equal(t, pq.LSN(120), h.keepalive(200), "the repeat must not cover the next message")

	require.NoError(t, second.Ack())
	assert.Equal(t, pq.LSN(200), h.keepalive(200))
}

// The look-ahead holds the position. With the run context live a row is only
// pending inside a transaction, so buffer one directly to check it alone.
func TestKeepaliveWaitsForLookAhead(t *testing.T) {
	h := newKeepaliveHarness(t, nil)
	h.buf.buffer(&Message{message: &format.Insert{TableName: "books"}, walStart: 20})
	assert.Zero(t, h.keepalive(100))
}

// Close cancels the run context before it closes the connection, and the sink
// reads keepalives until then. A send that gives up on ctx loses its message
// from every buffer, and Close's final status update would make the server
// skip it on restart, so no keepalive may confirm once ctx is cancelled.
func TestKeepaliveStopsConfirmingOnceRunContextIsCancelled(t *testing.T) {
	confirmedAfter := func(t *testing.T, dispatch func(h *keepaliveHarness)) pq.LSN {
		h := newKeepaliveHarness(t, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.buf.ctx = ctx                   // the sink hands handleKeepalive this same ctx
		h.buf.outCh = make(chan *Message) // nobody receives, so every send gives up on ctx
		dispatch(h)
		require.NoError(t, h.s.handleKeepalive(ctx, keepaliveData(100, false), h.buf, h.streamBuf))
		return h.s.LoadConfirmedXLogPos()
	}

	t.Run("non-transactional message dropped", func(t *testing.T) {
		assert.Zero(t, confirmedAfter(t, func(h *keepaliveHarness) {
			h.dispatch(nonTxLogicalMessage(10), 10)
		}))
	})
	t.Run("unsent last row discarded by the next BEGIN", func(t *testing.T) {
		assert.Zero(t, confirmedAfter(t, func(h *keepaliveHarness) {
			h.dispatch(&format.Begin{Xid: 5, FinalLSN: 30}, 10)
			h.dispatch(&format.Insert{XID: 5, TableName: "books"}, 20)
			h.dispatch(&format.Commit{CommitLSN: 30, TransactionEndLSN: 40}, 30) // the flush fails, the row stays pending
			h.dispatch(&format.Begin{Xid: 6, FinalLSN: 50}, 45)                  // and is discarded here
			h.dispatch(&format.Commit{CommitLSN: 50, TransactionEndLSN: 55}, 50)
		}))
	})
}

func TestKeepaliveNeverMovesConfirmedBackwards(t *testing.T) {
	h := newKeepaliveHarness(t, nil)
	h.s.UpdateConfirmedXLogPos(100)
	assert.Equal(t, pq.LSN(100), h.keepalive(90))
}

func TestKeepaliveReplyCarriesConfirmedWALEnd(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s, c := newStatusStream(time.Second) // confirmed starts at 42

	require.NoError(t, s.handleKeepalive(context.Background(), keepaliveData(500, true),
		&messageBuffer{outCh: s.messageCH}, &streamTxBuffer{}))

	writes, _ := c.snapshot()
	require.Len(t, writes, 1)
	received, flushed, applied := decodeStatusUpdate(t, writes[0])
	assert.Equal(t, uint64(500), received)
	assert.Equal(t, uint64(500), flushed, "the reply must carry the WAL end confirmed by this keepalive")
	assert.Equal(t, uint64(500), applied)
}
