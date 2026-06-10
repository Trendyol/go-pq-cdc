package replication

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestDispatchDefaultKeepsRowOrientedListenerContract(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 4)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.Begin{FinalLSN: pq.LSN(19)}, XLogData{WALStart: pq.LSN(10)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(11)}, buf, streamBuf)
	stream.dispatchMessage(&format.Commit{TransactionEndLSN: pq.LSN(20)}, XLogData{WALStart: pq.LSN(20)}, buf, streamBuf)

	requireMessageCount(t, out, 1)
	msg := <-out
	if _, ok := msg.message.(*format.Insert); !ok {
		t.Fatalf("message = %T, want *format.Insert", msg.message)
	}
	if msg.walStart != pq.LSN(11) {
		t.Fatalf("walStart = %s, want 0/B", msg.walStart)
	}
	if msg.ackLSN != pq.LSN(20) {
		t.Fatalf("ackLSN = %s, want 0/14", msg.ackLSN)
	}
}

func TestDispatchCanEmitTransactionBoundaries(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 8)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.Begin{FinalLSN: pq.LSN(19)}, XLogData{WALStart: pq.LSN(10)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(11)}, buf, streamBuf)
	stream.dispatchMessage(&format.Update{TableName: "books"}, XLogData{WALStart: pq.LSN(12)}, buf, streamBuf)
	stream.dispatchMessage(&format.Commit{TransactionEndLSN: pq.LSN(20)}, XLogData{WALStart: pq.LSN(13)}, buf, streamBuf)

	requireMessageCount(t, out, 4)
	assertMessage[*format.Begin](t, <-out, pq.LSN(10), pq.LSN(10))
	assertMessage[*format.Insert](t, <-out, pq.LSN(11), pq.LSN(11))
	assertMessage[*format.Update](t, <-out, pq.LSN(12), pq.LSN(20))
	assertMessage[*format.Commit](t, <-out, pq.LSN(13), pq.LSN(20))
}

func TestDispatchCanEmitStreamCommitBoundary(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 8)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.StreamStart{Xid: 7}, XLogData{WALStart: pq.LSN(30)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(31)}, buf, streamBuf)
	stream.dispatchMessage(&format.StreamStop{}, XLogData{WALStart: pq.LSN(32)}, buf, streamBuf)
	stream.dispatchMessage(&format.StreamCommit{Xid: 7, TransactionEndLSN: pq.LSN(40)}, XLogData{WALStart: pq.LSN(33)}, buf, streamBuf)

	requireMessageCount(t, out, 2)
	assertMessage[*format.Insert](t, <-out, pq.LSN(31), pq.LSN(40))
	assertMessage[*format.StreamCommit](t, <-out, pq.LSN(33), pq.LSN(40))
}

func TestDispatchStreamAbortDiscardsBufferedRows(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 8)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.StreamStart{Xid: 7}, XLogData{WALStart: pq.LSN(30)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(31)}, buf, streamBuf)
	stream.dispatchMessage(&format.StreamAbort{Xid: 7}, XLogData{WALStart: pq.LSN(32)}, buf, streamBuf)

	requireMessageCount(t, out, 0)
}

func TestProcessDefaultAckAdvancesConfirmedLSN(t *testing.T) {
	var ackErr error
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		ackErr = ctx.Ack()
	}).(*stream)
	stream.conn = newWriteOnlyConn()
	stream.UpdateXLogPos(pq.LSN(100))
	stream.messageCH <- &Message{
		message:  &format.Insert{TableName: "books"},
		walStart: pq.LSN(11),
		ackLSN:   pq.LSN(20),
	}
	close(stream.messageCH)

	go stream.process(context.Background())
	waitForProcessEnd(t, stream)

	if ackErr != nil {
		t.Fatalf("ack failed: %v", ackErr)
	}
	if stream.LoadConfirmedXLogPos() != pq.LSN(20) {
		t.Fatalf("confirmed = %s, want 0/14", stream.LoadConfirmedXLogPos())
	}
}

func TestProcessTransactionAwareAckOnlyAdvancesAtOrderedCommitBoundaries(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	commitAcks := make([]func() error, 0, 2)
	var nonCommitAckErr error
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		switch ctx.Message.(type) {
		case *format.Insert:
			nonCommitAckErr = ctx.Ack()
		case *format.Commit, *format.StreamCommit:
			commitAcks = append(commitAcks, ctx.Ack)
		}
	}).(*stream)
	stream.conn = newWriteOnlyConn()
	stream.UpdateXLogPos(pq.LSN(100))
	stream.messageCH <- &Message{message: &format.Insert{TableName: "books"}, walStart: pq.LSN(9), ackLSN: pq.LSN(10)}
	stream.messageCH <- &Message{message: &format.Commit{TransactionEndLSN: pq.LSN(10)}, walStart: pq.LSN(10), ackLSN: pq.LSN(10)}
	stream.messageCH <- &Message{message: &format.StreamCommit{Xid: 7, TransactionEndLSN: pq.LSN(20)}, walStart: pq.LSN(20), ackLSN: pq.LSN(20)}
	close(stream.messageCH)

	go stream.process(context.Background())
	waitForProcessEnd(t, stream)

	if nonCommitAckErr != nil {
		t.Fatalf("non-commit ack failed: %v", nonCommitAckErr)
	}
	if stream.LoadConfirmedXLogPos() != 0 {
		t.Fatalf("confirmed after non-commit ack = %s, want 0/0", stream.LoadConfirmedXLogPos())
	}
	if len(commitAcks) != 2 {
		t.Fatalf("commit ack count = %d, want 2", len(commitAcks))
	}
	if err := commitAcks[1](); err != nil {
		t.Fatalf("second commit ack failed: %v", err)
	}
	if stream.LoadConfirmedXLogPos() != 0 {
		t.Fatalf("confirmed after second commit ack = %s, want 0/0", stream.LoadConfirmedXLogPos())
	}
	if err := commitAcks[0](); err != nil {
		t.Fatalf("first commit ack failed: %v", err)
	}
	if stream.LoadConfirmedXLogPos() != pq.LSN(20) {
		t.Fatalf("confirmed after first commit ack = %s, want 0/14", stream.LoadConfirmedXLogPos())
	}
}

func TestProcessExposesWALMetadataToListener(t *testing.T) {
	received := make(chan ListenerContext, 1)
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		received <- ListenerContext{
			Message:  ctx.Message,
			WALStart: ctx.WALStart,
			AckLSN:   ctx.AckLSN,
		}
	}).(*stream)
	stream.messageCH <- &Message{
		message:  &format.Insert{TableName: "books"},
		walStart: pq.LSN(11),
		ackLSN:   pq.LSN(20),
	}
	close(stream.messageCH)

	go stream.process(context.Background())

	select {
	case ctx := <-received:
		if _, ok := ctx.Message.(*format.Insert); !ok {
			t.Fatalf("message = %T, want *format.Insert", ctx.Message)
		}
		if ctx.WALStart != pq.LSN(11) {
			t.Fatalf("WALStart = %s, want 0/B", ctx.WALStart)
		}
		if ctx.AckLSN != pq.LSN(20) {
			t.Fatalf("AckLSN = %s, want 0/14", ctx.AckLSN)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener was not called")
	}
}

func waitForProcessEnd(t *testing.T, stream *stream) {
	t.Helper()
	select {
	case <-stream.processEnd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("processor did not finish")
	}
}

type writeOnlyConn struct {
	out *bytes.Buffer
}

func newWriteOnlyConn() *writeOnlyConn {
	return &writeOnlyConn{out: &bytes.Buffer{}}
}

func (c *writeOnlyConn) Connect(context.Context) error { return nil }
func (c *writeOnlyConn) IsClosed() bool                { return false }
func (c *writeOnlyConn) Close(context.Context) error   { return nil }
func (c *writeOnlyConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errors.New("not implemented")
}
func (c *writeOnlyConn) Frontend() *pgproto3.Frontend {
	return pgproto3.NewFrontend(bytes.NewReader(nil), c.out)
}
func (c *writeOnlyConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

func requireMessageCount(t *testing.T, ch <-chan *Message, want int) {
	t.Helper()
	if len(ch) != want {
		t.Fatalf("message count = %d, want %d", len(ch), want)
	}
}

func assertMessage[T any](t *testing.T, msg *Message, walStart pq.LSN, ackLSN pq.LSN) {
	t.Helper()
	if _, ok := msg.message.(T); !ok {
		t.Fatalf("message = %T, want %T", msg.message, *new(T))
	}
	if msg.walStart != walStart {
		t.Fatalf("walStart = %s, want %s", msg.walStart, walStart)
	}
	if msg.ackLSN != ackLSN {
		t.Fatalf("ackLSN = %s, want %s", msg.ackLSN, ackLSN)
	}
}
