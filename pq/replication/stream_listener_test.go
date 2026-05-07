package replication

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
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
