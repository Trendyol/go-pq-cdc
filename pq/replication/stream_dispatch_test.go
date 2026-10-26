package replication

import (
	"context"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDispatchBuffer wires a message buffer to a buffered channel so dispatch
// results can be observed without a running sink.
func newDispatchBuffer() (*messageBuffer, <-chan *Message) {
	outCh := make(chan *Message, 4)
	return &messageBuffer{outCh: outCh}, outCh
}

func nonTxLogicalMessage(lsn pq.LSN) *format.LogicalDecodingMessage {
	return &format.LogicalDecodingMessage{
		Flags:   0,
		LSN:     lsn,
		Prefix:  "probe",
		Content: []byte("standalone"),
	}
}

func txLogicalMessage(lsn pq.LSN) *format.LogicalDecodingMessage {
	return &format.LogicalDecodingMessage{
		Flags:   1,
		LSN:     lsn,
		Prefix:  "probe",
		Content: []byte("in transaction"),
	}
}

// wrap turns a decoded message into the envelope processLoop consumes.
func wrap(decoded *format.LogicalDecodingMessage) *Message {
	return &Message{message: decoded, walStart: int64(decoded.LSN)}
}

func TestDispatchNonTransactionalMessageSkipsLookAhead(t *testing.T) {
	s := &stream{}
	buf, outCh := newDispatchBuffer()

	// A transaction is in flight: its identity must not leak into the message.
	buf.xid = 5
	buf.commitLSN = 900

	msg := nonTxLogicalMessage(77)
	s.dispatchMessage(msg, XLogData{WALStart: 100}, buf, &streamTxBuffer{})

	require.Len(t, outCh, 1, "a non-transactional message must be delivered immediately")
	got := <-outCh

	decoded, ok := got.message.(*format.LogicalDecodingMessage)
	require.True(t, ok)
	assert.Same(t, msg, decoded)
	assert.Equal(t, pq.LSN(77), decoded.LSN)
	assert.Equal(t, int64(100), got.walStart, "delivery position comes from the XLogData envelope")
	assert.Zero(t, got.xid, "xid must stay zero: the message belongs to no transaction")
	assert.Zero(t, got.commitLSN, "commitLSN must stay zero: the message belongs to no transaction")
}

func TestDispatchNonTransactionalMessageLeavesPendingDML(t *testing.T) {
	s := &stream{}
	buf, outCh := newDispatchBuffer()

	s.dispatchMessage(&format.Begin{Xid: 5, FinalLSN: 900}, XLogData{WALStart: 10}, buf, &streamTxBuffer{})

	// An open transaction with an undelivered DML in the look-ahead: releasing
	// it early would hand the consumer a row that may still abort.
	pending := &Message{message: &format.Insert{XID: 5, TableName: "books"}, walStart: 20, xid: 5}
	buf.buffer(pending)
	require.Empty(t, outCh)

	s.dispatchMessage(nonTxLogicalMessage(30), XLogData{WALStart: 30}, buf, &streamTxBuffer{})

	require.Len(t, outCh, 1)
	assert.Same(t, pending, buf.pending, "an uncommitted DML must not be released early")
	_, ok := (<-outCh).message.(*format.LogicalDecodingMessage)
	assert.True(t, ok, "only the non-transactional message is delivered")
}

func TestDispatchTransactionalMessageStillWaitsForCommit(t *testing.T) {
	s := &stream{}
	buf, outCh := newDispatchBuffer()

	s.dispatchMessage(&format.Begin{Xid: 5, FinalLSN: 900}, XLogData{WALStart: 10}, buf, &streamTxBuffer{})

	msg := txLogicalMessage(20)
	s.dispatchMessage(msg, XLogData{WALStart: 20}, buf, &streamTxBuffer{})
	assert.Empty(t, outCh, "a transactional message is delivered with its transaction")

	s.dispatchMessage(&format.Commit{TransactionEndLSN: 950}, XLogData{WALStart: 950}, buf, &streamTxBuffer{})

	require.Len(t, outCh, 1)
	got := <-outCh
	decoded, ok := got.message.(*format.LogicalDecodingMessage)
	require.True(t, ok)
	assert.Same(t, msg, decoded)
	assert.Equal(t, uint32(5), got.xid)
	assert.Equal(t, pq.LSN(900), got.commitLSN)
	assert.Equal(t, int64(950), got.walStart, "the last message carries the transaction-end LSN")
}

func TestProcessLoopSkipsGateForNonTransactionalMessage(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}}}
	guard, m := testGuard(q.query)
	cfg := guardCfg(config.VisibilityFailClosed)
	guard.cfg = cfg.VisibilityGuard

	var (
		received  []any
		commitLSN pq.LSN
	)
	s := NewStream("", cfg, m, func(ctx *ListenerContext) {
		received = append(received, ctx.Message)
		commitLSN = ctx.CommitLSN
		_ = ctx.Ack()
	}).(*stream)
	s.guard = guard

	// The reviewer's crash-loop sequence: more than one standalone message
	// before the first BEGIN, with the guard enabled.
	s.messageCH <- wrap(nonTxLogicalMessage(42))
	s.messageCH <- wrap(nonTxLogicalMessage(43))
	close(s.messageCH)
	require.NoError(t, s.processLoop(context.Background()))

	require.Len(t, received, 2)
	assert.IsType(t, &format.LogicalDecodingMessage{}, received[0])
	assert.Zero(t, commitLSN, "the consumer must not inherit a transaction's commit LSN")
	assert.Zero(t, q.calls.Load(), "xid 0 must not cost a poll on the primary")
	assert.Equal(t, pq.LSN(43), s.LoadConfirmedXLogPos(), "both messages stay ackable")
}
