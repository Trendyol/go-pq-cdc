package replication

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatedStream wires a stream to a scripted guard and records what the listener saw.
type gatedStream struct {
	s        *stream
	q        *scriptedQuery
	m        *countingMetric
	received []uint32 // xids delivered to the listener
	commits  []pq.LSN // CommitLSN seen by the listener, in delivery order
}

func newGatedStream(t *testing.T, cfg config.Config, rows [][]string) *gatedStream {
	t.Helper()
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	gs := &gatedStream{q: &scriptedQuery{rows: rows}}
	gs.s = NewStream("", cfg, nil, func(ctx *ListenerContext) {
		gs.received = append(gs.received, ctx.Message.(*format.Insert).XID)
		gs.commits = append(gs.commits, ctx.CommitLSN)
		_ = ctx.Ack()
	}).(*stream)
	guard, m := testGuard(gs.q.query)
	guard.cfg = cfg.VisibilityGuard
	gs.m = m
	gs.s.metric = m
	gs.s.guard = guard
	return gs
}

func (gs *gatedStream) run(msgs ...*Message) error {
	for _, m := range msgs {
		gs.s.messageCH <- m
	}
	close(gs.s.messageCH)
	return gs.s.processLoop(context.Background())
}

func insertMsg(xid uint32, lsn int64) *Message {
	return &Message{message: &format.Insert{XID: xid, TableName: "books"}, walStart: lsn, xid: xid, commitLSN: pq.LSN(lsn)}
}

func guardCfg(mode config.VisibilityFailMode) config.Config {
	return config.Config{VisibilityGuard: config.VisibilityGuardConfig{
		Enabled: true, FailMode: mode, Timeout: 30 * time.Millisecond, PollInterval: time.Millisecond,
	}}
}

func TestGateWaitsOncePerTransaction(t *testing.T) {
	gs := newGatedStream(t, guardCfg(config.VisibilityFailClosed), [][]string{{"f", "100:200:"}, {"f", "100:201:"}})
	// xid 200 needs two polls; xid 199 is below the cached xmax but not xmin, so it polls once more.
	require.NoError(t, gs.run(insertMsg(200, 1), insertMsg(200, 2), insertMsg(200, 3), insertMsg(150, 4)))

	assert.Equal(t, []uint32{200, 200, 200, 150}, gs.received)
	assert.Equal(t, int32(3), gs.q.calls.Load(), "3 messages of xid 200 = 2 polls, xid 150 = 1 poll")
	assert.Equal(t, pq.LSN(4), gs.s.LoadConfirmedXLogPos())
}

func TestGateHeartbeatBypass(t *testing.T) {
	cfg := guardCfg(config.VisibilityFailClosed)
	cfg.Heartbeat = config.HeartbeatConfig{Table: publication.Table{Schema: "public", Name: "cdc_heartbeat"}}
	gs := newGatedStream(t, cfg, [][]string{{"f", "100:200:"}})
	hb := &Message{message: &format.Insert{XID: 300, TableNamespace: "public", TableName: "cdc_heartbeat"}, walStart: 9, xid: 300}

	require.NoError(t, gs.run(hb))
	assert.Empty(t, gs.received)
	assert.Equal(t, int32(0), gs.q.calls.Load(), "heartbeat must not poll")
	assert.Equal(t, pq.LSN(9), gs.s.LoadConfirmedXLogPos(), "heartbeat is still auto-acked")
}

func TestGateFailClosedTimeoutStopsProcessing(t *testing.T) {
	gs := newGatedStream(t, guardCfg(config.VisibilityFailClosed), [][]string{{"f", "100:200:"}})

	err := gs.run(insertMsg(200, 1), insertMsg(200, 2))
	require.ErrorIs(t, err, ErrVisibilityGuard)
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	assert.Empty(t, gs.received, "message must be dropped un-dispatched")
	assert.Equal(t, pq.LSN(0), gs.s.LoadConfirmedXLogPos(), "message must stay un-acked for redelivery")
	assert.Equal(t, int32(1), gs.m.timeouts.Load())
	assert.Equal(t, int32(0), gs.m.failOpens.Load())
}

func TestGateFailOpenTimeoutDispatches(t *testing.T) {
	gs := newGatedStream(t, guardCfg(config.VisibilityFailOpen), [][]string{{"f", "100:200:"}})

	require.NoError(t, gs.run(insertMsg(200, 1), insertMsg(200, 2)))
	assert.Equal(t, []uint32{200, 200}, gs.received)
	assert.Equal(t, int32(1), gs.m.timeouts.Load())
	assert.Equal(t, int32(1), gs.m.failOpens.Load(), "one fail-open per transaction, not per message")
}

func TestGateFailOpenStillFailsOnGuardError(t *testing.T) {
	// pg_is_in_recovery flips mid-stream: failover. Open mode tolerates slow visibility, not a lost primary.
	gs := newGatedStream(t, guardCfg(config.VisibilityFailOpen), [][]string{{"f", "100:200:"}, {"t", "100:200:"}})

	err := gs.run(insertMsg(200, 1))
	require.ErrorIs(t, err, ErrVisibilityGuard)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	assert.Empty(t, gs.received)
	assert.Equal(t, int32(0), gs.m.failOpens.Load())
}

func TestGateCancelledMidWaitDropsMessageUndispatched(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	var delivered int
	s := NewStream("", guardCfg(config.VisibilityFailOpen), nil, func(*ListenerContext) { delivered++ }).(*stream)
	guard, m := testGuard(func(ctx context.Context, _ string) ([]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	guard.cfg.Timeout = time.Hour
	s.metric = m
	s.guard = guard
	s.messageCH <- insertMsg(200, 1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	require.NoError(t, s.processLoop(ctx))
	assert.Equal(t, 0, delivered, "an uncertified message must not reach the listener on shutdown")
	assert.Equal(t, pq.LSN(0), s.LoadConfirmedXLogPos())
	assert.Equal(t, int32(0), m.failOpens.Load(), "shutdown is not a fail-open")
}

func TestGateDisabledDoesNotTouchGuard(t *testing.T) {
	gs := newGatedStream(t, config.Config{}, [][]string{{"f", "100:200:"}})
	gs.s.guard = nil

	require.NoError(t, gs.run(insertMsg(200, 1)))
	assert.Equal(t, []uint32{200}, gs.received)
	assert.Equal(t, []pq.LSN{1}, gs.commits, "ListenerContext.CommitLSN comes from Message.commitLSN")
	assert.Equal(t, int32(0), gs.q.calls.Load())
}

func TestDispatchStampsXidAndCommitLSNOnNonStreamingAndStreamingPaths(t *testing.T) {
	out := make(chan *Message, 10)
	s := &stream{}
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}
	dispatch := func(msg any, lsn uint64) { s.dispatchMessage(msg, XLogData{WALStart: pq.LSN(lsn)}, buf, streamBuf) }

	// Non-streaming: BEGIN(100, final 40) a b COMMIT → both carry xid 100 and commit 40 (from Begin.FinalLSN);
	// the last one is rebuilt by flushWithLSN.
	dispatch(&format.Begin{Xid: 100, FinalLSN: 40}, 1)
	dispatch(&format.Insert{TableName: "a"}, 2)
	dispatch(&format.Insert{TableName: "b"}, 3)
	dispatch(&format.Commit{CommitLSN: 40, TransactionEndLSN: 50}, 4)
	// Streaming: STREAM START(200) c(sub 201) d STREAM STOP STREAM COMMIT(commit 90, end 99) → both carry the
	// top-level 200 and commit 90, which is only known at STREAM COMMIT.
	dispatch(&format.StreamStart{Xid: 200}, 5)
	dispatch(&format.Insert{XID: 201, TableName: "c"}, 6)
	dispatch(&format.Insert{XID: 200, TableName: "d"}, 7)
	dispatch(&format.StreamStop{}, 8)
	dispatch(&format.StreamCommit{Xid: 200, CommitLSN: 90, TransactionEndLSN: 99}, 9)
	close(out)

	type stamped struct {
		name   string
		xid    uint32
		lsn    int64
		commit pq.LSN
	}
	var got []stamped
	for m := range out {
		got = append(got, stamped{m.message.(*format.Insert).TableName, m.xid, m.walStart, m.commitLSN})
	}
	assert.Equal(t, []stamped{{"a", 100, 2, 40}, {"b", 100, 50, 40}, {"c", 200, 6, 90}, {"d", 200, 99, 90}}, got)
}

func TestCloseCancelsBeforeClosingGuardAndAfterProcessExit(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	var mu sync.Mutex
	var events []string
	record := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }

	guardConn := &standbyCaptureConn{}
	blockingQuery := func(ctx context.Context, _ string) ([]string, error) {
		<-ctx.Done()
		record("wait-cancelled")
		return nil, ctx.Err()
	}
	s := NewStream("", guardCfg(config.VisibilityFailClosed), nil, func(*ListenerContext) {}).(*stream)
	guard, m := testGuard(blockingQuery)
	guard.cfg.Timeout = time.Hour
	guard.conn = &closeRecorderConn{standbyCaptureConn: guardConn, onClose: func() { record("guard-closed") }}
	s.metric = m
	s.guard = guard

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.processStarted.Store(true)
	processDone := make(chan struct{})
	go func() {
		s.process(ctx)
		record("process-exited")
		close(processDone)
	}()
	s.messageCH <- insertMsg(200, 1) // blocks in guard.wait until Close cancels

	time.Sleep(20 * time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- s.Close(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on the guard wait")
	}
	<-processDone

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "wait-cancelled", events[0], "context must be cancelled before anything else")
	assert.Equal(t, "guard-closed", events[len(events)-1], "guard closes only after the process goroutine exited: %v", events)
	assert.True(t, guardConn.closed)
}

type closeRecorderConn struct {
	*standbyCaptureConn
	onClose func()
}

func (c *closeRecorderConn) Close(ctx context.Context) error {
	c.onClose()
	return c.standbyCaptureConn.Close(ctx)
}
