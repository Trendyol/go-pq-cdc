package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func onlineU32(b []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(b, v) }
func onlineU64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }
func onlineBegin(xid uint32, lsn uint64) []byte {
	return onlineU32(onlineU64(onlineU64([]byte{'B'}, lsn), 0), xid)
}
func onlineCommit(lsn, end uint64) []byte {
	return onlineU64(onlineU64(onlineU64([]byte{'C', 0}, lsn), end), 0)
}
func onlineRelation() []byte {
	b := onlineU32([]byte{'R'}, 7)
	b = append(b, []byte("public\x00items\x00")...)
	b = append(b, 'f', 0, 1, 1)
	b = append(b, []byte("id\x00")...)
	b = onlineU32(b, 23)
	return onlineU32(b, 0xffffffff)
}
func onlineTuple(b []byte, tag byte, value string) []byte {
	b = append(b, tag, 0, 1, 't')
	b = onlineU32(b, uint32(len(value)))
	return append(b, value...)
}
func onlineInsert() []byte { return onlineTuple(onlineU32([]byte{'I'}, 7), 'N', "42") }
func onlineUpdate() []byte {
	return onlineTuple(onlineTuple(onlineU32([]byte{'U'}, 7), 'O', "42"), 'N', "43")
}
func onlineDelete() []byte { return onlineTuple(onlineU32([]byte{'D'}, 7), 'O', "43") }
func onlineXLog(b []byte) *pgproto3.CopyData {
	d := onlineU64([]byte{'w'}, 50)
	d = onlineU64(d, 900)
	d = onlineU64(d, 0)
	return &pgproto3.CopyData{Data: append(d, b...)}
}
func onlineOptions() OnlineReaderOptions {
	return OnlineReaderOptions{
		ReplicationDSN: "postgres://unused", SQLDSN: "postgres://unused", Publication: "pub", Slot: "slot", StartLSN: 10, ProtoVersion: 1,
		Source: OnlineSourceIdentity{SystemID: "123", Database: "db", Timeline: 1},
		Limits: OnlineReaderLimits{TransactionBytes: 1024, QueueBytes: 2048, FeedbackInterval: 5 * time.Millisecond, VisibilityTimeout: time.Second, PollInterval: time.Millisecond},
	}
}

func TestOnlineCommittedEnvelope(t *testing.T) {
	a := newOnlineAssembler(1024, 2048)
	inputs := [][]byte{onlineBegin(99, 100), onlineRelation(), onlineInsert(), append(onlineU32([]byte{'Y'}, 900), []byte("public\x00custom\x00")...), onlineUpdate(), onlineDelete()}
	total := len(onlineCommit(100, 110))
	for _, b := range inputs {
		total += len(b)
		env, err := a.accept(XLogData{WALData: b, WALStart: 50})
		require.NoError(t, err)
		require.Nil(t, env, "rows must not escape before COMMIT")
	}
	env, err := a.accept(XLogData{WALData: onlineCommit(100, 110)})
	require.NoError(t, err)
	require.Equal(t, uint32(99), env.XID)
	require.Equal(t, pq.LSN(100), env.CommitLSN)
	require.Equal(t, pq.LSN(110), env.EndLSN)
	require.EqualValues(t, total, env.EncodedBytes)
	require.Len(t, env.Changes, 3)
	for i, ch := range env.Changes {
		require.Equal(t, uint32(i), ch.Ordinal)
		require.Positive(t, ch.EncodedBytes)
	}
	require.Equal(t, int32(42), env.Changes[0].Message.(*format.Insert).Decoded["id"])
	require.Equal(t, pq.LSN(50), env.Changes[0].Message.(*format.Insert).LSN)
	require.IsType(t, &format.Update{}, env.Changes[1].Message)
	require.IsType(t, &format.Delete{}, env.Changes[2].Message)
	require.False(t, a.inTransaction())
}

func TestOnlineInvalidInput(t *testing.T) {
	valid := [][]byte{onlineBegin(99, 100), onlineCommit(100, 110), onlineRelation(), onlineInsert(), onlineUpdate(), onlineDelete(), append(onlineU64([]byte{'O'}, 1), []byte("origin\x00")...), append(onlineU32([]byte{'Y'}, 900), []byte("public\x00custom\x00")...)}
	for _, b := range valid {
		for n := 0; n < len(b); n++ {
			a := newOnlineAssembler(4096, 4096)
			_, err := a.accept(XLogData{WALData: onlineBegin(99, 100)})
			require.NoError(t, err)
			_, err = a.accept(XLogData{WALData: onlineRelation()})
			require.NoError(t, err)
			_, err = a.accept(XLogData{WALData: b[:n]})
			require.Error(t, err, "truncated %q length %d", b[0], n)
		}
	}
	for _, b := range valid {
		a := newOnlineAssembler(4096, 4096)
		_, _ = a.accept(XLogData{WALData: onlineBegin(99, 100)})
		_, _ = a.accept(XLogData{WALData: onlineRelation()})
		_, err := a.accept(XLogData{WALData: append(append([]byte(nil), b...), 0)})
		require.Error(t, err, "trailing byte for %q", b[0])
	}
	for _, b := range [][]byte{{'S'}, {'E'}, {'A'}, {'c'}, {'M'}, {'T'}, {'?'}, onlineCommit(100, 110), onlineInsert()} {
		a := newOnlineAssembler(1024, 1024)
		_, err := a.accept(XLogData{WALData: b})
		require.Error(t, err)
	}
	t.Run("oversize", func(t *testing.T) {
		a := newOnlineAssembler(47, 1024)
		_, err := a.accept(XLogData{WALData: onlineBegin(99, 100)})
		require.NoError(t, err)
		_, err = a.accept(XLogData{WALData: onlineRelation()})
		require.ErrorIs(t, err, ErrOnlineTransactionLimit)
	})
	t.Run("commit mismatch", func(t *testing.T) {
		a := newOnlineAssembler(1024, 1024)
		_, _ = a.accept(XLogData{WALData: onlineBegin(99, 100)})
		_, err := a.accept(XLogData{WALData: onlineCommit(101, 110)})
		require.Error(t, err)
	})
	t.Run("invalid tuple tag and count", func(t *testing.T) {
		for _, mutate := range []func([]byte){func(b []byte) { b[8] = 'z' }, func(b []byte) { b[7] = 2 }, func(b []byte) { b[8] = 'b' }} {
			a := newOnlineAssembler(1024, 1024)
			_, _ = a.accept(XLogData{WALData: onlineBegin(99, 100)})
			_, _ = a.accept(XLogData{WALData: onlineRelation()})
			b := onlineInsert()
			mutate(b)
			_, err := a.accept(XLogData{WALData: b})
			require.Error(t, err)
		}
	})
	t.Run("options", func(t *testing.T) {
		for _, mutate := range []func(*OnlineReaderOptions){func(o *OnlineReaderOptions) { o.ProtoVersion = 2 }, func(o *OnlineReaderOptions) { o.StartLSN = 0 }, func(o *OnlineReaderOptions) { o.Source.SystemID = "" }, func(o *OnlineReaderOptions) { o.Limits.QueueBytes = 1 }, func(o *OnlineReaderOptions) { o.Limits.FeedbackInterval = 0 }, func(o *OnlineReaderOptions) { o.Publication = "pub\x00" }} {
			o := onlineOptions()
			mutate(&o)
			require.Error(t, o.Validate())
		}
	})
}

type onlineFakeTransport struct {
	input    chan pgproto3.BackendMessage
	feedback chan pq.LSN
	reads    chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func newOnlineFakeTransport() *onlineFakeTransport {
	return &onlineFakeTransport{input: make(chan pgproto3.BackendMessage, 32), feedback: make(chan pq.LSN, 128), reads: make(chan struct{}, 128), closed: make(chan struct{})}
}
func (f *onlineFakeTransport) receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	select {
	case f.reads <- struct{}{}:
	default:
	}
	select {
	case b := <-f.input:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.closed:
		return nil, io.EOF
	}
}
func (f *onlineFakeTransport) send(ctx context.Context, lsn pq.LSN) error {
	select {
	case f.feedback <- lsn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *onlineFakeTransport) close(context.Context) error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

type onlineFakeGate struct {
	waitFn func(context.Context, uint32) error
}

func (g onlineFakeGate) wait(ctx context.Context, xid uint32) error {
	if g.waitFn != nil {
		return g.waitFn(ctx, xid)
	}
	return nil
}
func (g onlineFakeGate) close(context.Context) error { return nil }
func onlineAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("barrier timeout")
		var z T
		return z
	}
}
func onlineFeedTransaction(f *onlineFakeTransport, xid uint32, lsn uint64) {
	f.input <- onlineXLog(onlineBegin(xid, lsn))
	f.input <- onlineXLog(onlineCommit(lsn, lsn+10))
}

func TestOnlineFeedbackAndCancellation(t *testing.T) {
	o := onlineOptions()
	o.Limits.TransactionBytes = 47
	o.Limits.QueueBytes = 47
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), o, f, onlineFakeGate{})
	defer func() { require.NoError(t, r.Close(context.Background())) }()
	onlineFeedTransaction(f, 1, 100)
	require.Eventually(t, func() bool { return r.BufferedBytes() == 47 }, time.Second, time.Millisecond)
	for i := 0; i < 3; i++ {
		require.Equal(t, pq.LSN(10), onlineAwait(t, f.feedback))
	}
	require.Error(t, r.AdvanceSafeACK(110), "not yet visibility-gated and consumed")
	env, err := r.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, pq.LSN(110), env.EndLSN)
	require.NoError(t, r.AdvanceSafeACK(110))
	require.Error(t, r.AdvanceSafeACK(10))
	for ack := onlineAwait(t, f.feedback); ack != 110; ack = onlineAwait(t, f.feedback) {
		require.LessOrEqual(t, ack, pq.LSN(110))
	}
	require.NoError(t, r.Close(context.Background()))
	onlineAwait(t, r.Done())
	require.ErrorIs(t, r.Err(), context.Canceled)
}

func TestOnlineVisibilityBarrier(t *testing.T) {
	entered := make(chan uint32, 2)
	release := make(chan struct{}, 2)
	gate := onlineFakeGate{waitFn: func(ctx context.Context, xid uint32) error {
		entered <- xid
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, gate)
	defer r.Close(context.Background())
	onlineFeedTransaction(f, 1, 100)
	onlineFeedTransaction(f, 2, 200)
	results := make(chan *OnlineCommittedEnvelope, 2)
	go func() {
		for i := 0; i < 2; i++ {
			env, err := r.Next(context.Background())
			if err == nil {
				results <- env
			}
		}
	}()
	require.Equal(t, uint32(1), onlineAwait(t, entered))
	require.Error(t, r.AdvanceSafeACK(110))
	select {
	case <-results:
		t.Fatal("escaped gate")
	default:
	}
	require.Equal(t, pq.LSN(10), onlineAwait(t, f.feedback), "feedback independent of SQL wait")
	release <- struct{}{}
	require.Equal(t, uint32(1), onlineAwait(t, results).XID)
	require.Equal(t, uint32(2), onlineAwait(t, entered))
	require.Error(t, r.AdvanceSafeACK(210))
	require.NoError(t, r.AdvanceSafeACK(110))
	release <- struct{}{}
	require.Equal(t, uint32(2), onlineAwait(t, results).XID)
	require.NoError(t, r.AdvanceSafeACK(210))
}

func TestOnlineGateFailure(t *testing.T) {
	fail := errors.New("guard lost")
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(context.Context, uint32) error { return fail }})
	onlineFeedTransaction(f, 1, 100)
	onlineFeedTransaction(f, 2, 200)
	env, err := r.Next(context.Background())
	require.Nil(t, env)
	require.ErrorIs(t, err, fail)
	onlineAwait(t, r.Done())
	require.ErrorIs(t, r.Err(), fail)
	require.Error(t, r.AdvanceSafeACK(110))
	env, err = r.Next(context.Background())
	require.Nil(t, env)
	require.ErrorIs(t, err, fail)
}

func TestOnlineCancelVisibility(t *testing.T) {
	entered := make(chan uint32, 1)
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(ctx context.Context, xid uint32) error { entered <- xid; <-ctx.Done(); return ctx.Err() }})
	onlineFeedTransaction(f, 1, 100)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := r.Next(ctx); result <- err }()
	onlineAwait(t, entered)
	cancel()
	require.ErrorIs(t, onlineAwait(t, result), context.Canceled)
	onlineAwait(t, r.Done())
	require.Error(t, r.AdvanceSafeACK(110))
}

func TestOnlineBackendInvalidInput(t *testing.T) {
	for _, raw := range []pgproto3.BackendMessage{&pgproto3.CopyData{}, &pgproto3.CopyData{Data: []byte{'w'}}, &pgproto3.CopyData{Data: []byte{'k'}}, &pgproto3.CopyData{Data: []byte{'?'}}, &pgproto3.ErrorResponse{Code: "XX000", Message: "source error"}, &pgproto3.CopyDone{}, &pgproto3.ReadyForQuery{}} {
		f := newOnlineFakeTransport()
		r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{})
		f.input <- raw
		onlineAwait(t, r.Done())
		require.Error(t, r.Err())
		env, err := r.Next(context.Background())
		require.Error(t, err)
		require.Nil(t, env)
	}
}

func TestOnlineSourceIdentityGate(t *testing.T) {
	o := onlineOptions()
	row := []string{"f", "db", "123", "1", "10:100:", "160001"}
	g := &onlineVisibilityGate{source: o.Source, timeout: time.Second, pollInterval: time.Millisecond, query: func(context.Context, string) ([]string, error) { return row, nil }}
	require.NoError(t, g.wait(context.Background(), 50))
	for _, change := range []struct {
		index int
		value string
	}{{0, "t"}, {1, "other"}, {2, "456"}, {3, "2"}, {4, "malformed"}, {5, "170000"}} {
		original := row[change.index]
		row[change.index] = change.value
		require.Error(t, g.wait(context.Background(), 50))
		row[change.index] = original
	}
	g.query = func(context.Context, string) ([]string, error) { return nil, errors.New("permission denied") }
	require.ErrorContains(t, g.wait(context.Background(), 50), "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system()")
}
