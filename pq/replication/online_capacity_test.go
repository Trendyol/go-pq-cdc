package replication

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestOnlineContinuousFeedback(t *testing.T) {
	o := onlineOptions()
	o.Limits.TransactionBytes = 47
	o.Limits.QueueBytes = 94
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), o, f, onlineFakeGate{})
	defer r.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := uint64(1); ; i++ {
			for _, raw := range []pgproto3.BackendMessage{onlineXLog(onlineBegin(uint32(i), i*100)), onlineXLog(onlineCommit(i*100, i*100+10))} {
				select {
				case f.input <- raw:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() {
		defer close(delivered)
		for i := 0; i < 100; i++ {
			env, err := r.Next(ctx)
			if err != nil {
				return
			}
			if err = r.AdvanceSafeACK(env.EndLSN); err != nil {
				return
			}
		}
	}()
	onlineAwait(t, delivered)
	// Eventually reaches full queue while the producer remains active. Feedback
	// continues from only the last caller-supplied safe ACK, not ServerWALEnd.
	require.Eventually(t, func() bool { return r.BufferedBytes() == 94 }, time.Second, time.Millisecond)
	for i := 0; i < 3; i++ {
		ack := onlineAwait(t, f.feedback)
		require.LessOrEqual(t, ack, pq.LSN(10010))
	}
	require.NoError(t, r.Close(context.Background()))
	cancel()
	onlineAwait(t, producerDone)
	onlineAwait(t, r.Done())
}

func TestOnlineMetadataMessages(t *testing.T) {
	a := newOnlineAssembler(1024, 2048)
	for _, b := range [][]byte{onlineRelation(), append(onlineU32([]byte{'Y'}, 900), []byte("public\x00type_name\x00")...), onlineBegin(1, 100), append(onlineU64([]byte{'O'}, 50), []byte("origin\x00")...), onlineInsert()} {
		env, err := a.accept(XLogData{WALData: b})
		require.NoError(t, err)
		require.Nil(t, env)
	}
	env, err := a.accept(XLogData{WALData: onlineCommit(100, 110)})
	require.NoError(t, err)
	require.Len(t, env.Changes, 1)
	require.Zero(t, env.Changes[0].Ordinal)
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{})
	defer r.Close(context.Background())
	f.input <- &pgproto3.NoticeResponse{Message: "normal notice"}
	f.input <- &pgproto3.ParameterStatus{Name: "server_version", Value: "16.1"}
	onlineFeedTransaction(f, 1, 100)
	env, err = r.Next(context.Background())
	require.NoError(t, err)
	require.Empty(t, env.Changes)
}

func TestOnlineMetadataBudget(t *testing.T) {
	relation := onlineRelation()
	a := newOnlineAssembler(1024, int64(len(relation)))
	_, err := a.accept(XLogData{WALData: relation})
	require.NoError(t, err)
	_, err = a.accept(XLogData{WALData: relation})
	require.NoError(t, err, "replacement not double charged")
	different := append([]byte(nil), relation...)
	different[4] = 8
	_, err = a.accept(XLogData{WALData: different})
	require.ErrorIs(t, err, ErrOnlineMetadataLimit)
}

func TestOnlineVisibilityTimeout(t *testing.T) {
	o := onlineOptions()
	g := &onlineVisibilityGate{source: o.Source, timeout: 5 * time.Millisecond, pollInterval: time.Millisecond, query: func(context.Context, string) ([]string, error) {
		return []string{"f", "db", "123", "1", "1:2:1", "160001"}, nil
	}}
	require.ErrorIs(t, g.wait(context.Background(), 1), ErrVisibilityTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, g.wait(ctx, 1), context.Canceled)
}

func TestOnlineKeepaliveSafeACK(t *testing.T) {
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{})
	defer r.Close(context.Background())
	b := onlineU64([]byte{'k'}, 9000)
	b = onlineU64(b, 0)
	b = append(b, 1)
	f.input <- &pgproto3.CopyData{Data: b}
	require.Equal(t, pq.LSN(10), onlineAwait(t, f.feedback))
	require.Error(t, r.AdvanceSafeACK(9000))
	b = append([]byte(nil), b...)
	b[17] = 2
	f.input <- &pgproto3.CopyData{Data: b}
	onlineAwait(t, r.Done())
	require.ErrorIs(t, r.Err(), ErrOnlineProtocol)
}

func TestOnlineWriteCancellation(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- onlineWrite(ctx, conn, func() error { close(entered); _, err := conn.Write([]byte("feedback")); return err })
	}()
	onlineAwait(t, entered)
	cancel()
	require.ErrorIs(t, onlineAwait(t, result), context.Canceled)
	// The watcher is finished and its deadline reset before the next operation.
	readDone := make(chan error, 1)
	go func() { buf := make([]byte, 1); _, err := peer.Read(buf); readDone <- err }()
	require.NoError(t, onlineWrite(context.Background(), conn, func() error { _, err := conn.Write([]byte{'x'}); return err }))
	require.NoError(t, onlineAwait(t, readDone))
}

func TestOnlineCloseDuringGate(t *testing.T) {
	f := newOnlineFakeTransport()
	entered := make(chan struct{})
	result := make(chan error, 1)
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(ctx context.Context, _ uint32) error { close(entered); <-ctx.Done(); return ctx.Err() }})
	onlineFeedTransaction(f, 1, 100)
	go func() { _, err := r.Next(context.Background()); result <- err }()
	onlineAwait(t, entered)
	require.NoError(t, r.Close(context.Background()))
	require.ErrorIs(t, onlineAwait(t, result), context.Canceled)
	onlineAwait(t, r.Done())
}

func TestOnlineCloseDeadline(t *testing.T) {
	f := newOnlineFakeTransport()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(context.Context, uint32) error {
		once.Do(func() { close(entered) })
		<-release
		return errors.New("test gate deliberately ignores cancellation")
	}})
	onlineFeedTransaction(f, 1, 100)
	nextDone := make(chan error, 1)
	go func() { _, err := r.Next(context.Background()); nextDone <- err }()
	onlineAwait(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Close(ctx), context.DeadlineExceeded)
	close(release)
	require.Error(t, onlineAwait(t, nextDone))
	onlineAwait(t, r.Done())
}
