package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/require"
)

func TestOnlineVisibilityPolicyAllPhases(t *testing.T) {
	for _, guardEnabled := range []bool{false, true} {
		name := "disabled"
		if guardEnabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			f := newOnlineFakeTransport()
			r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(context.Context, uint32) error { calls.Add(1); return nil }})
			defer func() { require.NoError(t, r.Close(context.Background())) }()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if !guardEnabled {
				r.SkipVisibilityChecks()
			}
			onlineFeedTransaction(f, 1, 100)
			_, err := r.Next(ctx)
			require.NoError(t, err)
			initialCalls := int64(0)
			if guardEnabled {
				initialCalls = 1
			}
			require.Equal(t, initialCalls, calls.Load(), "guard controls snapshot visibility")
			require.Equal(t, pq.LSN(10), r.Stats().SafeACK, "delivery does not auto-ACK")
			onlineFeedTransaction(f, 2, 200)
			env, err := r.Next(ctx)
			require.NoError(t, err)
			expected := int64(0)
			if guardEnabled {
				expected = 2
			}
			require.Equal(t, expected, calls.Load())
			require.Equal(t, pq.LSN(210), env.EndLSN)
			require.Equal(t, pq.LSN(10), r.Stats().SafeACK)
			require.Error(t, r.AdvanceSafeACK(211))
			require.NoError(t, r.AdvanceSafeACK(210))
		})
	}
}

func TestOnlineDisabledVisibilityStillRequiresCommit(t *testing.T) {
	f := newOnlineFakeTransport()
	r := newOnlineReader(context.Background(), onlineOptions(), f, onlineFakeGate{waitFn: func(context.Context, uint32) error { return errors.New("gate must not run") }})
	defer func() { require.NoError(t, r.Close(context.Background())) }()
	r.SkipVisibilityChecks()
	r.SkipVisibilityChecks() // repeated configuration is harmless
	f.input <- onlineXLog(onlineBegin(1, 100))
	f.input <- onlineXLog(onlineRelation())
	f.input <- onlineXLog(onlineInsert())
	// A read barrier confirms BEGIN and DML were processed, but no envelope
	// exists until COMMIT, even with visibility checking disabled.
	require.Eventually(t, func() bool { return r.Stats().PeakTransactionEncodedBytes > 20 }, time.Second, time.Millisecond)
	require.Zero(t, r.BufferedBytes())
	require.Error(t, r.AdvanceSafeACK(110))
	f.input <- onlineXLog(onlineCommit(100, 110))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	env, err := r.Next(ctx)
	require.NoError(t, err)
	require.Len(t, env.Changes, 1)
	require.Equal(t, pq.LSN(110), env.EndLSN)
	require.Equal(t, pq.LSN(10), r.Stats().SafeACK)
}
