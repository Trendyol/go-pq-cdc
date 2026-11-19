package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineFullQueueFeedbackAndCancellation(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE saturated(id bigint PRIMARY KEY,value text); INSERT INTO saturated VALUES(1,'one')")
	provisionOnline(t, conn, "saturated")
	cfg := onlineConfig(t, source, "saturated")
	cfg.Snapshot.Online.TransactionBytes = 4096
	cfg.Snapshot.Online.QueueBytes = 8192
	cfg.Snapshot.Online.DeliveryTimeout = 10 * time.Second
	parked := make(chan struct{})
	var once sync.Once
	r := startOnline(t, cfg, func(ctx context.Context, _ online.Event) error {
		once.Do(func() { close(parked) })
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("sink stall")
	}
	for i := 2; i < 25; i++ {
		onlineExec(t, conn, "INSERT INTO saturated VALUES($1,repeat('x',1024))", i)
	}
	onlineEventually(t, func() bool {
		return r.Connector.Stats().BufferedEncodedBytes > cfg.Snapshot.Online.QueueBytes-cfg.Snapshot.Online.TransactionBytes
	}, 5*time.Second, 10*time.Millisecond, "reserved transaction cannot fit: full queue receive pause")
	before := r.Connector.Stats()
	time.Sleep(2200 * time.Millisecond)
	after := r.Connector.Stats()
	require.GreaterOrEqual(t, after.FeedbackWrites, before.FeedbackWrites+2)
	require.Equal(t, before.SafeACK, after.SafeACK, "stalled sink advanced ACK")
	require.LessOrEqual(t, after.PeakQueueAndAssemblyEncodedBytes, int64(8192))
	started := time.Now()
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	require.Less(t, time.Since(started), 5*time.Second)
}
