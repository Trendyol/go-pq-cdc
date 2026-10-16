package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineCheckpointAtomicity(t *testing.T) {
	t.Run("restart-old-generation-stale-attempt", testOnlineCheckpointReplayFencing)
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'one'),(2,'two'),(3,'three')")
	provisionOnline(t, conn, "items")
	cfg := onlineConfig(t, source, "items")
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var delivered []online.Event
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		if e.Operation == online.Read {
			once.Do(func() {
				close(blocked)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
		err := target.Apply(ctx, e, onlineTargetOK)
		if err == nil {
			delivered = append(delivered, e)
		}
		return err
	})
	waitOnlineCapture(t, r)
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("READ did not park")
	}
	var low, high, checkpoint string
	var cursor *int64
	var complete bool
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT w.low_end::text,w.high_end::text,j.source_lsn::text,p.cursor_key,p.complete FROM cdc_online.windows w JOIN cdc_online.jobs j USING(job_id) JOIN cdc_online.table_progress p USING(job_id,table_schema,table_name) WHERE w.status='high'`).Scan(&low, &high, &checkpoint, &cursor, &complete))
	require.Equal(t, low, checkpoint)
	require.NotEqual(t, low, high)
	require.Nil(t, cursor)
	require.False(t, complete)
	onlineExec(t, conn, "UPDATE items SET value='later' WHERE id=3")
	// Give periodic source feedback multiple opportunities while durable READ is
	// blocked. Neither HIGH nor the later transaction is allowed into ACK.
	for deadline := time.Now().Add(1200 * time.Millisecond); time.Now().Before(deadline); {
		var unsafe bool
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn > $1::pg_lsn FROM pg_replication_slots WHERE slot_name='online_slot'", low).Scan(&unsafe))
		require.False(t, unsafe, "confirmation crossed unresolved LOW")
		time.Sleep(20 * time.Millisecond)
	}
	close(release)
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	compareOnlineTable(t, conn, target, "items")
	read2, postHigh := -1, -1
	for i, event := range delivered {
		if event.Operation == online.Read && event.Key == 2 {
			read2 = i
		}
		if event.Operation == online.Update && event.Key == 3 {
			postHigh = i
		}
	}
	require.GreaterOrEqual(t, read2, 0)
	require.Greater(t, postHigh, read2, "post-HIGH transaction overtook surviving READs")
}

func testOnlineCheckpointReplayFencing(t *testing.T) {
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, "CREATE TABLE replay_rows(id bigint PRIMARY KEY,value text); INSERT INTO replay_rows VALUES(1,'one'),(2,'two'),(3,'three')")
	provisionOnline(t, conn, "replay_rows")
	cfg := onlineConfig(t, source, "replay_rows")
	firstParked := make(chan struct{})
	var firstOnce sync.Once
	first := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		if e.Operation == online.Read {
			firstOnce.Do(func() { close(firstParked); <-ctx.Done() })
		}
		return ctx.Err()
	})
	waitOnlineCapture(t, first)
	select {
	case <-firstParked:
	case <-time.After(10 * time.Second):
		t.Fatal("first-generation READ did not park")
	}
	var oldAttempt, oldHigh, connectorID, jobID string
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT w.attempt_id::text,w.high_id::text,j.connector_id::text,j.job_id::text FROM cdc_online.windows w JOIN cdc_online.jobs j USING(job_id) WHERE w.status='high'`).Scan(&oldAttempt, &oldHigh, &connectorID, &jobID))
	first.Cancel()
	require.ErrorIs(t, onlineResult(t, first), context.Canceled)
	onlineEventually(t, func() bool {
		var inactive bool
		err := conn.QueryRow(context.Background(), "SELECT NOT active FROM pg_replication_slots WHERE slot_name='online_slot'").Scan(&inactive)
		return err == nil && inactive
	}, 5*time.Second, 5*time.Millisecond)
	parked, release := make(chan struct{}), make(chan struct{})
	var secondOnce sync.Once
	second := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		if e.Operation == online.Read {
			secondOnce.Do(func() {
				close(parked)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
		return target.Apply(ctx, e, onlineTargetOK)
	})
	waitOnlineCapture(t, second)
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("fresh-generation READ did not park")
	}
	var currentAttempt, currentLow, checkpoint string
	var generation int64
	var cursor *int64
	var abandoned string
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT w.attempt_id::text,w.low_end::text,j.source_lsn::text,j.generation,p.cursor_key FROM cdc_online.windows w JOIN cdc_online.jobs j USING(job_id) JOIN cdc_online.table_progress p USING(job_id,table_schema,table_name) WHERE w.status='high'`).Scan(&currentAttempt, &currentLow, &checkpoint, &generation, &cursor))
	require.Equal(t, int64(2), generation)
	require.NotEqual(t, oldAttempt, currentAttempt)
	require.Nil(t, cursor)
	require.Equal(t, currentLow, checkpoint)
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT status FROM cdc_online.windows WHERE attempt_id=$1::uuid", oldAttempt).Scan(&abandoned))
	require.Equal(t, "abandoned", abandoned)
	// Replay a valid old-generation HIGH while the fresh window is unresolved.
	// It must remain control-only and may not release the fresh LOW ACK ceiling.
	onlineExec(t, conn, `UPDATE cdc_online.watermarks SET connector_id=$1::uuid,job_id=$2::uuid,generation=1,attempt_id=$3::uuid,phase='HIGH',marker_id=$4::uuid`, connectorID, jobID, oldAttempt, oldHigh)
	onlineExec(t, conn, "UPDATE replay_rows SET value='newer-after-old-control' WHERE id=3")
	for deadline := time.Now().Add(1200 * time.Millisecond); time.Now().Before(deadline); {
		var unsafe bool
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn>$1::pg_lsn FROM pg_replication_slots WHERE slot_name='online_slot'", currentLow).Scan(&unsafe))
		require.False(t, unsafe, "old-generation control crossed unresolved LOW")
		time.Sleep(20 * time.Millisecond)
	}
	close(release)
	waitOnlineSnapshot(t, second)
	waitOnlineFence(t, conn)
	// A same-generation stale/unknown attempt must stop the root, not be ignored
	// as if it were a previous generation or falsely complete current progress.
	onlineExec(t, conn, `UPDATE cdc_online.watermarks SET generation=2,attempt_id=$1::uuid,phase='HIGH',marker_id=$2::uuid`, onlineID(t), onlineID(t))
	require.ErrorIs(t, onlineResult(t, second), online.ErrConditional)
	var unsafe bool
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
	require.False(t, unsafe)
	compareOnlineTable(t, conn, target, "replay_rows")
	owner, err := online.OpenStoreWithStartupTimeout(context.Background(), source.DSN, "online_slot", time.Second)
	require.NoError(t, err)
	defer owner.Close(context.Background())
	require.ErrorIs(t, owner.CompleteAttempt(context.Background(), oldAttempt, 1, true), online.ErrConditional)
	require.ErrorIs(t, owner.CompleteAttempt(context.Background(), oldAttempt, 2, true), online.ErrConditional)
	var preserved string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT status FROM cdc_online.windows WHERE attempt_id=$1::uuid", oldAttempt).Scan(&preserved))
	require.Equal(t, "abandoned", preserved)
}
