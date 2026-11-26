package integration

import (
	"context"
	"sync/atomic"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

// Catalog-only DDL is no longer polled during a snapshot. The snapshot uses its
// startup schema; this test demonstrates that changed PK catalog facts are not
// proactively detected, not that arbitrary concurrent DDL is safe.
func TestOnlineSnapshotCatalogValidationStartupOnly(t *testing.T) {
	source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE EXTENSION pg_stat_statements")
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value bigint)")
	onlineExec(t, conn, "INSERT INTO items SELECT i,i FROM generate_series(1,5) i")
	provisionOnline(t, conn, "items")
	var changed atomic.Bool
	var reads atomic.Int64
	var baseline int64
	calls := func(ctx context.Context) int64 {
		var n int64
		require.NoError(t, conn.QueryRow(ctx, `SELECT COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query LIKE '%FROM pg_publication p WHERE pubname%' OR query LIKE '%FROM pg_index i CROSS JOIN LATERAL%'`).Scan(&n))
		return n
	}
	r := startOnline(t, onlineConfig(t, source, "items"), func(_ context.Context, e online.Event) error {
		if e.Table.Name == "items" && e.Operation == online.Read {
			reads.Add(1)
		}
		return nil
	}, cdc.WithOnlineObserver(func(ctx context.Context, o online.Observation) error {
		if o.Phase == online.ReadResult && changed.CompareAndSwap(false, true) {
			baseline = calls(ctx)
			_, err := conn.Exec(ctx, "ALTER TABLE items DROP CONSTRAINT items_pkey")
			return err
		}
		return nil
	}))
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	// Completion synchronizes the observer's baseline write.
	require.True(t, changed.Load())
	require.Positive(t, baseline, "startup catalog checks must run")
	require.Equal(t, baseline, calls(context.Background()), "no repeated publication/key catalog checks")
	require.EqualValues(t, 5, reads.Load())
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
}
