package integration

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialProgress(t *testing.T) {
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, `CREATE TABLE ranges(id bigint PRIMARY KEY,value text); INSERT INTO ranges VALUES(-9223372036854775808,'min'),(-20,'negative'),(20,'sparse'),(900,'upper'); CREATE TABLE empty(id integer PRIMARY KEY,value text); CREATE TABLE other(id smallint PRIMARY KEY,value text); INSERT INTO other VALUES(-1,'other');`)
	provisionOnline(t, conn, "ranges", "empty", "other")
	installHighBarrier(t, conn)
	cfg := onlineConfig(t, source, "ranges", "empty", "other")
	// Order ranges first so behind/above-bound inserts exercise its first page.
	cfg.Snapshot.Tables = cfg.Publication.Tables
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
	waitOnlineCapture(t, r)
	waitHighBarrier(t, conn)
	onlineExec(t, conn, `INSERT INTO ranges VALUES(-100,'behind'),(1000,'above'); UPDATE ranges SET value='min-new' WHERE id=-9223372036854775808; DELETE FROM ranges WHERE id=-20`)
	onlineExec(t, conn, "SELECT pg_advisory_unlock(8791)")
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	for _, table := range []string{"ranges", "empty", "other"} {
		compareOnlineTable(t, conn, target, table)
	}
	var upper, cursor int64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT upper_key,cursor_key FROM cdc_online.table_progress WHERE table_name='ranges'").Scan(&upper, &cursor))
	require.Equal(t, int64(900), upper)
	require.Equal(t, int64(900), cursor)
	var low, high string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT low_end::text,high_end::text FROM cdc_online.windows WHERE table_name='empty' AND status='completed'").Scan(&low, &high))
	require.NotEqual(t, "0/0", low)
	require.NotEqual(t, "0/0", high)
}
