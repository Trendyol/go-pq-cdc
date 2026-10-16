package integration

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialSmoke(t *testing.T) {
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'old'),(2,'delete'),(3,'stable')")
	provisionOnline(t, conn, "items")
	installHighBarrier(t, conn)
	cfg := onlineConfig(t, source, "items")
	events := make(chan online.Event, 32)
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		if err := target.Apply(ctx, e, onlineTargetOK); err != nil {
			return err
		}
		events <- e
		return nil
	})
	waitOnlineCapture(t, r)
	waitHighBarrier(t, conn)
	// SELECT is complete and HIGH is uncommitted. Both rows of page one collide;
	// cursor must still advance to 2 and no stale READ may escape.
	onlineExec(t, conn, "UPDATE items SET value='new' WHERE id=1; DELETE FROM items WHERE id=2")
	onlineExec(t, conn, "SELECT pg_advisory_unlock(8791)")
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	close(events)
	seenUpdate, seenDelete, seenRead := false, false, false
	for e := range events {
		require.Equal(t, online.Table{Schema: "public", Name: "items"}, e.Table)
		if e.Operation == online.Read {
			require.Equal(t, int64(3), e.Key, "colliding READ escaped")
			seenRead = true
		}
		if e.Operation == online.Update {
			seenUpdate = true
		}
		if e.Operation == online.Delete {
			seenDelete = true
		}
	}
	require.True(t, seenUpdate && seenDelete && seenRead, "complete overlap event coverage")
	compareOnlineTable(t, conn, target, "items")
	var windows int
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM cdc_online.windows WHERE status='completed'").Scan(&windows))
	require.GreaterOrEqual(t, windows, 2)
	var cursor int64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT cursor_key FROM cdc_online.table_progress").Scan(&cursor))
	require.Equal(t, int64(3), cursor)
}
