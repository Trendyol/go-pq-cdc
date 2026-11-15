package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineCDCFollowsRelationSchema(t *testing.T) {
	source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE EXTENSION pg_stat_statements")
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value bigint)")
	provisionOnline(t, conn, "items")
	events := make(chan online.Event, 20)
	r := startOnline(t, onlineConfig(t, source, "items"), func(_ context.Context, e online.Event) error {
		if e.Table.Name == "items" {
			events <- e
		}
		return nil
	})
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	catalogCalls := func() int64 {
		var n int64
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query LIKE '%FROM pg_publication p WHERE pubname%'`).Scan(&n))
		return n
	}
	before := catalogCalls()
	require.Positive(t, before)
	next := func() online.Event {
		select {
		case e := <-events:
			return e
		case <-time.After(10 * time.Second):
			t.Fatal("CDC event timeout")
			return online.Event{}
		}
	}
	onlineExec(t, conn, "INSERT INTO items VALUES(1,10)")
	require.Len(t, next().Image, 2)
	onlineExec(t, conn, "ALTER TABLE items ADD COLUMN label text DEFAULT 'new'")
	onlineExec(t, conn, "UPDATE items SET value=11 WHERE id=1")
	added := next()
	require.Len(t, added.Image, 3)
	require.Equal(t, "new", added.Image["label"])
	onlineExec(t, conn, "ALTER TABLE items RENAME COLUMN label TO title")
	onlineExec(t, conn, "UPDATE items SET value=12 WHERE id=1")
	renamed := next()
	require.Equal(t, "new", renamed.Image["title"])
	require.NotContains(t, renamed.Image, "label")
	onlineExec(t, conn, "ALTER TABLE items DROP COLUMN title")
	onlineExec(t, conn, "UPDATE items SET value=13 WHERE id=1")
	require.Len(t, next().Image, 2)
	waitOnlineFence(t, conn)
	require.Equal(t, before, catalogCalls(), "no per-envelope publication catalog queries during CDC")
	// Unsupported new columns must fail before any business delivery.
	onlineExec(t, conn, "ALTER TABLE items ADD COLUMN unsupported numeric")
	onlineExec(t, conn, "UPDATE items SET unsupported=1 WHERE id=1")
	select {
	case <-r.Connector.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("unsupported type did not stop reader")
	}
	require.Error(t, r.Connector.Err())
	require.True(t, strings.Contains(r.Connector.Err().Error(), "unqualified") || strings.Contains(r.Connector.Err().Error(), "unsupported"), r.Connector.Err().Error())
	require.Empty(t, events)
}
