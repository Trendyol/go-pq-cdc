package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineExcludesLogicalDecodingMessages(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'old')")
	provisionOnline(t, conn, "items")
	cfg := onlineConfig(t, source, "items")
	updates := make(chan online.Event, 1)
	r := startOnline(t, cfg, func(_ context.Context, event online.Event) error {
		if event.Operation == online.Update && event.Table.Name == "items" {
			updates <- event
		}
		return nil
	})
	waitOnlineCapture(t, r)
	waitOnlineSnapshot(t, r)

	// Legacy protocol-1 streams now request logical messages by default. Online
	// must exclude both forms without dropping the subsequent business change.
	onlineExec(t, conn, "SELECT pg_logical_emit_message(true,'online-test','transactional')")
	onlineExec(t, conn, "SELECT pg_logical_emit_message(false,'online-test','nontransactional')")
	onlineExec(t, conn, "UPDATE items SET value='new' WHERE id=1")
	select {
	case event := <-updates:
		require.Equal(t, "new", event.Image["value"])
	case <-r.Connector.Done():
		t.Fatalf("logical message terminated online capture: %v", r.Connector.Err())
	case <-time.After(10 * time.Second):
		t.Fatal("business update did not arrive after logical messages")
	}
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
}
