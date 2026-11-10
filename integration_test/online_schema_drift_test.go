package integration

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineCatalogDriftWithoutChangedTableEvents(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); CREATE TABLE extra(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'old')")
	provisionOnline(t, conn, "items", "extra")
	cfg := onlineConfig(t, source, "items", "extra")
	cfg.Snapshot.Tables = publication.Tables{{Schema: "public", Name: "items"}}
	var delivered []online.Event
	r := startOnline(t, cfg, func(_ context.Context, event online.Event) error {
		delivered = append(delivered, event)
		return nil
	})
	waitOnlineCapture(t, r)
	waitOnlineSnapshot(t, r)

	// No READ or CDC image of extra can expose its new column. A valid items
	// transaction schedules the catalog check without changing items' schema.
	onlineExec(t, conn, "ALTER TABLE extra ADD COLUMN drift text")
	onlineExec(t, conn, "UPDATE items SET value='new' WHERE id=1")
	require.ErrorIs(t, onlineResult(t, r), online.ErrSchema)
	require.Len(t, delivered, 1, "catalog drift must stop before the valid CDC image is delivered")
	require.Equal(t, "items", delivered[0].Table.Name)
	require.Equal(t, online.Read, delivered[0].Operation)
	var unsafe bool
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
	require.False(t, unsafe, "catalog drift advanced ACK beyond durable source progress")
}
