package integration

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineControlProtocol(t *testing.T) {
	fixture := newOnlinePGFixture(t)
	conn := fixture.Connect(t)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); CREATE TABLE extra(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'one')")
	provisionOnline(t, conn, "items", "extra")
	onlineExec(t, conn, "CREATE PUBLICATION online_pub FOR TABLE items,extra WITH(publish='insert,update,delete,truncate')")
	_, err := online.ValidatePublication(context.Background(), conn, "online_pub")
	require.Error(t, err, "missing watermark membership accepted")
	require.NoError(t, online.EnsureWatermarkPublication(context.Background(), conn, "online_pub", true))
	for _, sql := range []string{
		"ALTER PUBLICATION online_pub SET (publish='insert,update,delete')",
		"ALTER TABLE extra REPLICA IDENTITY DEFAULT",
		"ALTER TABLE extra ENABLE ROW LEVEL SECURITY",
		"ALTER TABLE extra ADD COLUMN generated integer GENERATED ALWAYS AS (id+1) STORED",
		"ALTER TABLE extra DROP CONSTRAINT extra_pkey",
		"ALTER TABLE extra DROP CONSTRAINT extra_pkey; ALTER TABLE extra ALTER COLUMN id TYPE text USING id::text; ALTER TABLE extra ADD PRIMARY KEY(id)",
		"ALTER PUBLICATION online_pub ADD TABLE cdc_online.jobs",
		"ALTER PUBLICATION online_pub SET TABLE items, extra WHERE(id>0),cdc_online.watermarks",
		"ALTER PUBLICATION online_pub SET TABLE items, extra(id),cdc_online.watermarks",
	} {
		onlineExec(t, conn, "BEGIN")
		onlineExec(t, conn, sql)
		_, err := online.ValidatePublication(context.Background(), conn, "online_pub")
		require.Error(t, err, "accepted: %s", sql)
		onlineExec(t, conn, "ROLLBACK")
	}
	// Non-superuser owns capture, metadata and SQL visibility with only provisioned
	// table DML/SELECT, replication, and identity-function EXECUTE privileges.
	onlineExec(t, conn, `CREATE ROLE online_reader LOGIN REPLICATION PASSWORD 'online'; GRANT CONNECT ON DATABASE online_poc TO online_reader; GRANT USAGE ON SCHEMA public TO online_reader; GRANT SELECT ON items,extra TO online_reader; GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO online_reader;`)
	require.NoError(t, online.Provision(context.Background(), conn, "online_reader"))
	cfg := onlineConfig(t, fixture, "items", "extra")
	cfg.Publication.CreateIfNotExists = false
	cfg.Publication.Tables = nil
	cfg.Username = "online_reader"
	cfg.Password = "online"
	cfg.Snapshot.Tables = publication.Tables{{Schema: "public", Name: "items"}}
	// Actual catalog gate includes extra although READs select only items.
	onlineExec(t, conn, "ALTER TABLE extra ENABLE ROW LEVEL SECURITY")
	r := startOnline(t, cfg, func(context.Context, online.Event) error { return nil })
	require.Error(t, onlineResult(t, r))
	onlineExec(t, conn, "ALTER TABLE extra DISABLE ROW LEVEL SECURITY")
	var count int
	r = startOnline(t, cfg, func(_ context.Context, e online.Event) error {
		if e.Table.Schema == "cdc_online" {
			return fmt.Errorf("control leaked")
		}
		count++
		return nil
	})
	waitOnlineCapture(t, r)
	waitOnlineSnapshot(t, r)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	require.Equal(t, 1, count)
}

func TestOnlineOwnerLifetime(t *testing.T) {
	fixture := newOnlinePGFixture(t)
	conn := fixture.Connect(t)
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	run, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner, err := online.OpenStoreWithStartupTimeout(run, fixture.DSN, "owner_lifetime", 100*time.Millisecond)
	require.NoError(t, err)
	defer owner.Close(context.Background())
	// Wait longer than acquisition deadline: successful startup must leave a
	// lifetime owned by run, not by the expired/canceled startup context.
	select {
	case <-owner.Done():
		t.Fatalf("startup deadline killed owner: %v", owner.Err())
	case <-time.After(1200 * time.Millisecond):
	}
	cancel()
	select {
	case <-owner.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("run cancellation did not stop owner")
	}
}
