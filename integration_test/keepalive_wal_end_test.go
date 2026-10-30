package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unrelatedTable is written by these tests but is not in the publication
// (books only), like another application's busy table.
const unrelatedTable = "keepalive_unrelated"

// setupKeepaliveTest gives a subtest a fresh slot, the unrelated table and a
// 2s wal_sender_timeout, all undone in cleanup once the connector has closed.
func setupKeepaliveTest(ctx context.Context, t *testing.T, cfg config.Config) pq.Connection {
	t.Helper()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	dropSlot(ctx, conn, cfg.Slot.Name) // a leftover from an earlier run would replay its WAL
	require.NoError(t, SetupTestDB(ctx, conn, cfg))
	require.NoError(t, pgExec(ctx, conn, "CREATE TABLE IF NOT EXISTS "+unrelatedTable+" (id serial PRIMARY KEY, payload text)"))
	require.NoError(t, pgExec(ctx, conn, "ALTER SYSTEM SET wal_sender_timeout = '2s'"))
	require.NoError(t, pgExec(ctx, conn, "SELECT pg_reload_conf()"))

	t.Cleanup(func() {
		assert.NoError(t, pgExec(ctx, conn, "ALTER SYSTEM RESET wal_sender_timeout"))
		assert.NoError(t, pgExec(ctx, conn, "SELECT pg_reload_conf()"))
		assert.NoError(t, pgExec(ctx, conn, "DROP TABLE IF EXISTS "+unrelatedTable))
		// The walsender exits shortly after Close; an active slot cannot be dropped yet.
		assert.Eventually(t, func() bool {
			return pgExec(ctx, conn, fmt.Sprintf(
				"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", cfg.Slot.Name)) == nil
		}, 10*time.Second, 100*time.Millisecond, "drop slot %s", cfg.Slot.Name)
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, conn.Close(ctx))
	})
	return conn
}

func startKeepaliveConnector(ctx context.Context, t *testing.T, cfg config.Config, listener replication.ListenerFunc) cdc.Connector {
	t.Helper()
	connector, err := cdc.NewConnector(ctx, cfg, listener)
	require.NoError(t, err)
	t.Cleanup(connector.Close)
	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	return connector
}

// writeUnrelatedWAL commits rows outside the publication and switches WAL
// segments, so the server's WAL end moves segments past anything the slot
// decodes for the listener.
func writeUnrelatedWAL(ctx context.Context, t *testing.T, conn pq.Connection) {
	t.Helper()
	insert := "INSERT INTO " + unrelatedTable + " (payload) SELECT 'row-' || g FROM generate_series(1, %d) g"
	require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(insert, 1000)))
	for range 3 {
		require.NoError(t, pgExec(ctx, conn, "SELECT pg_switch_wal()"))
		// A switch without new WAL is a no-op, and the walsender pings once it decodes this.
		require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(insert, 1)))
	}
}

// sqlTrue reports whether a one-value boolean query returns true. It never
// fails the test, so it is safe inside Eventually.
func sqlTrue(ctx context.Context, conn pq.Connection, query string) bool {
	results, err := execQuery(ctx, conn, query)
	return err == nil && len(results) == 1 && len(results[0].Rows) == 1 && string(results[0].Rows[0][0]) == "t"
}

const walSegmentSize = "(SELECT setting::numeric FROM pg_settings WHERE name = 'wal_segment_size')"

// slotNearWALEnd: confirmed_flush_lsn is within one WAL segment of pg_current_wal_lsn().
func slotNearWALEnd(slot string) string {
	return fmt.Sprintf("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) < %s FROM pg_replication_slots WHERE slot_name = '%s'",
		walSegmentSize, slot)
}

// slotNotPast: confirmed_flush_lsn has not passed lsn.
func slotNotPast(slot string, lsn pq.LSN) string {
	return fmt.Sprintf("SELECT confirmed_flush_lsn <= '%s' FROM pg_replication_slots WHERE slot_name = '%s'", lsn, slot)
}

// clientWrotePast: the client reported as received a position more than one
// segment past lsn. Nothing past lsn is published, so only keepalives carry it.
func clientWrotePast(slot string, lsn pq.LSN) string {
	return fmt.Sprintf(`SELECT pg_wal_lsn_diff(r.write_lsn, '%s') > %s
		FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid = r.pid
		WHERE s.slot_name = '%s'`, lsn, walSegmentSize, slot)
}

// holdInserts hands every row to held without acking it. The Relation message
// is acked, so only the rows stay in flight.
func holdInserts(held chan<- *replication.ListenerContext) replication.ListenerFunc {
	return func(l *replication.ListenerContext) {
		if _, ok := l.Message.(*format.Insert); ok {
			held <- l
			return
		}
		_ = l.Ack()
	}
}

func receiveHeld(t *testing.T, held <-chan *replication.ListenerContext) *replication.ListenerContext {
	t.Helper()
	select {
	case l := <-held:
		return l
	case <-time.After(10 * time.Second):
		t.Fatal("row was not delivered")
		return nil
	}
}

// A publication that sees no changes while the rest of the database writes
// must not pin WAL, heartbeat or not. Without the keepalive confirm nothing is
// ever acked, so confirmed_flush_lsn stays where the slot was created.
func TestKeepaliveAdvancesIdleSlot(t *testing.T) {
	ctx := context.Background()
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_keepalive_idle"
	cdcCfg.Heartbeat = config.HeartbeatConfig{}

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		conn := setupKeepaliveTest(ctx, t, cdcCfg)
		startKeepaliveConnector(ctx, t, cdcCfg, func(l *replication.ListenerContext) { _ = l.Ack() })

		writeUnrelatedWAL(ctx, t, conn)

		require.Eventually(t, func() bool { return sqlTrue(ctx, conn, slotNearWALEnd(cdcCfg.Slot.Name)) },
			15*time.Second, 200*time.Millisecond, "confirmed_flush_lsn did not follow the WAL end on keepalives")
	})
}

// Keepalives past a delivered, unacked row must not confirm past its commit:
// on restart PostgreSQL skips only commits that start below confirmed_flush_lsn.
// Once the row is acked the slot catches up again.
func TestKeepaliveDoesNotConfirmPastUnackedRow(t *testing.T) {
	ctx := context.Background()
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_keepalive_unacked"
	cdcCfg.Heartbeat = config.HeartbeatConfig{}

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		conn := setupKeepaliveTest(ctx, t, cdcCfg)
		slot := cdcCfg.Slot.Name
		held := make(chan *replication.ListenerContext, 1)
		startKeepaliveConnector(ctx, t, cdcCfg, holdInserts(held))

		require.NoError(t, pgExec(ctx, conn, "INSERT INTO books (id, name) VALUES (1, 'held')"))
		row := receiveHeld(t, held)

		writeUnrelatedWAL(ctx, t, conn)
		require.Eventually(t, func() bool { return sqlTrue(ctx, conn, clientWrotePast(slot, row.CommitLSN)) },
			15*time.Second, 200*time.Millisecond, "no keepalive past the held row reached the client")
		assert.True(t, sqlTrue(ctx, conn, slotNotPast(slot, row.CommitLSN)),
			"confirmed_flush_lsn passed the unacked row's commit %s", row.CommitLSN)

		require.NoError(t, row.Ack())
		writeUnrelatedWAL(ctx, t, conn)
		require.Eventually(t, func() bool { return sqlTrue(ctx, conn, slotNearWALEnd(slot)) },
			15*time.Second, 200*time.Millisecond, "confirmed_flush_lsn did not catch up after the ack")
	})
}

// Rows delivered but never acked survive a connector restart while keepalives
// keep arriving.
func TestKeepaliveRedeliversUnackedRowsAfterRestart(t *testing.T) {
	ctx := context.Background()
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_keepalive_restart"
	cdcCfg.Heartbeat = config.HeartbeatConfig{}

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		conn := setupKeepaliveTest(ctx, t, cdcCfg)
		slot := cdcCfg.Slot.Name
		held := make(chan *replication.ListenerContext, 3)
		first := startKeepaliveConnector(ctx, t, cdcCfg, holdInserts(held))

		// The keepalive confirm is live: the idle slot follows the WAL end.
		writeUnrelatedWAL(ctx, t, conn)
		require.Eventually(t, func() bool { return sqlTrue(ctx, conn, slotNearWALEnd(slot)) },
			15*time.Second, 200*time.Millisecond, "idle slot did not follow the WAL end")

		for id := 1; id <= 3; id++ {
			require.NoError(t, pgExec(ctx, conn, fmt.Sprintf("INSERT INTO books (id, name) VALUES (%d, 'restart-%d')", id, id)))
		}
		firstRow := receiveHeld(t, held)
		receiveHeld(t, held)
		receiveHeld(t, held)

		writeUnrelatedWAL(ctx, t, conn)
		require.Eventually(t, func() bool { return sqlTrue(ctx, conn, clientWrotePast(slot, firstRow.CommitLSN)) },
			15*time.Second, 200*time.Millisecond, "no keepalive past the held rows reached the client")
		first.Close() // rows never acked: the final status update must not cover them

		redelivered := make(chan int32, 10)
		startKeepaliveConnector(ctx, t, cdcCfg, func(l *replication.ListenerContext) {
			if ins, ok := l.Message.(*format.Insert); ok {
				redelivered <- ins.Decoded["id"].(int32)
			}
			_ = l.Ack()
		})
		var ids []int32
		for range 3 {
			select {
			case id := <-redelivered:
				ids = append(ids, id)
			case <-time.After(15 * time.Second):
				t.Fatalf("only %v of the 3 unacked rows were redelivered", ids)
			}
		}
		assert.ElementsMatch(t, []int32{1, 2, 3}, ids)
	})
}
