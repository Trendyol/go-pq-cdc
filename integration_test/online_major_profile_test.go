package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestOnlineGeneratedColumnsStartup(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	for _, kind := range []string{"STORED"} {
		t.Run(kind, func(t *testing.T) {
			onlineExec(t, conn, "CREATE TABLE generated_rows(id bigint PRIMARY KEY,value bigint GENERATED ALWAYS AS (id+1) "+kind+"); ALTER TABLE generated_rows REPLICA IDENTITY FULL")
			cfg := onlineConfig(t, source, "generated_rows")
			calls := 0
			connector, err := cdc.NewOnlineConnector(context.Background(), cfg, func(context.Context, online.Event) error { calls++; return nil })
			require.NoError(t, err)
			run, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.ErrorIs(t, connector.Run(run), online.ErrSchema)
			require.Zero(t, calls)
			var slots, jobs int
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots").Scan(&slots))
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM cdc_online.jobs").Scan(&jobs))
			require.Zero(t, slots, "generated column rejection created slot")
			require.Zero(t, jobs, "generated column rejection created job")
			t.Logf("PG%d %s generated columns rejected before slot/job creation; effective_online_protocol=1", source.ServerVersionNum/10000, kind)
			onlineExec(t, conn, "DROP PUBLICATION online_pub; DROP TABLE generated_rows")
		})
	}
}

// This deliberately asserts the admitted limitation, not a delivery claim:
// PostgreSQL does not publish UNLOGGED tables, but InspectTable admits them.
func TestOnlineUNLOGGEDBusinessAdmission(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE UNLOGGED TABLE unlogged_rows(id bigint PRIMARY KEY,value text); ALTER TABLE unlogged_rows REPLICA IDENTITY FULL")
	var persistence string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT relpersistence::text FROM pg_class WHERE oid='unlogged_rows'::regclass").Scan(&persistence))
	require.Equal(t, "u", persistence)
	schema, err := online.InspectTable(context.Background(), conn, online.Table{Schema: "public", Name: "unlogged_rows"})
	require.NoError(t, err, "locked UNLOGGED business admission changed")
	require.Equal(t, "id", schema.Key)
	t.Log("admitted limitation: UNLOGGED business tables pass table admission; reader output settings remain unpinned; no UNLOGGED CDC guarantee")
}

func TestOnlineSlotStatesMajors(t *testing.T) {
	source := newOnlinePGFixtureDurability(t, true, "max_slot_wal_keep_size=1MB")
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE slot_rows(id bigint PRIMARY KEY); CREATE PUBLICATION slot_pub FOR TABLE slot_rows")
	modes := []string{"fresh", "temporary", "two_phase", "physical", "active", "lost"}
	if source.ServerVersionNum >= 170000 {
		modes = append(modes, "failover")
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			const name = "profile_slot"
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			missing, err := online.InspectSlot(ctx, conn, name)
			require.NoError(t, err)
			require.False(t, missing.Exists)
			switch mode {
			case "fresh", "active", "lost":
				lsn, err := online.CreateFreshSlot(ctx, conn, name, true)
				require.NoError(t, err)
				require.NotZero(t, lsn)
			case "temporary":
				onlineExec(t, conn, "SELECT pg_create_logical_replication_slot($1,'pgoutput',true,false)", name)
			case "two_phase":
				onlineExec(t, conn, "SELECT pg_create_logical_replication_slot($1,'pgoutput',false,true)", name)
			case "physical":
				onlineExec(t, conn, "SELECT pg_create_physical_replication_slot($1)", name)
			case "failover":
				onlineExec(t, conn, "SELECT pg_create_logical_replication_slot($1,'pgoutput',false,false,true)", name)
			}
			defer onlineExec(t, conn, "SELECT pg_drop_replication_slot($1)", name)
			if mode == "active" {
				holder, err := pgconn.Connect(ctx, source.DSN+"&replication=database")
				require.NoError(t, err)
				defer holder.Close(context.Background())
				holder.Frontend().Send(&pgproto3.Query{String: "START_REPLICATION SLOT profile_slot LOGICAL 0/0 (proto_version '1', publication_names 'slot_pub')"})
				require.NoError(t, holder.Frontend().Flush())
				message, err := holder.ReceiveMessage(ctx)
				require.NoError(t, err)
				require.IsType(t, &pgproto3.CopyBothResponse{}, message)
			}
			if mode == "lost" {
				onlineExec(t, conn, "SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT")
				var status string
				require.NoError(t, conn.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&status))
				require.Equal(t, "lost", status, "slot-loss negative control must be real")
				if source.ServerVersionNum >= 170000 {
					var reason string
					require.NoError(t, conn.QueryRow(ctx, "SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&reason))
					require.Equal(t, "wal_removed", reason)
					t.Logf("observed invalidation_reason=%s", reason)
				}
			}
			state, err := online.InspectSlot(ctx, conn, name)
			require.NoError(t, err)
			require.True(t, state.Exists)
			require.Equal(t, mode != "fresh", state.Invalid)
			_, err = online.CreateFreshSlot(ctx, conn, name, true)
			require.ErrorIs(t, err, online.ErrBinding, "must not recreate/adopt an existing slot")
			t.Logf("PG%d slot=%s invalid=%t effective_online_protocol=1", source.ServerVersionNum/10000, mode, state.Invalid)
		})
	}
}
