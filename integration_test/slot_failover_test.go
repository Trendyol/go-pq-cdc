package integration

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"testing"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/require"
)

// TestSlotFailover covers slot.failover against a real server: on
// PostgreSQL 17+ a fresh slot is created with FAILOVER true and an existing
// slot created without it is altered; older servers reject the option with a
// clear error instead of a CREATE_REPLICATION_SLOT syntax error.
func TestSlotFailover(t *testing.T) {
	const (
		existingSlot = "slot_test_failover_existing"
		freshSlot    = "slot_test_failover_fresh"
	)
	ctx := context.Background()
	logger.InitLogger(logger.NewSlog(slog.LevelError)) // slot is used without a connector here

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer func() { _ = postgresConn.Close(ctx) }()

	newSlot := func(name string, failover bool) *slot.Slot {
		cfg := Config.Slot
		cfg.Name = name
		cfg.CreateIfNotExists = true
		cfg.SlotActivityCheckerInterval = 1000
		cfg.Failover = failover
		return slot.NewSlot(Config.ReplicationDSN(), Config.DSN(), cfg, nil, nil)
	}
	dropSlot := func(name string) {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", name, name))
	}
	failoverFlag := func(name string) string {
		return pgScalar(t, ctx, postgresConn, fmt.Sprintf("SELECT failover FROM pg_replication_slots WHERE slot_name = '%s'", name))
	}

	version, err := strconv.Atoi(pgScalar(t, ctx, postgresConn, "SHOW server_version_num"))
	require.NoError(t, err)

	if version < 170000 {
		_, err := newSlot(freshSlot, true).Create(ctx)
		require.ErrorContains(t, err, "slot.failover requires PostgreSQL 17")
		return
	}

	// Existing slot without failover → enabled by ALTER_REPLICATION_SLOT on the next Create.
	dropSlot(existingSlot)
	defer dropSlot(existingSlot)
	_, err = newSlot(existingSlot, false).Create(ctx)
	require.NoError(t, err)
	require.Equal(t, "f", failoverFlag(existingSlot))
	_, err = newSlot(existingSlot, true).Create(ctx)
	require.NoError(t, err)
	require.Equal(t, "t", failoverFlag(existingSlot))
	// Idempotent: already enabled, nothing to alter.
	_, err = newSlot(existingSlot, true).Create(ctx)
	require.NoError(t, err)

	// Fresh slot created with FAILOVER true.
	dropSlot(freshSlot)
	defer dropSlot(freshSlot)
	_, err = newSlot(freshSlot, true).Create(ctx)
	require.NoError(t, err)
	require.Equal(t, "t", failoverFlag(freshSlot))
}

func pgScalar(t *testing.T, ctx context.Context, conn pq.Connection, sql string) string {
	t.Helper()
	results, err := conn.Exec(ctx, sql).ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)
	return string(results[0].Rows[0][0])
}
