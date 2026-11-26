package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

// Verify the actual connector wiring, including restart of a completed job.
// Schema SQL and startup identity validation remain active in both cases.
func TestOnlineVisibilityPolicyAllPhases(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("guard_%t", enabled), func(t *testing.T) {
			source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
			conn := source.Connect(t)
			onlineExec(t, conn, "CREATE EXTENSION pg_stat_statements")
			onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY, value bigint)")
			provisionOnline(t, conn, "items")
			cfg := onlineConfig(t, source, "items")
			cfg.VisibilityGuard.Enabled = enabled
			// A nonempty snapshot exercises LOW/HIGH consumption before tailing.
			onlineExec(t, conn, "INSERT INTO items VALUES(100,42)")
			for _, resumed := range []bool{false, true} {
				var delivered atomic.Int64
				var reads atomic.Int64
				r := startOnline(t, cfg, func(_ context.Context, e online.Event) error {
					if e.Table.Name == "items" {
						if e.Operation == online.Read {
							reads.Add(1)
						} else {
							delivered.Add(1)
						}
					}
					return nil
				})
				waitOnlineCapture(t, r)
				waitOnlineSnapshot(t, r)
				waitOnlineFence(t, conn)
				count := func() int64 {
					var n int64
					require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query LIKE '%pg_current_snapshot()%pg_catalog.pg_control_system()%'`).Scan(&n))
					return n
				}
				before := count()
				require.Positive(t, before, "startup identity/visibility query must run")
				if !enabled && !resumed {
					require.EqualValues(t, 1, before, "disabled guard only queries visibility at startup, not LOW/HIGH")
				}
				if enabled && !resumed {
					require.Greater(t, before, int64(1), "enabled guard checks LOW/HIGH transactions")
				}
				id := int64(1)
				if resumed {
					id = 2
				}
				onlineExec(t, conn, "INSERT INTO items VALUES($1,42)", id)
				require.Eventually(t, func() bool { return delivered.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
				waitOnlineFence(t, conn)
				after := count()
				if enabled {
					require.Greater(t, after, before, "enabled guard checks tail transactions")
				} else {
					require.Equal(t, before, after, "disabled guard performs no visibility SQL after completion")
				}
				if resumed {
					require.Zero(t, reads.Load(), "completed job restart must not snapshot existing rows")
				} else {
					require.EqualValues(t, 1, reads.Load(), "initial snapshot still delivers READs")
				}
				r.Cancel()
				require.ErrorIs(t, onlineResult(t, r), context.Canceled)
			}
		})
	}
}
