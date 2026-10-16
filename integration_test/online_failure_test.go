package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialFailureSafety(t *testing.T) {
	for _, fault := range []string{"handler", "bytes", "query", "owner", "missing-slot", "advanced-slot", "source-binding", "missing-metadata", "unbound-slot"} {
		t.Run(fault, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			conn := source.Connect(t)
			onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,repeat('x',1024)),(2,'two')")
			provisionOnline(t, conn, "items")
			cfg := onlineConfig(t, source, "items")
			if fault == "missing-metadata" {
				onlineExec(t, conn, "DROP SCHEMA cdc_online CASCADE")
				c, err := cdc.NewOnlineConnector(context.Background(), cfg, func(context.Context, online.Event) error { return nil })
				require.NoError(t, err)
				require.Error(t, c.Run(context.Background()))
				return
			}
			if fault == "unbound-slot" {
				onlineExec(t, conn, "SELECT pg_create_logical_replication_slot('online_slot','pgoutput')")
				c, err := cdc.NewOnlineConnector(context.Background(), cfg, func(context.Context, online.Event) error { return nil })
				require.NoError(t, err)
				require.ErrorIs(t, c.Run(context.Background()), online.ErrBinding)
				return
			}
			if fault == "bytes" {
				cfg.Snapshot.Online.ChunkBytes = 1
			}
			if fault == "query" {
				cfg.Snapshot.Online.QueryTimeout = 200 * time.Millisecond
				installHighBarrier(t, conn)
			}
			sinkError := errors.New("durable sink unavailable")
			entered := make(chan struct{}, 1)
			handler := func(ctx context.Context, e online.Event) error {
				if fault == "handler" {
					return sinkError
				}
				if fault == "owner" {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			r := startOnline(t, cfg, handler)
			waitOnlineCapture(t, r)
			if fault == "owner" {
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("handler did not start")
				}
				var pid uint32
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid IS NOT NULL LIMIT 1").Scan(&pid))
				onlineExec(t, conn, "SELECT pg_terminate_backend($1)", pid)
			}
			if fault == "handler" || fault == "bytes" || fault == "query" || fault == "owner" {
				err := onlineResult(t, r)
				require.Error(t, err)
				if fault == "handler" {
					require.ErrorIs(t, err, sinkError)
				}
				var done bool
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT complete FROM cdc_online.table_progress").Scan(&done))
				require.False(t, done)
				var unsafe bool
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
				require.False(t, unsafe)
				return
			}
			waitOnlineSnapshot(t, r)
			r.Cancel()
			require.ErrorIs(t, onlineResult(t, r), context.Canceled)
			onlineEventually(t, func() bool {
				var inactive bool
				err := conn.QueryRow(context.Background(), "SELECT NOT active FROM pg_replication_slots WHERE slot_name='online_slot'").Scan(&inactive)
				return err == nil && inactive
			}, 5*time.Second, 5*time.Millisecond, "source backend releases closed replication connection")
			switch fault {
			case "missing-slot":
				onlineExec(t, conn, "SELECT pg_drop_replication_slot('online_slot')")
			case "advanced-slot":
				onlineExec(t, conn, "SELECT pg_replication_slot_advance('online_slot',pg_current_wal_lsn())")
			case "source-binding":
				onlineExec(t, conn, "UPDATE cdc_online.jobs SET system_id='different'")
			}
			c, err := cdc.NewOnlineConnector(context.Background(), cfg, handler)
			require.NoError(t, err)
			err = c.Run(context.Background())
			require.ErrorIs(t, err, online.ErrBinding)
			if fault == "missing-slot" {
				var exists bool
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='online_slot')").Scan(&exists))
				require.False(t, exists, "slot silently recreated")
			}
		})
	}
}
