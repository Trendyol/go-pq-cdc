package online_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// This opt-in catalog gate requires three caller-owned disposable clusters:
// it replaces cdc_online and temporarily changes cluster WAL retention. The
// qualification owner supplies all three DSNs; an incomplete matrix is an error.
func TestOnlineCatalogAdmissionMajors(t *testing.T) {
	if os.Getenv("E02_ADMISSION_PG15_DSN") == "" && os.Getenv("E02_ADMISSION_PG16_DSN") == "" && os.Getenv("E02_ADMISSION_PG17_DSN") == "" {
		t.Skip("live catalog gate requires E02_ADMISSION_PG15_DSN, E02_ADMISSION_PG16_DSN, E02_ADMISSION_PG17_DSN")
	}
	for _, major := range []int{15, 16, 17} {
		t.Run(strconv.Itoa(major), func(t *testing.T) {
			dsn := os.Getenv(fmt.Sprintf("E02_ADMISSION_PG%d_DSN", major))
			if dsn == "" {
				t.Fatal("incomplete live catalog admission matrix")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			var version int
			var server, fsync, synchronous, wal string
			if err = conn.QueryRow(ctx, `SELECT current_setting('server_version_num')::int,version(),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('wal_level')`).Scan(&version, &server, &fsync, &synchronous, &wal); err != nil {
				t.Fatal(err)
			}
			if version/10000 != major {
				t.Fatalf("expected PG%d, got %d", major, version)
			}
			t.Logf("catalog admission gate: server_version_num=%d server=%s effective_online_protocol=1 fsync=%s synchronous_commit=%s wal_level=%s", version, server, fsync, synchronous, wal)
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := conn.Exec(ctx, sql, args...); err != nil {
					t.Fatal(err)
				}
			}
			exec(`DROP PUBLICATION IF EXISTS e02_admission; DROP TABLE IF EXISTS public.e02_items,public.e02_generated,public.e02_unlogged; DROP SCHEMA IF EXISTS cdc_online CASCADE`)
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				c, err := pgx.Connect(cleanup, dsn)
				if err == nil {
					defer c.Close(cleanup)
					_, _ = c.Exec(cleanup, `DROP PUBLICATION IF EXISTS e02_admission; DROP TABLE IF EXISTS public.e02_items,public.e02_generated,public.e02_unlogged; DROP SCHEMA IF EXISTS cdc_online CASCADE`)
				}
			})
			if err = online.Provision(ctx, conn, "postgres"); err != nil {
				t.Fatal(err)
			}
			if err = online.Provision(ctx, conn, "postgres"); err != nil {
				t.Fatalf("idempotent provisioning: %v", err)
			}
			if err = online.ValidateProvisioned(ctx, conn); err != nil {
				t.Fatal(err)
			}
			exec(`CREATE TABLE public.e02_items(id bigint PRIMARY KEY, value text); ALTER TABLE public.e02_items REPLICA IDENTITY FULL; CREATE PUBLICATION e02_admission FOR TABLE public.e02_items WITH (publish='insert,update,delete,truncate')`)
			if err = online.EnsureWatermarkPublication(ctx, conn, "e02_admission", true); err != nil {
				t.Fatal(err)
			}
			if schemas, err := online.ValidatePublication(ctx, conn, "e02_admission"); err != nil || len(schemas) != 1 {
				t.Fatalf("publication admission: schemas=%v err=%v", schemas, err)
			}
			for _, generated := range []string{"STORED"} {
				exec(`CREATE TABLE public.e02_generated(id bigint PRIMARY KEY, value bigint GENERATED ALWAYS AS (id+1) ` + generated + `); ALTER TABLE public.e02_generated REPLICA IDENTITY FULL; ALTER PUBLICATION e02_admission ADD TABLE public.e02_generated`)
				if _, err = online.ValidatePublication(ctx, conn, "e02_admission"); !errors.Is(err, online.ErrSchema) {
					t.Fatalf("PG%d %s generated column admitted: %v", major, generated, err)
				}
				t.Logf("PG%d %s generated column rejected", major, generated)
				exec(`DROP TABLE public.e02_generated`)
			}
			// Locked limitation: business UNLOGGED admission is deliberately intact.
			exec(`CREATE UNLOGGED TABLE public.e02_unlogged(id bigint PRIMARY KEY, value text); ALTER TABLE public.e02_unlogged REPLICA IDENTITY FULL`)
			if _, err := online.InspectTable(ctx, conn, online.Table{Schema: "public", Name: "e02_unlogged"}); err != nil {
				t.Fatalf("locked UNLOGGED admission changed: %v", err)
			}
			t.Log("locked limitations: UNLOGGED business tables admitted; reader output settings unpinned")
			// Bound retained WAL only in these disposable catalog-gate databases,
			// so checkpoint-driven slot loss can be asserted rather than mocked.
			var retention string
			if err = conn.QueryRow(ctx, `SHOW max_slot_wal_keep_size`).Scan(&retention); err != nil {
				t.Fatal(err)
			}
			exec(`ALTER SYSTEM SET max_slot_wal_keep_size='1MB'`)
			exec(`SELECT pg_reload_conf()`)
			defer func() {
				exec(`ALTER SYSTEM SET max_slot_wal_keep_size=` + "'" + retention + "'")
				exec(`SELECT pg_reload_conf()`)
			}()
			for _, mode := range []string{"fresh", "two_phase", "failover", "physical", "active", "lost"} {
				if mode == "failover" && major < 17 {
					continue
				}
				t.Run(mode, func(t *testing.T) {
					exec := func(sql string, args ...any) {
						t.Helper()
						if _, err := conn.Exec(ctx, sql, args...); err != nil {
							t.Fatal(err)
						}
					}
					const name = "e02_admission_slot"
					if slot, err := online.InspectSlot(ctx, conn, name); err != nil || slot.Exists {
						t.Fatalf("missing slot: %+v %v", slot, err)
					}
					switch mode {
					case "fresh", "active", "lost":
						if lsn, err := online.CreateFreshSlot(ctx, conn, name, true); err != nil || lsn == 0 {
							t.Fatalf("fresh slot: %v %v", lsn, err)
						}
					case "two_phase":
						exec(`SELECT pg_create_logical_replication_slot($1,'pgoutput',false,true)`, name)
					case "failover":
						exec(`SELECT pg_create_logical_replication_slot($1,'pgoutput',false,false,true)`, name)
					case "physical":
						exec(`SELECT pg_create_physical_replication_slot($1)`, name)
					}
					defer exec(`SELECT pg_drop_replication_slot($1)`, name)
					if mode == "active" {
						holder, err := pgconn.Connect(ctx, dsn+"&replication=database")
						if err != nil {
							t.Fatal(err)
						}
						defer holder.Close(context.Background())
						holder.Frontend().Send(&pgproto3.Query{String: `START_REPLICATION SLOT e02_admission_slot LOGICAL 0/0 (proto_version '1', publication_names 'e02_admission')`})
						if err = holder.Frontend().Flush(); err != nil {
							t.Fatal(err)
						}
						message, err := holder.ReceiveMessage(ctx)
						if _, ok := message.(*pgproto3.CopyBothResponse); err != nil || !ok {
							t.Fatalf("protocol-1 slot activation: %T %v", message, err)
						}
					}
					if mode == "lost" {
						exec(`SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT`)
						var status string
						if err := conn.QueryRow(ctx, `SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&status); err != nil || status != "lost" {
							t.Fatalf("slot-loss negative control not achieved: status=%s err=%v", status, err)
						}
						if major >= 17 {
							var reason string
							if err := conn.QueryRow(ctx, `SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&reason); err != nil || reason != "wal_removed" {
								t.Fatalf("slot invalidation reason=%s err=%v", reason, err)
							}
							t.Logf("PG%d invalidation_reason=%s", major, reason)
						}
					}
					slot, err := online.InspectSlot(ctx, conn, name)
					if err != nil || !slot.Exists || slot.Invalid != (mode != "fresh") {
						t.Fatalf("%s slot admission: %+v %v", mode, slot, err)
					}
					if _, err = online.CreateFreshSlot(ctx, conn, name, true); !errors.Is(err, online.ErrBinding) {
						t.Fatalf("existing %s slot recreated/adopted: %v", mode, err)
					}
					t.Logf("PG%d slot=%s exists=%t invalid=%t effective_online_protocol=1", major, mode, slot.Exists, slot.Invalid)
				})
			}
		})
	}
}
