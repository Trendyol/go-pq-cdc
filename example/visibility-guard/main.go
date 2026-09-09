package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5"
)

// The handler plays a downstream service: for every insert event it opens a
// fresh snapshot on the primary and checks whether the row is there yet.
// VISIBLE is what you expect; MISS is the read-after-write window. See README.md.
func main() {
	guard := flag.Bool("guard", false, "enable visibilityGuard")
	failMode := flag.String("fail-mode", "closed", "visibilityGuard.failMode: closed | open")
	timeout := flag.Duration("timeout", 10*time.Second, "visibilityGuard.timeout (must be < wal_sender_timeout/2)")
	failover := flag.Bool("failover", false, "slot.failover (PostgreSQL 17+)")
	flag.Parse()

	ctx := context.Background()
	cfg := config.Config{
		Host:     "127.0.0.1",
		Port:     5436,
		Username: "cdc_user",
		Password: "cdc_pass",
		Database: "cdc_db",
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations:        publication.Operations{publication.OperationInsert},
			Tables: publication.Tables{
				{Name: "orders", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
			Failover:                    *failover,
		},
		VisibilityGuard: config.VisibilityGuardConfig{
			Enabled:  *guard,
			FailMode: config.VisibilityFailMode(*failMode),
			Timeout:  *timeout,
		},
		Metric: config.MetricConfig{Port: 8081},
		Logger: config.LoggerConfig{LogLevel: slog.LevelInfo},
	}

	// Separate autocommit connection: each query takes a fresh snapshot,
	// exactly what a service reading the primary right after the event would see.
	reader, err := pgx.Connect(ctx, cfg.DSNWithoutSSL())
	if err != nil {
		slog.Error("reader connect", "error", err)
		os.Exit(1)
	}
	defer reader.Close(ctx)

	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			id := ins.Decoded["id"]
			var visible bool
			var walNow string
			err := reader.QueryRow(lCtx.Context,
				"SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1), pg_current_wal_lsn()::text", id,
			).Scan(&visible, &walNow)
			switch {
			case err != nil:
				slog.Error("read after event", "id", id, "error", err)
			case visible:
				slog.Info("VISIBLE", "id", id, "note", ins.Decoded["note"], "commitLSN", lCtx.CommitLSN.String(), "walNow", walNow)
			default:
				slog.Warn("MISS: event received but row not visible yet", "id", id, "note", ins.Decoded["note"], "commitLSN", lCtx.CommitLSN.String(), "walNow", walNow)
			}
		}
		if err := lCtx.Ack(); err != nil {
			slog.Error("ack", "error", err)
		}
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}
	defer connector.Close()
	connector.Start(ctx)
}
