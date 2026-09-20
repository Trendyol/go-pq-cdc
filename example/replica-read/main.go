package main

import (
	"context"
	"flag"
	"fmt"
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

// The handler plays a service that reads a standby after every insert event.
// At event time it reports what the loose (>=) and strict (>) replay-LSN checks
// say and whether the row is there. Then it waits for the strict check on one
// connection and reads on another, the way a pool would. See README.md.
func main() {
	checkPort := flag.Int("check-port", 5439, "standby answering the replay-LSN check (5438 standby1, 5439 standby2)")
	readPort := flag.Int("read-port", 5439, "standby serving the row read")
	guard := flag.Bool("guard", true, "visibilityGuard on the primary")
	flag.Parse()

	ctx := context.Background()
	cfg := config.Config{
		Host:     "127.0.0.1",
		Port:     5437,
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
		},
		VisibilityGuard: config.VisibilityGuardConfig{Enabled: *guard},
		Metric:          config.MetricConfig{Port: 8082},
		Logger:          config.LoggerConfig{LogLevel: slog.LevelInfo},
	}

	check := connect(ctx, *checkPort)
	read := connect(ctx, *readPort)

	const exists = "SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)"
	handler := func(lCtx *replication.ListenerContext) {
		defer func() { must(lCtx.Ack(), "ack") }()
		ins, ok := lCtx.Message.(*format.Insert)
		if !ok {
			return
		}
		id, lsn := ins.Decoded["id"], lCtx.CommitLSN.String()

		var replay string
		var loose, strict, visible bool
		must(check.QueryRow(lCtx.Context,
			"SELECT r::text, r >= $1::pg_lsn, r > $1::pg_lsn FROM pg_last_wal_replay_lsn() r", lsn,
		).Scan(&replay, &loose, &strict), "check")
		must(read.QueryRow(lCtx.Context, exists, id).Scan(&visible), "read")
		slog.Info("at event", "id", id, "commitLSN", lsn, "replay", replay, "loose(>=)", loose, "strict(>)", strict, "visible", visible)

		start := time.Now()
		for !strict {
			time.Sleep(50 * time.Millisecond)
			must(check.QueryRow(lCtx.Context, "SELECT pg_last_wal_replay_lsn() > $1::pg_lsn", lsn).Scan(&strict), "check")
		}
		must(read.QueryRow(lCtx.Context, exists, id).Scan(&visible), "read")
		if visible {
			slog.Info("VISIBLE after strict check", "id", id, "waited", time.Since(start).Round(time.Millisecond))
		} else {
			slog.Warn("NOT FOUND after strict check passed: check and read hit different standbys", "id", id, "checkPort", *checkPort, "readPort", *readPort)
		}
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	must(err, "new connector")
	defer connector.Close()
	connector.Start(ctx)
}

// Autocommit, READ COMMITTED: each statement takes its snapshot after the
// previous statement returned, which is what the strict rule relies on.
func connect(ctx context.Context, port int) *pgx.Conn {
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://cdc_user:cdc_pass@127.0.0.1:%d/cdc_db", port))
	must(err, "connect")
	return conn
}

func must(err error, what string) {
	if err != nil {
		slog.Error(what, "error", err)
		os.Exit(1)
	}
}
