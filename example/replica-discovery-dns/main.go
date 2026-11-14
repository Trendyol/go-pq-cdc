package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5"
)

// The handler plays a service that reads a standby after every insert event.
// For each row it reports how long the event was held after the row was
// inserted and whether the row is readable, right now, on mars and on venus:
// the two standbys of docker-compose.yml. A standby the connector gates is
// always readable; one it does not gate may not be. See README.md.
func main() {
	configFile := flag.String("config", "/config/connector-both.yml", "connector YAML config")
	flag.Parse()

	ctx := context.Background()
	standbys := []struct{ name, dsn string }{
		{"mars", "postgres://cdc_user:cdc_pass@10.99.0.11:5432/cdc_db"},
		{"venus", "postgres://cdc_user:cdc_pass@10.99.0.12:5432/cdc_db"},
	}

	handler := func(lCtx *replication.ListenerContext) {
		defer func() {
			if err := lCtx.Ack(); err != nil {
				slog.Error("ack", "error", err)
			}
		}()
		ins, ok := lCtx.Message.(*format.Insert)
		if !ok {
			return
		}
		id := ins.Decoded["id"]
		args := []any{"id", id}
		if created, ok := ins.Decoded["created_at"].(time.Time); ok {
			args = append(args, "held_ms", time.Since(created).Milliseconds())
		}
		for _, s := range standbys {
			args = append(args, s.name, visible(lCtx.Context, s.dsn, id))
		}
		slog.Info("event", args...)
	}

	connector, err := cdc.NewConnectorWithConfigFile(ctx, *configFile, handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}
	defer connector.Close()
	connector.Start(ctx)
}

// visible reads the row on one standby over a fresh connection, in autocommit.
func visible(ctx context.Context, dsn string, id any) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		slog.Error("connect standby", "error", err)
		return false
	}
	defer conn.Close(ctx)
	var found bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)", id).Scan(&found); err != nil {
		slog.Error("read standby", "error", err)
		return false
	}
	return found
}
