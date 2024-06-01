package main

import (
	"context"
	"github.com/3n0ugh/dcpg"
	"github.com/3n0ugh/dcpg/config"
	"github.com/3n0ugh/dcpg/pq"
	"github.com/3n0ugh/dcpg/pq/message/format"
	"log/slog"
	"os"
)

/*
	psql "postgres://dcp_user:dcp_pass@127.0.0.1/dcp_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;
*/

func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:      "127.0.0.1",
		Username:  "dcp_user",
		Password:  "dcp_pass",
		Database:  "dcp_db",
		DebugMode: false,
		Publication: config.PublicationConfig{
			Name:         "dcp_publication",
			Create:       true,
			DropIfExists: false,
		},
		Slot: config.SlotConfig{
			Name:   "dcp_slot",
			Create: true,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
	}

	connector, err := dcpg.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	connector.Start(ctx)
}

func Handler(ctx pq.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("insert message received", "new", msg.Decoded)
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
	case *format.Update:
		slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
