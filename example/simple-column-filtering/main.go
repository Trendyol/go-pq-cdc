package main

import (
	"context"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

/*
	psql "postgres://cdc_user:cdc_pass@127.0.0.1/cdc_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 secret_id text NOT NULL,
	 created_on timestamptz
	);

	INSERT INTO users (name, secret_id, created_on)
	SELECT
		'Oyleli' || i,
		'secret' || i,
		NOW()
	FROM generate_series(1, 100) AS i;

	-- secret_id will NOT be included in the change events because it's not included in the publication configuration
	-- see https://www.postgresql.org/docs/current/logical-replication-col-lists.html

*/

func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      5433,
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				publication.Table{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
					// Only the specified columns will be included in the change events. In this example, secret_id column will be excluded.
					Columns: []string{"id", "name", "created_on"},
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("insert message received", "new", msg.Decoded)
		slog.Info("secret_id value", "new", msg.Decoded["secret_id"])
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
		slog.Info("secret_id value", "old", msg.OldDecoded["secret_id"])
	case *format.Update:
		slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
		slog.Info("secret_id value", "new", msg.NewDecoded["secret_id"], "old", msg.OldDecoded["secret_id"])
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
