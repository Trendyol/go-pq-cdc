// Package main demonstrates CDC with REPLICA IDENTITY USING INDEX tables.
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
	cd example/replica-identity-using-index

	docker compose up -d
	go run .

	psql "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db"

	INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');
	UPDATE users SET name = 'Alice Updated' WHERE email = 'alice@example.com';
	DELETE FROM users WHERE email = 'alice@example.com';
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
				publication.OperationUpdate,
				publication.OperationDelete,
			},
			Tables: publication.Tables{
				{
					Name:                 "users",
					Schema:               "public",
					ReplicaIdentity:      publication.ReplicaIdentityUsingIndex,
					ReplicaIdentityIndex: "users_email_unique_idx",
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

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("insert message received", "new", msg.Decoded)
	case *format.Update:
		slog.Info(
			"update message received",
			"oldTupleType", string(rune(msg.OldTupleType)),
			"new", msg.NewDecoded,
			"old", msg.OldDecoded,
		)
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
