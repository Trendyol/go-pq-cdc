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

	INSERT INTO test_events (id, event_time, event_stuff, code)
	SELECT
		gen_random_uuid(),
		date_trunc('day', d.day) + (random() * interval '86399 seconds'),
		(ARRAY ['stuff','things','items','trinkets','money'])[floor(random() * 5 + 1)::int],
		substr(md5(random()::text), 1, 10)
	FROM generate_series(
				'2026-04-01'::date,
				'2026-04-10'::date,
				interval '1 day'
		) AS d(day),
     generate_series(1, 1000) AS r(n);

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
					Name:            "test_events",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
					// test_events is a partitioned table, so we need to set Partitioned to true to ensure that the publication is created with the root table name
					// If this isn't set (default false) cdc events will still be captured but the table name in the message will be the partition table name (e.g. test_events_1) instead of the root table name (test_events)
					// Sets publish_via_partition_root = true in the publication
					Partitioned: true,
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
		// Table name will be `test_events`, even though the table part `test_events_<partition>` is the one that receives the insert
		slog.Info("insert message received", "table", msg.TableName, "message", msg.Decoded)
	case *format.Delete:
		slog.Info("delete message received", "table", msg.TableName, "message", msg.OldDecoded)
	case *format.Update:
		slog.Info("update message received", "table", msg.TableName, "message", msg.NewDecoded)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
