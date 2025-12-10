package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

/*
Simulation guide (low-traffic DB + high-traffic other DB)
=========================================================

This example spins up a Postgres instance with two databases:
  - cdc_db   : the database used by the CDC connector (low traffic)
  - high_db  : a second database used only to generate WAL (high traffic)

Step 1: Start Postgres
----------------------

	cd example/simple-with-heartbeat
	docker compose up -d

Step 2: Run the connector (first WITHOUT heartbeat)
---------------------------------------------------

	# In this file, Heartbeat.Enabled is initially set to false.
	go run .

Step 3: Generate WAL in high_db (different database)
----------------------------------------------------

	# New terminal:
	psql "postgres://cdc_user:cdc_pass@127.0.0.1:5433/high_db"

	DO $$
	BEGIN
	  FOR i IN 1..50000 LOOP
	    INSERT INTO public.hightraffic(value) VALUES (md5(random()::text));
	  END LOOP;
	END;
	$$;

Step 4: Observe slot vs global WAL on cdc_db
--------------------------------------------

	psql "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db"

	SELECT slot_name, restart_lsn, confirmed_flush_lsn
	FROM pg_replication_slots
	WHERE slot_name = 'cdc_slot';

	SELECT pg_current_wal_lsn();

Because cdc_db itself is almost idle and heartbeat is disabled:
  - pg_current_wal_lsn() will move forward due to high_db traffic
  - restart_lsn / confirmed_flush_lsn for cdc_slot may lag behind

Step 5: Enable heartbeat and rerun
----------------------------------

  - Stop the Go process.
  - Set Heartbeat.Enabled = true below.
  - go run .

Now, even if cdc_db has low application traffic, the heartbeat will insert
into public.test_heartbeat_table inside cdc_db at a fixed interval, producing
commits that advance confirmed_flush_lsn and restart_lsn.
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
				{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
				},
				{
					Name:            "test_heartbeat_table",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Heartbeat: config.HeartbeatConfig{
			// For the first run of the simulation, leave this as false.
			// Then set to true and rerun to see the effect of heartbeat.
			Enabled:  false,
			Query:    `INSERT INTO public.test_heartbeat_table(txt) VALUES ('hb')`,
			Interval: 5 * time.Second,
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
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
	case *format.Update:
		slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
