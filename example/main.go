package main

import (
	"context"
	dcp "gitlab.trendyol.com/pq-dcp"
	"gitlab.trendyol.com/pq-dcp/message/format"
	"log/slog"
	"os"
)

func main() {
	ctx := context.Background()
	cfg := dcp.Config{
		Host:     "127.0.0.1",
		Username: "dcp_user",
		Password: "dcp_pass",
		Database: "dcp_db",
		Publication: dcp.PublicationConfig{
			Name: "dcp_publication",
			// Create:       true,
			// DropIfExists: true,
			ScopeTables: nil,
			All:         true,
		},
		Slot: dcp.SlotConfig{
			Name: "dcp_slot_5",
			// Create: true,
		},
	}

	connector, err := dcp.NewConnector(ctx, cfg)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	ch, err := connector.Start(ctx)
	if err != nil {
		slog.Error("connector start", "error", err)
		os.Exit(1)
	}

	for {
		event, ok := <-ch
		if !ok {
			slog.Info("DONE !")
			break
		}

		switch msg := event.Message.(type) {
		case *format.Insert:
			slog.Info("insert message received", "new", msg.Decoded)
		case *format.Delete:
			slog.Info("delete message received", "old", msg.OldDecoded)
		case *format.Update:
			slog.Info("update message received", "new", msg.NewDecoded, "old", msg.OldDecoded)
		}

		if err = event.Ack(); err != nil {
			slog.Error("ack", "error", err)
		}
	}
}

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
	FROM generate_series(1, 1000000) AS i;
*/
