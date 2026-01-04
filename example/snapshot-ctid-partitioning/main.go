package main

import (
	"context"
	"log"
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

// This example demonstrates CTID block partitioning for tables with string primary keys.
// CTID partitioning is used when:
// 1. Table has non-integer primary key (string, UUID, composite)
// 2. Table has no primary key
//
// CTID uses PostgreSQL's physical block locations for efficient partitioning,
// avoiding slow OFFSET-based queries.

func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      5434,
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication_ctid",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				{
					Name:            "products",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
				{
					Name:            "orders",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot_ctid",
			SlotActivityCheckerInterval: 3000,
		},
		Snapshot: config.SnapshotConfig{
			Enabled:           true,
			Mode:              config.SnapshotModeInitial,
			ChunkSize:         1000, // Rows per chunk
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{
			Port: 8082,
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

	slog.Info("Starting CDC with CTID block partitioning example")
	slog.Info("This example uses tables with STRING primary keys")
	slog.Info("Snapshot will use CTID block partitioning instead of slow OFFSET-based queries")

	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("INSERT", "table", msg.TableName, "data", msg.Decoded)
	case *format.Delete:
		slog.Info("DELETE", "table", msg.TableName, "data", msg.OldDecoded)
	case *format.Update:
		slog.Info("UPDATE", "table", msg.TableName, "new", msg.NewDecoded, "old", msg.OldDecoded)
	case *format.Snapshot:
		handleSnapshot(msg)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}

func handleSnapshot(s *format.Snapshot) {
	switch s.EventType {
	case format.SnapshotEventTypeBegin:
		log.Printf("📸 SNAPSHOT BEGIN | LSN: %s | Time: %s",
			s.LSN.String(),
			s.ServerTime.Format("15:04:05"))

	case format.SnapshotEventTypeData:
		// Log every 100th row to avoid flooding
		slog.Debug("snapshot data", "table", s.Table, "data", s.Data)

	case format.SnapshotEventTypeEnd:
		log.Printf("📸 SNAPSHOT END | LSN: %s | Time: %s",
			s.LSN.String(),
			s.ServerTime.Format("15:04:05"))
		log.Println("✅ Snapshot completed using CTID block partitioning!")
	}
}
