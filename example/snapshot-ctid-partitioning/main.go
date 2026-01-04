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

// This example demonstrates CTID block partitioning and explicit partition strategy configuration.
//
// CTID partitioning is automatically used when:
// 1. Table has non-integer primary key (string, UUID, composite)
// 2. Table has no primary key
//
// You can also EXPLICITLY specify the partition strategy using SnapshotPartitionStrategy:
// - "ctid_block": Force CTID block partitioning (useful for hash-based integer PKs)
// - "integer_range": Force integer range partitioning (default for sequential integer PKs)
// - "offset": Force OFFSET-based partitioning (slow, but always works)
// - "" (empty): Auto-detect based on PK type (default behavior)
//
// This is particularly useful when you have INTEGER PKs that are NOT sequential
// (e.g., hash-based, random, or imported from external systems), where range
// partitioning would create very uneven chunks.

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
				// Table with STRING PK - CTID will be auto-detected
				{
					Name:            "products",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
				// Table with STRING PK - CTID will be auto-detected
				{
					Name:            "orders",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
				// Table with INTEGER PK but using CTID explicitly
				// Useful when integer PK is hash-based (not sequential)
				{
					Name:                      "events",
					ReplicaIdentity:           publication.ReplicaIdentityFull,
					Schema:                    "public",
					SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
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
	slog.Info("Tables: products (STRING PK, auto CTID), orders (STRING PK, auto CTID), events (INT PK, forced CTID)")
	slog.Info("The 'events' table demonstrates explicit SnapshotPartitionStrategy override")

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
