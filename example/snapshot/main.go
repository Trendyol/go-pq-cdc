package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

var (
	snapshotRowCount  atomic.Int64
	cdcInsertCount    atomic.Int64
	cdcUpdateCount    atomic.Int64
	cdcDeleteCount    atomic.Int64
	snapshotStartTime time.Time
)

func main() {
	ctx := context.Background()

	cfg := config.Config{
		Host:      "127.0.0.1",
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,

		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "snapshot_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				publication.Table{
					Name:            "products",
					ReplicaIdentity: publication.ReplicaIdentityFull,
					Schema:          "public",
				},
			},
		},

		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "snapshot_slot",
			SlotActivityCheckerInterval: 3000,
		},

		Snapshot: config.SnapshotConfig{
			Enabled:            true,
			Mode:               config.SnapshotModeInitial,
			BatchSize:          5000, // Read 5000 rows per batch
			CheckpointInterval: 5,    // Save checkpoint every 5 batches
			MaxRetries:         3,    // Retry 3 times on failure
			RetryDelay:         5 * time.Second,
			Timeout:            10 * time.Minute,
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
		slog.Error("failed to create connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()

	slog.Info("🚀 Starting CDC with snapshot...",
		"batchSize", cfg.Snapshot.BatchSize,
		"checkpointInterval", cfg.Snapshot.CheckpointInterval)

	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Snapshot:
		handleSnapshot(msg)

	case *format.Insert:
		cdcInsertCount.Add(1)
		slog.Info("📝 CDC INSERT",
			"table", msg.TableName,
			"data", msg.Decoded,
			"totalInserts", cdcInsertCount.Load())

	case *format.Update:
		cdcUpdateCount.Add(1)
		slog.Info("✏️  CDC UPDATE",
			"table", msg.TableName,
			"old", msg.OldDecoded,
			"new", msg.NewDecoded,
			"totalUpdates", cdcUpdateCount.Load())

	case *format.Delete:
		cdcDeleteCount.Add(1)
		slog.Info("🗑️  CDC DELETE",
			"table", msg.TableName,
			"old", msg.OldDecoded,
			"totalDeletes", cdcDeleteCount.Load())
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("failed to ack", "error", err)
	}
}

func handleSnapshot(msg *format.Snapshot) {
	switch msg.EventType {
	case format.SnapshotEventTypeBegin:
		snapshotStartTime = time.Now()
		snapshotRowCount.Store(0)
		slog.Info("📸 SNAPSHOT STARTED",
			"lsn", msg.LSN.String(),
			"time", msg.ServerTime.Format(time.RFC3339))

	case format.SnapshotEventTypeData:
		count := snapshotRowCount.Add(1)

		// Log every 10,000 rows
		if count%10000 == 0 {
			elapsed := time.Since(snapshotStartTime)
			rowsPerSec := float64(count) / elapsed.Seconds()
			slog.Info("📦 SNAPSHOT PROGRESS",
				"table", msg.Table,
				"rows", count,
				"elapsed", elapsed.Round(time.Second),
				"rowsPerSec", fmt.Sprintf("%.0f", rowsPerSec))
		}

		// Log sample data for first few rows
		if count <= 3 {
			slog.Info("📦 SNAPSHOT DATA",
				"table", msg.Table,
				"data", msg.Data,
				"isLast", msg.IsLast)
		}

	case format.SnapshotEventTypeEnd:
		elapsed := time.Since(snapshotStartTime)
		rowsPerSec := float64(msg.TotalRows) / elapsed.Seconds()
		slog.Info("✅ SNAPSHOT COMPLETED",
			"totalRows", msg.TotalRows,
			"duration", elapsed.Round(time.Second),
			"avgRowsPerSec", fmt.Sprintf("%.0f", rowsPerSec),
			"lsn", msg.LSN.String())

		printStatistics(msg.TotalRows, elapsed)
	}
}

func printStatistics(totalRows int64, duration time.Duration) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 SNAPSHOT STATISTICS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total Rows:        %d\n", totalRows)
	fmt.Printf("Duration:          %s\n", duration.Round(time.Second))
	fmt.Printf("Avg Rows/Second:   %.0f\n", float64(totalRows)/duration.Seconds())
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("🎯 Now listening for CDC events...")
	fmt.Println("   Try: INSERT, UPDATE, DELETE operations")
	fmt.Println(strings.Repeat("=", 60) + "\n")
}
