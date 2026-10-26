package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	_ "github.com/lib/pq"
)

const (
	databaseName                    = "snapshot_example"
	snapshotID                      = "partitioned_snapshot_example"
	partitionedTableName            = "partitioned_events"
	filteredPartitionedTableName    = "filtered_partitioned_events"
	regularTableName                = "regular_events"
	partitionedExpectedRows         = int64(910)
	filteredPartitionedExpectedRows = int64(25)
	regularExpectedRows             = int64(75)
	chunkSize                       = int64(10)
	publicationName                 = "partitioned_snapshot_publication"
)

var (
	partitionedReceivedRows         atomic.Int64
	filteredPartitionedReceivedRows atomic.Int64
	regularReceivedRows             atomic.Int64
	snapshotCompleted               = make(chan struct{}, 1)
	cdcEvents                       = make(chan liveEvent, 32)
	handlerErrors                   = make(chan error, 1)
)

type liveEvent struct {
	operation string
	table     string
	id        string
}

type snapshotMetadata struct {
	parentSize           int64
	partitionedRows      int64
	regularRows          int64
	partitionedProcessed int64
	regularProcessed     int64
	filteredProcessed    int64
	totalChunks          int
	completedChunks      int
	partitionedChunks    int
	partitionedLeafs     int
	partitionedRootScan  int
	regularChunks        int
	regularPhysicalScan  int
	filteredChunks       int
	filteredLeafs        int
	filteredRootScan     int
	jobCompleted         bool
}

func main() {
	ctx := context.Background()
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := 55432
	if value := os.Getenv("POSTGRES_PORT"); value != "" {
		parsedPort, err := strconv.Atoi(value)
		if err != nil {
			fatal("parse POSTGRES_PORT", err)
		}
		port = parsedPort
	}

	tables := publication.Tables{
		{
			Schema:          "public",
			Name:            partitionedTableName,
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Partitioned:     true,
		},
		{
			Schema:          "public",
			Name:            regularTableName,
			ReplicaIdentity: publication.ReplicaIdentityDefault,
		},
		{
			Schema:          "public",
			Name:            filteredPartitionedTableName,
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Partitioned:     true,
			QueryCondition:  "status = 'active'",
		},
	}

	cfg := config.Config{
		Host:     host,
		Port:     port,
		Username: "postgres",
		Password: "postgres",
		Database: databaseName,
		Publication: publication.Config{
			Name:              publicationName,
			Operations:        publication.Operations{publication.OperationInsert, publication.OperationUpdate, publication.OperationDelete},
			Tables:            tables,
			CreateIfNotExists: true,
		},
		Slot: slot.Config{
			Name:                        snapshotID,
			SlotActivityCheckerInterval: time.Second,
			CreateIfNotExists:           true,
		},
		Snapshot: config.SnapshotConfig{
			Enabled:           true,
			Mode:              config.SnapshotModeInitial,
			Resnapshot:        true,
			Tables:            tables,
			ChunkSize:         chunkSize,
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Logger: config.LoggerConfig{LogLevel: slog.LevelInfo},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handleMessage)
	if err != nil {
		fatal("create connector", err)
	}
	go connector.Start(ctx)
	defer connector.Close()

	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := connector.WaitUntilReady(readyCtx); err != nil {
		fatal("wait for connector", err)
	}

	select {
	case <-snapshotCompleted:
	default:
		fatal("verify snapshot completion", fmt.Errorf("snapshot END event was not received"))
	}

	if err := verifySnapshot(ctx, host, port); err != nil {
		fatal("verify snapshot", err)
	}
	if err := simulateCDC(ctx, host, port); err != nil {
		fatal("verify CDC", err)
	}
}

func handleMessage(ctx *replication.ListenerContext) {
	switch message := ctx.Message.(type) {
	case *format.Snapshot:
		handleSnapshot(message)
	case *format.Insert:
		cdcEvents <- liveEvent{operation: "insert", table: message.TableName, id: fmt.Sprint(message.Decoded["event_id"])}
	case *format.Update:
		cdcEvents <- liveEvent{operation: "update", table: message.TableName, id: fmt.Sprint(message.NewDecoded["event_id"])}
	case *format.Delete:
		cdcEvents <- liveEvent{operation: "delete", table: message.TableName, id: fmt.Sprint(message.OldDecoded["event_id"])}
	}
	if err := ctx.Ack(); err != nil {
		select {
		case handlerErrors <- err:
		default:
		}
	}
}

func handleSnapshot(message *format.Snapshot) {
	if message.EventType == format.SnapshotEventTypeEnd {
		select {
		case snapshotCompleted <- struct{}{}:
		default:
		}
		return
	}
	if message.EventType != format.SnapshotEventTypeData {
		return
	}

	var count int64
	switch message.Table {
	case partitionedTableName:
		count = partitionedReceivedRows.Add(1)
	case regularTableName:
		count = regularReceivedRows.Add(1)
	case filteredPartitionedTableName:
		count = filteredPartitionedReceivedRows.Add(1)
	default:
		slog.Error("unexpected snapshot table", "table", message.Table)
		return
	}
	if count == 1 || count%25 == 0 {
		slog.Info("snapshot rows received", "table", message.Table, "count", count)
	}
}

func simulateCDC(ctx context.Context, host string, port int) error {
	db, err := openDatabase(host, port)
	if err != nil {
		return err
	}
	defer db.Close()

	steps := []struct {
		want  liveEvent
		query string
	}{
		{liveEvent{"insert", partitionedTableName, "2001"}, `INSERT INTO public.partitioned_events VALUES (2001, DATE '2026-03-15', '{"state":"inserted"}')`},
		{liveEvent{"update", partitionedTableName, "2001"}, `UPDATE public.partitioned_events SET payload = '{"state":"updated"}' WHERE event_id = 2001 AND created_date = DATE '2026-03-15'`},
		{liveEvent{"delete", partitionedTableName, "2001"}, `DELETE FROM public.partitioned_events WHERE event_id = 2001 AND created_date = DATE '2026-03-15'`},
		{liveEvent{"insert", regularTableName, "1001"}, `INSERT INTO public.regular_events VALUES (1001, '{"state":"inserted"}')`},
		{liveEvent{"update", regularTableName, "1001"}, `UPDATE public.regular_events SET payload = '{"state":"updated"}' WHERE event_id = 1001`},
		{liveEvent{"delete", regularTableName, "1001"}, `DELETE FROM public.regular_events WHERE event_id = 1001`},
		// QueryCondition applies to snapshot reads only; inactive live changes must still arrive.
		{liveEvent{"insert", filteredPartitionedTableName, "1001"}, `INSERT INTO public.filtered_partitioned_events VALUES (1001, DATE '2026-03-15', 'inactive', '{"state":"inserted"}')`},
		{liveEvent{"update", filteredPartitionedTableName, "1001"}, `UPDATE public.filtered_partitioned_events SET payload = '{"state":"updated"}' WHERE event_id = 1001 AND created_date = DATE '2026-03-15'`},
		{liveEvent{"delete", filteredPartitionedTableName, "1001"}, `DELETE FROM public.filtered_partitioned_events WHERE event_id = 1001 AND created_date = DATE '2026-03-15'`},
	}

	for _, step := range steps {
		if _, err := db.ExecContext(ctx, step.query); err != nil {
			return fmt.Errorf("execute %s on %s: %w", step.want.operation, step.want.table, err)
		}
		select {
		case got := <-cdcEvents:
			if got != step.want {
				return fmt.Errorf("unexpected CDC event: want=%+v got=%+v", step.want, got)
			}
			slog.Info("CDC event received", "operation", got.operation, "table", got.table, "event_id", got.id)
		case err := <-handlerErrors:
			return fmt.Errorf("ack CDC event: %w", err)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for %s on %s", step.want.operation, step.want.table)
		}
	}

	slog.Info("snapshot followed by CDC completed successfully", "cdc_events", len(steps))
	return nil
}

func openDatabase(host string, port int) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=postgres password=postgres dbname=%s sslmode=disable", host, port, databaseName)
	return sql.Open("postgres", dsn)
}

func verifySnapshot(ctx context.Context, host string, port int) error {
	db, err := openDatabase(host, port)
	if err != nil {
		return err
	}
	defer db.Close()

	var metadata snapshotMetadata

	err = db.QueryRowContext(ctx, `
		SELECT total_chunks, completed_chunks, completed
		FROM cdc_snapshot_job
		WHERE slot_name = $1
	`, snapshotID).Scan(&metadata.totalChunks, &metadata.completedChunks, &metadata.jobCompleted)
	if err != nil {
		return err
	}

	if err := db.QueryRowContext(ctx, `SELECT pg_relation_size('public.partitioned_events'), (SELECT count(*) FROM public.partitioned_events), (SELECT count(*) FROM public.regular_events)`).Scan(&metadata.parentSize, &metadata.partitionedRows, &metadata.regularRows); err != nil {
		return err
	}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(DISTINCT physical_table_schema || '.' || physical_table_name),
		       COUNT(*) FILTER (WHERE physical_table_name IS NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, partitionedTableName).Scan(&metadata.partitionedChunks, &metadata.partitionedLeafs, &metadata.partitionedRootScan, &metadata.partitionedProcessed); err != nil {
		return err
	}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(DISTINCT physical_table_schema || '.' || physical_table_name),
		       COUNT(*) FILTER (WHERE physical_table_name IS NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, filteredPartitionedTableName).Scan(&metadata.filteredChunks, &metadata.filteredLeafs, &metadata.filteredRootScan, &metadata.filteredProcessed); err != nil {
		return err
	}

	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE physical_table_name IS NOT NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, regularTableName).Scan(&metadata.regularChunks, &metadata.regularPhysicalScan, &metadata.regularProcessed); err != nil {
		return err
	}

	slog.Info("snapshot metadata",
		"parent_relation_size", metadata.parentSize,
		"rows_visible_through_parent", metadata.partitionedRows,
		"configured_chunk_size", chunkSize,
		"total_chunks", metadata.totalChunks,
		"completed_chunks", metadata.completedChunks,
		"partitioned_chunks", metadata.partitionedChunks,
		"partitioned_leafs", metadata.partitionedLeafs,
		"partitioned_root_scans", metadata.partitionedRootScan,
		"partitioned_rows", metadata.partitionedProcessed,
		"regular_chunks", metadata.regularChunks,
		"regular_physical_scans", metadata.regularPhysicalScan,
		"regular_rows", metadata.regularProcessed,
		"filtered_partitioned_chunks", metadata.filteredChunks,
		"filtered_partitioned_leafs", metadata.filteredLeafs,
		"filtered_partitioned_root_scans", metadata.filteredRootScan,
		"filtered_partitioned_rows", metadata.filteredProcessed,
	)

	if err := validateSnapshotMetadata(metadata); err != nil {
		return err
	}

	if err := verifyPartitionRows(ctx, db); err != nil {
		return err
	}

	slog.Info("partitioned, filtered partitioned, and regular tables completed together",
		"physical_tables", metadata.partitionedLeafs,
		"total_chunks", metadata.totalChunks,
		"partitioned_rows", metadata.partitionedProcessed,
		"filtered_partitioned_rows", metadata.filteredProcessed,
		"regular_rows", metadata.regularProcessed)
	return nil
}

func validateSnapshotMetadata(metadata snapshotMetadata) error {
	if metadata.parentSize != 0 || metadata.partitionedRows != partitionedExpectedRows || metadata.regularRows != regularExpectedRows {
		return fmt.Errorf("fixture mismatch: parent_size=%d partitioned_rows=%d regular_rows=%d", metadata.parentSize, metadata.partitionedRows, metadata.regularRows)
	}
	if !metadata.jobCompleted || metadata.completedChunks != metadata.totalChunks {
		return fmt.Errorf("snapshot did not complete: completed=%t chunks=%d/%d", metadata.jobCompleted, metadata.completedChunks, metadata.totalChunks)
	}
	if metadata.partitionedLeafs != 5 || metadata.partitionedRootScan != 0 {
		return fmt.Errorf("partition chunks are invalid: physical_tables=%d root_chunks=%d", metadata.partitionedLeafs, metadata.partitionedRootScan)
	}
	if metadata.regularChunks == 0 || metadata.regularPhysicalScan != 0 {
		return fmt.Errorf("regular table chunks are invalid: chunks=%d physical_chunks=%d", metadata.regularChunks, metadata.regularPhysicalScan)
	}
	if metadata.filteredChunks == 0 || metadata.filteredLeafs != 3 || metadata.filteredRootScan != 0 {
		return fmt.Errorf("filtered partition chunks are invalid: chunks=%d physical_tables=%d root_chunks=%d", metadata.filteredChunks, metadata.filteredLeafs, metadata.filteredRootScan)
	}
	if metadata.partitionedProcessed != partitionedExpectedRows || metadata.partitionedProcessed != partitionedReceivedRows.Load() {
		return fmt.Errorf("partitioned row mismatch: processed=%d received=%d", metadata.partitionedProcessed, partitionedReceivedRows.Load())
	}
	if metadata.regularProcessed != regularExpectedRows || metadata.regularProcessed != regularReceivedRows.Load() {
		return fmt.Errorf("regular row mismatch: processed=%d received=%d", metadata.regularProcessed, regularReceivedRows.Load())
	}
	if metadata.filteredProcessed != filteredPartitionedExpectedRows || metadata.filteredProcessed != filteredPartitionedReceivedRows.Load() {
		return fmt.Errorf("filtered partitioned row mismatch: processed=%d received=%d", metadata.filteredProcessed, filteredPartitionedReceivedRows.Load())
	}
	return nil
}

func verifyPartitionRows(ctx context.Context, db *sql.DB) error {
	expectedPartitionRows := map[string]int64{
		"partitioned_events_2026_01": 100,
		"partitioned_events_2026_02": 10,
		"partitioned_events_2026_03": 0,
		"partitioned_events_2026_04": 500,
		"partitioned_events_2026_05": 300,
	}
	rows, err := db.QueryContext(ctx, `
		SELECT physical_table_name, COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_name = $2
		GROUP BY physical_table_name
	`, snapshotID, partitionedTableName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			return err
		}
		expected, ok := expectedPartitionRows[name]
		if !ok || count != expected {
			return fmt.Errorf("unexpected partition row count: partition=%s expected=%d processed=%d", name, expected, count)
		}
		delete(expectedPartitionRows, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(expectedPartitionRows) != 0 {
		return fmt.Errorf("partitions missing from snapshot metadata: %v", expectedPartitionRows)
	}

	return nil
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
