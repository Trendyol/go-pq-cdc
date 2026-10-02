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

	if err := verifySnapshot(host, port); err != nil {
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

func verifySnapshot(host string, port int) error {
	db, err := openDatabase(host, port)
	if err != nil {
		return err
	}
	defer db.Close()

	var (
		totalChunks          int
		completedChunks      int
		jobCompleted         bool
		parentSize           int64
		partitionedRows      int64
		regularRows          int64
		partitionedChunks    int
		partitionedLeafs     int
		partitionedRootScan  int
		partitionedProcessed int64
		regularChunks        int
		regularPhysicalScan  int
		regularProcessed     int64
		filteredChunks       int
		filteredLeafs        int
		filteredRootScan     int
		filteredProcessed    int64
	)

	err = db.QueryRow(`
		SELECT total_chunks, completed_chunks, completed
		FROM cdc_snapshot_job
		WHERE slot_name = $1
	`, snapshotID).Scan(&totalChunks, &completedChunks, &jobCompleted)
	if err != nil {
		return err
	}

	if err := db.QueryRow(`SELECT pg_relation_size('public.partitioned_events'), (SELECT count(*) FROM public.partitioned_events), (SELECT count(*) FROM public.regular_events)`).Scan(&parentSize, &partitionedRows, &regularRows); err != nil {
		return err
	}

	if err := db.QueryRow(`
		SELECT COUNT(*),
		       COUNT(DISTINCT physical_table_schema || '.' || physical_table_name),
		       COUNT(*) FILTER (WHERE physical_table_name IS NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, partitionedTableName).Scan(&partitionedChunks, &partitionedLeafs, &partitionedRootScan, &partitionedProcessed); err != nil {
		return err
	}

	if err := db.QueryRow(`
		SELECT COUNT(*),
		       COUNT(DISTINCT physical_table_schema || '.' || physical_table_name),
		       COUNT(*) FILTER (WHERE physical_table_name IS NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, filteredPartitionedTableName).Scan(&filteredChunks, &filteredLeafs, &filteredRootScan, &filteredProcessed); err != nil {
		return err
	}

	if err := db.QueryRow(`
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE physical_table_name IS NOT NULL),
		       COALESCE(SUM(rows_processed), 0)
		FROM cdc_snapshot_chunks
		WHERE slot_name = $1 AND table_schema = 'public' AND table_name = $2
	`, snapshotID, regularTableName).Scan(&regularChunks, &regularPhysicalScan, &regularProcessed); err != nil {
		return err
	}

	slog.Info("snapshot metadata",
		"parent_relation_size", parentSize,
		"rows_visible_through_parent", partitionedRows,
		"configured_chunk_size", chunkSize,
		"total_chunks", totalChunks,
		"completed_chunks", completedChunks,
		"partitioned_chunks", partitionedChunks,
		"partitioned_leafs", partitionedLeafs,
		"partitioned_root_scans", partitionedRootScan,
		"partitioned_rows", partitionedProcessed,
		"regular_chunks", regularChunks,
		"regular_physical_scans", regularPhysicalScan,
		"regular_rows", regularProcessed,
		"filtered_partitioned_chunks", filteredChunks,
		"filtered_partitioned_leafs", filteredLeafs,
		"filtered_partitioned_root_scans", filteredRootScan,
		"filtered_partitioned_rows", filteredProcessed,
	)

	if parentSize != 0 || partitionedRows != partitionedExpectedRows || regularRows != regularExpectedRows {
		return fmt.Errorf("fixture mismatch: parent_size=%d partitioned_rows=%d regular_rows=%d", parentSize, partitionedRows, regularRows)
	}
	if !jobCompleted || completedChunks != totalChunks {
		return fmt.Errorf("snapshot did not complete: completed=%t chunks=%d/%d", jobCompleted, completedChunks, totalChunks)
	}
	if partitionedLeafs != 5 || partitionedRootScan != 0 {
		return fmt.Errorf("partition chunks are invalid: physical_tables=%d root_chunks=%d", partitionedLeafs, partitionedRootScan)
	}
	if regularChunks == 0 || regularPhysicalScan != 0 {
		return fmt.Errorf("regular table chunks are invalid: chunks=%d physical_chunks=%d", regularChunks, regularPhysicalScan)
	}
	if filteredChunks == 0 || filteredLeafs != 3 || filteredRootScan != 0 {
		return fmt.Errorf("filtered partition chunks are invalid: chunks=%d physical_tables=%d root_chunks=%d", filteredChunks, filteredLeafs, filteredRootScan)
	}
	if partitionedProcessed != partitionedExpectedRows || partitionedProcessed != partitionedReceivedRows.Load() {
		return fmt.Errorf("partitioned row mismatch: processed=%d received=%d", partitionedProcessed, partitionedReceivedRows.Load())
	}
	if regularProcessed != regularExpectedRows || regularProcessed != regularReceivedRows.Load() {
		return fmt.Errorf("regular row mismatch: processed=%d received=%d", regularProcessed, regularReceivedRows.Load())
	}
	if filteredProcessed != filteredPartitionedExpectedRows || filteredProcessed != filteredPartitionedReceivedRows.Load() {
		return fmt.Errorf("filtered partitioned row mismatch: processed=%d received=%d", filteredProcessed, filteredPartitionedReceivedRows.Load())
	}

	expectedPartitionRows := map[string]int64{
		"partitioned_events_2026_01": 100,
		"partitioned_events_2026_02": 10,
		"partitioned_events_2026_03": 0,
		"partitioned_events_2026_04": 500,
		"partitioned_events_2026_05": 300,
	}
	rows, err := db.Query(`
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

	slog.Info("partitioned, filtered partitioned, and regular tables completed together",
		"physical_tables", partitionedLeafs,
		"total_chunks", totalChunks,
		"partitioned_rows", partitionedProcessed,
		"filtered_partitioned_rows", filteredProcessed,
		"regular_rows", regularProcessed)
	return nil
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
