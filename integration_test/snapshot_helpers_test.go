package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

// Helper functions for snapshot tests

func createTestTable(ctx context.Context, conn pq.Connection, tableName string) error {
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			age INT NOT NULL
		);
	`, tableName, tableName)
	return pgExec(ctx, conn, query)
}

func createTextPrimaryKeyTable(ctx context.Context, conn pq.Connection, tableName string) error {
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			payload TEXT NOT NULL
		);
	`, tableName, tableName)
	return pgExec(ctx, conn, query)
}

func execQuery(ctx context.Context, conn pq.Connection, query string) ([]*pgconn.Result, error) {
	resultReader := conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, err
	}
	if err = resultReader.Close(); err != nil {
		return nil, err
	}
	return results, nil
}

func cleanupSnapshotTest(t *testing.T, ctx context.Context, tableName string, slotName string, publicationName string) {
	// Open a fresh connection for cleanup
	conn, err := newPostgresConn()
	if err != nil {
		t.Logf("Warning: Failed to create cleanup connection: %v", err)
		return
	}
	defer conn.Close(ctx)

	// Drop test table
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if err := pgExec(ctx, conn, query); err != nil {
		t.Logf("Warning: Failed to drop table: %v", err)
	}

	// Clean metadata tables (if they exist)
	_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotName))
	_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName))

	// Drop publication and slot
	_ = pgExec(ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	_ = pgExec(ctx, conn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", slotName, slotName))

	t.Log("✅ Cleanup completed")
}

// Metric fetching helpers

func fetchSnapshotInProgressMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_in_progress")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}

func fetchSnapshotTotalRowsMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_total_rows")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}

func fetchSnapshotTotalChunksMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_total_chunks")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}

func fetchSnapshotCompletedChunksMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_completed_chunks")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}

func fetchSnapshotTotalTablesMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_total_tables")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}
