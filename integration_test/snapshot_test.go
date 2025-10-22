package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotBasicSingleChunk tests the simplest snapshot scenario:
// - Single table with few rows (single chunk)
// - Verify BEGIN, DATA, END events
// - Verify metadata tables
// - Verify metrics
func TestSnapshotBasicSingleChunk(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with small dataset
	tableName := "snapshot_basic_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_basic"
	cdcCfg.Publication.Name = "pub_snapshot_basic"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100 // Large enough for single chunk

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert test data BEFORE snapshot
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	testData := []map[string]any{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 35},
		{"id": 4, "name": "Diana", "age": 40},
		{"id": 5, "name": "Eve", "age": 45},
	}

	for _, data := range testData {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, '%s', %d)",
			tableName, data["id"], data["name"], data["age"])
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	cdcInsertReceived := []map[string]any{}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	// Start connector with snapshot
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Collect snapshot events
	timeout := time.After(5 * time.Second)
	snapshotCompleted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			switch m := msg.(type) {
			case *format.Snapshot:
				switch m.EventType {
				case format.SnapshotEventTypeBegin:
					snapshotBeginReceived = true
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", m.LSN.String())
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
					t.Logf("📸 Snapshot DATA received: %v", m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s", m.LSN.String())
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot events")
		}
	}

	// Wait a bit for CDC to start
	time.Sleep(500 * time.Millisecond)

	// Now insert new data after snapshot (CDC phase)
	t.Log("📝 Inserting new data for CDC test...")
	newData := map[string]any{"id": 100, "name": "NewUser", "age": 50}
	query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, '%s', %d)",
		tableName, newData["id"], newData["name"], newData["age"])
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Collect CDC insert event
	select {
	case msg := <-messageCh:
		if insertMsg, ok := msg.(*format.Insert); ok {
			cdcInsertReceived = append(cdcInsertReceived, insertMsg.Decoded)
			t.Logf("🔄 CDC INSERT received: %v", insertMsg.Decoded)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for CDC insert event")
	}

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 5, "Should receive 5 DATA events from snapshot")

		// Verify data content
		for i, expected := range testData {
			assert.Equal(t, int32(expected["id"].(int)), snapshotDataReceived[i]["id"])
			assert.Equal(t, expected["name"], snapshotDataReceived[i]["name"])
			assert.Equal(t, int32(expected["age"].(int)), snapshotDataReceived[i]["age"])
		}
	})

	t.Run("Verify CDC Continuation", func(t *testing.T) {
		assert.Len(t, cdcInsertReceived, 1, "Should receive 1 CDC INSERT event")
		assert.Equal(t, int32(100), cdcInsertReceived[0]["id"])
		assert.Equal(t, "NewUser", cdcInsertReceived[0]["name"])
	})

	t.Run("Verify No Duplicates", func(t *testing.T) {
		// CDC should not receive data that was in snapshot
		for _, cdcData := range cdcInsertReceived {
			for _, snapshotData := range snapshotDataReceived {
				assert.NotEqual(t, cdcData["id"], snapshotData["id"],
					"CDC should not receive data that was in snapshot")
			}
		}
	})

	t.Run("Verify Snapshot Metadata", func(t *testing.T) {
		// Query snapshot job metadata
		query := fmt.Sprintf("SELECT completed, total_chunks, completed_chunks, snapshot_lsn FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be marked as completed")
		assert.Equal(t, "1", totalChunks, "Should have 1 chunk for small table")
		assert.Equal(t, "1", completedChunks, "Should have 1 completed chunk")

		t.Logf("✅ Metadata verified: completed=%t, total_chunks=%s, completed_chunks=%s",
			completed, totalChunks, completedChunks)
	})

	t.Run("Verify Chunk Metadata", func(t *testing.T) {
		query := fmt.Sprintf("SELECT status, rows_processed FROM cdc_snapshot_chunks WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.Len(t, results[0].Rows, 1, "Should have 1 chunk")

		row := results[0].Rows[0]
		status := string(row[0])
		rowsProcessed := string(row[1])

		assert.Equal(t, "completed", status, "Chunk should be completed")
		assert.Equal(t, "5", rowsProcessed, "Should have processed 5 rows")

		t.Logf("✅ Chunk verified: status=%s, rows_processed=%s", status, rowsProcessed)
	})

	t.Run("Verify Snapshot Metrics", func(t *testing.T) {
		// Check snapshot metrics
		inProgress, err := fetchSnapshotInProgressMetric()
		if err == nil {
			assert.Equal(t, 0, inProgress, "Snapshot should not be in progress")
		}

		totalRows, err := fetchSnapshotTotalRowsMetric()
		if err == nil {
			assert.Equal(t, 5, totalRows, "Should have processed 5 rows in snapshot")
		}

		t.Logf("✅ Metrics verified: in_progress=%d, total_rows=%d", inProgress, totalRows)
	})
}

// Helper functions

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

// TestSnapshotMultipleChunks tests chunk-based processing:
// - Large table split into multiple chunks
// - Verify all chunks are processed
// - Verify chunk metadata (4 chunks, all completed)
// - Verify total rows processed
func TestSnapshotMultipleChunks(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with larger dataset
	tableName := "snapshot_multi_chunk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_multi_chunk"
	cdcCfg.Publication.Name = "pub_snapshot_multi_chunk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25 // Small chunk size to create multiple chunks
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second
	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert 100 rows (should create 4 chunks)
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 100 rows for multi-chunk test...")
	for i := 1; i <= 100; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false

	messageCh := make(chan any, 200)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	// Start connector with snapshot
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Collect snapshot events
	timeout := time.After(30 * time.Second)
	snapshotCompleted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			switch m := msg.(type) {
			case *format.Snapshot:
				switch m.EventType {
				case format.SnapshotEventTypeBegin:
					snapshotBeginReceived = true
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", m.LSN.String())
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
					if len(snapshotDataReceived)%25 == 0 {
						t.Logf("📸 Snapshot progress: %d/100 rows received", len(snapshotDataReceived))
					}
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s", m.LSN.String())
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot events. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 100, "Should receive 100 DATA events from snapshot")
	})

	t.Run("Verify All Rows Captured", func(t *testing.T) {
		// Check that all IDs from 1 to 100 are present
		receivedIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			receivedIDs[id] = true
		}

		for i := int32(1); i <= 100; i++ {
			assert.True(t, receivedIDs[i], "ID %d should be in snapshot", i)
		}
		t.Logf("✅ All 100 unique IDs captured in snapshot")
	})

	t.Run("Verify Chunk Metadata - 4 Chunks", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT 
				COUNT(*) as total_chunks,
				COUNT(*) FILTER (WHERE status = 'completed') as completed_chunks,
				SUM(rows_processed) as total_rows
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		totalChunks := string(row[0])
		completedChunks := string(row[1])
		totalRows := string(row[2])

		assert.Equal(t, "4", totalChunks, "Should have 4 chunks (100 rows / 25 chunk size)")
		assert.Equal(t, "4", completedChunks, "All 4 chunks should be completed")
		assert.Equal(t, "100", totalRows, "Total rows processed should be 100")

		t.Logf("✅ Chunk metadata verified: total=%s, completed=%s, rows=%s",
			totalChunks, completedChunks, totalRows)
	})

	t.Run("Verify Individual Chunks", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT chunk_index, chunk_start, chunk_size, status, rows_processed
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			ORDER BY chunk_index
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.Len(t, results[0].Rows, 4, "Should have 4 chunk rows")

		expectedChunks := []struct {
			index         string
			start         string
			size          string
			rowsProcessed string
		}{
			{"0", "0", "25", "25"},
			{"1", "25", "25", "25"},
			{"2", "50", "25", "25"},
			{"3", "75", "25", "25"},
		}

		for i, expected := range expectedChunks {
			row := results[0].Rows[i]
			chunkIndex := string(row[0])
			chunkStart := string(row[1])
			chunkSize := string(row[2])
			status := string(row[3])
			rowsProcessed := string(row[4])

			assert.Equal(t, expected.index, chunkIndex, "Chunk %d index", i)
			assert.Equal(t, expected.start, chunkStart, "Chunk %d start", i)
			assert.Equal(t, expected.size, chunkSize, "Chunk %d size", i)
			assert.Equal(t, "completed", status, "Chunk %d status", i)
			assert.Equal(t, expected.rowsProcessed, rowsProcessed, "Chunk %d rows processed", i)

			t.Logf("✅ Chunk %d verified: start=%s, size=%s, rows=%s, status=%s",
				i, chunkStart, chunkSize, rowsProcessed, status)
		}
	})

	t.Run("Verify Job Metadata", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed, total_chunks, completed_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be marked as completed")
		assert.Equal(t, "4", totalChunks, "Job should have 4 total chunks")
		assert.Equal(t, "4", completedChunks, "Job should have 4 completed chunks")

		t.Logf("✅ Job metadata verified: completed=%t, total=%s, completed=%s",
			completed, totalChunks, completedChunks)
	})

	t.Run("Verify Snapshot Metrics", func(t *testing.T) {
		totalChunks, err := fetchSnapshotTotalChunksMetric()
		if err == nil {
			assert.Equal(t, 4, totalChunks, "Should have 4 total chunks in metrics")
		}

		completedChunks, err := fetchSnapshotCompletedChunksMetric()
		if err == nil {
			assert.Equal(t, 4, completedChunks, "Should have 4 completed chunks in metrics")
		}

		totalRows, err := fetchSnapshotTotalRowsMetric()
		if err == nil {
			assert.Equal(t, 100, totalRows, "Should have processed 100 rows in metrics")
		}

		t.Logf("✅ Metrics verified: total_chunks=%d, completed_chunks=%d, total_rows=%d",
			totalChunks, completedChunks, totalRows)
	})
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
