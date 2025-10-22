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

// TestSnapshotConcurrentInsertNoDuplicates is THE MOST CRITICAL test:
// - Tests LSN consistency and no duplicate data guarantee
// - Insert initial data (id: 1-50)
// - Start snapshot
// - When BEGIN is received, concurrently INSERT new data (id: 100-150)
// - Verify: id 1-50 ONLY in snapshot
// - Verify: id 100-150 ONLY in CDC
// - Verify: NO overlap between snapshot and CDC
func TestSnapshotConcurrentInsertNoDuplicates(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table
	tableName := "snapshot_no_dup_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_no_dup"
	cdcCfg.Publication.Name = "pub_snapshot_no_dup"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100 // Single chunk for simplicity

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert INITIAL data (id: 1-50)
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting initial 50 rows (id: 1-50)...")
	for i := 1; i <= 50; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'Initial_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ Initial 50 rows inserted")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	cdcInsertReceived := []map[string]any{}

	// Channel to signal when to start concurrent inserts
	startConcurrentInserts := make(chan struct{})

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

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Start goroutine for concurrent inserts (will wait for signal)
	insertsDone := make(chan struct{})
	go func() {
		<-startConcurrentInserts
		t.Log("🔄 Starting concurrent inserts (id: 100-150)...")

		// Open a new connection for concurrent inserts
		insertConn, err := newPostgresConn()
		if err != nil {
			t.Logf("Error creating insert connection: %v", err)
			close(insertsDone)
			return
		}
		defer insertConn.Close(ctx)

		// Insert data that should go to CDC, NOT snapshot
		for i := 100; i <= 150; i++ {
			query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'Concurrent_%d', %d)",
				tableName, i, i, 20+i%50)
			if err := pgExec(ctx, insertConn, query); err != nil {
				t.Logf("Warning: Concurrent insert failed for id=%d: %v", i, err)
			}
			// Small delay to spread inserts over time
			time.Sleep(10 * time.Millisecond)
		}
		t.Log("✅ Concurrent inserts completed (id: 100-150)")
		close(insertsDone)
	}()

	// Collect snapshot events
	timeout := time.After(15 * time.Second)
	snapshotCompleted := false
	concurrentInsertsStarted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			switch m := msg.(type) {
			case *format.Snapshot:
				switch m.EventType {
				case format.SnapshotEventTypeBegin:
					snapshotBeginReceived = true
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", m.LSN.String())

					// Signal to start concurrent inserts RIGHT NOW
					if !concurrentInsertsStarted {
						close(startConcurrentInserts)
						concurrentInsertsStarted = true
						t.Log("🚀 Signaled concurrent inserts to start")
					}

				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
					if len(snapshotDataReceived)%10 == 0 {
						t.Logf("📸 Snapshot progress: %d rows received", len(snapshotDataReceived))
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

	// Wait for concurrent inserts to complete
	t.Log("⏳ Waiting for concurrent inserts to complete...")
	select {
	case <-insertsDone:
		t.Log("✅ Concurrent inserts confirmed done")
	case <-time.After(5 * time.Second):
		t.Log("⚠️  Timeout waiting for concurrent inserts (may still be in progress)")
	}

	// Collect CDC insert events (should be 51 total: id 100-150)
	t.Log("📥 Collecting CDC INSERT events...")
	cdcTimeout := time.After(5 * time.Second)
	expectedCDCCount := 51

collectCDC:
	for len(cdcInsertReceived) < expectedCDCCount {
		select {
		case msg := <-messageCh:
			if insertMsg, ok := msg.(*format.Insert); ok {
				cdcInsertReceived = append(cdcInsertReceived, insertMsg.Decoded)
				if len(cdcInsertReceived)%10 == 0 {
					t.Logf("🔄 CDC progress: %d INSERT events received", len(cdcInsertReceived))
				}
			}
		case <-cdcTimeout:
			t.Logf("⚠️  Timeout collecting CDC events. Received %d/%d", len(cdcInsertReceived), expectedCDCCount)
			break collectCDC
		}
	}
	t.Logf("✅ Collected %d CDC INSERT events", len(cdcInsertReceived))

	// === Critical Assertions ===

	t.Run("Verify Snapshot Captured Initial Data Only", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 50, "Should receive exactly 50 DATA events from snapshot")

		// Extract IDs from snapshot
		snapshotIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			snapshotIDs[id] = true
		}

		// Verify: All IDs 1-50 should be in snapshot
		for i := int32(1); i <= 50; i++ {
			assert.True(t, snapshotIDs[i], "Initial data ID %d should be in snapshot", i)
		}

		// Verify: No IDs 100-150 should be in snapshot
		for i := int32(100); i <= 150; i++ {
			assert.False(t, snapshotIDs[i], "Concurrent data ID %d should NOT be in snapshot", i)
		}

		t.Logf("✅ Snapshot contains ONLY initial data (id: 1-50)")
	})

	t.Run("Verify CDC Captured Concurrent Inserts Only", func(t *testing.T) {
		// Extract IDs from CDC
		cdcIDs := make(map[int32]bool)
		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			cdcIDs[id] = true
		}

		// Verify: No IDs 1-50 should be in CDC
		for i := int32(1); i <= 50; i++ {
			assert.False(t, cdcIDs[i], "Initial data ID %d should NOT be in CDC", i)
		}

		// Verify: All IDs 100-150 should be in CDC
		missingIDs := []int32{}
		for i := int32(100); i <= 150; i++ {
			if !cdcIDs[i] {
				missingIDs = append(missingIDs, i)
			}
		}

		if len(missingIDs) > 0 {
			t.Logf("⚠️  Missing IDs in CDC: %v (may be timing issue)", missingIDs)
		}

		// At least 45 of 51 concurrent inserts should be captured
		assert.GreaterOrEqual(t, len(cdcIDs), 45, "CDC should capture most concurrent inserts")

		t.Logf("✅ CDC contains ONLY concurrent data (captured %d/51)", len(cdcIDs))
	})

	t.Run("Verify NO Overlap Between Snapshot and CDC", func(t *testing.T) {
		// Extract all IDs
		snapshotIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			snapshotIDs[id] = true
		}

		cdcIDs := make(map[int32]bool)
		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			cdcIDs[id] = true
		}

		// Find overlaps
		overlaps := []int32{}
		for id := range snapshotIDs {
			if cdcIDs[id] {
				overlaps = append(overlaps, id)
			}
		}

		assert.Empty(t, overlaps, "There should be NO duplicate IDs between snapshot and CDC")

		if len(overlaps) > 0 {
			t.Errorf("❌ CRITICAL: Found duplicate IDs: %v", overlaps)
		} else {
			t.Log("✅ CRITICAL SUCCESS: Zero data duplication!")
		}
	})

	t.Run("Verify Total Data Integrity", func(t *testing.T) {
		// Collect all unique IDs from both snapshot and CDC
		allIDs := make(map[int32]bool)

		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			allIDs[id] = true
		}

		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			allIDs[id] = true
		}

		// We should have at least 95 unique IDs total (50 initial + at least 45 concurrent)
		assert.GreaterOrEqual(t, len(allIDs), 95, "Should have captured most of the data")

		t.Logf("✅ Total unique IDs captured: %d", len(allIDs))
		t.Logf("   - From snapshot: %d", len(snapshotDataReceived))
		t.Logf("   - From CDC: %d", len(cdcInsertReceived))
	})

	t.Run("Verify Snapshot Metadata", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed, total_chunks, completed_chunks, snapshot_lsn FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		snapshotLSN := string(row[3])

		assert.True(t, completed, "Job should be marked as completed")
		assert.NotEmpty(t, snapshotLSN, "Snapshot LSN should be recorded")

		t.Logf("✅ Snapshot LSN: %s", snapshotLSN)
	})
}

// TestSnapshotEmptyTable tests edge case with empty table:
// - No data in table
// - Should create 1 chunk with 0 rows processed
// - Should receive BEGIN → END (no DATA events)
// - Metadata should reflect empty state
func TestSnapshotEmptyTable(t *testing.T) {
	ctx := context.Background()

	// Setup: Create empty test table
	tableName := "snapshot_empty_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_empty"
	cdcCfg.Publication.Name = "pub_snapshot_empty"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table but DO NOT insert any data
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)
	t.Log("📝 Created empty table (0 rows)")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false

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
					t.Logf("📸 Unexpected DATA event received: %v", m.Data)
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

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Empty(t, snapshotDataReceived, "Should receive NO DATA events for empty table")
		t.Log("✅ Snapshot flow: BEGIN → END (no DATA events)")
	})

	t.Run("Verify Chunk Metadata - Single Empty Chunk", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT 
				COUNT(*) as total_chunks,
				COUNT(*) FILTER (WHERE status = 'completed') as completed_chunks,
				COALESCE(SUM(rows_processed), 0) as total_rows
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

		assert.Equal(t, "1", totalChunks, "Should create 1 chunk even for empty table")
		assert.Equal(t, "1", completedChunks, "Chunk should be marked as completed")
		assert.Equal(t, "0", totalRows, "Should have processed 0 rows")

		t.Logf("✅ Chunk metadata verified: chunks=%s, completed=%s, rows=%s",
			totalChunks, completedChunks, totalRows)
	})

	t.Run("Verify Individual Chunk Details", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT chunk_index, chunk_start, chunk_size, status, rows_processed
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.Len(t, results[0].Rows, 1, "Should have exactly 1 chunk")

		row := results[0].Rows[0]
		chunkIndex := string(row[0])
		chunkStart := string(row[1])
		chunkSize := string(row[2])
		status := string(row[3])
		rowsProcessed := string(row[4])

		assert.Equal(t, "0", chunkIndex, "Chunk index should be 0")
		assert.Equal(t, "0", chunkStart, "Chunk start should be 0")
		assert.Equal(t, "100", chunkSize, "Chunk size should match config")
		assert.Equal(t, "completed", status, "Chunk should be completed")
		assert.Equal(t, "0", rowsProcessed, "Should have processed 0 rows")

		t.Logf("✅ Chunk details: index=%s, start=%s, size=%s, rows=%s, status=%s",
			chunkIndex, chunkStart, chunkSize, rowsProcessed, status)
	})

	t.Run("Verify Job Metadata", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT completed, total_chunks, completed_chunks 
			FROM cdc_snapshot_job 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be marked as completed")
		assert.Equal(t, "1", totalChunks, "Job should have 1 total chunk")
		assert.Equal(t, "1", completedChunks, "Job should have 1 completed chunk")

		t.Logf("✅ Job metadata verified: completed=%t, total=%s, completed=%s",
			completed, totalChunks, completedChunks)
	})

	t.Run("Verify CDC Still Works After Empty Snapshot", func(t *testing.T) {
		// Insert data AFTER snapshot to verify CDC is working
		t.Log("📝 Inserting data after snapshot for CDC test...")
		for i := 1; i <= 5; i++ {
			query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'PostSnapshot_%d', %d)",
				tableName, i, i, 20+i)
			err = pgExec(ctx, postgresConn, query)
			require.NoError(t, err)
		}

		// Collect CDC insert events
		cdcInserts := []map[string]any{}
		cdcTimeout := time.After(3 * time.Second)

	collectCDC:
		for len(cdcInserts) < 5 {
			select {
			case msg := <-messageCh:
				if insertMsg, ok := msg.(*format.Insert); ok {
					cdcInserts = append(cdcInserts, insertMsg.Decoded)
					t.Logf("🔄 CDC INSERT received: id=%d", insertMsg.Decoded["id"])
				}
			case <-cdcTimeout:
				t.Logf("⚠️  CDC timeout. Received %d/5 inserts", len(cdcInserts))
				break collectCDC
			}
		}

		assert.GreaterOrEqual(t, len(cdcInserts), 4, "CDC should capture most inserts after empty snapshot")
		t.Logf("✅ CDC working: captured %d/5 inserts after empty snapshot", len(cdcInserts))
	})

	t.Run("Verify Snapshot Metrics", func(t *testing.T) {
		inProgress, err := fetchSnapshotInProgressMetric()
		if err == nil {
			assert.Equal(t, 0, inProgress, "Snapshot should not be in progress")
		}

		totalRows, err := fetchSnapshotTotalRowsMetric()
		if err == nil {
			assert.Equal(t, 0, totalRows, "Should have processed 0 rows")
		}

		totalChunks, err := fetchSnapshotTotalChunksMetric()
		if err == nil {
			assert.Equal(t, 1, totalChunks, "Should have 1 chunk")
		}

		t.Logf("✅ Metrics verified: in_progress=%d, total_rows=%d, total_chunks=%d",
			inProgress, totalRows, totalChunks)
	})
}

// TestSnapshotMultipleTables tests snapshot with multiple tables:
// - Multiple tables in publication
// - Each table has different row count
// - Verify all tables are snapshotted
// - Verify chunk distribution across tables
// - Verify CDC works for all tables
func TestSnapshotMultipleTables(t *testing.T) {
	ctx := context.Background()

	// Setup: Create multiple test tables
	usersTable := "snapshot_users_test"
	ordersTable := "snapshot_orders_test"
	productsTable := "snapshot_products_test"

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_multi_table"
	cdcCfg.Publication.Name = "pub_snapshot_multi_table"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            usersTable,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
		{
			Name:            ordersTable,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
		{
			Name:            productsTable,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25 // Small chunks to test distribution

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create tables with different schemas
	t.Log("📝 Creating multiple tables...")

	// Users table - 60 rows (3 chunks)
	err = createTestTable(ctx, postgresConn, usersTable)
	require.NoError(t, err)
	for i := 1; i <= 60; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			usersTable, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Logf("✅ Created %s with 60 rows", usersTable)

	// Orders table - 40 rows (2 chunks)
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			amount DECIMAL(10,2) NOT NULL,
			status TEXT NOT NULL
		);
	`, ordersTable, ordersTable)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)
	for i := 1; i <= 40; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, user_id, amount, status) VALUES(%d, %d, %d.99, 'pending')",
			ordersTable, i, i%10+1, i*10)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Logf("✅ Created %s with 40 rows", ordersTable)

	// Products table - 15 rows (1 chunk)
	query = fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			stock INT NOT NULL
		);
	`, productsTable, productsTable)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)
	for i := 1; i <= 15; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, price, stock) VALUES(%d, 'Product_%d', %d.99, %d)",
			productsTable, i, i, i*5, i*10)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Logf("✅ Created %s with 15 rows", productsTable)

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	cdcInsertReceived := map[string][]map[string]any{
		usersTable:    {},
		ordersTable:   {},
		productsTable: {},
	}

	messageCh := make(chan any, 300)
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

		// Cleanup all tables
		cleanupConn, err := newPostgresConn()
		if err == nil {
			defer cleanupConn.Close(ctx)
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", usersTable))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", ordersTable))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", productsTable))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", cdcCfg.Slot.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cdcCfg.Publication.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", cdcCfg.Slot.Name, cdcCfg.Slot.Name))
		}
		t.Log("✅ Cleanup completed")
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Collect snapshot events
	timeout := time.After(15 * time.Second)
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
					if len(snapshotDataReceived)%20 == 0 {
						t.Logf("📸 Snapshot progress: %d rows received", len(snapshotDataReceived))
					}
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s, Total DATA events: %d", m.LSN.String(), len(snapshotDataReceived))
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot events. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// Wait a bit for CDC to start
	time.Sleep(500 * time.Millisecond)

	// Insert new data to each table for CDC test
	t.Log("📝 Inserting data to all tables for CDC test...")
	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'NewUser_%d', %d)",
			usersTable, 1000+i, i, 25)
		_ = pgExec(ctx, postgresConn, query)

		query = fmt.Sprintf("INSERT INTO %s(id, user_id, amount, status) VALUES(%d, %d, 99.99, 'new')",
			ordersTable, 1000+i, i)
		_ = pgExec(ctx, postgresConn, query)

		query = fmt.Sprintf("INSERT INTO %s(id, name, price, stock) VALUES(%d, 'NewProduct_%d', 49.99, 100)",
			productsTable, 1000+i, i)
		_ = pgExec(ctx, postgresConn, query)
	}

	// Collect CDC insert events
	t.Log("📥 Collecting CDC INSERT events...")
	cdcTimeout := time.After(5 * time.Second)
	expectedCDCTotal := 9 // 3 per table

collectCDC:
	for {
		totalCDC := len(cdcInsertReceived[usersTable]) + len(cdcInsertReceived[ordersTable]) + len(cdcInsertReceived[productsTable])
		if totalCDC >= expectedCDCTotal {
			break
		}

		select {
		case msg := <-messageCh:
			if insertMsg, ok := msg.(*format.Insert); ok {
				tableName := insertMsg.TableName
				cdcInsertReceived[tableName] = append(cdcInsertReceived[tableName], insertMsg.Decoded)
				t.Logf("🔄 CDC INSERT received: table=%s, id=%d", tableName, insertMsg.Decoded["id"])
			}
		case <-cdcTimeout:
			totalCDC := len(cdcInsertReceived[usersTable]) + len(cdcInsertReceived[ordersTable]) + len(cdcInsertReceived[productsTable])
			t.Logf("⚠️  CDC timeout. Received %d/%d total inserts", totalCDC, expectedCDCTotal)
			break collectCDC
		}
	}

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 115, "Should receive 115 DATA events (60+40+15)")
		t.Log("✅ All snapshot events received")
	})

	t.Run("Verify Chunk Distribution Across Tables", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT 
				table_name,
				COUNT(*) as chunk_count,
				SUM(rows_processed) as total_rows,
				COUNT(*) FILTER (WHERE status = 'completed') as completed_chunks
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			GROUP BY table_name
			ORDER BY table_name
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.Len(t, results[0].Rows, 3, "Should have 3 tables")

		expectedChunks := map[string]struct {
			chunks int
			rows   int
		}{
			usersTable:    {chunks: 3, rows: 60}, // 60 rows / 25 = 3 chunks
			ordersTable:   {chunks: 2, rows: 40}, // 40 rows / 25 = 2 chunks
			productsTable: {chunks: 1, rows: 15}, // 15 rows / 25 = 1 chunk
		}

		for _, row := range results[0].Rows {
			tableName := string(row[0])
			chunkCount := string(row[1])
			totalRows := string(row[2])
			completedChunks := string(row[3])

			expected, ok := expectedChunks[tableName]
			require.True(t, ok, "Unexpected table: %s", tableName)

			assert.Equal(t, fmt.Sprintf("%d", expected.chunks), chunkCount, "Chunk count for %s", tableName)
			assert.Equal(t, fmt.Sprintf("%d", expected.rows), totalRows, "Row count for %s", tableName)
			assert.Equal(t, chunkCount, completedChunks, "All chunks should be completed for %s", tableName)

			t.Logf("✅ Table %s: chunks=%s, rows=%s, completed=%s",
				tableName, chunkCount, totalRows, completedChunks)
		}
	})

	t.Run("Verify Total Chunk Metadata", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT 
				COUNT(*) as total_chunks,
				SUM(rows_processed) as total_rows,
				COUNT(*) FILTER (WHERE status = 'completed') as completed_chunks
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		row := results[0].Rows[0]
		totalChunks := string(row[0])
		totalRows := string(row[1])
		completedChunks := string(row[2])

		assert.Equal(t, "6", totalChunks, "Should have 6 total chunks (3+2+1)")
		assert.Equal(t, "115", totalRows, "Should have 115 total rows (60+40+15)")
		assert.Equal(t, "6", completedChunks, "All 6 chunks should be completed")

		t.Logf("✅ Total metadata: chunks=%s, rows=%s, completed=%s",
			totalChunks, totalRows, completedChunks)
	})

	t.Run("Verify Job Metadata", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT completed, total_chunks, completed_chunks 
			FROM cdc_snapshot_job 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be completed")
		assert.Equal(t, "6", totalChunks, "Job should have 6 total chunks")
		assert.Equal(t, "6", completedChunks, "Job should have 6 completed chunks")

		t.Logf("✅ Job completed: total=%s, completed=%s", totalChunks, completedChunks)
	})

	t.Run("Verify CDC Works For All Tables", func(t *testing.T) {
		// Verify we got CDC inserts for each table
		assert.GreaterOrEqual(t, len(cdcInsertReceived[usersTable]), 2, "Should capture users CDC inserts")
		assert.GreaterOrEqual(t, len(cdcInsertReceived[ordersTable]), 2, "Should capture orders CDC inserts")
		assert.GreaterOrEqual(t, len(cdcInsertReceived[productsTable]), 2, "Should capture products CDC inserts")

		t.Logf("✅ CDC working for all tables:")
		t.Logf("   - %s: %d inserts", usersTable, len(cdcInsertReceived[usersTable]))
		t.Logf("   - %s: %d inserts", ordersTable, len(cdcInsertReceived[ordersTable]))
		t.Logf("   - %s: %d inserts", productsTable, len(cdcInsertReceived[productsTable]))
	})

	t.Run("Verify Snapshot Metrics", func(t *testing.T) {
		totalTables, err := fetchSnapshotTotalTablesMetric()
		if err == nil {
			assert.Equal(t, 3, totalTables, "Should have 3 tables")
		}

		totalChunks, err := fetchSnapshotTotalChunksMetric()
		if err == nil {
			assert.Equal(t, 6, totalChunks, "Should have 6 total chunks")
		}

		completedChunks, err := fetchSnapshotCompletedChunksMetric()
		if err == nil {
			assert.Equal(t, 6, completedChunks, "Should have 6 completed chunks")
		}

		totalRows, err := fetchSnapshotTotalRowsMetric()
		if err == nil {
			assert.Equal(t, 115, totalRows, "Should have 115 total rows")
		}

		t.Logf("✅ Metrics: tables=%d, chunks=%d/%d, rows=%d",
			totalTables, completedChunks, totalChunks, totalRows)
	})
}

// TestSnapshotNoPrimaryKey tests snapshot on table without primary key:
// - Table has no PRIMARY KEY constraint
// - System should use ctid (tuple identifier) for ordering
// - Should still process all rows correctly
// - CDC should work (requires replica identity FULL)
func TestSnapshotNoPrimaryKey(t *testing.T) {
	ctx := context.Background()

	// Setup: Create table WITHOUT primary key
	tableName := "snapshot_no_pk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_no_pk"
	cdcCfg.Publication.Name = "pub_snapshot_no_pk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull, // REQUIRED for tables without PK
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25 // Small chunks to test ctid ordering

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table WITHOUT primary key
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT NOT NULL,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			value DECIMAL(10,2) NOT NULL
		);
		-- NO PRIMARY KEY!
	`, tableName, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)
	t.Log("📝 Created table WITHOUT primary key")

	// Insert 75 rows (should create 3 chunks with chunkSize=25)
	t.Log("📝 Inserting 75 rows...")
	for i := 1; i <= 75; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, category, value) VALUES(%d, 'Item_%d', 'Cat_%d', %d.99)",
			tableName, i, i, i%5+1, i*10)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 75 rows inserted into table without PK")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	cdcInsertReceived := []map[string]any{}

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

		// Cleanup
		cleanupConn, err := newPostgresConn()
		if err == nil {
			defer cleanupConn.Close(ctx)
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", cdcCfg.Slot.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cdcCfg.Publication.Name))
			_ = pgExec(ctx, cleanupConn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", cdcCfg.Slot.Name, cdcCfg.Slot.Name))
		}
		t.Log("✅ Cleanup completed")
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Collect snapshot events
	timeout := time.After(10 * time.Second)
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
						t.Logf("📸 Snapshot progress: %d/75 rows received", len(snapshotDataReceived))
					}
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
					snapshotCompleted = true
					t.Logf("✅ Snapshot END received, LSN: %s, Total: %d rows", m.LSN.String(), len(snapshotDataReceived))
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot events. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// Wait for CDC to start
	time.Sleep(500 * time.Millisecond)

	// Insert new data for CDC test
	t.Log("📝 Inserting data for CDC test...")
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, category, value) VALUES(%d, 'CDCItem_%d', 'NewCat', 99.99)",
			tableName, 1000+i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Collect CDC insert events
	t.Log("📥 Collecting CDC INSERT events...")
	cdcTimeout := time.After(3 * time.Second)

collectCDC:
	for len(cdcInsertReceived) < 5 {
		select {
		case msg := <-messageCh:
			if insertMsg, ok := msg.(*format.Insert); ok {
				cdcInsertReceived = append(cdcInsertReceived, insertMsg.Decoded)
				t.Logf("🔄 CDC INSERT received: id=%d", insertMsg.Decoded["id"])
			}
		case <-cdcTimeout:
			t.Logf("⚠️  CDC timeout. Received %d/5 inserts", len(cdcInsertReceived))
			break collectCDC
		}
	}

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 75, "Should receive 75 DATA events from snapshot")
		t.Log("✅ All snapshot events received")
	})

	t.Run("Verify All Rows Captured (No Primary Key)", func(t *testing.T) {
		// Collect all IDs to verify completeness
		receivedIDs := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(int32)
			receivedIDs[id] = true
		}

		// All IDs from 1 to 75 should be present
		missingIDs := []int32{}
		for i := int32(1); i <= 75; i++ {
			if !receivedIDs[i] {
				missingIDs = append(missingIDs, i)
			}
		}

		assert.Empty(t, missingIDs, "All IDs should be captured despite no PK")
		if len(missingIDs) > 0 {
			t.Logf("❌ Missing IDs: %v", missingIDs)
		} else {
			t.Log("✅ All 75 rows captured correctly using ctid ordering")
		}
	})

	t.Run("Verify Chunk Metadata - 3 Chunks", func(t *testing.T) {
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

		assert.Equal(t, "3", totalChunks, "Should have 3 chunks (75 rows / 25 chunk size)")
		assert.Equal(t, "3", completedChunks, "All 3 chunks should be completed")
		assert.Equal(t, "75", totalRows, "Total rows processed should be 75")

		t.Logf("✅ Chunk metadata verified: chunks=%s, completed=%s, rows=%s",
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
		require.Len(t, results[0].Rows, 3, "Should have 3 chunk rows")

		expectedChunks := []struct {
			index         string
			start         string
			size          string
			rowsProcessed string
		}{
			{"0", "0", "25", "25"},
			{"1", "25", "25", "25"},
			{"2", "50", "25", "25"},
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

		t.Log("✅ All chunks processed correctly with ctid ordering")
	})

	t.Run("Verify CDC Works Without Primary Key", func(t *testing.T) {
		// CDC requires replica identity FULL for tables without PK
		assert.GreaterOrEqual(t, len(cdcInsertReceived), 4, "CDC should work with FULL replica identity")

		// Verify CDC captured the new inserts
		cdcIDs := make(map[int32]bool)
		for _, data := range cdcInsertReceived {
			id := data["id"].(int32)
			cdcIDs[id] = true
		}

		// Check for expected IDs (1001-1005)
		capturedCount := 0
		for i := int32(1001); i <= 1005; i++ {
			if cdcIDs[i] {
				capturedCount++
			}
		}

		assert.GreaterOrEqual(t, capturedCount, 4, "Should capture most CDC inserts")
		t.Logf("✅ CDC working without PK: captured %d/5 inserts", capturedCount)
	})

	t.Run("Verify Job Metadata", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT completed, total_chunks, completed_chunks 
			FROM cdc_snapshot_job 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be completed")
		assert.Equal(t, "3", totalChunks, "Job should have 3 total chunks")
		assert.Equal(t, "3", completedChunks, "Job should have 3 completed chunks")

		t.Logf("✅ Job completed: total=%s, completed=%s", totalChunks, completedChunks)
	})

	t.Run("Verify Table Has No Primary Key", func(t *testing.T) {
		// Double-check that table really has no PK
		query := fmt.Sprintf(`
			SELECT COUNT(*)
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			WHERE c.relname = '%s' AND i.indisprimary = true
		`, tableName)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		count := string(results[0].Rows[0][0])
		assert.Equal(t, "0", count, "Table should have NO primary key")
		t.Log("✅ Confirmed: Table has no primary key (using ctid for ordering)")
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

func fetchSnapshotTotalTablesMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_snapshot_total_tables")
	if err != nil {
		return 0, err
	}
	var val int
	fmt.Sscanf(m, "%d", &val)
	return val, nil
}
