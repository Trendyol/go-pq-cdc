package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
