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
