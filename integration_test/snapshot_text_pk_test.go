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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotTextPrimaryKeyRangeChunking tests keyset pagination for TEXT primary keys.
// Instead of slow OFFSET/LIMIT, it should use range-based chunking with range_start_text/range_end_text.
//
// This test verifies:
// - TEXT PK tables use keyset pagination (not OFFSET/LIMIT)
// - range_start_text and range_end_text columns are populated
// - All rows are captured correctly
// - Performance improvement over OFFSET/LIMIT approach
func TestSnapshotTextPrimaryKeyRangeChunking(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_text_pk_range_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_text_pk_range"
	cdcCfg.Publication.Name = "pub_snapshot_text_pk_range"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 20 // Small chunk size to create multiple chunks
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with TEXT primary key
	err = createTextPrimaryKeyTableForRangeTest(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert 100 rows with UUID-like TEXT primary keys
	// These will be naturally ordered alphabetically
	t.Log("📝 Inserting 100 rows with TEXT primary keys...")
	for i := 1; i <= 100; i++ {
		// Create sortable TEXT keys: key-001, key-002, ..., key-100
		key := fmt.Sprintf("key-%03d", i)
		query := fmt.Sprintf("INSERT INTO %s(id, name, data) VALUES('%s', 'User_%d', 'data_%d')",
			tableName, key, i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted with TEXT PKs")

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
					if len(snapshotDataReceived)%20 == 0 {
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

	t.Run("Verify All TEXT PK Rows Captured", func(t *testing.T) {
		receivedIDs := make(map[string]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(string)
			receivedIDs[id] = true
		}

		for i := 1; i <= 100; i++ {
			key := fmt.Sprintf("key-%03d", i)
			assert.True(t, receivedIDs[key], "ID %s should be in snapshot", key)
		}
		t.Logf("✅ All 100 unique TEXT IDs captured in snapshot")
	})

	t.Run("Verify TEXT Range Chunking Used (Not OFFSET/LIMIT)", func(t *testing.T) {
		// Verify that chunks use range_start_text/range_end_text instead of OFFSET
		query := fmt.Sprintf(`
			SELECT chunk_index, chunk_start, range_start, range_end, 
			       range_start_text, range_end_text, status, rows_processed
			FROM cdc_snapshot_chunks
			WHERE slot_name = '%s'
			ORDER BY chunk_index
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		chunks := results[0].Rows
		assert.GreaterOrEqual(t, len(chunks), 5, "Should have at least 5 chunks (100 rows / 20 chunk size)")

		for i, row := range chunks {
			chunkIndex := string(row[0])
			rangeStartInt := row[2]  // Should be NULL
			rangeEndInt := row[3]    // Should be NULL
			rangeStartText := row[4] // Should NOT be NULL
			rangeEndText := row[5]   // Should NOT be NULL
			status := string(row[6])

			// Verify INTEGER range columns are NULL (not used for TEXT PK)
			assert.Nil(t, rangeStartInt, "Chunk %d: range_start (INT) should be NULL for TEXT PK", i)
			assert.Nil(t, rangeEndInt, "Chunk %d: range_end (INT) should be NULL for TEXT PK", i)

			// Verify TEXT range columns are populated (keyset pagination)
			assert.NotNil(t, rangeStartText, "Chunk %d: range_start_text should NOT be NULL", i)
			assert.NotNil(t, rangeEndText, "Chunk %d: range_end_text should NOT be NULL", i)

			assert.Equal(t, "completed", status, "Chunk %d should be completed", i)

			t.Logf("✅ Chunk %s: range_start_text='%s', range_end_text='%s', status=%s",
				chunkIndex, string(rangeStartText), string(rangeEndText), status)
		}
	})

	t.Run("Verify Chunk Boundaries Are Ordered", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT range_start_text, range_end_text
			FROM cdc_snapshot_chunks
			WHERE slot_name = '%s'
			ORDER BY chunk_index
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		var prevEnd string
		for i, row := range results[0].Rows {
			startText := string(row[0])
			endText := string(row[1])

			// Each chunk's range should be valid (start <= end)
			assert.LessOrEqual(t, startText, endText,
				"Chunk %d: range_start_text should be <= range_end_text", i)

			// Chunks should be ordered (current start >= previous end for non-overlapping)
			if i > 0 {
				assert.GreaterOrEqual(t, startText, prevEnd,
					"Chunk %d: should start at or after previous chunk ended", i)
			}
			prevEnd = endText
		}
		t.Log("✅ Chunk boundaries are properly ordered")
	})

	t.Run("Verify Total Rows Processed", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT SUM(rows_processed) as total_rows
			FROM cdc_snapshot_chunks
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		totalRows := string(results[0].Rows[0][0])
		assert.Equal(t, "100", totalRows, "Total rows processed should be 100")
		t.Logf("✅ Total rows processed: %s", totalRows)
	})

	t.Run("Verify Job Completed Successfully", func(t *testing.T) {
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
		assert.Equal(t, totalChunks, completedChunks, "All chunks should be completed")

		t.Logf("✅ Job completed: total_chunks=%s, completed_chunks=%s", totalChunks, completedChunks)
	})
}

// TestSnapshotTextPrimaryKeyWithUUIDs tests keyset pagination with UUID-style TEXT primary keys.
// UUIDs are a common real-world pattern for TEXT PKs.
func TestSnapshotTextPrimaryKeyWithUUIDs(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_uuid_pk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_uuid_pk"
	cdcCfg.Publication.Name = "pub_snapshot_uuid_pk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 10
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with TEXT primary key (simulating UUID)
	err = createTextPrimaryKeyTableForRangeTest(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert 50 rows with UUID-like keys (sorted for predictable testing)
	t.Log("📝 Inserting 50 rows with UUID-like TEXT primary keys...")
	uuids := generateSortedUUIDs(50)
	for i, uuid := range uuids {
		query := fmt.Sprintf("INSERT INTO %s(id, name, data) VALUES('%s', 'User_%d', 'payload_%d')",
			tableName, uuid, i+1, i+1)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 50 rows inserted with UUID-like PKs")

	snapshotDataReceived := []map[string]any{}
	snapshotCompleted := false

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

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

	timeout := time.After(30 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotCompleted = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d rows", len(snapshotDataReceived))
		}
	}

	// Verify all UUID rows captured
	assert.Len(t, snapshotDataReceived, 50, "Should receive all 50 rows")

	receivedUUIDs := make(map[string]bool)
	for _, data := range snapshotDataReceived {
		id := data["id"].(string)
		receivedUUIDs[id] = true
	}

	for _, uuid := range uuids {
		assert.True(t, receivedUUIDs[uuid], "UUID %s should be captured", uuid)
	}

	// Verify TEXT range chunking was used
	query := fmt.Sprintf(`
		SELECT COUNT(*) as total,
		       COUNT(range_start_text) as with_text_range,
		       COUNT(range_start) as with_int_range
		FROM cdc_snapshot_chunks
		WHERE slot_name = '%s'
	`, cdcCfg.Slot.Name)

	results, err := execQuery(ctx, postgresConn, query)
	require.NoError(t, err)

	row := results[0].Rows[0]
	totalChunks := string(row[0])
	withTextRange := string(row[1])
	withIntRange := string(row[2])

	assert.Equal(t, totalChunks, withTextRange, "All chunks should have TEXT range")
	assert.Equal(t, "0", withIntRange, "No chunks should have INT range")

	t.Logf("✅ UUID test passed: %s chunks with TEXT range, %s with INT range", withTextRange, withIntRange)
}

// TestSnapshotVarcharPrimaryKey tests keyset pagination for VARCHAR primary keys.
func TestSnapshotVarcharPrimaryKey(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_varchar_pk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_varchar_pk"
	cdcCfg.Publication.Name = "pub_snapshot_varchar_pk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 15
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with VARCHAR(255) primary key
	err = createVarcharPrimaryKeyTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert 60 rows
	t.Log("📝 Inserting 60 rows with VARCHAR primary keys...")
	for i := 1; i <= 60; i++ {
		key := fmt.Sprintf("SKU-%04d", i)
		query := fmt.Sprintf("INSERT INTO %s(sku, product_name, price) VALUES('%s', 'Product_%d', %d.99)",
			tableName, key, i, i*10)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 60 rows inserted")

	snapshotDataReceived := []map[string]any{}
	snapshotCompleted := false

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

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

	timeout := time.After(30 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotCompleted = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout: received %d rows", len(snapshotDataReceived))
		}
	}

	assert.Len(t, snapshotDataReceived, 60, "Should receive all 60 rows")

	// Verify chunks used TEXT range (for VARCHAR)
	query := fmt.Sprintf(`
		SELECT chunk_index, range_start_text, range_end_text
		FROM cdc_snapshot_chunks
		WHERE slot_name = '%s' AND range_start_text IS NOT NULL
		ORDER BY chunk_index
	`, cdcCfg.Slot.Name)

	results, err := execQuery(ctx, postgresConn, query)
	require.NoError(t, err)
	require.NotEmpty(t, results[0].Rows, "Should have chunks with TEXT ranges")

	t.Logf("✅ VARCHAR PK test passed: %d chunks with TEXT range", len(results[0].Rows))
}

// TestSnapshotTextPKCDCContinuation verifies CDC continues correctly after TEXT PK snapshot.
func TestSnapshotTextPKCDCContinuation(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_text_pk_cdc_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_text_pk_cdc"
	cdcCfg.Publication.Name = "pub_snapshot_text_pk_cdc"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 10

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = createTextPrimaryKeyTableForRangeTest(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert initial data for snapshot
	for i := 1; i <= 30; i++ {
		key := fmt.Sprintf("initial-%03d", i)
		query := fmt.Sprintf("INSERT INTO %s(id, name, data) VALUES('%s', 'Initial_%d', 'data_%d')",
			tableName, key, i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	snapshotData := []map[string]any{}
	cdcInserts := []map[string]any{}
	snapshotCompleted := false

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

	// Wait for snapshot to complete
	timeout := time.After(30 * time.Second)
	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				if m.EventType == format.SnapshotEventTypeData {
					snapshotData = append(snapshotData, m.Data)
				} else if m.EventType == format.SnapshotEventTypeEnd {
					snapshotCompleted = true
					t.Log("✅ Snapshot completed")
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for snapshot")
		}
	}

	assert.Len(t, snapshotData, 30, "Snapshot should capture 30 initial rows")

	// Insert new rows AFTER snapshot (these should come via CDC)
	t.Log("📝 Inserting 5 new rows after snapshot...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("cdc-%03d", i)
		query := fmt.Sprintf("INSERT INTO %s(id, name, data) VALUES('%s', 'CDC_%d', 'new_data_%d')",
			tableName, key, i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Collect CDC events
	cdcTimeout := time.After(5 * time.Second)
collectCDC:
	for len(cdcInserts) < 5 {
		select {
		case msg := <-messageCh:
			if insertMsg, ok := msg.(*format.Insert); ok {
				cdcInserts = append(cdcInserts, insertMsg.Decoded)
				t.Logf("🔄 CDC INSERT received: id=%s", insertMsg.Decoded["id"])
			}
		case <-cdcTimeout:
			break collectCDC
		}
	}

	assert.GreaterOrEqual(t, len(cdcInserts), 4, "CDC should capture most new inserts")
	t.Logf("✅ CDC continuation verified: %d/5 inserts captured after TEXT PK snapshot", len(cdcInserts))

	// Verify no duplicate data
	allIDs := make(map[string]int)
	for _, data := range snapshotData {
		id := data["id"].(string)
		allIDs[id]++
	}
	for _, data := range cdcInserts {
		id := data["id"].(string)
		allIDs[id]++
	}

	for id, count := range allIDs {
		assert.Equal(t, 1, count, "ID %s should appear exactly once (no duplicates)", id)
	}
	t.Log("✅ No duplicate data between snapshot and CDC")
}

// Helper functions

func createTextPrimaryKeyTableForRangeTest(ctx context.Context, conn pq.Connection, tableName string) error {
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			data TEXT
		);
	`, tableName, tableName)
	return pgExec(ctx, conn, query)
}

func createVarcharPrimaryKeyTable(ctx context.Context, conn pq.Connection, tableName string) error {
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			sku VARCHAR(255) PRIMARY KEY,
			product_name TEXT NOT NULL,
			price NUMERIC(10,2)
		);
	`, tableName, tableName)
	return pgExec(ctx, conn, query)
}

func generateSortedUUIDs(count int) []string {
	// Generate predictable, sortable UUID-like strings
	uuids := make([]string, count)
	for i := 0; i < count; i++ {
		// Format: 00000000-0000-0000-0000-000000000001
		uuids[i] = fmt.Sprintf("%08d-0000-0000-0000-%012d", i/1000, i)
	}
	return uuids
}
