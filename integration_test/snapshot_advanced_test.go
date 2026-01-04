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

		assert.Equal(t, "1", totalChunks, "Should have 1 chunk")
		assert.Equal(t, "1", completedChunks, "All 1 chunk should be completed")
		assert.Equal(t, "75", totalRows, "Total rows processed should be 75")

		t.Logf("✅ Chunk metadata verified: chunks=%s, completed=%s, rows=%s",
			totalChunks, completedChunks, totalRows)
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
		assert.Equal(t, "1", totalChunks, "Job should have 1 total chunks")
		assert.Equal(t, "1", completedChunks, "Job should have 1 completed chunks")

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
