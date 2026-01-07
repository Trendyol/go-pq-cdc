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
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotCTIDPartitioning tests CTID block-based partitioning
// for tables with string primary keys.
// CTID partitioning should be used instead of slow OFFSET-based queries.
func TestSnapshotCTIDPartitioning(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with STRING primary key
	tableName := "snapshot_ctid_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_ctid"
	cdcCfg.Publication.Name = "pub_snapshot_ctid"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 50 // Small chunk size to create multiple chunks
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with TEXT primary key (should trigger CTID partitioning)
	err = createTextPrimaryKeyTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 200 rows with STRING primary keys...")
	for i := 1; i <= 200; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, payload) VALUES('ID-%08d', 'Payload for item %d')",
			tableName, i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 200 rows inserted with STRING PKs")

	// Setup: Message collection
	snapshotBeginReceived := false
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false

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
					if len(snapshotDataReceived)%50 == 0 {
						t.Logf("📸 Snapshot progress: %d/200 rows received", len(snapshotDataReceived))
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
		assert.Len(t, snapshotDataReceived, 200, "Should receive 200 DATA events from snapshot")
	})

	t.Run("Verify All Rows Captured", func(t *testing.T) {
		// Check that all IDs from ID-00000001 to ID-00000200 are present
		receivedIDs := make(map[string]bool)
		for _, data := range snapshotDataReceived {
			id := data["id"].(string)
			receivedIDs[id] = true
		}

		for i := 1; i <= 200; i++ {
			expectedID := fmt.Sprintf("ID-%08d", i)
			assert.True(t, receivedIDs[expectedID], "ID %s should be in snapshot", expectedID)
		}
		t.Logf("✅ All 200 unique STRING IDs captured in snapshot")
	})

	t.Run("Verify CTID Partition Strategy Used", func(t *testing.T) {
		// Query the chunks metadata to verify CTID strategy was used
		query := fmt.Sprintf(`
			SELECT partition_strategy, COUNT(*) as chunk_count
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			GROUP BY partition_strategy
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy,
			"Partition strategy should be 'ctid_block' for STRING PK table")
		t.Logf("✅ CTID block partitioning strategy confirmed: %s", strategy)
	})

	t.Run("Verify All Chunks Completed", func(t *testing.T) {
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
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		row := results[0].Rows[0]
		var totalChunks, completedChunks, totalRows int
		fmt.Sscanf(string(row[0]), "%d", &totalChunks)
		fmt.Sscanf(string(row[1]), "%d", &completedChunks)
		fmt.Sscanf(string(row[2]), "%d", &totalRows)

		assert.Equal(t, totalChunks, completedChunks, "All chunks should be completed")
		assert.GreaterOrEqual(t, totalRows, 200, "Should process at least 200 rows")
		t.Logf("✅ Chunks: %d total, %d completed, %d rows processed", totalChunks, completedChunks, totalRows)
	})

	t.Run("Verify Block Ranges Set", func(t *testing.T) {
		// Verify that block_start is set for all CTID chunks
		// Note: block_end is NULL for the LAST chunk (no upper bound to capture all rows)
		query := fmt.Sprintf(`
			SELECT block_start, block_end, is_last_chunk
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s' AND partition_strategy = 'ctid_block'
			ORDER BY chunk_index
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		if len(results) > 0 && len(results[0].Rows) > 0 {
			for i, row := range results[0].Rows {
				blockStart := string(row[0])
				blockEnd := string(row[1])
				isLastChunk := string(row[2])

				// block_start should always be set
				assert.NotEmpty(t, blockStart, "block_start should be set for chunk %d", i)

				// Last chunk has block_end = NULL (no upper bound for safety)
				if isLastChunk == "t" || isLastChunk == "true" {
					assert.Empty(t, blockEnd, "block_end should be NULL for last chunk (no upper bound)")
				} else {
					assert.NotEmpty(t, blockEnd, "block_end should be set for non-last chunk %d", i)
				}
			}
			t.Log("✅ Block ranges are properly set for CTID chunks (last chunk has no upper bound)")
		}
	})
}

// TestSnapshotCTIDVsOffset compares CTID and Offset partitioning for non-integer PKs
// CTID should be preferred over Offset for performance
func TestSnapshotCTIDVsOffset(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with NO primary key (will use CTID)
	tableName := "snapshot_no_pk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_no_pk"
	cdcCfg.Publication.Name = "pub_snapshot_no_pk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 30
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table WITHOUT primary key
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			data TEXT NOT NULL,
			value INT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
	`, tableName, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	t.Log("📝 Inserting 100 rows into table WITHOUT primary key...")
	for i := 1; i <= 100; i++ {
		insertQuery := fmt.Sprintf("INSERT INTO %s(data, value) VALUES('Data-%d', %d)",
			tableName, i, i*10)
		err = pgExec(ctx, postgresConn, insertQuery)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted")

	// Setup: Message collection
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false

	messageCh := make(chan any, 200)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	// Start connector
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

	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// === Assertions ===

	t.Run("Verify All Rows Captured from No-PK Table", func(t *testing.T) {
		assert.Len(t, snapshotDataReceived, 100, "Should receive 100 DATA events")
	})

	t.Run("Verify CTID Strategy for No-PK Table", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT partition_strategy
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			LIMIT 1
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy,
			"No-PK table should use CTID block partitioning")
		t.Logf("✅ No-PK table correctly uses CTID partitioning: %s", strategy)
	})
}

// TestSnapshotIntegerPKStillUsesRange verifies that integer PK tables
// still use the optimized range partitioning (not CTID)
func TestSnapshotIntegerPKStillUsesRange(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_int_pk_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_int_pk"
	cdcCfg.Publication.Name = "pub_snapshot_int_pk"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with INTEGER primary key
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 100 rows with INTEGER primary keys...")
	for i := 1; i <= 100; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted")

	// Setup
	snapshotEndReceived := false
	messageCh := make(chan any, 200)
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

	// Wait for snapshot to complete
	timeout := time.After(30 * time.Second)
	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeEnd {
				snapshotEndReceived = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot")
		}
	}

	// === Assertions ===

	t.Run("Verify Integer Range Strategy for INT PK Table", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT partition_strategy
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			LIMIT 1
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyIntegerRange), strategy,
			"Integer PK table should use integer_range partitioning")
		t.Logf("✅ Integer PK table correctly uses range partitioning: %s", strategy)
	})

	t.Run("Verify Range Bounds Set", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT range_start, range_end 
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s' AND partition_strategy = 'integer_range'
			LIMIT 5
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		if len(results) > 0 && len(results[0].Rows) > 0 {
			for _, row := range results[0].Rows {
				rangeStart := string(row[0])
				rangeEnd := string(row[1])
				assert.NotEmpty(t, rangeStart, "range_start should be set")
				assert.NotEmpty(t, rangeEnd, "range_end should be set")
			}
			t.Log("✅ Range bounds are properly set for integer PK chunks")
		}
	})
}

// TestSnapshotExplicitCTIDOverride tests that users can explicitly force CTID
// partitioning even for tables with integer PKs (useful for hash-based PKs)
func TestSnapshotExplicitCTIDOverride(t *testing.T) {
	ctx := context.Background()

	// Setup: Create table with INTEGER PK but force CTID partitioning
	tableName := "snapshot_explicit_ctid_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_explicit_ctid"
	cdcCfg.Publication.Name = "pub_snapshot_explicit_ctid"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
			// EXPLICITLY force CTID partitioning even though table has integer PK
			SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with INTEGER primary key
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 100 rows with INTEGER PKs (but forcing CTID strategy)...")
	for i := 1; i <= 100; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 100 rows inserted")

	// Setup
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	messageCh := make(chan any, 200)
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

	// Collect snapshot events
	timeout := time.After(30 * time.Second)
	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// === Assertions ===

	t.Run("Verify All Rows Captured", func(t *testing.T) {
		assert.Len(t, snapshotDataReceived, 100, "Should receive 100 DATA events")
	})

	t.Run("Verify CTID Override for Integer PK Table", func(t *testing.T) {
		// The key test: even though table has integer PK, CTID should be used
		// because we explicitly set SnapshotPartitionStrategy: "ctid_block"
		query := fmt.Sprintf(`
			SELECT partition_strategy
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			LIMIT 1
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy,
			"Explicit CTID override should be respected for integer PK table")
		t.Logf("✅ Explicit CTID override works: integer PK table uses '%s' instead of 'integer_range'", strategy)
	})

	t.Run("Verify Block Ranges Set (not Range bounds)", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT block_start, block_end, range_start, range_end, is_last_chunk
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			LIMIT 1
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		row := results[0].Rows[0]
		blockStart := string(row[0])
		blockEnd := string(row[1])
		rangeStart := string(row[2])
		rangeEnd := string(row[3])
		isLastChunk := string(row[4])

		// CTID chunks should have block_start set, not range_start/range_end
		assert.NotEmpty(t, blockStart, "block_start should be set for CTID chunks")
		// Note: Last chunk has block_end = NULL (no upper bound for safety)
		if isLastChunk == "t" || isLastChunk == "true" {
			t.Log("📝 Single chunk is also the last chunk, block_end is NULL (expected)")
		} else {
			assert.NotEmpty(t, blockEnd, "block_end should be set for non-last CTID chunks")
		}
		assert.Empty(t, rangeStart, "range_start should NOT be set for CTID chunks")
		assert.Empty(t, rangeEnd, "range_end should NOT be set for CTID chunks")
		t.Log("✅ Block ranges correctly set for explicitly overridden CTID chunks")
	})
}

// TestSnapshotExplicitOffsetStrategy tests that users can force OFFSET strategy
func TestSnapshotExplicitOffsetStrategy(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_explicit_offset_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_snapshot_explicit_offset"
	cdcCfg.Publication.Name = "pub_snapshot_explicit_offset"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            tableName,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
			// Force OFFSET strategy (slow but deterministic ordering)
			SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyOffset,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 25
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with INTEGER primary key
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	t.Log("📝 Inserting 50 rows...")
	for i := 1; i <= 50; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User_%d', %d)",
			tableName, i, i, 20+i%50)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ 50 rows inserted")

	// Setup
	snapshotEndReceived := false
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

	// Wait for snapshot to complete
	timeout := time.After(30 * time.Second)
	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeEnd {
				snapshotEndReceived = true
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot")
		}
	}

	// === Assertions ===

	t.Run("Verify Offset Strategy Override", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT partition_strategy
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			LIMIT 1
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyOffset), strategy,
			"Explicit OFFSET strategy should be used")
		t.Logf("✅ Explicit OFFSET strategy works: '%s'", strategy)
	})
}

// TestSnapshotCTIDConsistency verifies that CTID partitioning uses snapshot-consistent
// block counts. The pg_relation_size call happens WITHIN the snapshot transaction,
// ensuring chunk boundaries match what workers see.
func TestSnapshotCTIDConsistency(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_ctid_consistency_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_ctid_consistency"
	cdcCfg.Publication.Name = "pub_ctid_consistency"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:                      tableName,
			Schema:                    "public",
			ReplicaIdentity:           publication.ReplicaIdentityFull,
			SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with TEXT primary key
	err = createTextPrimaryKeyTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert a known number of rows
	initialRows := 500
	t.Logf("📝 Inserting %d rows...", initialRows)
	for i := 1; i <= initialRows; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, payload) VALUES('CONSIST-%08d', 'Consistency test %d')",
			tableName, i, i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Logf("✅ %d rows inserted", initialRows)

	// Setup snapshot handler
	snapshotDataReceived := []map[string]any{}
	snapshotEndReceived := false
	messageCh := make(chan any, 1000)

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

	// Collect snapshot events
	timeout := time.After(60 * time.Second)
	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					snapshotDataReceived = append(snapshotDataReceived, m.Data)
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout. Received %d DATA events", len(snapshotDataReceived))
		}
	}

	// === Assertions ===

	t.Run("Verify All Rows Captured", func(t *testing.T) {
		assert.Equal(t, initialRows, len(snapshotDataReceived),
			"Should capture exactly the number of inserted rows")
		t.Logf("✅ Captured %d/%d rows", len(snapshotDataReceived), initialRows)
	})

	t.Run("Verify CTID Strategy Used", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT partition_strategy, COUNT(*) as chunk_count
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			GROUP BY partition_strategy
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunk metadata")

		strategy := string(results[0].Rows[0][0])
		assert.Equal(t, string(snapshot.PartitionStrategyCTIDBlock), strategy,
			"CTID block strategy should be used")
		t.Logf("✅ CTID strategy confirmed: %s", strategy)
	})

	t.Run("Verify Chunks Have Block Boundaries", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT chunk_index, block_start, block_end, is_last_chunk
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
			ORDER BY chunk_index
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have chunks")

		chunks := results[0].Rows
		t.Logf("📊 Found %d chunks", len(chunks))

		for i, chunk := range chunks {
			chunkIndex := string(chunk[0])
			blockStart := string(chunk[1])
			blockEnd := string(chunk[2])
			isLast := string(chunk[3])

			t.Logf("  Chunk %s: block_start=%s, block_end=%s, is_last=%s",
				chunkIndex, blockStart, blockEnd, isLast)

			// All chunks should have block_start
			assert.NotEmpty(t, blockStart, "Chunk %d should have block_start", i)

			// Last chunk should have is_last_chunk=true and block_end=NULL
			if i == len(chunks)-1 {
				assert.True(t, isLast == "t" || isLast == "true",
					"Last chunk should have is_last_chunk=true")
				assert.Empty(t, blockEnd,
					"Last chunk should have block_end=NULL (no upper bound)")
			}
		}
		t.Log("✅ Chunk block boundaries verified")
	})

	t.Run("Verify Job Completed Successfully", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT completed, total_chunks, completed_chunks
			FROM cdc_snapshot_job 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0)

		row := results[0].Rows[0]
		completed := string(row[0])
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed == "t" || completed == "true", "Job should be completed")
		assert.Equal(t, totalChunks, completedChunks, "All chunks should be processed")
		t.Logf("✅ Job completed: total=%s, completed=%s", totalChunks, completedChunks)
	})
}

// TestSnapshotCTIDNoDataLoss specifically tests that no rows are lost
// when using CTID partitioning, especially for the last chunk.
func TestSnapshotCTIDNoDataLoss(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_ctid_no_loss_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_ctid_no_loss"
	cdcCfg.Publication.Name = "pub_ctid_no_loss"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:                      tableName,
			Schema:                    "public",
			ReplicaIdentity:           publication.ReplicaIdentityFull,
			SnapshotPartitionStrategy: publication.SnapshotPartitionStrategyCTIDBlock,
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 50 // Small chunk to create multiple chunks
	cdcCfg.Snapshot.HeartbeatInterval = 30 * time.Second
	cdcCfg.Snapshot.ClaimTimeout = 30 * time.Second

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with TEXT primary key
	err = createTextPrimaryKeyTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert rows that will span multiple blocks
	// PostgreSQL block size is typically 8KB, so we need enough data to span blocks
	rowCount := 300
	t.Logf("📝 Inserting %d rows...", rowCount)
	for i := 1; i <= rowCount; i++ {
		// Larger payload to ensure we span multiple blocks
		payload := fmt.Sprintf("Large payload for row %d - padding data to ensure block spanning: %s",
			i, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
		query := fmt.Sprintf("INSERT INTO %s(id, payload) VALUES('NOLOSS-%08d', '%s')",
			tableName, i, payload)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Logf("✅ %d rows inserted", rowCount)

	// Get actual row count from database for comparison
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	countResults, err := execQuery(ctx, postgresConn, countQuery)
	require.NoError(t, err)
	var dbRowCount int
	fmt.Sscanf(string(countResults[0].Rows[0][0]), "%d", &dbRowCount)
	t.Logf("📊 Database row count: %d", dbRowCount)

	// Setup snapshot handler
	receivedIDs := make(map[string]bool)
	snapshotEndReceived := false
	messageCh := make(chan any, 1000)

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

	// Collect snapshot events
	timeout := time.After(90 * time.Second)
	for !snapshotEndReceived {
		select {
		case msg := <-messageCh:
			if m, ok := msg.(*format.Snapshot); ok {
				switch m.EventType {
				case format.SnapshotEventTypeData:
					if id, ok := m.Data["id"].(string); ok {
						receivedIDs[id] = true
					}
				case format.SnapshotEventTypeEnd:
					snapshotEndReceived = true
				}
			}
		case <-timeout:
			t.Fatalf("Timeout. Received %d unique IDs", len(receivedIDs))
		}
	}

	// === Assertions ===

	t.Run("Verify No Data Loss", func(t *testing.T) {
		assert.Equal(t, dbRowCount, len(receivedIDs),
			"Snapshot should capture exactly %d rows, got %d", dbRowCount, len(receivedIDs))

		// Verify all expected IDs are present
		missingIDs := []string{}
		for i := 1; i <= rowCount; i++ {
			expectedID := fmt.Sprintf("NOLOSS-%08d", i)
			if !receivedIDs[expectedID] {
				missingIDs = append(missingIDs, expectedID)
			}
		}

		if len(missingIDs) > 0 {
			t.Logf("❌ Missing IDs (first 10): %v", missingIDs[:min(10, len(missingIDs))])
		}
		assert.Empty(t, missingIDs, "No rows should be missing")
		t.Logf("✅ All %d rows captured without data loss", len(receivedIDs))
	})

	t.Run("Verify Multiple Chunks Used", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT COUNT(*) FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s'
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		var chunkCount int
		fmt.Sscanf(string(results[0].Rows[0][0]), "%d", &chunkCount)

		assert.Greater(t, chunkCount, 1, "Should have multiple chunks for proper CTID testing")
		t.Logf("✅ Used %d chunks", chunkCount)
	})

	t.Run("Verify Last Chunk Processed", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT chunk_index, status, rows_processed
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s' AND is_last_chunk = true
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.True(t, len(results) > 0 && len(results[0].Rows) > 0, "Should have last chunk")

		status := string(results[0].Rows[0][1])
		assert.Equal(t, "completed", status, "Last chunk should be completed")
		t.Log("✅ Last chunk (with no upper bound) processed successfully")
	})
}
