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
		// Verify that block_start and block_end are set for CTID chunks
		query := fmt.Sprintf(`
			SELECT block_start, block_end 
			FROM cdc_snapshot_chunks 
			WHERE slot_name = '%s' AND partition_strategy = 'ctid_block'
			LIMIT 5
		`, cdcCfg.Slot.Name)

		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		if len(results) > 0 && len(results[0].Rows) > 0 {
			for _, row := range results[0].Rows {
				blockStart := string(row[0])
				blockEnd := string(row[1])
				// Block values should be set (not NULL)
				assert.NotEmpty(t, blockStart, "block_start should be set")
				assert.NotEmpty(t, blockEnd, "block_end should be set")
			}
			t.Log("✅ Block ranges are properly set for CTID chunks")
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
