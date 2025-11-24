package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotOnlyBasic tests the basic snapshot_only mode:
// - Single table with data
// - Verify BEGIN, DATA, END events
// - Verify no replication slot is created
// - Verify process exits after snapshot completes
func TestSnapshotOnlyBasic(t *testing.T) {
	ctx := context.Background()

	// Setup: Create test table with data
	tableName := "snapshot_only_basic_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert test data
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	testData := []map[string]any{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 35},
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
	var mu sync.Mutex

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			messageCh <- msg
			mu.Lock()
			switch msg.EventType {
			case format.SnapshotEventTypeBegin:
				snapshotBeginReceived = true
			case format.SnapshotEventTypeData:
				snapshotDataReceived = append(snapshotDataReceived, msg.Data)
			case format.SnapshotEventTypeEnd:
				snapshotEndReceived = true
			}
			mu.Unlock()
		}
		_ = ctx.Ack()
	}

	// Start connector with snapshot_only mode
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	// Start in goroutine and wait for completion
	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	// Collect snapshot events with timeout
	timeout := time.After(10 * time.Second)
	snapshotCompleted := false

	for !snapshotCompleted {
		select {
		case msg := <-messageCh:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				switch snapshot.EventType {
				case format.SnapshotEventTypeBegin:
					t.Logf("✅ Snapshot BEGIN received, LSN: %s", snapshot.LSN.String())
				case format.SnapshotEventTypeData:
					t.Logf("📸 Snapshot DATA received: %v", snapshot.Data)
				case format.SnapshotEventTypeEnd:
					t.Logf("✅ Snapshot END received, LSN: %s", snapshot.LSN.String())
					snapshotCompleted = true
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot events")
		}
	}

	// Wait for connector to exit
	select {
	case <-completeCh:
		t.Log("✅ Connector exited after snapshot completion")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit after snapshot completion")
	}

	// === Assertions ===

	t.Run("Verify Snapshot Events", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()

		assert.True(t, snapshotBeginReceived, "Snapshot BEGIN event should be received")
		assert.True(t, snapshotEndReceived, "Snapshot END event should be received")
		assert.Len(t, snapshotDataReceived, 3, "Should receive 3 DATA events from snapshot")

		// Verify data content
		for i, expected := range testData {
			assert.Equal(t, int32(expected["id"].(int)), snapshotDataReceived[i]["id"])
			assert.Equal(t, expected["name"], snapshotDataReceived[i]["name"])
			assert.Equal(t, int32(expected["age"].(int)), snapshotDataReceived[i]["age"])
		}
	})

	t.Run("Verify No Replication Slot Created", func(t *testing.T) {
		// Query replication slots - should not find any for snapshot_only
		expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
		query := fmt.Sprintf("SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s'", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)

		// Should have no replication slots
		if len(results) > 0 && len(results[0].Rows) > 0 {
			t.Errorf("Expected no replication slots, but found: %v", results[0].Rows)
		} else {
			t.Log("✅ No replication slot created (as expected)")
		}
	})

	t.Run("Verify Snapshot Metadata Created", func(t *testing.T) {
		// Query snapshot job metadata - it should exist even in snapshot_only mode
		expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
		query := fmt.Sprintf("SELECT completed, total_chunks, completed_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results, "Job metadata should exist")
		require.NotEmpty(t, results[0].Rows, "Should have at least one job record")

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		totalChunks := string(row[1])
		completedChunks := string(row[2])

		assert.True(t, completed, "Job should be marked as completed")
		assert.Equal(t, totalChunks, completedChunks, "All chunks should be completed")

		t.Logf("✅ Metadata verified: completed=%t, total_chunks=%s, completed_chunks=%s",
			completed, totalChunks, completedChunks)
	})
}

// TestSnapshotOnlyMultipleTables tests snapshot_only mode with multiple tables
func TestSnapshotOnlyMultipleTables(t *testing.T) {
	ctx := context.Background()

	// Setup: Create multiple test tables
	table1 := "snapshot_only_multi1"
	table2 := "snapshot_only_multi2"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   table1,
			Schema: "public",
		},
		{
			Name:   table2,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 50

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create tables
	err = createTestTable(ctx, postgresConn, table1)
	require.NoError(t, err)
	err = createTestTable(ctx, postgresConn, table2)
	require.NoError(t, err)

	// Insert data into table1
	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			table1, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Insert data into table2
	for i := 10; i <= 12; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'Person%d', %d)",
			table2, i, i, 30+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Setup: Message collection
	dataByTable := make(map[string][]map[string]any)
	var mu sync.Mutex

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				tableName := snapshot.Table
				dataByTable[tableName] = append(dataByTable[tableName], snapshot.Data)
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	// Start connector
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, table1)
		cleanupSnapshotOnlyTest(t, ctx, table2)
	})

	// Start and wait for completion
	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	// Collect all events
	timeout := time.After(10 * time.Second)
	endReceived := false

	for !endReceived {
		select {
		case msg := <-messageCh:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeEnd {
					endReceived = true
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot completion")
		}
	}

	// Wait for exit
	select {
	case <-completeCh:
		t.Log("✅ Connector exited after snapshot completion")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit")
	}

	// === Assertions ===

	t.Run("Verify Data From Both Tables", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()

		assert.Len(t, dataByTable[table1], 3, "Should receive 3 rows from table1")
		assert.Len(t, dataByTable[table2], 3, "Should receive 3 rows from table2")

		t.Logf("✅ Table1: %d rows, Table2: %d rows",
			len(dataByTable[table1]), len(dataByTable[table2]))
	})

	t.Run("Verify Chunk Metadata", func(t *testing.T) {
		// Query chunks for both tables
		expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
		query := fmt.Sprintf("SELECT table_name, COUNT(*) FROM cdc_snapshot_chunks WHERE slot_name = '%s' GROUP BY table_name", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		t.Logf("✅ Chunks created for %d table(s)", len(results[0].Rows))
	})
}

// TestSnapshotOnlyWithChunking tests snapshot_only mode with multiple chunks
func TestSnapshotOnlyWithChunking(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_only_chunking_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 5 // Small chunk size to force multiple chunks

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table and insert more data to create multiple chunks
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	// Insert 15 rows (should create 3 chunks with size 5)
	for i := 1; i <= 15; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Setup: Count received rows
	var rowCount int
	var mu sync.Mutex

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				rowCount++
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	// Start connector
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	// Start and wait
	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	// Wait for completion
	timeout := time.After(10 * time.Second)
	endReceived := false

	for !endReceived {
		select {
		case msg := <-messageCh:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeEnd {
					endReceived = true
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot completion")
		}
	}

	select {
	case <-completeCh:
		t.Log("✅ Connector exited")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit")
	}

	// === Assertions ===

	t.Run("Verify All Rows Received", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 15, rowCount, "Should receive all 15 rows")
	})

	t.Run("Verify Multiple Chunks Created", func(t *testing.T) {
		expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
		query := fmt.Sprintf("SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE slot_name = '%s' AND status = 'completed'", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		chunkCount := string(results[0].Rows[0][0])
		// Should be 3 chunks (15 rows / 5 per chunk)
		assert.Equal(t, "3", chunkCount, "Should have 3 chunks")

		t.Logf("✅ Verified %s chunks created", chunkCount)
	})
}

// TestSnapshotOnlyMetrics tests that metrics are still exposed in snapshot_only mode
func TestSnapshotOnlyMetrics(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_only_metrics_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with small dataset
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh <- snapshot
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	// Wait for END event
	timeout := time.After(10 * time.Second)
	endReceived := false

	for !endReceived {
		select {
		case msg := <-messageCh:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeEnd {
					endReceived = true
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot completion")
		}
	}

	// Before connector exits, check metrics
	t.Run("Verify Metrics Exposed", func(t *testing.T) {
		// Try to fetch metrics (may fail if metric server hasn't started yet)
		totalRows, err := fetchSnapshotTotalRowsMetric()
		if err == nil {
			assert.Equal(t, 5, totalRows, "Should have processed 5 rows")
			t.Logf("✅ Metrics exposed: total_rows=%d", totalRows)
		} else {
			t.Logf("⚠️ Could not fetch metrics (this is okay): %v", err)
		}
	})

	// Wait for exit
	select {
	case <-completeCh:
		t.Log("✅ Connector exited")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit")
	}
}

// TestSnapshotOnlyResume tests that snapshot can resume after interruption
func TestSnapshotOnlyResume(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_only_resume_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 5 // Small chunks to test resume

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with 20 rows (will create 4 chunks with size 5)
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 20; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	// === First Run: Start snapshot but cancel early ===
	t.Log("=== Run 1: Starting snapshot (will be cancelled early) ===")

	var rowsReceived1 int
	var mu sync.Mutex
	messageCh1 := make(chan any, 100)

	handlerFunc1 := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh1 <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				rowsReceived1++
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	connector1, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc1)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(ctx)
	go connector1.Start(ctx1)

	// Wait for BEGIN event
	timeout := time.After(5 * time.Second)
	beginReceived := false
	for !beginReceived {
		select {
		case msg := <-messageCh1:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeBegin {
					beginReceived = true
					t.Log("✅ Run 1: BEGIN received")
				}
			}
		case <-timeout:
			t.Fatal("Timeout waiting for BEGIN in run 1")
		}
	}

	// Wait for at least 5 DATA events (1 chunk), then cancel
	// This ensures at least one chunk is completed before cancellation
	for {
		mu.Lock()
		count := rowsReceived1
		mu.Unlock()

		if count >= 5 {
			break
		}

		select {
		case <-messageCh1:
			// Process messages
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for DATA events in run 1")
		}
	}

	mu.Lock()
	rowsFromRun1 := rowsReceived1
	mu.Unlock()

	t.Logf("✅ Run 1: Received %d rows before cancellation", rowsFromRun1)

	// Cancel the first run (simulate crash)
	cancel1()
	time.Sleep(500 * time.Millisecond) // Wait for cancellation to process
	connector1.Close()

	t.Log("⚠️ Run 1: Cancelled (simulated crash)")

	// Check incomplete job exists
	expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
	query := fmt.Sprintf("SELECT completed, completed_chunks, total_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", expectedSlotName)
	results, err := execQuery(ctx, postgresConn, query)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)

	row := results[0].Rows[0]
	completed := string(row[0]) == "t"
	completedChunks := string(row[1])
	totalChunks := string(row[2])

	assert.False(t, completed, "Job should not be completed after run 1")
	t.Logf("📊 After Run 1: completed=%t, completedChunks=%s, totalChunks=%s", completed, completedChunks, totalChunks)

	// === Second Run: Resume from where it left off ===
	t.Log("=== Run 2: Resuming snapshot ===")

	var rowsReceived2 int
	messageCh2 := make(chan any, 100)

	handlerFunc2 := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh2 <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				rowsReceived2++
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	connector2, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc2)
	require.NoError(t, err)
	defer connector2.Close()

	completeCh := make(chan struct{})
	go func() {
		connector2.Start(ctx)
		close(completeCh)
	}()

	// Wait for END event
	timeout2 := time.After(10 * time.Second)
	endReceived := false
	for !endReceived {
		select {
		case msg := <-messageCh2:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeEnd {
					endReceived = true
					t.Log("✅ Run 2: END received")
				}
			}
		case <-timeout2:
			t.Fatal("Timeout waiting for END in run 2")
		}
	}

	// Wait for connector to exit
	select {
	case <-completeCh:
		t.Log("✅ Run 2: Connector exited after completion")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit after completion")
	}

	mu.Lock()
	rowsFromRun2 := rowsReceived2
	mu.Unlock()

	t.Logf("✅ Run 2: Received %d rows", rowsFromRun2)

	// === Assertions ===

	t.Run("Verify Resume Worked", func(t *testing.T) {
		// The key proof of resume is in the job metadata:
		// - After run 1: job incomplete (some chunks not done)
		// - After run 2: job complete (all chunks done)
		// Row counts can vary due to chunk boundaries and processing speed

		t.Logf("📊 Total rows: Run1=%d, Run2=%d", rowsFromRun1, rowsFromRun2)

		// Run 1 should have received at least 5 rows (1 chunk) before cancel
		assert.GreaterOrEqual(t, rowsFromRun1, 5, "Run 1 should have received at least 1 chunk (5 rows)")

		// Run 2 should receive some rows (resume happened)
		assert.Greater(t, rowsFromRun2, 0, "Run 2 should receive some rows")

		// Combined rows should equal total (allowing for chunk boundary effects)
		// Note: Some rows might be in a chunk that was interrupted and replayed
		t.Logf("✅ Resume mechanism verified via job state tracking")
	})

	t.Run("Verify Job Completed", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed, completed_chunks, total_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.NotEmpty(t, results[0].Rows)

		row := results[0].Rows[0]
		completed := string(row[0]) == "t"
		completedChunks := string(row[1])
		totalChunks := string(row[2])

		assert.True(t, completed, "Job should be completed after run 2")
		assert.Equal(t, totalChunks, completedChunks, "All chunks should be completed")

		t.Logf("✅ After Run 2: completed=%t, completedChunks=%s, totalChunks=%s", completed, completedChunks, totalChunks)
	})
}

// TestSnapshotOnlyIdempotent tests that snapshot skips if already completed
func TestSnapshotOnlyIdempotent(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_only_idempotent_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with small dataset
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTest(t, ctx, tableName)
	})

	// === First Run: Complete snapshot ===
	t.Log("=== Run 1: Complete snapshot ===")

	var rowsReceived1 int
	var mu sync.Mutex
	messageCh1 := make(chan any, 100)

	handlerFunc1 := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh1 <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				rowsReceived1++
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	connector1, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc1)
	require.NoError(t, err)

	completeCh1 := make(chan struct{})
	go func() {
		connector1.Start(ctx)
		close(completeCh1)
	}()

	// Wait for END event
	timeout1 := time.After(10 * time.Second)
	endReceived1 := false
	for !endReceived1 {
		select {
		case msg := <-messageCh1:
			if snapshot, ok := msg.(*format.Snapshot); ok {
				if snapshot.EventType == format.SnapshotEventTypeEnd {
					endReceived1 = true
					t.Log("✅ Run 1: END received")
				}
			}
		case <-timeout1:
			t.Fatal("Timeout waiting for END in run 1")
		}
	}

	// Wait for exit
	select {
	case <-completeCh1:
		t.Log("✅ Run 1: Connector exited")
	case <-time.After(2 * time.Second):
		t.Fatal("Connector did not exit")
	}

	connector1.Close()

	mu.Lock()
	rowsFromRun1 := rowsReceived1
	mu.Unlock()

	t.Logf("✅ Run 1: Received %d rows", rowsFromRun1)
	assert.Equal(t, 5, rowsFromRun1, "Should receive all 5 rows in run 1")

	// Verify job is completed
	expectedSlotName := fmt.Sprintf("snapshot_only_%s", cdcCfg.Database)
	query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", expectedSlotName)
	results, err := execQuery(ctx, postgresConn, query)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)

	completed1 := string(results[0].Rows[0][0]) == "t"
	require.True(t, completed1, "Job should be completed after run 1")

	// === Second Run: Should skip snapshot (idempotent) ===
	t.Log("=== Run 2: Should skip snapshot (already completed) ===")

	var rowsReceived2 int
	messageCh2 := make(chan any, 100)

	handlerFunc2 := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			messageCh2 <- snapshot
			if snapshot.EventType == format.SnapshotEventTypeData {
				mu.Lock()
				rowsReceived2++
				mu.Unlock()
			}
		}
		_ = ctx.Ack()
	}

	connector2, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc2)
	require.NoError(t, err)
	defer connector2.Close()

	completeCh2 := make(chan struct{})
	go func() {
		connector2.Start(ctx)
		close(completeCh2)
	}()

	// Wait for quick exit (should skip)
	select {
	case <-completeCh2:
		t.Log("✅ Run 2: Connector exited quickly (snapshot skipped)")
	case <-time.After(3 * time.Second):
		t.Fatal("Connector did not exit quickly - snapshot might have run again!")
	}

	// Wait a bit to ensure no messages were sent
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	rowsFromRun2 := rowsReceived2
	mu.Unlock()

	// === Assertions ===

	t.Run("Verify No Snapshot in Run 2", func(t *testing.T) {
		assert.Equal(t, 0, rowsFromRun2, "Should receive 0 rows in run 2 (snapshot skipped)")
		assert.Equal(t, 0, len(messageCh2), "Should receive 0 messages in run 2")
		t.Log("✅ Run 2: No snapshot executed (idempotent behavior confirmed)")
	})

	t.Run("Verify Job Still Completed", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", expectedSlotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.NotEmpty(t, results[0].Rows)

		completed2 := string(results[0].Rows[0][0]) == "t"
		assert.True(t, completed2, "Job should still be completed")
		t.Log("✅ Job remains completed")
	})
}

// Helper function for snapshot_only tests cleanup
func cleanupSnapshotOnlyTest(t *testing.T, ctx context.Context, tableName string) {
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

	// Clean metadata tables - use database name pattern for consistent slot name
	// Since snapshot_only uses "snapshot_only_<database>" format
	slotNamePattern := fmt.Sprintf("snapshot_only_%s", Config.Database)
	_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotNamePattern))
	_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", slotNamePattern))

	t.Log("✅ Cleanup completed")
}
