package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForceResnapshotCleansMetadataAndReprocesses tests the forceResnapshot feature:
// 1. Run initial snapshot and verify completion
// 2. Add new data to the table
// 3. Run with forceResnapshot=true and verify it reprocesses ALL data (including new)
// 4. Verify metadata was cleaned and recreated
func TestForceResnapshotCleansMetadataAndReprocesses(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_force_test"
	slotName := "slot_force_resnapshot"
	pubName := "pub_force_resnapshot"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, slotName, pubName)
	})

	// Create table and insert initial data
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	initialData := []map[string]any{
		{"id": 1, "name": "Alice", "age": 25},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Charlie", "age": 35},
	}

	for _, data := range initialData {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, '%s', %d)",
			tableName, data["id"], data["name"], data["age"])
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// === Phase 1: Initial snapshot (forceResnapshot=false) ===
	t.Log("=== Phase 1: Running initial snapshot ===")

	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Publication.Name = pubName
	cdcCfg.Publication.Tables = publication.Tables{
		{Name: tableName, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = config.SnapshotModeInitial
	cdcCfg.Snapshot.ChunkSize = 100
	cdcCfg.Snapshot.ForceResnapshot = false // Normal mode

	firstSnapshotData := []map[string]any{}
	firstSnapshotComplete := false

	connector1, err := cdc.NewConnector(ctx, cdcCfg, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeData {
				firstSnapshotData = append(firstSnapshotData, msg.Data)
			}
			if msg.EventType == format.SnapshotEventTypeEnd {
				firstSnapshotComplete = true
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	go connector1.Start(ctx)

	waitCtx1, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	err = connector1.WaitUntilReady(waitCtx1)
	require.NoError(t, err)

	// Wait for snapshot to complete
	require.Eventually(t, func() bool {
		return firstSnapshotComplete
	}, 5*time.Second, 100*time.Millisecond, "First snapshot should complete")

	connector1.Close()

	t.Logf("✅ First snapshot completed with %d rows", len(firstSnapshotData))
	assert.Len(t, firstSnapshotData, 3, "First snapshot should have 3 rows")

	// Verify job is marked as completed
	query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName)
	results, err := execQuery(ctx, postgresConn, query)
	require.NoError(t, err)
	require.NotEmpty(t, results[0].Rows)
	assert.Equal(t, "t", string(results[0].Rows[0][0]), "Job should be marked as completed")

	// === Phase 2: Add more data ===
	t.Log("=== Phase 2: Adding more data to table ===")

	newData := []map[string]any{
		{"id": 4, "name": "Diana", "age": 40},
		{"id": 5, "name": "Eve", "age": 45},
	}

	for _, data := range newData {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, '%s', %d)",
			tableName, data["id"], data["name"], data["age"])
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	t.Log("✅ Added 2 more rows to table (total: 5)")

	// === Phase 3: Run with forceResnapshot=false (should skip) ===
	t.Log("=== Phase 3: Running with forceResnapshot=false (should skip snapshot) ===")

	cdcCfg2 := cdcCfg
	cdcCfg2.Snapshot.ForceResnapshot = false

	secondSnapshotData := []map[string]any{}
	secondSnapshotStarted := false

	connector2, err := cdc.NewConnector(ctx, cdcCfg2, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeBegin {
				secondSnapshotStarted = true
			}
			if msg.EventType == format.SnapshotEventTypeData {
				secondSnapshotData = append(secondSnapshotData, msg.Data)
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	go connector2.Start(ctx)

	waitCtx2, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	err = connector2.WaitUntilReady(waitCtx2)
	require.NoError(t, err)

	// Give it a moment to potentially start snapshot
	time.Sleep(1 * time.Second)

	connector2.Close()

	assert.False(t, secondSnapshotStarted, "Snapshot should NOT start when forceResnapshot=false and job is completed")
	assert.Empty(t, secondSnapshotData, "No snapshot data should be received")
	t.Log("✅ Snapshot was correctly skipped (job already completed)")

	// === Phase 4: Run with forceResnapshot=true (should reprocess ALL data) ===
	t.Log("=== Phase 4: Running with forceResnapshot=true (should reprocess all data) ===")

	cdcCfg3 := cdcCfg
	cdcCfg3.Snapshot.ForceResnapshot = true // Force resnapshot!

	thirdSnapshotData := []map[string]any{}
	thirdSnapshotComplete := false

	connector3, err := cdc.NewConnector(ctx, cdcCfg3, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeData {
				thirdSnapshotData = append(thirdSnapshotData, msg.Data)
			}
			if msg.EventType == format.SnapshotEventTypeEnd {
				thirdSnapshotComplete = true
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	go connector3.Start(ctx)

	waitCtx3, cancel3 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel3()
	err = connector3.WaitUntilReady(waitCtx3)
	require.NoError(t, err)

	// Wait for snapshot to complete
	require.Eventually(t, func() bool {
		return thirdSnapshotComplete
	}, 5*time.Second, 100*time.Millisecond, "Force resnapshot should complete")

	connector3.Close()

	t.Logf("✅ Force resnapshot completed with %d rows", len(thirdSnapshotData))

	// === Assertions ===
	t.Run("ForceResnapshot processes all data", func(t *testing.T) {
		assert.Len(t, thirdSnapshotData, 5, "Force resnapshot should process ALL 5 rows (3 original + 2 new)")
	})

	t.Run("ForceResnapshot includes new data", func(t *testing.T) {
		// Check that new IDs (4, 5) are in the snapshot
		ids := make([]int32, 0)
		for _, data := range thirdSnapshotData {
			if id, ok := data["id"].(int32); ok {
				ids = append(ids, id)
			}
		}
		assert.Contains(t, ids, int32(4), "Should contain new row with id=4")
		assert.Contains(t, ids, int32(5), "Should contain new row with id=5")
	})

	t.Run("Metadata was recreated", func(t *testing.T) {
		// Verify job metadata exists and is completed
		query := fmt.Sprintf("SELECT completed, total_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results[0].Rows)

		completed := string(results[0].Rows[0][0]) == "t"
		assert.True(t, completed, "New job should be marked as completed")
	})
}

// TestForceResnapshotOnlyMode tests forceResnapshot with snapshot_only mode
func TestForceResnapshotOnlyMode(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_force_only_test"
	slotName := "snapshot_only_" + Config.Database // snapshot_only mode uses this naming

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		// Clean up
		conn, _ := newPostgresConn()
		if conn != nil {
			_ = pgExec(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
			_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotName))
			_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName))
			conn.Close(ctx)
		}
	})

	// Create table and insert data
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)", tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// === Phase 1: Initial snapshot_only ===
	t.Log("=== Phase 1: Running initial snapshot_only ===")

	cdcCfg := Config
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = config.SnapshotModeSnapshotOnly
	cdcCfg.Snapshot.ChunkSize = 100
	cdcCfg.Snapshot.ForceResnapshot = false
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}

	firstCount := 0
	firstComplete := false

	connector1, err := cdc.NewConnector(ctx, cdcCfg, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeData {
				firstCount++
			}
			if msg.EventType == format.SnapshotEventTypeEnd {
				firstComplete = true
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	connector1.Start(ctx) // snapshot_only mode exits after completion
	time.Sleep(3 * time.Second)
	connector1.Close()

	assert.True(t, firstComplete, "First snapshot_only should complete")
	assert.Equal(t, 3, firstCount, "First snapshot should have 3 rows")
	t.Logf("✅ First snapshot_only completed with %d rows", firstCount)

	// === Phase 2: Add more data ===
	for i := 4; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)", tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}
	t.Log("✅ Added 2 more rows")

	// === Phase 3: Run with forceResnapshot=true ===
	t.Log("=== Phase 3: Running snapshot_only with forceResnapshot=true ===")

	cdcCfg2 := cdcCfg
	cdcCfg2.Snapshot.ForceResnapshot = true

	secondCount := 0
	secondComplete := false

	connector2, err := cdc.NewConnector(ctx, cdcCfg2, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeData {
				secondCount++
			}
			if msg.EventType == format.SnapshotEventTypeEnd {
				secondComplete = true
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	connector2.Start(ctx)
	time.Sleep(3 * time.Second)
	connector2.Close()

	assert.True(t, secondComplete, "Force resnapshot should complete")
	assert.Equal(t, 5, secondCount, "Force resnapshot should have ALL 5 rows")
	t.Logf("✅ Force resnapshot completed with %d rows", secondCount)
}

// TestForceResnapshotDoesNotAffectOtherSlots verifies that forceResnapshot
// only cleans metadata for the specific slot, not affecting other connectors
func TestForceResnapshotDoesNotAffectOtherSlots(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_multi_slot_test"
	slotName1 := "slot_force_multi_1"
	slotName2 := "slot_force_multi_2"
	pubName1 := "pub_force_multi_1"
	pubName2 := "pub_force_multi_2"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, slotName1, pubName1)
		// Also clean slot2
		conn, _ := newPostgresConn()
		if conn != nil {
			_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotName2))
			_ = pgExec(ctx, conn, fmt.Sprintf("DELETE FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName2))
			_ = pgExec(ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName2))
			_ = pgExec(ctx, conn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", slotName2, slotName2))
			conn.Close(ctx)
		}
	})

	// Create table and insert data
	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)", tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// === Setup: Run snapshot for BOTH slots ===
	runSnapshot := func(slotName, pubName string) {
		cfg := Config
		cfg.Slot.Name = slotName
		cfg.Publication.Name = pubName
		cfg.Publication.Tables = publication.Tables{
			{Name: tableName, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
		}
		cfg.Snapshot.Enabled = true
		cfg.Snapshot.Mode = config.SnapshotModeInitial
		cfg.Snapshot.ChunkSize = 100
		cfg.Snapshot.ForceResnapshot = false

		complete := false
		connector, err := cdc.NewConnector(ctx, cfg, func(ctx *replication.ListenerContext) {
			if msg, ok := ctx.Message.(*format.Snapshot); ok {
				if msg.EventType == format.SnapshotEventTypeEnd {
					complete = true
				}
			}
			_ = ctx.Ack()
		})
		require.NoError(t, err)

		go connector.Start(ctx)
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_ = connector.WaitUntilReady(waitCtx)

		require.Eventually(t, func() bool { return complete }, 5*time.Second, 100*time.Millisecond)
		connector.Close()
	}

	t.Log("=== Running initial snapshot for slot1 ===")
	runSnapshot(slotName1, pubName1)

	t.Log("=== Running initial snapshot for slot2 ===")
	runSnapshot(slotName2, pubName2)

	// Verify both jobs exist and are completed
	for _, slot := range []string{slotName1, slotName2} {
		query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", slot)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results[0].Rows, "Job for %s should exist", slot)
		assert.Equal(t, "t", string(results[0].Rows[0][0]), "Job for %s should be completed", slot)
	}
	t.Log("✅ Both slots have completed snapshots")

	// === Force resnapshot on slot1 ONLY ===
	t.Log("=== Running forceResnapshot on slot1 ONLY ===")

	cfg := Config
	cfg.Slot.Name = slotName1
	cfg.Publication.Name = pubName1
	cfg.Publication.Tables = publication.Tables{
		{Name: tableName, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
	}
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Mode = config.SnapshotModeInitial
	cfg.Snapshot.ChunkSize = 100
	cfg.Snapshot.ForceResnapshot = true // Force!

	complete := false
	connector, err := cdc.NewConnector(ctx, cfg, func(ctx *replication.ListenerContext) {
		if msg, ok := ctx.Message.(*format.Snapshot); ok {
			if msg.EventType == format.SnapshotEventTypeEnd {
				complete = true
			}
		}
		_ = ctx.Ack()
	})
	require.NoError(t, err)

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = connector.WaitUntilReady(waitCtx)

	require.Eventually(t, func() bool { return complete }, 5*time.Second, 100*time.Millisecond)
	connector.Close()

	t.Log("✅ Force resnapshot on slot1 completed")

	// === Verify slot2 was NOT affected ===
	t.Run("Slot2 metadata should be unaffected", func(t *testing.T) {
		query := fmt.Sprintf("SELECT completed FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName2)
		results, err := execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		require.NotEmpty(t, results[0].Rows, "Job for slot2 should still exist")
		assert.Equal(t, "t", string(results[0].Rows[0][0]), "Job for slot2 should still be completed")

		// Verify chunks still exist for slot2
		query = fmt.Sprintf("SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotName2)
		results, err = execQuery(ctx, postgresConn, query)
		require.NoError(t, err)
		count := string(results[0].Rows[0][0])
		assert.NotEqual(t, "0", count, "Chunks for slot2 should still exist")

		t.Logf("✅ Slot2 metadata verified: job exists, chunks count=%s", count)
	})
}
