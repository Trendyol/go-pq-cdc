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

// TestSnapshotQueryConditionGlobal tests global query condition:
// - Insert rows with different statuses
// - Apply global condition: status = 'active'
// - Verify only matching rows are captured in snapshot
// - Verify CDC captures all changes regardless of condition
func TestSnapshotQueryConditionGlobal(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_query_condition_global_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_query_condition_global"
	cdcCfg.Publication.Name = "pub_query_condition_global"
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
	cdcCfg.Snapshot.QueryCondition = "status = 'active'"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table with status column
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL
		);
	`, tableName, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Insert test data: some active, some inactive
	testData := []map[string]any{
		{"id": 1, "name": "Alice", "status": "active"},
		{"id": 2, "name": "Bob", "status": "inactive"},
		{"id": 3, "name": "Charlie", "status": "active"},
		{"id": 4, "name": "David", "status": "inactive"},
		{"id": 5, "name": "Eve", "status": "active"},
	}

	for _, data := range testData {
		query := fmt.Sprintf("INSERT INTO %s(id, name, status) VALUES(%d, '%s', '%s')",
			tableName, data["id"], data["name"], data["status"])
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	// Setup: Message collection
	snapshotDataReceived := []map[string]any{}
	cdcInsertReceived := []map[string]any{}

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			if msg.EventType == format.SnapshotEventTypeData {
				messageCh <- msg
			}
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
				snapshotDataReceived = append(snapshotDataReceived, m.Data)
			}
		case <-timeout:
			snapshotCompleted = true
		}
	}

	// Wait a bit for CDC to start
	time.Sleep(500 * time.Millisecond)

	// Insert new data after snapshot (should be captured by CDC regardless of condition)
	t.Log("📝 Inserting new data for CDC test...")
	newData := map[string]any{"id": 100, "name": "NewUser", "status": "inactive"}
	query = fmt.Sprintf("INSERT INTO %s(id, name, status) VALUES(%d, '%s', '%s')",
		tableName, newData["id"], newData["name"], newData["status"])
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Collect CDC insert event
	select {
	case msg := <-messageCh:
		if insertMsg, ok := msg.(*format.Insert); ok {
			cdcInsertReceived = append(cdcInsertReceived, insertMsg.Decoded)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for CDC insert event")
	}

	// === Assertions ===

	t.Run("Verify Snapshot Filtered by Global Condition", func(t *testing.T) {
		// Should only receive rows with status = 'active'
		assert.Len(t, snapshotDataReceived, 3, "Should receive 3 rows with status='active'")

		// Verify all received rows have status='active'
		for _, data := range snapshotDataReceived {
			assert.Equal(t, "active", data["status"], "All snapshot rows should have status='active'")
		}

		// Verify specific rows
		ids := make(map[int32]bool)
		for _, data := range snapshotDataReceived {
			ids[data["id"].(int32)] = true
		}
		assert.True(t, ids[1], "Should include Alice (id=1, active)")
		assert.True(t, ids[3], "Should include Charlie (id=3, active)")
		assert.True(t, ids[5], "Should include Eve (id=5, active)")
		assert.False(t, ids[2], "Should NOT include Bob (id=2, inactive)")
		assert.False(t, ids[4], "Should NOT include David (id=4, inactive)")
	})

	t.Run("Verify CDC Captures All Changes", func(t *testing.T) {
		// CDC should capture all changes regardless of snapshot condition
		assert.Len(t, cdcInsertReceived, 1, "Should receive 1 CDC INSERT event")
		assert.Equal(t, int32(100), cdcInsertReceived[0]["id"])
		assert.Equal(t, "inactive", cdcInsertReceived[0]["status"], "CDC should capture inactive status even though snapshot condition filters it")
	})
}

// TestSnapshotQueryConditionPerTable tests per-table query condition:
// - Two tables with different conditions
// - Verify each table uses its own condition
// - Verify global condition is combined with per-table condition
func TestSnapshotQueryConditionPerTable(t *testing.T) {
	ctx := context.Background()

	table1Name := "snapshot_query_condition_table1"
	table2Name := "snapshot_query_condition_table2"

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_query_condition_per_table"
	cdcCfg.Publication.Name = "pub_query_condition_per_table"
	cdcCfg.Publication.Tables = publication.Tables{
		{
			Name:            table1Name,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
			QueryCondition:  "deleted_at IS NULL",
		},
		{
			Name:            table2Name,
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
			QueryCondition:  "status != 'cancelled'",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ChunkSize = 100
	cdcCfg.Snapshot.QueryCondition = "created_at >= '2024-01-01'"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create table1 with deleted_at column
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			deleted_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
	`, table1Name, table1Name)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Create table2 with status column
	query = fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
	`, table2Name, table2Name)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Insert test data for table1
	query = fmt.Sprintf(`
		INSERT INTO %s(id, name, deleted_at, created_at) VALUES
		(1, 'User1', NULL, '2024-02-01'),
		(2, 'User2', NOW(), '2024-02-01'),
		(3, 'User3', NULL, '2024-02-01')
	`, table1Name)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Insert test data for table2
	query = fmt.Sprintf(`
		INSERT INTO %s(id, name, status, created_at) VALUES
		(1, 'Order1', 'completed', '2024-02-01'),
		(2, 'Order2', 'cancelled', '2024-02-01'),
		(3, 'Order3', 'pending', '2024-02-01')
	`, table2Name)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Setup: Message collection
	snapshotDataByTable := make(map[string][]map[string]any)

	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Snapshot:
			if msg.EventType == format.SnapshotEventTypeData {
				messageCh <- msg
			}
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, table1Name, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
		cleanupSnapshotTest(t, ctx, table2Name, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
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
				tableKey := fmt.Sprintf("%s.%s", m.Schema, m.Table)
				snapshotDataByTable[tableKey] = append(snapshotDataByTable[tableKey], m.Data)
			}
		case <-timeout:
			snapshotCompleted = true
		}
	}

	// === Assertions ===

	t.Run("Verify Table1 Uses Per-Table Condition", func(t *testing.T) {
		tableKey := fmt.Sprintf("public.%s", table1Name)
		data := snapshotDataByTable[tableKey]

		// Should only receive rows where deleted_at IS NULL
		// Note: Global condition (created_at >= '2024-01-01') should also be applied
		// But since all rows have created_at = '2024-02-01', all should pass global condition
		// So we expect 2 rows (id=1 and id=3, both have deleted_at IS NULL)
		assert.GreaterOrEqual(t, len(data), 2, "Should receive at least 2 rows with deleted_at IS NULL")

		// Verify all received rows have deleted_at IS NULL
		for _, row := range data {
			assert.Nil(t, row["deleted_at"], "All snapshot rows should have deleted_at IS NULL")
		}

		// Verify specific rows
		ids := make(map[int32]bool)
		for _, row := range data {
			ids[row["id"].(int32)] = true
		}
		assert.True(t, ids[1], "Should include User1 (id=1, deleted_at IS NULL)")
		assert.True(t, ids[3], "Should include User3 (id=3, deleted_at IS NULL)")
		assert.False(t, ids[2], "Should NOT include User2 (id=2, deleted_at is set)")
	})

	t.Run("Verify Table2 Uses Per-Table Condition", func(t *testing.T) {
		tableKey := fmt.Sprintf("public.%s", table2Name)
		data := snapshotDataByTable[tableKey]

		// Should only receive rows where status != 'cancelled'
		assert.GreaterOrEqual(t, len(data), 2, "Should receive at least 2 rows with status != 'cancelled'")

		// Verify all received rows have status != 'cancelled'
		for _, row := range data {
			assert.NotEqual(t, "cancelled", row["status"], "All snapshot rows should have status != 'cancelled'")
		}

		// Verify specific rows
		ids := make(map[int32]bool)
		for _, row := range data {
			ids[row["id"].(int32)] = true
		}
		assert.True(t, ids[1], "Should include Order1 (id=1, status=completed)")
		assert.True(t, ids[3], "Should include Order3 (id=3, status=pending)")
		assert.False(t, ids[2], "Should NOT include Order2 (id=2, status=cancelled)")
	})
}

