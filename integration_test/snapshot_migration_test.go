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

// TestSnapshotSchemaMigration tests backward compatibility when upgrading from old schema
// Scenario: User has old metadata tables without block_start/block_end columns
// Expected: New connector version should automatically add missing columns
func TestSnapshotSchemaMigration(t *testing.T) {
	ctx := context.Background()

	// Setup test
	tableName := "migration_test_users"
	slotName := "slot_migration_test"
	pubName := "pub_migration_test"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	// Create test table
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		)
	`, tableName, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Insert test data
	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		insertQuery := fmt.Sprintf("INSERT INTO %s (name) VALUES ('%s')", tableName, name)
		err = pgExec(ctx, postgresConn, insertQuery)
		require.NoError(t, err)
	}

	// Step 1: Simulate old schema (without new columns)
	// Create metadata tables manually with OLD schema (missing block_start, block_end, etc.)
	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS cdc_snapshot_chunks;
		DROP TABLE IF EXISTS cdc_snapshot_job;
		CREATE TABLE cdc_snapshot_job (
			slot_name TEXT PRIMARY KEY,
			snapshot_id TEXT NOT NULL,
			snapshot_lsn TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL,
			completed BOOLEAN DEFAULT FALSE,
			total_chunks INT NOT NULL DEFAULT 0,
			completed_chunks INT NOT NULL DEFAULT 0
		)
	`)
	require.NoError(t, err)

	err = pgExec(ctx, postgresConn, `
		CREATE TABLE cdc_snapshot_chunks (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start BIGINT NOT NULL,
			chunk_size BIGINT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			rows_processed BIGINT DEFAULT 0,
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)
	`)
	require.NoError(t, err)

	// Verify old schema (columns should NOT exist)
	results, err := execQuery(ctx, postgresConn, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name = 'cdc_snapshot_chunks' 
			AND column_name = 'block_start'
		)
	`)
	require.NoError(t, err)
	blockStartExists := string(results[0].Rows[0][0]) == "t"
	assert.False(t, blockStartExists, "block_start should NOT exist in old schema")

	// Step 2: Initialize new connector (should trigger migration)
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Publication.Name = pubName
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

	snapshotReceived := false
	messageCh := make(chan any, 100)
	handlerFunc := func(ctx *replication.ListenerContext) {
		if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
			if snapshot.EventType == format.SnapshotEventTypeEnd {
				snapshotReceived = true
			}
			messageCh <- snapshot
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		cleanupSnapshotTest(t, ctx, tableName, slotName, pubName)
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = connector.WaitUntilReady(waitCtx)
	require.NoError(t, err)

	// Wait for snapshot to complete
	timeout := time.After(5 * time.Second)
	for !snapshotReceived {
		select {
		case <-messageCh:
			// Process messages
		case <-timeout:
			t.Fatal("Timeout waiting for snapshot")
		}
	}

	// Step 3: Verify migration applied (new columns should exist)
	results, err = execQuery(ctx, postgresConn, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name = 'cdc_snapshot_chunks' 
			AND column_name = 'block_start'
		)
	`)
	require.NoError(t, err)
	blockStartExistsAfter := string(results[0].Rows[0][0]) == "t"
	assert.True(t, blockStartExistsAfter, "block_start should exist after migration")

	results, err = execQuery(ctx, postgresConn, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name = 'cdc_snapshot_chunks' 
			AND column_name = 'block_end'
		)
	`)
	require.NoError(t, err)
	blockEndExists := string(results[0].Rows[0][0]) == "t"
	assert.True(t, blockEndExists, "block_end should exist after migration")

	results, err = execQuery(ctx, postgresConn, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name = 'cdc_snapshot_chunks' 
			AND column_name = 'is_last_chunk'
		)
	`)
	require.NoError(t, err)
	isLastChunkExists := string(results[0].Rows[0][0]) == "t"
	assert.True(t, isLastChunkExists, "is_last_chunk should exist after migration")

	results, err = execQuery(ctx, postgresConn, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name = 'cdc_snapshot_chunks' 
			AND column_name = 'partition_strategy'
		)
	`)
	require.NoError(t, err)
	partitionStrategyExists := string(results[0].Rows[0][0]) == "t"
	assert.True(t, partitionStrategyExists, "partition_strategy should exist after migration")

	// Step 4: Verify snapshot completed successfully
	assert.True(t, snapshotReceived, "should receive snapshot end event")

	t.Log("✅ Schema migration successful - old schema upgraded to new schema")
}

// TestSnapshotIdempotentMigration tests that running migration multiple times is safe
func TestSnapshotIdempotentMigration(t *testing.T) {
	ctx := context.Background()

	tableName := "migration_test_products"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	// Create test table
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (id SERIAL PRIMARY KEY, name TEXT)
	`, tableName, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	// Insert some test data
	err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO %s (name) VALUES ('test1'), ('test2')", tableName))
	require.NoError(t, err)

	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only" // Use snapshot_only so connector exits after snapshot
	cdcCfg.Snapshot.ChunkSize = 10

	handlerFunc := func(ctx *replication.ListenerContext) {
		_ = ctx.Ack()
	}

	// Helper to run connector and wait for completion
	runConnector := func(runNum int) {
		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err, "Run %d: NewConnector should not fail", runNum)

		completeCh := make(chan struct{})
		go func() {
			connector.Start(ctx)
			close(completeCh)
		}()

		// Wait for snapshot_only to complete (with timeout)
		select {
		case <-completeCh:
			t.Logf("✅ Run %d: Connector completed successfully", runNum)
		case <-time.After(10 * time.Second):
			t.Fatalf("Run %d: Timeout waiting for connector to complete", runNum)
		}

		connector.Close()
	}

	// Run 1: First initialization (creates tables + applies migration)
	runConnector(1)

	// Run 2: Second initialization (should be idempotent - no errors)
	runConnector(2)

	// Run 3: Third initialization (still idempotent)
	runConnector(3)

	// Cleanup
	cleanupSnapshotOnlyTest(t, ctx, tableName)

	// Verify schema is correct
	results, err := execQuery(ctx, postgresConn, `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_name = 'cdc_snapshot_chunks'
	`)
	require.NoError(t, err)
	var columnCount int
	fmt.Sscanf(string(results[0].Rows[0][0]), "%d", &columnCount)
	assert.Greater(t, columnCount, 10, "should have all columns including migrated ones")

	t.Log("✅ Idempotent migration successful - multiple runs cause no issues")
}
