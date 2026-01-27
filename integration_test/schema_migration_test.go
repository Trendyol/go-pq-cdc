package integration

import (
	"context"
	"fmt"
	"sync"
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

const (
	jobTableName                  = "cdc_snapshot_job"
	chunksTableName               = "cdc_snapshot_chunks"
	testInsertQuery               = "INSERT INTO %s(id, name, age) VALUES(1, 'Test', 25)"
	snapshotTimeoutMsg            = "Timeout waiting for snapshot to complete"
	deleteFromTableBySlotQuery    = "DELETE FROM %s WHERE slot_name = '%s'"
	dropTableIfExistsQuery        = "DROP TABLE IF EXISTS %s"
	snapshotOnlySlotNamePattern   = "snapshot_only_%s"
)

func TestSchemaMigrationFreshInstall(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_fresh_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

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
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

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
			t.Fatal(snapshotTimeoutMsg)
		}
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	jobExists, err := tableExists(ctx, conn, jobTableName)
	require.NoError(t, err)
	assert.True(t, jobExists, "job table should exist")

	chunksExists, err := tableExists(ctx, conn, chunksTableName)
	require.NoError(t, err)
	assert.True(t, chunksExists, "chunks table should exist")

	jobColumns, err := getTableColumnNames(ctx, conn, jobTableName)
	require.NoError(t, err)
	assert.Contains(t, jobColumns, "slot_name")
	assert.Contains(t, jobColumns, "snapshot_id")
	assert.Contains(t, jobColumns, "completed")

	chunksColumns, err := getTableColumnNames(ctx, conn, chunksTableName)
	require.NoError(t, err)
	assert.Contains(t, chunksColumns, "partition_strategy")
	assert.Contains(t, chunksColumns, "block_start")
	assert.Contains(t, chunksColumns, "rows_processed")
}

func TestSchemaMigrationMissingColumn(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_missing_col_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createLegacyChunksTableMissingRowsProcessed(ctx, postgresConn)
	require.NoError(t, err)

	err = createFullJobTable(ctx, postgresConn)
	require.NoError(t, err)

	existsBefore, err := columnExists(ctx, postgresConn, chunksTableName, "rows_processed")
	require.NoError(t, err)
	assert.False(t, existsBefore, "rows_processed should not exist before migration")

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

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
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

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
			t.Fatal(snapshotTimeoutMsg)
		}
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	existsAfter, err := columnExists(ctx, conn, chunksTableName, "rows_processed")
	require.NoError(t, err)
	assert.True(t, existsAfter, "rows_processed should exist after migration")
}

func TestSchemaMigrationMultipleMissingColumns(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_multi_col_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createMinimalChunksTable(ctx, postgresConn)
	require.NoError(t, err)

	err = createFullJobTable(ctx, postgresConn)
	require.NoError(t, err)

	missingColumns := []string{"block_start", "block_end", "is_last_chunk", "partition_strategy", "rows_processed"}
	for _, col := range missingColumns {
		exists, err := columnExists(ctx, postgresConn, chunksTableName, col)
		require.NoError(t, err)
		assert.False(t, exists, "%s should not exist before migration", col)
	}

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

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
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

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
			t.Fatal(snapshotTimeoutMsg)
		}
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, col := range missingColumns {
		exists, err := columnExists(ctx, conn, chunksTableName, col)
		require.NoError(t, err)
		assert.True(t, exists, "%s should exist after migration", col)
	}
}

func TestSchemaMigrationExtraColumnsIgnored(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_extra_col_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createChunksTableWithExtraColumn(ctx, postgresConn)
	require.NoError(t, err)

	err = createFullJobTable(ctx, postgresConn)
	require.NoError(t, err)

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

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
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

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
			t.Fatal(snapshotTimeoutMsg)
		}
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	extraColExists, err := columnExists(ctx, conn, chunksTableName, "future_column")
	require.NoError(t, err)
	assert.True(t, extraColExists, "extra column should still exist")
}

func TestSchemaMigrationIdempotentExecution(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_idempotent_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	runSnapshot := func(runNum int) {
		messageCh := make(chan any, 100)
		handlerFunc := func(ctx *replication.ListenerContext) {
			if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
				messageCh <- snapshot
			}
			_ = ctx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err, "Run %d: connector creation should succeed", runNum)

		go func() {
			connector.Start(ctx)
		}()

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
				t.Fatalf("Run %d: Timeout waiting for snapshot to complete", runNum)
			}
		}

		connector.Close()

		conn, err := newPostgresConn()
		require.NoError(t, err)
		defer conn.Close(ctx)
		slotName := fmt.Sprintf(snapshotOnlySlotNamePattern, Config.Database)
		_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, chunksTableName, slotName))
		_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, jobTableName, slotName))
	}

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	runSnapshot(1)
	runSnapshot(2)
	runSnapshot(3)

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	jobExists, err := tableExists(ctx, conn, jobTableName)
	require.NoError(t, err)
	assert.True(t, jobExists)

	chunksExists, err := tableExists(ctx, conn, chunksTableName)
	require.NoError(t, err)
	assert.True(t, chunksExists)
}

func TestSchemaMigrationConcurrentStartup(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_concurrent_test"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createLegacyChunksTableMissingRowsProcessed(ctx, postgresConn)
	require.NoError(t, err)

	err = createFullJobTable(ctx, postgresConn)
	require.NoError(t, err)

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)", tableName, i, i, 20+i)
		err = pgExec(ctx, postgresConn, query)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	instanceCount := 3
	var wg sync.WaitGroup
	errCh := make(chan error, instanceCount)

	for i := 0; i < instanceCount; i++ {
		wg.Add(1)
		go func(instanceNum int) {
			defer wg.Done()

			cdcCfg := Config
			cdcCfg.Snapshot.Tables = publication.Tables{
				{Name: tableName, Schema: "public"},
			}
			cdcCfg.Snapshot.Enabled = true
			cdcCfg.Snapshot.Mode = "snapshot_only"
			cdcCfg.Snapshot.ChunkSize = 100
			cdcCfg.Snapshot.InstanceID = fmt.Sprintf("instance-%d", instanceNum)

			messageCh := make(chan any, 100)
			handlerFunc := func(ctx *replication.ListenerContext) {
				if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
					messageCh <- snapshot
				}
				_ = ctx.Ack()
			}

			connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
			if err != nil {
				errCh <- fmt.Errorf("instance %d: connector creation failed: %w", instanceNum, err)
				return
			}

			go func() {
				connector.Start(ctx)
			}()

			timeout := time.After(15 * time.Second)
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
					errCh <- fmt.Errorf("instance %d: timeout", instanceNum)
					connector.Close()
					return
				}
			}

			connector.Close()
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Concurrent startup error: %v", err)
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	exists, err := columnExists(ctx, conn, chunksTableName, "rows_processed")
	require.NoError(t, err)
	assert.True(t, exists, "rows_processed column should exist after concurrent migrations")
}

func dropMetadataTables(ctx context.Context, conn pq.Connection) error {
	_ = pgExec(ctx, conn, fmt.Sprintf(dropTableIfExistsQuery, chunksTableName))
	_ = pgExec(ctx, conn, fmt.Sprintf(dropTableIfExistsQuery, jobTableName))
	return nil
}

func createFullJobTable(ctx context.Context, conn pq.Connection) error {
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			slot_name TEXT PRIMARY KEY,
			snapshot_id TEXT NOT NULL,
			snapshot_lsn TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL,
			completed BOOLEAN DEFAULT FALSE,
			total_chunks INT NOT NULL DEFAULT 0,
			completed_chunks INT NOT NULL DEFAULT 0
		)
	`, jobTableName)
	return pgExec(ctx, conn, query)
}

func createLegacyChunksTableMissingRowsProcessed(ctx context.Context, conn pq.Connection) error {
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start BIGINT NOT NULL,
			chunk_size BIGINT NOT NULL,
			range_start BIGINT,
			range_end BIGINT,
			block_start BIGINT,
			block_end BIGINT,
			is_last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
			partition_strategy TEXT NOT NULL DEFAULT 'offset',
			status TEXT NOT NULL DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)
	`, chunksTableName)
	return pgExec(ctx, conn, query)
}

func createMinimalChunksTable(ctx context.Context, conn pq.Connection) error {
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start BIGINT NOT NULL,
			chunk_size BIGINT NOT NULL,
			range_start BIGINT,
			range_end BIGINT,
			status TEXT NOT NULL DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)
	`, chunksTableName)
	return pgExec(ctx, conn, query)
}

func createChunksTableWithExtraColumn(ctx context.Context, conn pq.Connection) error {
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start BIGINT NOT NULL,
			chunk_size BIGINT NOT NULL,
			range_start BIGINT,
			range_end BIGINT,
			block_start BIGINT,
			block_end BIGINT,
			is_last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
			partition_strategy TEXT NOT NULL DEFAULT 'offset',
			status TEXT NOT NULL DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			rows_processed BIGINT DEFAULT 0,
			future_column TEXT DEFAULT 'future_value',
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)
	`, chunksTableName)
	return pgExec(ctx, conn, query)
}

func tableExists(ctx context.Context, conn pq.Connection, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = '%s'
		)
	`, tableName)

	results, err := execQuery(ctx, conn, query)
	if err != nil {
		return false, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	return string(results[0].Rows[0][0]) == "t", nil
}

func columnExists(ctx context.Context, conn pq.Connection, tableName, columnName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.columns 
			WHERE table_schema = 'public' 
			AND table_name = '%s'
			AND column_name = '%s'
		)
	`, tableName, columnName)

	results, err := execQuery(ctx, conn, query)
	if err != nil {
		return false, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	return string(results[0].Rows[0][0]) == "t", nil
}

func getTableColumnNames(ctx context.Context, conn pq.Connection, tableName string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = '%s'
		ORDER BY ordinal_position
	`, tableName)

	results, err := execQuery(ctx, conn, query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}

	columns := make([]string, 0, len(results[0].Rows))
	for _, row := range results[0].Rows {
		if len(row) > 0 {
			columns = append(columns, string(row[0]))
		}
	}

	return columns, nil
}

func getColumnDataType(ctx context.Context, conn pq.Connection, tableName, columnName string) (string, error) {
	query := fmt.Sprintf(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = '%s' AND column_name = '%s'
	`, tableName, columnName)

	results, err := execQuery(ctx, conn, query)
	if err != nil {
		return "", err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return "", nil
	}

	return string(results[0].Rows[0][0]), nil
}

func createChunksTableWithWrongType(ctx context.Context, conn pq.Connection) error {
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			slot_name TEXT NOT NULL,
			table_schema TEXT NOT NULL,
			table_name TEXT NOT NULL,
			chunk_index INT NOT NULL,
			chunk_start INT NOT NULL,
			chunk_size INT NOT NULL,
			range_start BIGINT,
			range_end BIGINT,
			block_start BIGINT,
			block_end BIGINT,
			is_last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
			partition_strategy TEXT NOT NULL DEFAULT 'offset',
			status TEXT NOT NULL DEFAULT 'pending',
			claimed_by TEXT,
			claimed_at TIMESTAMP,
			heartbeat_at TIMESTAMP,
			completed_at TIMESTAMP,
			rows_processed BIGINT DEFAULT 0,
			UNIQUE(slot_name, table_schema, table_name, chunk_index)
		)
	`, chunksTableName)
	return pgExec(ctx, conn, query)
}

func TestSchemaMigrationTypeChange(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_type_change_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createChunksTableWithWrongType(ctx, postgresConn)
	require.NoError(t, err)

	err = createFullJobTable(ctx, postgresConn)
	require.NoError(t, err)

	chunkStartTypeBefore, err := getColumnDataType(ctx, postgresConn, chunksTableName, "chunk_start")
	require.NoError(t, err)
	assert.Equal(t, "integer", chunkStartTypeBefore, "chunk_start should be integer before migration")

	chunkSizeTypeBefore, err := getColumnDataType(ctx, postgresConn, chunksTableName, "chunk_size")
	require.NoError(t, err)
	assert.Equal(t, "integer", chunkSizeTypeBefore, "chunk_size should be integer before migration")

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

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
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	go func() {
		connector.Start(ctx)
	}()

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
			t.Fatal(snapshotTimeoutMsg)
		}
	}

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	chunkStartTypeAfter, err := getColumnDataType(ctx, conn, chunksTableName, "chunk_start")
	require.NoError(t, err)
	assert.Equal(t, "bigint", chunkStartTypeAfter, "chunk_start should be bigint after migration")

	chunkSizeTypeAfter, err := getColumnDataType(ctx, conn, chunksTableName, "chunk_size")
	require.NoError(t, err)
	assert.Equal(t, "bigint", chunkSizeTypeAfter, "chunk_size should be bigint after migration")
}

func TestSchemaMigrationTypeChangeSameType(t *testing.T) {
	ctx := context.Background()

	tableName := "schema_migration_same_type_test"
	cdcCfg := Config
	cdcCfg.Snapshot.Tables = publication.Tables{
		{Name: tableName, Schema: "public"},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	err = dropMetadataTables(ctx, postgresConn)
	require.NoError(t, err)

	err = createTestTable(ctx, postgresConn, tableName)
	require.NoError(t, err)

	query := fmt.Sprintf(testInsertQuery, tableName)
	err = pgExec(ctx, postgresConn, query)
	require.NoError(t, err)

	runSnapshot := func() {
		messageCh := make(chan any, 100)
		handlerFunc := func(ctx *replication.ListenerContext) {
			if snapshot, ok := ctx.Message.(*format.Snapshot); ok {
				messageCh <- snapshot
			}
			_ = ctx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err)

		go func() {
			connector.Start(ctx)
		}()

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
				t.Fatal(snapshotTimeoutMsg)
			}
		}

		connector.Close()

		conn, err := newPostgresConn()
		require.NoError(t, err)
		defer conn.Close(ctx)
		slotName := fmt.Sprintf(snapshotOnlySlotNamePattern, Config.Database)
		_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, chunksTableName, slotName))
		_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, jobTableName, slotName))
	}

	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSchemaMigrationTest(t, ctx, tableName)
	})

	runSnapshot()

	conn, err := newPostgresConn()
	require.NoError(t, err)
	defer conn.Close(ctx)

	chunkStartType, err := getColumnDataType(ctx, conn, chunksTableName, "chunk_start")
	require.NoError(t, err)
	assert.Equal(t, "bigint", chunkStartType, "chunk_start should remain bigint")

	runSnapshot()

	chunkStartTypeAfter, err := getColumnDataType(ctx, conn, chunksTableName, "chunk_start")
	require.NoError(t, err)
	assert.Equal(t, "bigint", chunkStartTypeAfter, "chunk_start should still be bigint after second run")
}

func cleanupSchemaMigrationTest(t *testing.T, ctx context.Context, tableName string) {
	conn, err := newPostgresConn()
	if err != nil {
		t.Logf("Warning: Failed to create cleanup connection: %v", err)
		return
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf(dropTableIfExistsQuery, tableName)
	if err := pgExec(ctx, conn, query); err != nil {
		t.Logf("Warning: Failed to drop table: %v", err)
	}

	slotName := fmt.Sprintf(snapshotOnlySlotNamePattern, Config.Database)
	_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, chunksTableName, slotName))
	_ = pgExec(ctx, conn, fmt.Sprintf(deleteFromTableBySlotQuery, jobTableName, slotName))

	t.Log("Cleanup completed")
}
