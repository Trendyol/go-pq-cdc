package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
)

// initializeCoordinator sets up the snapshot job as coordinator
// 1. Cleanup any incomplete job (from previous crash)
// 2. Create metadata and chunks
// 3. Export snapshot for workers
func (s *Snapshotter) initializeCoordinator(ctx context.Context, slotName string) error {
	logger.Info("[coordinator] initializing job", "slotName", slotName)

	// Check if job already exists
	existingJob, err := s.loadJob(ctx, slotName)
	if err == nil && existingJob != nil && !existingJob.Completed {
		// Incomplete job found - coordinator crashed during snapshot
		// PROBLEM: Old snapshot transaction is gone, old LSN is stale
		// SOLUTION: Restart snapshot from scratch to maintain consistency
		logger.Warn("[coordinator] incomplete job found, restarting from scratch")
		logger.Info("[coordinator] reason: stale LSN would cause data duplication")

		if err = s.cleanupJob(ctx, slotName); err != nil {
			return errors.Wrap(err, "cleanup incomplete job")
		}

		logger.Info("[coordinator] cleanup complete, starting fresh")
		// Fall through to create fresh metadata
	}

	// Create metadata and chunks (committed to DB)
	if err := s.createMetadata(ctx, slotName); err != nil {
		return errors.Wrap(err, "create metadata")
	}

	// Export snapshot (keeps transaction open for workers)
	if err := s.exportSnapshotTransaction(ctx); err != nil {
		return errors.Wrap(err, "export snapshot")
	}

	logger.Info("[coordinator] initialization complete")
	return nil
}

// createMetadata captures LSN, creates job and chunks metadata
func (s *Snapshotter) createMetadata(ctx context.Context, slotName string) error {
	logger.Info("[coordinator] creating metadata and chunks")

	// Capture LSN before any transaction
	currentLSN, err := s.getCurrentLSN(ctx)
	if err != nil {
		return errors.Wrap(err, "get current LSN")
	}

	// Create job metadata with placeholder snapshot ID
	job := &Job{
		SlotName:    slotName,
		SnapshotID:  "PENDING", // Will be updated after snapshot export
		SnapshotLSN: currentLSN,
		StartedAt:   time.Now().UTC(),
		Completed:   false,
	}

	// Create chunks for each table
	totalChunks := 0
	for _, table := range s.tables {
		chunks, err := s.createTableChunks(ctx, slotName, table)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("create chunks for table %s.%s", table.Schema, table.Name))
		}

		// Save chunks
		for _, chunk := range chunks {
			if err := s.saveChunk(ctx, chunk); err != nil {
				return errors.Wrap(err, "save chunk")
			}
		}

		totalChunks += len(chunks)
		logger.Info("[coordinator] chunks created",
			"table", fmt.Sprintf("%s.%s", table.Schema, table.Name),
			"chunks", len(chunks))
	}

	job.TotalChunks = totalChunks

	// Save job metadata
	if err := s.saveJob(ctx, job); err != nil {
		return errors.Wrap(err, "save job")
	}

	logger.Info("[coordinator] metadata committed", "totalChunks", totalChunks, "lsn", currentLSN.String())
	return nil
}

// exportSnapshotTransaction begins a REPEATABLE READ transaction and exports snapshot ID
// This transaction is kept OPEN for workers to use the same snapshot
func (s *Snapshotter) exportSnapshotTransaction(ctx context.Context) error {
	exportSnapshotConn, err := pq.NewConnection(ctx, s.dsn)
	if err != nil {
		return errors.Wrap(err, "create pg export snapshot connection")
	}
	s.exportSnapshotConn = exportSnapshotConn

	logger.Info("[coordinator] exporting snapshot")

	// Start transaction on snapshot connection (will stay open)
	if err := s.execSQL(ctx, exportSnapshotConn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return errors.Wrap(err, "begin snapshot transaction")
	}

	// Export snapshot
	snapshotID, err := s.exportSnapshot(ctx, exportSnapshotConn)
	if err != nil {
		_ = s.execSQL(ctx, exportSnapshotConn, "ROLLBACK")
		return errors.Wrap(err, "export snapshot")
	}

	logger.Info("[coordinator] snapshot exported", "snapshotID", snapshotID)

	// Update job with real snapshot ID
	if err := s.updateJobSnapshotID(ctx, snapshotID); err != nil {
		_ = s.execSQL(ctx, exportSnapshotConn, "ROLLBACK")
		return errors.Wrap(err, "update job snapshot ID")
	}

	logger.Info("[coordinator] snapshot transaction ready for workers")
	return nil
}

// cleanupJob removes metadata for an incomplete snapshot job
func (s *Snapshotter) cleanupJob(ctx context.Context, slotName string) error {
	return s.retryDBOperation(ctx, func() error {
		// Delete chunks
		chunksQuery := fmt.Sprintf(`
			DELETE FROM %s
			WHERE slot_name = '%s'
		`, chunksTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, chunksQuery); err != nil {
			return errors.Wrap(err, "delete chunks")
		}

		// Delete job
		jobQuery := fmt.Sprintf(`
			DELETE FROM %s
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, jobQuery); err != nil {
			return errors.Wrap(err, "delete job")
		}

		logger.Info("[metadata] job cleaned up", "slotName", slotName)
		return nil
	})
}

// updateJobSnapshotID updates the snapshot ID in the job metadata (for initial creation)
func (s *Snapshotter) updateJobSnapshotID(ctx context.Context, snapshotID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET snapshot_id = '%s'
			WHERE snapshot_id = 'PENDING'
		`, jobTableName, snapshotID)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "update snapshot ID")
		}

		logger.Debug("[metadata] snapshot ID updated", "snapshotID", snapshotID)
		return nil
	})
}

// setupJob initializes tables, handles coordinator election, and returns the job
// Returns: job, isCoordinator flag, error
func (s *Snapshotter) setupJob(ctx context.Context, slotName, instanceID string) (*Job, bool, error) {
	// Initialize tables
	if err := s.initTables(ctx); err != nil {
		return nil, false, errors.Wrap(err, "initialize tables")
	}

	// Check if job already exists and is completed
	existingJob, err := s.loadJob(ctx, slotName)
	if err != nil {
		logger.Debug("[snapshot] no existing job found")
	}
	if existingJob != nil && existingJob.Completed {
		return nil, false, nil // Signal completion
	}

	// Try to become coordinator
	isCoordinator, err := s.tryAcquireCoordinatorLock(ctx, slotName)
	if err != nil {
		return nil, false, errors.Wrap(err, "acquire coordinator lock")
	}

	if isCoordinator {
		logger.Info("[snapshot] elected as coordinator", "instanceID", instanceID)
		if err := s.initializeCoordinator(ctx, slotName); err != nil {
			return nil, false, errors.Wrap(err, "initialize coordinator")
		}
	} else {
		logger.Info("[snapshot] joining as worker", "instanceID", instanceID)
		if err := s.waitForCoordinator(ctx, slotName); err != nil {
			return nil, false, errors.Wrap(err, "wait for coordinator")
		}
	}

	// Load the job
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return nil, false, errors.Wrap(err, "load job")
	}
	if job == nil {
		return nil, false, errors.New("job not found after initialization")
	}

	logger.Info("[snapshot] job loaded", "snapshotID", job.SnapshotID, "lsn", job.SnapshotLSN.String())
	return job, isCoordinator, nil
}

func (s *Snapshotter) initTables(ctx context.Context) error {
	// Create job metadata table
	jobTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			slot_name TEXT PRIMARY KEY,
			snapshot_id TEXT NOT NULL,
			snapshot_lsn TEXT NOT NULL,
			started_at TIMESTAMP NOT NULL,
			completed BOOLEAN DEFAULT FALSE,
			total_chunks INT NOT NULL DEFAULT 0,
			completed_chunks INT NOT NULL DEFAULT 0
		)
	`, jobTableName)

	if err := s.execSQL(ctx, s.metadataConn, jobTableSQL); err != nil {
		return errors.Wrap(err, "create job table")
	}

	// Create chunks work queue table
	chunksTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
	`, chunksTableName)

	if err := s.execSQL(ctx, s.metadataConn, chunksTableSQL); err != nil {
		return errors.Wrap(err, "create chunks table")
	}

	// Create indexes for efficient queries
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chunks_claim ON %s(slot_name, status, claimed_at) WHERE status IN ('pending', 'in_progress')", chunksTableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chunks_status ON %s(slot_name, status)", chunksTableName),
	}

	for _, indexSQL := range indexes {
		if err := s.execSQL(ctx, s.metadataConn, indexSQL); err != nil {
			return errors.Wrap(err, "create index")
		}
	}

	logger.Debug("[metadata] snapshot tables initialized")
	return nil
}

// getCurrentLSN gets the current Write-Ahead Log LSN
func (s *Snapshotter) getCurrentLSN(ctx context.Context) (pq.LSN, error) {
	var lsn pq.LSN

	err := s.retryDBOperation(ctx, func() error {
		results, err := s.execQuery(ctx, s.metadataConn, "SELECT pg_current_wal_lsn()")
		if err != nil {
			return errors.Wrap(err, "execute pg_current_wal_lsn")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
			return errors.New("no LSN returned")
		}

		lsnStr := string(results[0].Rows[0][0])
		lsn, err = pq.ParseLSN(lsnStr)
		if err != nil {
			return errors.Wrap(err, "parse LSN")
		}

		return nil
	})

	return lsn, err
}

// processChunk processes a single chunk
func (s *Snapshotter) processChunk(ctx context.Context, chunk *Chunk, lsn pq.LSN, handler Handler) (int64, error) {
	// Get ORDER BY clause for the table
	table := publication.Table{
		Schema: chunk.TableSchema,
		Name:   chunk.TableName,
	}

	orderByClause, err := s.getOrderByClause(ctx, table)
	if err != nil {
		return 0, errors.Wrap(err, "get order by clause")
	}

	// Build query for this chunk
	query := fmt.Sprintf(
		"SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
		chunk.TableSchema,
		chunk.TableName,
		orderByClause,
		chunk.ChunkSize,
		chunk.ChunkStart,
	)

	logger.Debug("[chunk] executing query", "query", query)

	results, err := s.execQuery(ctx, s.workerConn, query)
	if err != nil {
		return 0, errors.Wrap(err, "execute chunk query")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		// Empty chunk
		return 0, nil
	}

	result := results[0]
	rowCount := int64(len(result.Rows))

	// Process each row
	for i, row := range result.Rows {
		rowData, err := s.parseRow(result.FieldDescriptions, row)
		if err != nil {
			return rowCount, errors.Wrap(err, "parse row")
		}

		isLast := (i == len(result.Rows)-1) && (rowCount < chunk.ChunkSize)

		// Send data event
		if err := handler(&format.Snapshot{
			EventType:  format.SnapshotEventTypeData,
			Table:      chunk.TableName,
			Schema:     chunk.TableSchema,
			Data:       rowData,
			ServerTime: time.Now().UTC(),
			LSN:        lsn,
			IsLast:     isLast,
		}); err != nil {
			return rowCount, errors.Wrap(err, "handle snapshot row")
		}
	}

	return rowCount, nil
}

// getOrderByClause returns the ORDER BY clause for a table
func (s *Snapshotter) getOrderByClause(ctx context.Context, table publication.Table) (string, error) {
	// Try to get primary key columns fallback to ctid
	query := fmt.Sprintf(`
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = '%s.%s'::regclass AND i.indisprimary
		ORDER BY a.attnum
	`, table.Schema, table.Name)

	results, err := s.execQuery(ctx, s.workerConn, query)
	if err != nil {
		return "", errors.Wrap(err, "query primary key")
	}

	if len(results) > 0 && len(results[0].Rows) > 0 {
		// Build ORDER BY from primary key columns
		var columns []string
		for _, row := range results[0].Rows {
			if len(row) > 0 {
				columns = append(columns, string(row[0]))
			}
		}
		if len(columns) > 0 {
			orderBy := strings.Join(columns, ", ")
			logger.Debug("[chunk] using primary key for ordering", "table", table.Name, "orderBy", orderBy)
			return orderBy, nil
		}
	}

	// No primary key, use ctid (PostgreSQL internal row identifier)
	logger.Debug("[chunk] no primary key, using ctid", "table", table.Name)
	return "ctid", nil
}

// createTableChunks divides a table into chunks
func (s *Snapshotter) createTableChunks(ctx context.Context, slotName string, table publication.Table) ([]*Chunk, error) {
	rowCount, err := s.estimateTableRowCount(ctx, table.Schema, table.Name)
	if err != nil {
		logger.Warn("[chunk] failed to estimate row count, using single chunk", "table", table.Name, "error", err)
		rowCount = 0
	}

	chunkSize := s.config.ChunkSize

	// Empty or unknown table: single chunk
	if rowCount == 0 {
		return []*Chunk{
			{
				SlotName:    slotName,
				TableSchema: table.Schema,
				TableName:   table.Name,
				ChunkIndex:  0,
				ChunkStart:  0,
				ChunkSize:   chunkSize,
				Status:      ChunkStatusPending,
			},
		}, nil
	}

	// Calculate number of chunks (ceiling division)
	numChunks := (rowCount + chunkSize - 1) / chunkSize
	chunks := make([]*Chunk, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		// Last chunk may have fewer rows (handles estimate vs actual difference)
		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
		}
		chunks = append(chunks, chunk)
	}

	logger.Debug("[chunk] chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", numChunks)
	return chunks, nil
}

// parseRow converts PostgreSQL row data to map with proper type conversion
func (s *Snapshotter) parseRow(fields []pgconn.FieldDescription, row [][]byte) (map[string]any, error) {
	rowData := make(map[string]any)

	for i, field := range fields {
		if i >= len(row) {
			break
		}

		columnName := field.Name
		columnValue := row[i]

		if columnValue == nil {
			rowData[columnName] = nil
			continue
		}

		// Convert to appropriate type using pgtype
		val, err := s.decodeColumnData(columnValue, field.DataTypeOID)
		if err != nil {
			logger.Debug("[chunk] failed to decode column, using string", "column", columnName, "error", err)
			rowData[columnName] = string(columnValue)
			continue
		}

		rowData[columnName] = val
	}

	return rowData, nil
}

// saveChunk saves a chunk to the database (only INSERT, coordinator creates once)
func (s *Snapshotter) saveChunk(ctx context.Context, chunk *Chunk) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, table_schema, table_name, chunk_index, 
				chunk_start, chunk_size, status
			) VALUES ('%s', '%s', '%s', %d, %d, %d, '%s')
		`, chunksTableName,
			chunk.SlotName,
			chunk.TableSchema,
			chunk.TableName,
			chunk.ChunkIndex,
			chunk.ChunkStart,
			chunk.ChunkSize,
			string(chunk.Status),
		)

		_, err := s.execQuery(ctx, s.metadataConn, query)
		return err
	})
}

// estimateTableRowCount estimates the number of rows in a table
// Note: This is an estimate from pg_class.reltuples, actual count may differ
// Last chunk will handle any difference (may have fewer rows than chunkSize)
func (s *Snapshotter) estimateTableRowCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf("SELECT reltuples::bigint FROM pg_class WHERE oid = '%s.%s'::regclass", schema, table)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return 0, errors.Wrap(err, "estimate table row count")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return 0, nil
	}

	countStr := string(results[0].Rows[0][0])
	if countStr == "" || countStr == "-1" {
		return 0, nil
	}

	var count int64
	if _, err := fmt.Sscanf(countStr, "%d", &count); err != nil {
		return 0, errors.Wrap(err, "parse row count")
	}

	return count, nil
}

// saveJob creates a new job (coordinator only, protected by advisory lock)
func (s *Snapshotter) saveJob(ctx context.Context, job *Job) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, snapshot_id, snapshot_lsn, started_at, 
				completed, total_chunks, completed_chunks
			) VALUES ('%s', '%s', '%s', '%s', %t, %d, %d)
		`, jobTableName,
			job.SlotName,
			job.SnapshotID,
			job.SnapshotLSN.String(),
			job.StartedAt.Format("2006-01-02 15:04:05"),
			job.Completed,
			job.TotalChunks,
			job.CompletedChunks,
		)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "create job")
		}

		logger.Debug("[metadata] job created", "slotName", job.SlotName, "snapshotID", job.SnapshotID)
		return nil
	})
}
