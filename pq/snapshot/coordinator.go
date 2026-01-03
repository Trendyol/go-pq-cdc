package snapshot

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
)

type primaryKeyColumn struct {
	Name     string
	DataType string
}

// initializeCoordinator sets up the snapshot job as coordinator
// 1. Cleanup any incomplete job (from previous crash)
// 2. Create metadata and chunks
// 3. Export snapshot for workers
func (s *Snapshotter) initializeCoordinator(ctx context.Context, slotName string, currentLSN pq.LSN) error {
	logger.Debug("[coordinator] initializing job", "slotName", slotName)

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
	if err := s.createMetadata(ctx, slotName, currentLSN); err != nil {
		return errors.Wrap(err, "create metadata")
	}

	// Export snapshot (keeps transaction open for workers)
	if err := s.exportSnapshotTransaction(ctx); err != nil {
		return errors.Wrap(err, "export snapshot")
	}

	logger.Debug("[coordinator] initialization complete")
	return nil
}

// createMetadata captures LSN, creates job and chunks metadata
func (s *Snapshotter) createMetadata(ctx context.Context, slotName string, currentLSN pq.LSN) error {
	logger.Info("[coordinator] creating metadata and chunks")

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
		chunks := s.createTableChunks(ctx, slotName, table)

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

	// Disable timeouts for snapshot transaction
	if err := s.execSQL(ctx, exportSnapshotConn, "SET idle_in_transaction_session_timeout = 0"); err != nil {
		return errors.Wrap(err, "set idle timeout")
	}

	if err := s.execSQL(ctx, exportSnapshotConn, "SET statement_timeout = 0"); err != nil {
		return errors.Wrap(err, "set statement timeout")
	}

	// Start transaction on snapshot connection (will stay open)
	if err := s.execSQL(ctx, exportSnapshotConn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return errors.Wrap(err, "begin snapshot transaction")
	}

	go s.snapshotTransactionKeepalive(ctx, exportSnapshotConn)

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

func (s *Snapshotter) snapshotTransactionKeepalive(ctx context.Context, conn pq.Connection) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Keepalive: SELECT 1
			_ = s.execSQL(context.Background(), conn, "SELECT 1")
		}
	}
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
func (s *Snapshotter) setupJob(ctx context.Context, slotName, instanceID string) (bool, error) {
	if err := s.initTables(ctx); err != nil {
		return false, errors.Wrap(err, "initialize tables")
	}

	lockAcquired, err := s.tryAcquireCoordinatorLock(ctx, slotName)
	if err != nil {
		return false, errors.Wrap(err, "acquire coordinator lock")
	}

	if lockAcquired {
		currentLSN, err := s.getCurrentLSN(ctx)
		if err != nil {
			return false, errors.Wrap(err, "get current LSN")
		}

		logger.Debug("[snapshot] elected as coordinator", "instanceID", instanceID)
		if err := s.initializeCoordinator(ctx, slotName, currentLSN); err != nil {
			return false, errors.Wrap(err, "initialize coordinator")
		}
		return true, nil
	}

	logger.Debug("[snapshot] joining as worker", "instanceID", instanceID)
	if err = s.waitForCoordinator(ctx, slotName); err != nil {
		return false, errors.Wrap(err, "wait for coordinator")
	}

	return false, nil
}

//nolint:funlen
func (s *Snapshotter) initTables(ctx context.Context) error {
	// Check if job table exists
	jobTableExists, err := s.tableExists(ctx, jobTableName)
	if err != nil {
		return errors.Wrap(err, "check job table existence")
	}

	if !jobTableExists {
		jobTableSQL := fmt.Sprintf(`
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

		if err := s.execSQL(ctx, s.metadataConn, jobTableSQL); err != nil {
			return errors.Wrap(err, "create job table")
		}
		logger.Debug("[metadata] job table created")
	} else {
		logger.Debug("[metadata] job table already exists, skipping creation")
	}

	// Check if chunks table exists
	chunksTableExists, err := s.tableExists(ctx, chunksTableName)
	if err != nil {
		return errors.Wrap(err, "check chunks table existence")
	}

	if !chunksTableExists {
		chunksTableSQL := fmt.Sprintf(`
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
				last_pk BIGINT,
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
		logger.Debug("[metadata] chunks table created")
	} else {
		logger.Debug("[metadata] chunks table already exists, skipping creation")
	}

	// Create indexes for efficient queries
	indexes := map[string]string{
		"idx_chunks_claim":  fmt.Sprintf("CREATE INDEX idx_chunks_claim ON %s(slot_name, status, claimed_at) WHERE status IN ('pending', 'in_progress')", chunksTableName),
		"idx_chunks_status": fmt.Sprintf("CREATE INDEX idx_chunks_status ON %s(slot_name, status)", chunksTableName),
	}

	for indexName, indexSQL := range indexes {
		indexExists, err := s.indexExists(ctx, indexName)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("check index %s existence", indexName))
		}

		if !indexExists {
			if err := s.execSQL(ctx, s.metadataConn, indexSQL); err != nil {
				return errors.Wrap(err, fmt.Sprintf("create index %s", indexName))
			}
			logger.Debug("[metadata] index created", "index", indexName)
		} else {
			logger.Debug("[metadata] index already exists, skipping creation", "index", indexName)
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

// ChunkResult contains the result of processing a chunk
type ChunkResult struct {
	RowCount int64
	LastPK   *int64 // Last processed primary key value (for keyset pagination)
}

// processChunk processes a single chunk
func (s *Snapshotter) processChunk(ctx context.Context, conn pq.Connection, chunk *Chunk, lsn pq.LSN, handler Handler) (ChunkResult, error) {
	// Get ORDER BY clause for the table
	table := publication.Table{
		Schema: chunk.TableSchema,
		Name:   chunk.TableName,
	}

	orderByClause, pkColumns, err := s.getOrderByClause(ctx, conn, table)
	if err != nil {
		return ChunkResult{}, errors.Wrap(err, "get order by clause")
	}

	// Build query for this chunk
	query := s.buildChunkQuery(chunk, orderByClause, pkColumns)

	logger.Debug("[chunk] executing query", "query", query)

	results, err := s.execQuery(ctx, conn, query)
	if err != nil {
		return ChunkResult{}, errors.Wrap(err, "execute chunk query")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		// Empty chunk
		return ChunkResult{RowCount: 0, LastPK: nil}, nil
	}

	result := results[0]
	rowCount := int64(len(result.Rows))

	chunkTime := time.Now().UTC()

	// Track last PK value for keyset pagination
	var lastPK *int64
	pkColumnIndex := -1

	// Find PK column index for keyset mode
	if chunk.isKeysetMode() && len(pkColumns) == 1 {
		for i, field := range result.FieldDescriptions {
			if field.Name == pkColumns[0] {
				pkColumnIndex = i
				break
			}
		}
	}

	// Process each row
	for i, row := range result.Rows {
		rowData := s.parseRow(result.FieldDescriptions, row)

		isLast := (i == len(result.Rows)-1) && (rowCount < chunk.ChunkSize)

		// Send data event
		_ = handler(&format.Snapshot{
			EventType:  format.SnapshotEventTypeData,
			Table:      chunk.TableName,
			Schema:     chunk.TableSchema,
			Data:       rowData,
			ServerTime: chunkTime,
			LSN:        lsn,
			IsLast:     isLast,
		})

		// Track last PK for keyset pagination
		if pkColumnIndex >= 0 && i == len(result.Rows)-1 {
			if pkValue, err := strconv.ParseInt(string(row[pkColumnIndex]), 10, 64); err == nil {
				lastPK = &pkValue
			}
		}
	}

	return ChunkResult{RowCount: rowCount, LastPK: lastPK}, nil
}

func (s *Snapshotter) buildChunkQuery(chunk *Chunk, orderByClause string, pkColumns []string) string {
	// Case 1: Keyset pagination mode (RangeStart set, RangeEnd nil)
	// Used for sparse primary key tables (snowflake IDs, etc.)
	if chunk.isKeysetMode() && len(pkColumns) == 1 {
		pkColumn := pkColumns[0]

		return fmt.Sprintf(
			`SELECT * FROM "%s"."%s" WHERE "%s" > %d ORDER BY %s LIMIT %d`,
			chunk.TableSchema,
			chunk.TableName,
			pkColumn,
			*chunk.RangeStart,
			orderByClause,
			chunk.ChunkSize,
		)
	}

	// Case 2: Range-based mode (both RangeStart and RangeEnd set)
	// Used for dense primary key tables
	if chunk.hasRangeBounds() && len(pkColumns) == 1 {
		pkColumn := pkColumns[0]

		return fmt.Sprintf(
			`SELECT * FROM "%s"."%s" WHERE "%s" >= %d AND "%s" <= %d ORDER BY %s LIMIT %d`,
			chunk.TableSchema,
			chunk.TableName,
			pkColumn,
			*chunk.RangeStart,
			pkColumn,
			*chunk.RangeEnd,
			orderByClause,
			chunk.ChunkSize,
		)
	}

	// Case 3: Offset-based mode (fallback for composite PKs or no PK)
	return fmt.Sprintf(
		`SELECT * FROM "%s"."%s" ORDER BY %s LIMIT %d OFFSET %d`,
		chunk.TableSchema,
		chunk.TableName,
		orderByClause,
		chunk.ChunkSize,
		chunk.ChunkStart,
	)
}

// getOrderByClause returns the ORDER BY clause for a table
func (s *Snapshotter) getOrderByClause(ctx context.Context, conn pq.Connection, table publication.Table) (string, []string, error) {
	if entry, ok := s.loadOrderByCache(table); ok {
		return entry.clause, cloneStringSlice(entry.columns), nil
	}

	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, conn, table)
	if err != nil {
		return "", nil, err
	}

	if len(columns) > 0 {
		var columnNames []string
		var quotedColumnNames []string
		for _, column := range columns {
			columnNames = append(columnNames, column.Name)
			quotedColumnNames = append(quotedColumnNames, fmt.Sprintf(`"%s"`, column.Name))
		}
		orderBy := strings.Join(quotedColumnNames, ", ")
		logger.Debug("[chunk] using primary key for ordering", "table", table.Name, "orderBy", orderBy)
		s.storeOrderByCache(table, orderBy, columnNames)
		return orderBy, columnNames, nil
	}

	// No primary key, use ctid (PostgreSQL internal row identifier)
	logger.Debug("[chunk] no primary key, using ctid", "table", table.Name)
	s.storeOrderByCache(table, "ctid", nil)
	return "ctid", nil, nil
}

func (s *Snapshotter) loadOrderByCache(table publication.Table) (orderByCacheEntry, bool) {
	key := s.orderByCacheKey(table)

	s.orderByMu.RLock()
	entry, ok := s.orderByCache[key]
	s.orderByMu.RUnlock()

	if !ok {
		return orderByCacheEntry{}, false
	}

	return orderByCacheEntry{
		clause:  entry.clause,
		columns: cloneStringSlice(entry.columns),
	}, true
}

func (s *Snapshotter) storeOrderByCache(table publication.Table, clause string, columns []string) {
	key := s.orderByCacheKey(table)

	s.orderByMu.Lock()
	s.orderByCache[key] = orderByCacheEntry{
		clause:  clause,
		columns: cloneStringSlice(columns),
	}
	s.orderByMu.Unlock()
}

func (s *Snapshotter) orderByCacheKey(table publication.Table) string {
	return fmt.Sprintf("%s.%s", table.Schema, table.Name)
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// createTableChunks divides a table into chunks
func (s *Snapshotter) createTableChunks(ctx context.Context, slotName string, table publication.Table) []*Chunk {
	pkColumn, ok, err := s.getSingleIntegerPrimaryKey(ctx, table)
	if err != nil {
		logger.Warn("[chunk] failed to inspect primary key", "table", table.Name, "error", err)
	}

	switch s.config.ChunkingMode {
	case config.SnapshotChunkingModeRange:
		if !ok {
			logger.Warn("[chunk] chunking mode override requires single integer PK, falling back to OFFSET", "table", table.Name, "mode", s.config.ChunkingMode)
			return s.createOffsetChunks(ctx, slotName, table)
		}
		if rangeChunks := s.createRangeChunks(ctx, slotName, table, pkColumn); len(rangeChunks) > 0 {
			return rangeChunks
		}
		logger.Warn("[chunk] range chunking unavailable under override, falling back to OFFSET", "table", table.Name)
		return s.createOffsetChunks(ctx, slotName, table)

	case config.SnapshotChunkingModeKeyset:
		if !ok {
			logger.Warn("[chunk] chunking mode override requires single integer PK, falling back to OFFSET", "table", table.Name, "mode", s.config.ChunkingMode)
			return s.createOffsetChunks(ctx, slotName, table)
		}
		minValue, _, boundsOK, boundsErr := s.getPrimaryKeyBounds(ctx, table, pkColumn)
		if boundsErr != nil || !boundsOK {
			logger.Warn("[chunk] failed to read primary key bounds for keyset override, falling back to OFFSET", "table", table.Name, "error", boundsErr)
			return s.createOffsetChunks(ctx, slotName, table)
		}
		rowCount, rcErr := s.getTableRawCount(ctx, table.Schema, table.Name)
		if rcErr != nil {
			logger.Warn("[chunk] failed to get row count for keyset override, falling back to OFFSET", "table", table.Name, "error", rcErr)
			return s.createOffsetChunks(ctx, slotName, table)
		}
		if rowCount == 0 {
			return []*Chunk{
				{
					SlotName:    slotName,
					TableSchema: table.Schema,
					TableName:   table.Name,
					ChunkIndex:  0,
					ChunkStart:  0,
					ChunkSize:   s.config.ChunkSize,
					Status:      ChunkStatusPending,
				},
			}
		}
		logger.Info("[chunk] chunking mode override: keyset", "table", table.Name)
		return s.createKeysetChunks(slotName, table, pkColumn, minValue, rowCount, s.config.ChunkSize)

	case config.SnapshotChunkingModeOffset:
		logger.Info("[chunk] chunking mode override: offset", "table", table.Name)
		return s.createOffsetChunks(ctx, slotName, table)

	default: // auto (default behavior)
		if ok {
			if rangeChunks := s.createRangeChunks(ctx, slotName, table, pkColumn); len(rangeChunks) > 0 {
				return rangeChunks
			}
			logger.Warn("[chunk] range chunking unavailable, falling back to OFFSET", "table", table.Name)
		}
		return s.createOffsetChunks(ctx, slotName, table)
	}
}

// sparsityThreshold defines when a table is considered "sparse"
// If (maxValue - minValue) / rowCount > sparsityThreshold, use keyset pagination
const sparsityThreshold = 100

// maxChunksPerTable prevents memory exhaustion from astronomical chunk counts
const maxChunksPerTable = 1_000_000

func (s *Snapshotter) createRangeChunks(ctx context.Context, slotName string, table publication.Table, pkColumn string) []*Chunk {
	minValue, maxValue, ok, err := s.getPrimaryKeyBounds(ctx, table, pkColumn)
	if err != nil {
		logger.Warn("[chunk] failed to read primary key bounds", "table", table.Name, "error", err)
		return nil
	}

	if !ok {
		// Empty table, create single chunk to keep accounting simple
		return []*Chunk{
			{
				SlotName:    slotName,
				TableSchema: table.Schema,
				TableName:   table.Name,
				ChunkIndex:  0,
				ChunkStart:  0,
				ChunkSize:   s.config.ChunkSize,
				Status:      ChunkStatusPending,
			},
		}
	}

	chunkSize := s.config.ChunkSize
	totalRange := (maxValue - minValue) + 1

	// Get actual row count to detect sparse tables
	rowCount, err := s.getTableRawCount(ctx, table.Schema, table.Name)
	if err != nil {
		logger.Warn("[chunk] failed to get row count for sparsity check", "table", table.Name, "error", err)
		return nil
	}

	if rowCount == 0 {
		// Empty table based on COUNT, create single chunk
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
		}
	}

	// Calculate sparsity ratio to detect sparse primary key distributions
	// Example: Snowflake IDs where minValue=1, maxValue=7234567890123456789, but only 1000 rows
	sparsityRatio := float64(totalRange) / float64(rowCount)

	if sparsityRatio > sparsityThreshold {
		logger.Info("[chunk] sparse primary key detected, using keyset pagination",
			"table", table.Name,
			"minValue", minValue,
			"maxValue", maxValue,
			"rowCount", rowCount,
			"sparsityRatio", sparsityRatio,
		)
		return s.createKeysetChunks(slotName, table, pkColumn, minValue, rowCount, chunkSize)
	}

	// Dense table: use traditional range-based chunking
	numChunks := (totalRange + chunkSize - 1) / chunkSize

	// Safety guard: prevent memory exhaustion from extreme chunk counts
	if numChunks > maxChunksPerTable {
		logger.Warn("[chunk] range chunking would create too many chunks, falling back to keyset pagination",
			"table", table.Name,
			"calculatedChunks", numChunks,
			"maxAllowed", maxChunksPerTable,
		)
		return s.createKeysetChunks(slotName, table, pkColumn, minValue, rowCount, chunkSize)
	}

	chunks := make([]*Chunk, 0, numChunks)

	for i := int64(0); i < numChunks; i++ {
		rangeStart := minValue + (i * chunkSize)
		rangeEnd := rangeStart + chunkSize - 1
		if rangeEnd > maxValue {
			rangeEnd = maxValue
		}

		startValue := rangeStart
		endValue := rangeEnd

		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
			RangeStart:  &startValue,
			RangeEnd:    &endValue,
		}
		chunks = append(chunks, chunk)
	}

	logger.Debug("[chunk] range chunks created",
		"table", table.Name,
		"chunkSize", chunkSize,
		"numChunks", numChunks,
		"rangeMin", minValue,
		"rangeMax", maxValue,
	)
	return chunks
}

// createKeysetChunks creates chunks for sparse primary key tables using percentile-based boundaries.
// Instead of dividing the ID range into fixed intervals (which fails for sparse data),
// this calculates actual PK value boundaries using SQL window functions.
//
// This approach:
// - Uses O(1) memory regardless of PK range (no astronomical slice allocations)
// - Calculates exact chunk boundaries based on actual data distribution
// - Allows parallel chunk processing (no sequential dependency)
// - Works correctly with snowflake IDs, UUIDs cast to bigint, and other sparse distributions
func (s *Snapshotter) createKeysetChunks(slotName string, table publication.Table, pkColumn string, minValue, rowCount, chunkSize int64) []*Chunk {
	// Calculate number of chunks based on actual row count (not PK range)
	numChunks := (rowCount + chunkSize - 1) / chunkSize
	if numChunks == 0 {
		numChunks = 1
	}

	// Safety guard with coverage: increase chunkSize so we still cover all rows
	if numChunks > maxChunksPerTable {
		chunkSize = (rowCount + maxChunksPerTable - 1) / maxChunksPerTable
		if chunkSize <= 0 {
			chunkSize = 1
		}
		numChunks = (rowCount + chunkSize - 1) / chunkSize
		logger.Warn("[chunk] row count would create too many chunks, increasing chunk size",
			"table", table.Name,
			"rowCount", rowCount,
			"newChunkSize", chunkSize,
			"numChunks", numChunks,
			"maxAllowed", maxChunksPerTable,
		)
	}

	// For small number of chunks, calculate exact boundaries using NTILE
	// This enables parallel processing without sequential dependencies
	if numChunks <= 10000 {
		chunks := s.createChunksWithExactBoundaries(slotName, table, pkColumn, minValue, int(numChunks), chunkSize)
		if chunks != nil {
			return chunks
		}
		// Fall through to keyset mode if boundary calculation fails
		logger.Warn("[chunk] exact boundary calculation failed, using keyset mode", "table", table.Name)
	}

	// For very large number of chunks or when exact boundaries fail,
	// use keyset pagination with sequential processing
	return s.createSequentialKeysetChunks(slotName, table, pkColumn, minValue, numChunks, chunkSize)
}

// createChunksWithExactBoundaries calculates exact chunk boundaries using SQL NTILE
// This allows parallel chunk processing for sparse tables
func (s *Snapshotter) createChunksWithExactBoundaries(slotName string, table publication.Table, pkColumn string, minValue int64, numChunks int, chunkSize int64) []*Chunk {
	ctx := context.Background()

	// Use NTILE to divide rows into equal groups and get boundary values
	// Use quoted identifiers to handle special characters in table/column names
	query := fmt.Sprintf(`
		WITH chunk_boundaries AS (
			SELECT 
				"%s" as pk_value,
				NTILE(%d) OVER (ORDER BY "%s") as chunk_num
			FROM "%s"."%s"
		)
		SELECT 
			chunk_num - 1 as chunk_index,
			MIN(pk_value) as range_start,
			MAX(pk_value) as range_end
		FROM chunk_boundaries
		GROUP BY chunk_num
		ORDER BY chunk_num
	`, pkColumn, numChunks, pkColumn, table.Schema, table.Name)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		logger.Warn("[chunk] failed to calculate exact boundaries", "table", table.Name, "error", err)
		return nil
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		logger.Warn("[chunk] no boundary results returned", "table", table.Name)
		return nil
	}

	chunks := make([]*Chunk, 0, len(results[0].Rows))
	for _, row := range results[0].Rows {
		if len(row) < 3 {
			continue
		}

		var chunkIndex int
		if _, err := fmt.Sscanf(string(row[0]), "%d", &chunkIndex); err != nil {
			logger.Warn("[chunk] failed to parse chunk index", "error", err)
			continue
		}

		rangeStart, err := strconv.ParseInt(string(row[1]), 10, 64)
		if err != nil {
			logger.Warn("[chunk] failed to parse range start", "error", err)
			continue
		}

		rangeEnd, err := strconv.ParseInt(string(row[2]), 10, 64)
		if err != nil {
			logger.Warn("[chunk] failed to parse range end", "error", err)
			continue
		}

		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  chunkIndex,
			ChunkStart:  int64(chunkIndex) * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
			RangeStart:  &rangeStart,
			RangeEnd:    &rangeEnd,
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		return nil
	}

	logger.Info("[chunk] exact boundary chunks created",
		"table", table.Name,
		"chunkSize", chunkSize,
		"numChunks", len(chunks),
		"pkColumn", pkColumn,
	)
	return chunks
}

// createSequentialKeysetChunks creates chunks that must be processed sequentially
// Used when exact boundary calculation is not feasible (very large tables)
func (s *Snapshotter) createSequentialKeysetChunks(slotName string, table publication.Table, pkColumn string, minValue, numChunks, chunkSize int64) []*Chunk {
	chunks := make([]*Chunk, 0, numChunks)

	// For keyset pagination, we use RangeStart to indicate the cursor position
	// RangeEnd is set to nil to signal keyset mode
	// First chunk starts at minValue-1 so the first row is included

	for i := int64(0); i < numChunks; i++ {
		var startValue int64
		if i == 0 {
			// First chunk: start just before minValue to include it
			startValue = minValue - 1
		} else {
			// Subsequent chunks: use a sentinel value indicating sequential dependency
			// The actual start will be determined by reading previous chunk's last PK
			startValue = -(i + 1) // Negative value signals "needs previous chunk's cursor"
		}

		chunk := &Chunk{
			SlotName:    slotName,
			TableSchema: table.Schema,
			TableName:   table.Name,
			ChunkIndex:  int(i),
			ChunkStart:  i * chunkSize,
			ChunkSize:   chunkSize,
			Status:      ChunkStatusPending,
			RangeStart:  &startValue,
			RangeEnd:    nil, // nil RangeEnd signals keyset pagination mode
		}
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] sequential keyset chunks created",
		"table", table.Name,
		"chunkSize", chunkSize,
		"numChunks", numChunks,
		"pkColumn", pkColumn,
	)
	return chunks
}

func (s *Snapshotter) createOffsetChunks(ctx context.Context, slotName string, table publication.Table) []*Chunk {
	rowCount, err := s.getTableRawCount(ctx, table.Schema, table.Name)
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
		}
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

	logger.Info("[chunk] offset chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", numChunks)
	return chunks
}

func (s *Snapshotter) getPrimaryKeyColumnsDetailed(ctx context.Context, conn pq.Connection, table publication.Table) ([]primaryKeyColumn, error) {
	query := fmt.Sprintf(`
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = '"%s"."%s"'::regclass AND i.indisprimary
		ORDER BY a.attnum
	`, table.Schema, table.Name)

	results, err := s.execQuery(ctx, conn, query)
	if err != nil {
		logger.Debug("[chunk] primary key query failed", "table", table.Name, "schema", table.Schema, "error", err)
		return nil, errors.Wrap(err, "query primary key")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}

	var columns []primaryKeyColumn
	for _, row := range results[0].Rows {
		if len(row) < 2 {
			continue
		}
		columns = append(columns, primaryKeyColumn{
			Name:     string(row[0]),
			DataType: strings.ToLower(string(row[1])),
		})
	}
	return columns, nil
}

func (s *Snapshotter) getSingleIntegerPrimaryKey(ctx context.Context, table publication.Table) (string, bool, error) {
	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, s.metadataConn, table)
	if err != nil {
		logger.Debug("[chunk] failed to get primary key columns", "table", table.Name, "error", err)
		return "", false, err
	}

	if len(columns) == 0 {
		logger.Debug("[chunk] no primary key found", "table", table.Name)
		return "", false, nil
	}

	if len(columns) != 1 {
		logger.Debug("[chunk] composite primary key not supported for range chunking",
			"table", table.Name, "pkColumns", len(columns))
		return "", false, nil
	}

	if !isIntegerType(columns[0].DataType) {
		logger.Debug("[chunk] primary key is not integer type",
			"table", table.Name, "pkColumn", columns[0].Name, "dataType", columns[0].DataType)
		return "", false, nil
	}

	logger.Debug("[chunk] single integer primary key found",
		"table", table.Name, "pkColumn", columns[0].Name, "dataType", columns[0].DataType)
	return columns[0].Name, true, nil
}

func isIntegerType(dataType string) bool {
	switch dataType {
	case "smallint", "integer", "bigint", "int2", "int4", "int8":
		return true
	default:
		return false
	}
}

func (s *Snapshotter) getPrimaryKeyBounds(ctx context.Context, table publication.Table, pkColumn string) (int64, int64, bool, error) {
	query := fmt.Sprintf(`
		SELECT MIN("%s")::bigint AS min_value, MAX("%s")::bigint AS max_value
		FROM "%s"."%s"
	`, pkColumn, pkColumn, table.Schema, table.Name)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return 0, 0, false, errors.Wrap(err, "query primary key bounds")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, 0, false, nil
	}

	row := results[0].Rows[0]
	if len(row) < 2 || row[0] == nil || row[1] == nil {
		return 0, 0, false, nil
	}

	minValue, err := strconv.ParseInt(string(row[0]), 10, 64)
	if err != nil {
		return 0, 0, false, errors.Wrap(err, "parse min value")
	}

	maxValue, err := strconv.ParseInt(string(row[1]), 10, 64)
	if err != nil {
		return 0, 0, false, errors.Wrap(err, "parse max value")
	}

	return minValue, maxValue, true, nil
}

// parseRow converts PostgreSQL row data to map with proper type conversion
func (s *Snapshotter) parseRow(fields []pgconn.FieldDescription, row [][]byte) map[string]any {
	rowData := make(map[string]any, len(fields))

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

	return rowData
}

// saveChunk saves a chunk to the database (only INSERT, coordinator creates once)
func (s *Snapshotter) saveChunk(ctx context.Context, chunk *Chunk) error {
	return s.retryDBOperation(ctx, func() error {
		rangeStart := "NULL"
		if chunk.RangeStart != nil {
			rangeStart = fmt.Sprintf("%d", *chunk.RangeStart)
		}

		rangeEnd := "NULL"
		if chunk.RangeEnd != nil {
			rangeEnd = fmt.Sprintf("%d", *chunk.RangeEnd)
		}

		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, table_schema, table_name, chunk_index, 
				chunk_start, chunk_size, range_start, range_end, status
			) VALUES ('%s', '%s', '%s', %d, %d, %d, %s, %s, '%s')
		`, chunksTableName,
			chunk.SlotName,
			chunk.TableSchema,
			chunk.TableName,
			chunk.ChunkIndex,
			chunk.ChunkStart,
			chunk.ChunkSize,
			rangeStart,
			rangeEnd,
			string(chunk.Status),
		)

		_, err := s.execQuery(ctx, s.metadataConn, query)
		return err
	})
}

func (s *Snapshotter) getTableRawCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, schema, table)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return 0, errors.Wrap(err, "table row count")
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
			job.StartedAt.Format(postgresTimestampFormat),
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

// tryAcquireCoordinatorLock attempts to acquire the PostgreSQL advisory lock for coordinator role
func (s *Snapshotter) tryAcquireCoordinatorLock(ctx context.Context, slotName string) (bool, error) {
	// Use PostgreSQL advisory lock
	// Hash the slot name to create a consistent lock ID
	lockID := hashString(slotName)

	query := fmt.Sprintf("SELECT pg_try_advisory_lock(%d)", lockID)
	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "acquire coordinator lock")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, errors.New("no lock result returned")
	}

	// PostgreSQL returns 't' for true, 'f' for false
	acquired := string(results[0].Rows[0][0]) == "t"
	return acquired, nil
}

// hashString creates a numeric hash for PostgreSQL advisory lock
func hashString(s string) int64 {
	var hash int64
	for i := 0; i < len(s); i++ {
		hash = hash*31 + int64(s[i])
	}
	// Keep positive
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// tableExists checks if a table exists using information_schema
// This approach only requires SELECT permission on information_schema
func (s *Snapshotter) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = '%s'
		)
	`, tableName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "query table existence")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, errors.New("no result returned from table existence check")
	}

	// PostgreSQL returns 't' for true, 'f' for false
	exists := string(results[0].Rows[0][0]) == "t"
	return exists, nil
}

// indexExists checks if an index exists using pg_indexes
// This approach only requires SELECT permission on pg_indexes
func (s *Snapshotter) indexExists(ctx context.Context, indexName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM pg_indexes 
			WHERE schemaname = 'public' 
			AND indexname = '%s'
		)
	`, indexName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "query index existence")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, errors.New("no result returned from index existence check")
	}

	// PostgreSQL returns 't' for true, 'f' for false
	exists := string(results[0].Rows[0][0]) == "t"
	return exists, nil
}
