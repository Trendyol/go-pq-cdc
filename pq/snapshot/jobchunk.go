package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
)

// ChunkStatus represents the status of a chunk
type ChunkStatus string

const (
	ChunkStatusPending    ChunkStatus = "pending"
	ChunkStatusInProgress ChunkStatus = "in_progress"
	ChunkStatusCompleted  ChunkStatus = "completed"
)

// Chunk represents a unit of work for snapshot processing
type Chunk struct {
	ID            int64
	SlotName      string
	TableSchema   string
	TableName     string
	ChunkIndex    int
	ChunkStart    int64
	ChunkSize     int64
	Status        ChunkStatus
	ClaimedBy     string
	ClaimedAt     *time.Time
	HeartbeatAt   *time.Time
	CompletedAt   *time.Time
	RowsProcessed int64
}

// Job represents the overall snapshot job metadata
type Job struct {
	SlotName        string
	SnapshotID      string
	SnapshotLSN     pq.LSN
	StartedAt       time.Time
	Completed       bool
	TotalChunks     int
	CompletedChunks int
}

const (
	jobTableName    = "cdc_snapshot_job"
	chunksTableName = "cdc_snapshot_chunks"
)

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

	if err := s.execSQL(ctx, s.stateConn, jobTableSQL); err != nil {
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

	if err := s.execSQL(ctx, s.stateConn, chunksTableSQL); err != nil {
		return errors.Wrap(err, "create chunks table")
	}

	// Create indexes for efficient queries
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chunks_claim ON %s(slot_name, status, claimed_at) WHERE status IN ('pending', 'in_progress')", chunksTableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chunks_status ON %s(slot_name, status)", chunksTableName),
	}

	for _, indexSQL := range indexes {
		if err := s.execSQL(ctx, s.stateConn, indexSQL); err != nil {
			return errors.Wrap(err, "create index")
		}
	}

	logger.Debug("snapshot tables initialized")
	return nil
}

// saveJob creates a new job (coordinator only, protected by advisory lock)
func (s *Snapshotter) saveJob(ctx context.Context, job *Job) error {
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

	if _, err := s.execQuery(ctx, s.stateConn, query); err != nil {
		return errors.Wrap(err, "create job")
	}

	logger.Debug("job created", "slotName", job.SlotName, "snapshotID", job.SnapshotID)
	return nil
}

// markJobAsCompleted marks the job as completed (safe to call multiple times)
func (s *Snapshotter) markJobAsCompleted(ctx context.Context, slotName string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET completed = true
		WHERE slot_name = '%s'
	`, jobTableName, slotName)

	if _, err := s.execQuery(ctx, s.stateConn, query); err != nil {
		return errors.Wrap(err, "mark job as completed")
	}

	logger.Info("job marked as completed", "slotName", slotName)
	return nil
}

// loadJob loads the job metadata
func (s *Snapshotter) loadJob(ctx context.Context, slotName string) (*Job, error) {
	query := fmt.Sprintf(`
		SELECT slot_name, snapshot_id, snapshot_lsn, started_at, 
		       completed, total_chunks, completed_chunks
		FROM %s WHERE slot_name = '%s'
	`, jobTableName, slotName)

	results, err := s.execQuery(ctx, s.stateConn, query)
	if err != nil {
		return nil, errors.Wrap(err, "load job")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}

	row := results[0].Rows[0]
	if len(row) < 7 {
		return nil, errors.New("invalid job row")
	}

	job := &Job{
		SlotName:   string(row[0]),
		SnapshotID: string(row[1]),
	}

	// Parse LSN
	job.SnapshotLSN, err = pq.ParseLSN(string(row[2]))
	if err != nil {
		return nil, errors.Wrap(err, "parse snapshot LSN")
	}

	// Parse timestamp
	job.StartedAt, err = parseTimestamp(string(row[3]))
	if err != nil {
		return nil, errors.Wrap(err, "parse started_at timestamp")
	}

	job.Completed = string(row[4]) == "t" || string(row[4]) == "true"
	fmt.Sscanf(string(row[5]), "%d", &job.TotalChunks)
	fmt.Sscanf(string(row[6]), "%d", &job.CompletedChunks)

	return job, nil
}

// LoadJob is the public API for connector
func (s *Snapshotter) LoadJob(ctx context.Context, slotName string) (*Job, error) {
	return s.loadJob(ctx, slotName)
}

// saveChunk saves a chunk to the database (only INSERT, coordinator creates once)
func (s *Snapshotter) saveChunk(ctx context.Context, chunk *Chunk) error {
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

	_, err := s.execQuery(ctx, s.stateConn, query)
	return err
}

// claimNextChunk attempts to claim a pending chunk using SELECT FOR UPDATE SKIP LOCKED
func (s *Snapshotter) claimNextChunk(ctx context.Context, slotName, instanceID string, claimTimeout time.Duration) (*Chunk, error) {
	now := time.Now().UTC()
	timeoutThreshold := now.Add(-claimTimeout)

	// Claim a pending chunk OR reclaim a stale in-progress chunk
	query := fmt.Sprintf(`
		WITH available_chunk AS (
			SELECT id FROM %s
			WHERE slot_name = '%s'
			  AND (
				  status = 'pending'
				  OR (status = 'in_progress' AND heartbeat_at < '%s')
			  )
			ORDER BY chunk_index
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s c
		SET status = 'in_progress',
		    claimed_by = '%s',
		    claimed_at = '%s',
		    heartbeat_at = '%s'
		FROM available_chunk
		WHERE c.id = available_chunk.id
		RETURNING c.id, c.table_schema, c.table_name, 
		          c.chunk_index, c.chunk_start, c.chunk_size, c.rows_processed
	`, chunksTableName,
		slotName,
		timeoutThreshold.Format("2006-01-02 15:04:05"),
		chunksTableName,
		instanceID,
		now.Format("2006-01-02 15:04:05"),
		now.Format("2006-01-02 15:04:05"),
	)

	results, err := s.execQuery(ctx, s.stateConn, query)
	if err != nil {
		return nil, errors.Wrap(err, "claim chunk")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil // No chunks available
	}

	row := results[0].Rows[0]
	if len(row) < 7 {
		return nil, errors.New("invalid chunk row")
	}

	// Parse chunk data with validation
	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}

	// Validate and parse each field
	if _, err := fmt.Sscanf(string(row[0]), "%d", &chunk.ID); err != nil {
		return nil, errors.Wrap(err, "parse chunk ID")
	}
	chunk.TableSchema = string(row[1])
	chunk.TableName = string(row[2])
	if _, err := fmt.Sscanf(string(row[3]), "%d", &chunk.ChunkIndex); err != nil {
		return nil, errors.Wrap(err, "parse chunk index")
	}
	if _, err := fmt.Sscanf(string(row[4]), "%d", &chunk.ChunkStart); err != nil {
		return nil, errors.Wrap(err, "parse chunk start")
	}
	if _, err := fmt.Sscanf(string(row[5]), "%d", &chunk.ChunkSize); err != nil {
		return nil, errors.Wrap(err, "parse chunk size")
	}
	if _, err := fmt.Sscanf(string(row[6]), "%d", &chunk.RowsProcessed); err != nil {
		return nil, errors.Wrap(err, "parse rows processed")
	}

	return chunk, nil
}

// updateChunkHeartbeat updates the heartbeat timestamp for a chunk with retry
func (s *Snapshotter) updateChunkHeartbeat(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := fmt.Sprintf(`
			UPDATE %s SET heartbeat_at = '%s' WHERE id = %d
		`, chunksTableName, now.Format("2006-01-02 15:04:05"), chunkID)

		_, err := s.execQuery(ctx, s.stateConn, query)
		return err
	})
}

// markChunkCompleted marks a chunk as completed and atomically increments completed_chunks
func (s *Snapshotter) markChunkCompleted(ctx context.Context, slotName string, chunkID, rowsProcessed int64) error {
	now := time.Now().UTC()

	// Update chunk status
	chunkQuery := fmt.Sprintf(`
		UPDATE %s
		SET status = 'completed',
		    completed_at = '%s',
		    rows_processed = %d
		WHERE id = %d
	`, chunksTableName, now.Format("2006-01-02 15:04:05"), rowsProcessed, chunkID)

	if _, err := s.execQuery(ctx, s.stateConn, chunkQuery); err != nil {
		return errors.Wrap(err, "update chunk status")
	}

	// Atomically increment completed_chunks counter
	jobQuery := fmt.Sprintf(`
		UPDATE %s
		SET completed_chunks = completed_chunks + 1
		WHERE slot_name = '%s'
	`, jobTableName, slotName)

	if _, err := s.execQuery(ctx, s.stateConn, jobQuery); err != nil {
		return errors.Wrap(err, "increment completed chunks")
	}

	return nil
}

// checkJobCompleted checks if all chunks are completed
func (s *Snapshotter) checkJobCompleted(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'completed') as completed
		FROM %s
		WHERE slot_name = '%s'
	`, chunksTableName, slotName)

	results, err := s.execQuery(ctx, s.stateConn, query)
	if err != nil {
		return false, errors.Wrap(err, "check job completed")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return false, nil
	}

	row := results[0].Rows[0]
	var total, completed int
	fmt.Sscanf(string(row[0]), "%d", &total)
	fmt.Sscanf(string(row[1]), "%d", &completed)

	return total > 0 && total == completed, nil
}

// createTableChunks divides a table into chunks
func (s *Snapshotter) createTableChunks(ctx context.Context, slotName string, table publication.Table) ([]*Chunk, error) {
	rowCount, err := s.estimateTableRowCount(ctx, table.Schema, table.Name)
	if err != nil {
		logger.Warn("failed to estimate row count, using single chunk", "table", table.Name, "error", err)
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

	logger.Debug("chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", numChunks)
	return chunks, nil
}

// estimateTableRowCount estimates the number of rows in a table
// Note: This is an estimate from pg_class.reltuples, actual count may differ
// Last chunk will handle any difference (may have fewer rows than chunkSize)
func (s *Snapshotter) estimateTableRowCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf("SELECT reltuples::bigint FROM pg_class WHERE oid = '%s.%s'::regclass", schema, table)

	results, err := s.execQuery(ctx, s.conn, query)
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

// Helper functions

func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}
