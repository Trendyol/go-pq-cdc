package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

// waitForCoordinator waits for the coordinator to initialize job and create chunks
// Workers need both job metadata and chunks to be ready before they can start processing
func (s *Snapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	timeout := 5 * time.Minute
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for coordinator to initialize")
		}

		// Check if both job and chunks are ready
		yes, status, err := s.isCoordinatorDidItsJob(ctx, slotName)
		switch {
		case err != nil:
			logger.Debug("[worker] waiting for coordinator", "status", status, "error", err)
		case yes:
			logger.Debug("[worker] coordinator ready, starting work", "status", status)
			return nil
		default:
			logger.Debug("[worker] waiting for coordinator", "status", status)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue waiting
		}
	}
}

// isCoordinatorDidItsJob checks if both job and chunks are ready for processing
// Returns (ready, status_message, error)
func (s *Snapshotter) isCoordinatorDidItsJob(ctx context.Context, slotName string) (bool, string, error) {
	// Step 1: Check if job exists
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return false, "job not found", err
	}
	if job == nil {
		return false, "job not created yet", nil
	}

	// Step 2: Check if snapshot ID is set (coordinator has exported snapshot)
	if job.SnapshotID == "" || job.SnapshotID == "PENDING" {
		return false, "snapshot not exported yet", nil
	}

	// Step 3: Check if chunks are available
	hasChunks, err := s.hasChunksReady(ctx, slotName)
	if err != nil {
		return false, "error checking chunks", err
	}
	if !hasChunks {
		return false, "chunks not created yet", nil
	}

	// Everything is ready!
	return true, fmt.Sprintf("ready (job=%s, chunks=available)", job.SnapshotID), nil
}

// hasChunksReady checks if there are chunks available for processing
func (s *Snapshotter) hasChunksReady(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) > 0 
		FROM %s 
		WHERE slot_name = '%s'
	`, chunksTableName, slotName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "check chunks ready")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	// Parse boolean result
	hasChunks := string(results[0].Rows[0][0]) == "t"
	return hasChunks, nil
}

// executeWorker sets up metrics, transactions, and processes chunks
func (s *Snapshotter) executeWorker(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, startTime time.Time) error {
	// Set metrics
	s.metric.SetSnapshotInProgress(true)
	s.metric.SetSnapshotTotalTables(len(s.tables))
	s.metric.SetSnapshotTotalChunks(job.TotalChunks)
	defer func() {
		s.metric.SetSnapshotInProgress(false)
		s.metric.SetSnapshotDurationSeconds(time.Since(startTime).Seconds())
	}()

	// Send BEGIN marker
	_ = handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeBegin,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})

	// Process chunks (each chunk will have its own transaction)
	if err := s.workerProcess(ctx, slotName, instanceID, job, handler); err != nil {
		return errors.Wrap(err, "worker process")
	}

	return nil
}

// workerProcess processes chunks as a worker
func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) error {
	heartbeatCtx, currentChunk := s.startHeartbeat(ctx)
	defer heartbeatCtx()

	for {
		hasMore, err := s.processNextChunk(ctx, slotName, instanceID, job, handler, currentChunk)
		if err != nil {
			return err
		}
		if !hasMore {
			logger.Debug("[worker] no more chunks available", "instanceID", instanceID)
			return nil
		}
	}
}

// startHeartbeat initializes and starts the heartbeat goroutine
func (s *Snapshotter) startHeartbeat(ctx context.Context) (cancel context.CancelFunc, chunkChan chan<- int64) {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	currentChunk := make(chan int64, 1)
	go s.heartbeatWorker(heartbeatCtx, currentChunk, s.config.HeartbeatInterval)
	return cancelHeartbeat, currentChunk
}

// processNextChunk claims and processes a single chunk
// Returns (hasMore, error) where hasMore indicates if there are more chunks to process
func (s *Snapshotter) processNextChunk(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunkChan chan<- int64) (bool, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	// Claim next chunk
	chunk, err := s.claimNextChunk(ctx, slotName, instanceID, s.config.ClaimTimeout)
	if err != nil {
		return false, errors.Wrap(err, "claim next chunk")
	}
	if chunk == nil {
		return false, nil // No more chunks
	}

	// Log chunk start
	s.logChunkStart(instanceID, chunk)

	// Notify heartbeat
	s.notifyHeartbeat(chunkChan, chunk.ID)

	// Process chunk
	rowsProcessed, err := s.processChunkWithTransaction(ctx, chunk, job.SnapshotID, job.SnapshotLSN, handler)
	if err != nil {
		logger.Error("[worker] chunk processing failed", "chunkID", chunk.ID, "error", err)
		return true, nil // Continue with next chunk
	}

	// Post-process: mark complete and update metrics
	s.completeChunk(ctx, slotName, instanceID, chunk, rowsProcessed)

	return true, nil // More chunks may be available
}

// logChunkStart logs the start of chunk processing
func (s *Snapshotter) logChunkStart(instanceID string, chunk *Chunk) {
	logger.Debug("[worker] processing chunk",
		"instanceID", instanceID,
		"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
		"chunkIndex", chunk.ChunkIndex,
		"chunkStart", chunk.ChunkStart,
		"chunkSize", chunk.ChunkSize)
}

// notifyHeartbeat sends chunk ID to heartbeat worker
func (s *Snapshotter) notifyHeartbeat(chunkChan chan<- int64, chunkID int64) {
	select {
	case chunkChan <- chunkID:
	default:
	}
}

// completeChunk marks chunk as completed and updates metrics
func (s *Snapshotter) completeChunk(ctx context.Context, slotName, instanceID string, chunk *Chunk, rowsProcessed int64) {
	// Mark chunk as completed
	if err := s.markChunkCompleted(ctx, slotName, chunk.ID, rowsProcessed); err != nil {
		logger.Warn("[worker] failed to mark chunk as completed", "error", err)
		return
	}

	// Update metrics
	s.metric.SnapshotRowsIncrement(rowsProcessed)
	s.updateCompletedChunksMetric(ctx, slotName)

	// Log completion
	logger.Debug("[worker] chunk completed",
		"instanceID", instanceID,
		"chunkID", chunk.ID,
		"rowsProcessed", rowsProcessed)
}

// updateCompletedChunksMetric updates the completed chunks metric
func (s *Snapshotter) updateCompletedChunksMetric(ctx context.Context, slotName string) {
	if job, _ := s.loadJob(ctx, slotName); job != nil {
		s.metric.SetSnapshotCompletedChunks(job.CompletedChunks)
	}
}

// processChunkWithTransaction processes a single chunk within its own transaction
// This allows each chunk to have an independent transaction lifecycle with retry support
func (s *Snapshotter) processChunkWithTransaction(ctx context.Context, chunk *Chunk, snapshotID string, lsn pq.LSN, handler Handler) (int64, error) {
	var rowsProcessed int64

	err := s.retryDBOperation(ctx, func() error {
		rows, err := s.executeInTransaction(ctx, snapshotID, func(conn pq.Connection) (int64, error) {
			return s.processChunk(ctx, conn, chunk, lsn, handler)
		})
		if err != nil {
			return err
		}
		rowsProcessed = rows
		return nil
	})

	if err != nil {
		return 0, errors.Wrap(err, "process chunk with transaction")
	}

	return rowsProcessed, nil
}

// executeInTransaction executes a function within a snapshot transaction
// Uses a fresh connection for each chunk to avoid dirty connection state
func (s *Snapshotter) executeInTransaction(ctx context.Context, snapshotID string, fn func(pq.Connection) (int64, error)) (int64, error) {
	// Create a fresh connection for this chunk
	chunkConn, err := pq.NewConnection(ctx, s.dsn)
	if err != nil {
		return 0, errors.Wrap(err, "create chunk connection")
	}
	defer chunkConn.Close(ctx)

	tx := &snapshotTransaction{
		snapshotter: s,
		ctx:         ctx,
		snapshotID:  snapshotID,
		conn:        chunkConn, // Use dedicated connection for this transaction
	}

	if err := tx.begin(); err != nil {
		return 0, err
	}
	defer tx.rollbackIfNeeded()

	rows, err := fn(chunkConn)
	if err != nil {
		return 0, errors.Wrap(err, "execute function")
	}

	if err := tx.commit(); err != nil {
		return 0, err
	}

	return rows, nil
}

// snapshotTransaction manages a single snapshot transaction lifecycle
type snapshotTransaction struct {
	ctx         context.Context
	conn        pq.Connection
	snapshotter *Snapshotter
	snapshotID  string
	committed   bool
}

// begin starts the transaction and sets the snapshot
func (tx *snapshotTransaction) begin() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return errors.Wrap(err, "begin transaction")
	}

	if err := tx.snapshotter.setTransactionSnapshot(tx.ctx, tx.conn, tx.snapshotID); err != nil {
		// BEGIN succeeded but snapshot failed - must rollback
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
		return errors.Wrap(err, "set transaction snapshot")
	}

	return nil
}

// commit commits the transaction
func (tx *snapshotTransaction) commit() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "COMMIT"); err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	tx.committed = true
	return nil
}

// rollbackIfNeeded rolls back the transaction if not committed
func (tx *snapshotTransaction) rollbackIfNeeded() {
	if !tx.committed {
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
	}
}

// heartbeatWorker periodically updates the heartbeat for the current chunk
func (s *Snapshotter) heartbeatWorker(ctx context.Context, currentChunk <-chan int64, interval time.Duration) {
	var activeChunkID int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case chunkID := <-currentChunk:
			activeChunkID = chunkID
		case <-ticker.C:
			if activeChunkID > 0 {
				if err := s.updateChunkHeartbeat(ctx, activeChunkID); err != nil {
					logger.Warn("[heartbeat] failed to update", "chunkID", activeChunkID, "error", err)
				} else {
					logger.Debug("[heartbeat] updated", "chunkID", activeChunkID)
				}
			}
		}
	}
}

// markJobAsCompleted marks the job as completed (safe to call multiple times)
func (s *Snapshotter) markJobAsCompleted(ctx context.Context, slotName string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET completed = true
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "mark job as completed")
		}

		logger.Info("[metadata] job marked as completed", "slotName", slotName)
		return nil
	})
}

// claimNextChunk attempts to claim a pending chunk using SELECT FOR UPDATE SKIP LOCKED
func (s *Snapshotter) claimNextChunk(ctx context.Context, slotName, instanceID string, claimTimeout time.Duration) (*Chunk, error) {
	var chunk *Chunk

	err := s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := s.buildClaimChunkQuery(slotName, instanceID, now, claimTimeout)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return errors.Wrap(err, "claim chunk")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			chunk = nil
			return nil // No chunks available (not an error)
		}

		row := results[0].Rows[0]
		if len(row) < 7 {
			return errors.New("invalid chunk row")
		}

		chunk, err = s.parseClaimedChunk(row, slotName, instanceID, now)
		return err
	})

	return chunk, err
}

// buildClaimChunkQuery builds the SQL query for claiming a chunk
func (s *Snapshotter) buildClaimChunkQuery(slotName, instanceID string, now time.Time, claimTimeout time.Duration) string {
	timeoutThreshold := now.Add(-claimTimeout)
	return fmt.Sprintf(`
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
}

// parseClaimedChunk parses the chunk row data
func (s *Snapshotter) parseClaimedChunk(row [][]byte, slotName, instanceID string, now time.Time) (*Chunk, error) {
	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}

	// Parse each field with validation
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

	return chunk, nil
}

// updateChunkHeartbeat updates the heartbeat timestamp for a chunk with retry
func (s *Snapshotter) updateChunkHeartbeat(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := fmt.Sprintf(`
			UPDATE %s SET heartbeat_at = '%s' WHERE id = %d
		`, chunksTableName, now.Format("2006-01-02 15:04:05"), chunkID)

		_, err := s.execQuery(ctx, s.healthcheckConn, query)
		return err
	})
}

// markChunkCompleted marks a chunk as completed and atomically increments completed_chunks
// NOTE: Uses metadataConn (not workerConn) to avoid serialization conflicts
// workerConn is in REPEATABLE READ snapshot transaction, metadata updates should be separate
func (s *Snapshotter) markChunkCompleted(ctx context.Context, slotName string, chunkID, rowsProcessed int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()

		// Update chunk status - use metadataConn for metadata updates
		chunkQuery := fmt.Sprintf(`
			UPDATE %s
			SET status = 'completed',
			    completed_at = '%s',
			    rows_processed = %d
			WHERE id = %d
		`, chunksTableName, now.Format("2006-01-02 15:04:05"), rowsProcessed, chunkID)

		if _, err := s.execQuery(ctx, s.metadataConn, chunkQuery); err != nil {
			return errors.Wrap(err, "update chunk status")
		}

		// Atomically increment completed_chunks counter
		// Using metadataConn allows multiple workers to safely increment without serialization conflicts
		jobQuery := fmt.Sprintf(`
			UPDATE %s
			SET completed_chunks = completed_chunks + 1
			WHERE slot_name = '%s'
		`, jobTableName, slotName)

		if _, err := s.execQuery(ctx, s.metadataConn, jobQuery); err != nil {
			return errors.Wrap(err, "increment completed chunks")
		}

		return nil
	})
}
