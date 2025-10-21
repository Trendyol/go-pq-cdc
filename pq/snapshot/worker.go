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

// waitForCoordinator waits for the coordinator to initialize the job
func (s *Snapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for coordinator to initialize job")
		}

		job, err := s.loadJob(ctx, slotName)
		if err == nil && job != nil {
			logger.Info("[worker] coordinator ready", "snapshotID", job.SnapshotID)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue waiting
		}
	}
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
	if err := handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeBegin,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	}); err != nil {
		return errors.Wrap(err, "send begin marker")
	}

	// Process chunks (each chunk will have its own transaction)
	if err := s.workerProcess(ctx, slotName, instanceID, job, handler); err != nil {
		return errors.Wrap(err, "worker process")
	}

	return nil
}

// workerProcess processes chunks as a worker
func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) error {
	heartbeatInterval := s.config.HeartbeatInterval
	claimTimeout := s.config.ClaimTimeout

	// Start heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()

	currentChunk := make(chan int64, 1)
	go s.heartbeatWorker(heartbeatCtx, currentChunk, heartbeatInterval)

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Try to claim a chunk
		chunk, err := s.claimNextChunk(ctx, slotName, instanceID, claimTimeout)
		if err != nil {
			return errors.Wrap(err, "claim next chunk")
		}

		if chunk == nil {
			// No more chunks available
			logger.Debug("[worker] no more chunks available", "instanceID", instanceID)
			break
		}

		logger.Info("[worker] processing chunk",
			"instanceID", instanceID,
			"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
			"chunkIndex", chunk.ChunkIndex,
			"chunkStart", chunk.ChunkStart,
			"chunkSize", chunk.ChunkSize)

		// Notify heartbeat worker
		select {
		case currentChunk <- chunk.ID:
		default:
		}

		// Process the chunk with its own transaction and retry logic
		rowsProcessed, err := s.processChunkWithTransaction(ctx, chunk, job.SnapshotID, job.SnapshotLSN, handler)
		if err != nil {
			logger.Error("[worker] chunk processing failed", "chunkID", chunk.ID, "error", err)
			// Continue with next chunk instead of failing entire snapshot
			continue
		}

		// Mark chunk as completed
		if err := s.markChunkCompleted(ctx, slotName, chunk.ID, rowsProcessed); err != nil {
			logger.Warn("[worker] failed to mark chunk as completed", "error", err)
		}

		// Update metrics
		s.metric.SnapshotRowsIncrement(rowsProcessed)

		// Update completed chunks metric
		currentJob, _ := s.loadJob(ctx, slotName)
		if currentJob != nil {
			s.metric.SetSnapshotCompletedChunks(currentJob.CompletedChunks)
		}

		logger.Info("[worker] chunk completed",
			"instanceID", instanceID,
			"chunkID", chunk.ID,
			"rowsProcessed", rowsProcessed)
	}

	return nil
}

// processChunkWithTransaction processes a single chunk within its own transaction
// This allows each chunk to have an independent transaction lifecycle with retry support
func (s *Snapshotter) processChunkWithTransaction(ctx context.Context, chunk *Chunk, snapshotID string, lsn pq.LSN, handler Handler) (int64, error) {
	var rowsProcessed int64

	// Retry logic for the entire chunk processing (transaction + data processing)
	err := s.retryDBOperation(ctx, func() error {
		// Start transaction for this chunk
		if err := s.execSQL(ctx, s.workerConn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return errors.Wrap(err, "begin transaction")
		}

		// Ensure rollback on error
		committed := false
		defer func() {
			if !committed {
				_ = s.execSQL(ctx, s.workerConn, "ROLLBACK")
			}
		}()

		// Set transaction snapshot to ensure consistent read across all chunks
		if err := s.setTransactionSnapshot(ctx, snapshotID); err != nil {
			return errors.Wrap(err, "set transaction snapshot")
		}

		// Process the chunk data
		rows, err := s.processChunk(ctx, chunk, lsn, handler)
		if err != nil {
			return errors.Wrap(err, "process chunk data")
		}
		rowsProcessed = rows

		// Commit transaction
		if err := s.execSQL(ctx, s.workerConn, "COMMIT"); err != nil {
			return errors.Wrap(err, "commit transaction")
		}
		committed = true

		return nil
	})

	if err != nil {
		return 0, errors.Wrap(err, "process chunk with transaction")
	}

	return rowsProcessed, nil
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

		// Parse chunk data with validation
		chunk = &Chunk{
			SlotName:    slotName,
			Status:      ChunkStatusInProgress,
			ClaimedBy:   instanceID,
			ClaimedAt:   &now,
			HeartbeatAt: &now,
		}

		// Validate and parse each field
		if _, err := fmt.Sscanf(string(row[0]), "%d", &chunk.ID); err != nil {
			return errors.Wrap(err, "parse chunk ID")
		}
		chunk.TableSchema = string(row[1])
		chunk.TableName = string(row[2])
		if _, err := fmt.Sscanf(string(row[3]), "%d", &chunk.ChunkIndex); err != nil {
			return errors.Wrap(err, "parse chunk index")
		}
		if _, err := fmt.Sscanf(string(row[4]), "%d", &chunk.ChunkStart); err != nil {
			return errors.Wrap(err, "parse chunk start")
		}
		if _, err := fmt.Sscanf(string(row[5]), "%d", &chunk.ChunkSize); err != nil {
			return errors.Wrap(err, "parse chunk size")
		}

		return nil
	})

	return chunk, err
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
