package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
)

// TakeSnapshot performs a chunk-based snapshot (works for single or multiple instances)
func (s *Snapshotter) TakeSnapshot(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Info("chunk-based snapshot starting", "instanceID", instanceID)

	// Phase 1: Setup job (tables, coordinator election)
	job, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return errors.Wrap(err, "setup job")
	}
	if job == nil {
		logger.Info("snapshot already completed")
		return nil // Already done
	}

	// Phase 2: Execute worker processing
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return errors.Wrap(err, "execute worker")
	}

	// Phase 3: Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("snapshot completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

// setupJob initializes tables, handles coordinator election, and returns the job
func (s *Snapshotter) setupJob(ctx context.Context, slotName, instanceID string) (*Job, error) {
	// Initialize tables
	if err := s.initTables(ctx); err != nil {
		return nil, errors.Wrap(err, "initialize tables")
	}

	// Check if job already exists and is completed
	existingJob, err := s.loadJob(ctx, slotName)
	if err != nil {
		logger.Debug("no existing job found")
	}
	if existingJob != nil && existingJob.Completed {
		return nil, nil // Signal completion
	}

	// Try to become coordinator
	isCoordinator, err := s.tryAcquireCoordinatorLock(ctx, slotName)
	if err != nil {
		return nil, errors.Wrap(err, "acquire coordinator lock")
	}

	if isCoordinator {
		logger.Info("instance elected as coordinator", "instanceID", instanceID)
		defer s.releaseCoordinatorLock(ctx, slotName)
		if err := s.coordinatorInitialize(ctx, slotName); err != nil {
			return nil, errors.Wrap(err, "coordinator initialize")
		}
	} else {
		logger.Info("instance joining as worker", "instanceID", instanceID)
		if err := s.waitForCoordinator(ctx, slotName); err != nil {
			return nil, errors.Wrap(err, "wait for coordinator")
		}
	}

	// Load the job
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return nil, errors.Wrap(err, "load job")
	}
	if job == nil {
		return nil, errors.New("job not found after initialization")
	}

	logger.Info("job loaded", "snapshotID", job.SnapshotID, "lsn", job.SnapshotLSN.String())
	return job, nil
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

	// Start transaction
	if err := s.beginTransaction(ctx); err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	defer s.rollbackTransaction(ctx)

	if err := s.setTransactionSnapshot(ctx, job.SnapshotID); err != nil {
		return errors.Wrap(err, "set transaction snapshot")
	}

	// Process chunks
	if err := s.workerProcess(ctx, slotName, instanceID, job.SnapshotLSN, handler); err != nil {
		return errors.Wrap(err, "worker process")
	}

	// Commit transaction
	return s.commitTransaction(ctx)
}

// finalizeSnapshot checks completion and sends END marker
func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}

	if !allCompleted {
		return nil // Not done yet
	}

	logger.Info("all chunks completed, marking job as complete")

	// Mark job as completed (idempotent - safe for multiple workers)
	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("failed to mark job as completed", "error", err)
	}

	// Send END marker
	return handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})
}

// coordinatorInitialize initializes the snapshot job (creates chunks)
func (s *Snapshotter) coordinatorInitialize(ctx context.Context, slotName string) error {
	logger.Info("coordinator: initializing job", "slotName", slotName)

	// Check if job already exists
	existingJob, err := s.loadJob(ctx, slotName)
	if err == nil && existingJob != nil && !existingJob.Completed {
		logger.Info("coordinator: job already initialized, resuming")
		return nil
	}

	// Capture LSN
	currentLSN, err := s.getCurrentLSN(ctx)
	if err != nil {
		return errors.Wrap(err, "get current LSN")
	}

	// Start transaction and export snapshot
	if err := s.beginTransaction(ctx); err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	defer s.rollbackTransaction(ctx)

	snapshotID, err := s.exportSnapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "export snapshot")
	}

	logger.Info("coordinator: snapshot exported", "snapshotID", snapshotID, "lsn", currentLSN.String())

	// Create job metadata
	job := &Job{
		SlotName:    slotName,
		SnapshotID:  snapshotID,
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
		logger.Info("coordinator: chunks created", "table", fmt.Sprintf("%s.%s", table.Schema, table.Name), "chunks", len(chunks))
	}

	job.TotalChunks = totalChunks

	// Save job metadata
	if err := s.saveJob(ctx, job); err != nil {
		return errors.Wrap(err, "save job")
	}

	logger.Info("coordinator: job initialized", "totalChunks", totalChunks)
	return nil
}

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
			logger.Info("job initialized by coordinator", "snapshotID", job.SnapshotID)
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

// workerProcess processes chunks as a worker
func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, lsn pq.LSN, handler Handler) error {
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
			logger.Debug("no more chunks available", "instanceID", instanceID)
			break
		}

		logger.Info("processing chunk",
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

		// Process the chunk
		rowsProcessed, err := s.processChunk(ctx, chunk, lsn, handler)
		if err != nil {
			logger.Error("chunk processing failed", "chunkID", chunk.ID, "error", err)
			// Continue with next chunk instead of failing entire snapshot
			continue
		}

		// Mark chunk as completed
		if err := s.markChunkCompleted(ctx, slotName, chunk.ID, rowsProcessed); err != nil {
			logger.Warn("failed to mark chunk as completed", "error", err)
		}

		// Update metrics
		s.metric.SnapshotRowsIncrement(rowsProcessed)

		// Update completed chunks metric
		job, _ := s.loadJob(ctx, slotName)
		if job != nil {
			s.metric.SetSnapshotCompletedChunks(job.CompletedChunks)
		}

		logger.Info("chunk completed",
			"instanceID", instanceID,
			"chunkID", chunk.ID,
			"rowsProcessed", rowsProcessed)
	}

	return nil
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
					logger.Warn("failed to update chunk heartbeat", "chunkID", activeChunkID, "error", err)
				} else {
					logger.Debug("heartbeat updated", "chunkID", activeChunkID)
				}
			}
		}
	}
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

	logger.Debug("executing chunk query", "query", query)

	results, err := s.execQuery(ctx, s.conn, query)
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

// exportSnapshot exports the current transaction snapshot for use by other connections
// Requires: REPLICATION privilege, wal_level=logical, max_replication_slots>0
func (s *Snapshotter) exportSnapshot(ctx context.Context) (string, error) {
	results, err := s.execQuery(ctx, s.conn, "SELECT pg_export_snapshot()")
	if err != nil {
		// Provide helpful error message if prerequisites not met
		if strings.Contains(err.Error(), "permission denied") {
			return "", errors.New("pg_export_snapshot requires REPLICATION privilege. Run: ALTER USER your_user WITH REPLICATION")
		}
		if strings.Contains(err.Error(), "wal_level") {
			return "", errors.New("pg_export_snapshot requires wal_level='logical'. Set in postgresql.conf and restart")
		}
		return "", errors.Wrap(err, "export snapshot")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return "", errors.New("no snapshot ID returned")
	}

	snapshotID := string(results[0].Rows[0][0])
	logger.Info("snapshot exported", "snapshotID", snapshotID)
	return snapshotID, nil
}

// setTransactionSnapshot sets the current transaction to use an exported snapshot
func (s *Snapshotter) setTransactionSnapshot(ctx context.Context, snapshotID string) error {
	query := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID)
	if err := s.execSQL(ctx, s.conn, query); err != nil {
		return errors.Wrap(err, "set transaction snapshot")
	}

	logger.Debug("transaction snapshot set", "snapshotID", snapshotID)
	return nil
}
