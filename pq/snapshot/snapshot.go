package snapshot

import (
	"context"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Handler SnapshotHandler is a function that handles snapshot events
type Handler func(event *format.Snapshot) error

type Snapshotter struct {
	chunkDataConn   pq.Connection // Reads chunk data from source tables (SELECT * FROM users...)
	jobMetadataConn pq.Connection // Manages job/chunk state (cdc_snapshot_job, cdc_snapshot_chunks tables)
	heartbeatConn   pq.Connection // Dedicated for heartbeat updates (concurrent goroutine)
	snapshotTxConn  pq.Connection // Holds snapshot transaction open (pg_export_snapshot)
	metric          metric.Metric
	typeMap         *pgtype.Map
	tables          publication.Tables
	config          config.SnapshotConfig
}

func New(snapshotConfig config.SnapshotConfig, tables publication.Tables, chunkDataConn pq.Connection, jobMetadataConn pq.Connection, heartbeatConn pq.Connection, snapshotTxConn pq.Connection, m metric.Metric) *Snapshotter {
	return &Snapshotter{
		chunkDataConn:   chunkDataConn,
		jobMetadataConn: jobMetadataConn,
		heartbeatConn:   heartbeatConn,
		snapshotTxConn:  snapshotTxConn,
		config:          snapshotConfig,
		tables:          tables,
		typeMap:         pgtype.NewMap(),
		metric:          m,
	}
}

// Take performs a chunk-based snapshot (works for single or multiple instances)
// Returns the snapshot LSN for CDC streaming to continue from
// NOTE: If coordinator, snapshot transaction is kept OPEN after Take() completes
// The transaction stays alive until CDC starts or connector closes
func (s *Snapshotter) Take(ctx context.Context, handler Handler, slotName string) (pq.LSN, error) {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Info("chunk-based snapshot starting", "instanceID", instanceID)

	// Phase 1: Setup job (tables, coordinator election, metadata creation)
	job, isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return 0, errors.Wrap(err, "setup job")
	}
	if job == nil {
		logger.Info("snapshot already completed")
		// Load existing job to get LSN
		existingJob, err := s.loadJob(ctx, slotName)
		if err != nil || existingJob == nil {
			return 0, errors.New("completed job not found")
		}
		return existingJob.SnapshotLSN, nil
	}

	// Phase 2: Execute worker processing (ALL instances work, including coordinator)
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		// On error, cleanup snapshot transaction if coordinator
		if isCoordinator {
			logger.Warn("error during snapshot, rolling back snapshot transaction")
			s.rollbackSnapshotTransaction(ctx)
		}
		return 0, errors.Wrap(err, "execute worker")
	}

	// Phase 3: Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		// On error, cleanup snapshot transaction if coordinator
		if isCoordinator {
			logger.Warn("error during finalize, rolling back snapshot transaction")
			s.rollbackSnapshotTransaction(ctx)
		}
		return 0, errors.Wrap(err, "finalize snapshot")
	}

	// NOTE: Snapshot transaction is NOT closed here!
	// For coordinator, it stays open to maintain snapshot consistency until CDC starts
	logger.Info("snapshot completed", "instanceID", instanceID, "duration", time.Since(startTime), "lsn", job.SnapshotLSN.String())
	if isCoordinator {
		logger.Info("coordinator: snapshot transaction kept OPEN for CDC")
	}
	return job.SnapshotLSN, nil
}

// finalizeSnapshot checks completion and sends END marker
func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}

	if !allCompleted {
		return nil // Not done yet, keep processing
	}

	logger.Info("all chunks completed, marking job as complete")

	// Mark job as completed (idempotent - safe for multiple workers)
	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("failed to mark job as completed", "error", err)
	}

	// Send END marker
	if err := handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	}); err != nil {
		return err
	}

	return nil
}

// CloseSnapshotTransaction closes the snapshot transaction (called after CDC starts)
func (s *Snapshotter) CloseSnapshotTransaction(ctx context.Context) {
	logger.Info("closing snapshot transaction")
	if err := s.commitSnapshotTransaction(ctx); err != nil {
		logger.Warn("failed to commit snapshot transaction", "error", err)
	}
}

// decodeColumnData decodes PostgreSQL column data using pgtype
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	if dt, ok := s.typeMap.TypeForOID(dataTypeOID); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
