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
	conn      pq.Connection
	stateConn pq.Connection
	metric    metric.Metric
	typeMap   *pgtype.Map
	tables    publication.Tables
	config    config.SnapshotConfig
}

func New(snapshotConfig config.SnapshotConfig, tables publication.Tables, conn pq.Connection, stateConn pq.Connection, m metric.Metric) *Snapshotter {
	return &Snapshotter{
		conn:      conn,
		stateConn: stateConn,
		config:    snapshotConfig,
		tables:    tables,
		typeMap:   pgtype.NewMap(),
		metric:    m,
	}
}

// TODO: genel bi retrylara bak, birden fazla çağrılan yerler vs.

// Take performs a chunk-based snapshot (works for single or multiple instances)
// Returns the snapshot LSN for CDC streaming to continue from
func (s *Snapshotter) Take(ctx context.Context, handler Handler, slotName string) (pq.LSN, error) {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Info("chunk-based snapshot starting", "instanceID", instanceID)

	// Phase 1: Setup job (tables, coordinator election)
	job, err := s.setupJob(ctx, slotName, instanceID)
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

	// Phase 2: Execute worker processing
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return 0, errors.Wrap(err, "execute worker")
	}

	// Phase 3: Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return 0, errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("snapshot completed", "instanceID", instanceID, "duration", time.Since(startTime), "lsn", job.SnapshotLSN.String())
	return job.SnapshotLSN, nil
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

// decodeColumnData decodes PostgreSQL column data using pgtype
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	if dt, ok := s.typeMap.TypeForOID(dataTypeOID); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
