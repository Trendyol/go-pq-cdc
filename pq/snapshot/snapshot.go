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
	ctx context.Context

	workerConn         pq.Connection
	metadataConn       pq.Connection
	healthcheckConn    pq.Connection
	exportSnapshotConn pq.Connection

	dsn     string
	metric  metric.Metric
	typeMap *pgtype.Map
	tables  publication.Tables
	config  config.SnapshotConfig
}

func New(ctx context.Context, snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string, m metric.Metric) (*Snapshotter, error) {
	metadataConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create metadata connection")
	}

	workerConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create worker connection")
	}

	healthcheckConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create pg export snapshot connection")
	}

	return &Snapshotter{
		dsn:             dsn,
		workerConn:      workerConn,
		metadataConn:    metadataConn,
		healthcheckConn: healthcheckConn,
		config:          snapshotConfig,
		tables:          tables,
		typeMap:         pgtype.NewMap(),
		metric:          m,
	}, nil
}

// Prepare sets up snapshot metadata and exports snapshot transaction
// This must be called BEFORE creating the replication slot to avoid data loss
// Returns the snapshot LSN that should be used for replication slot creation
//
// Flow:
//  1. Coordinator election
//  2. Capture current LSN
//  3. Create metadata (job, chunks)
//  4. Export snapshot transaction (keeps transaction OPEN)
//  5. Return LSN for slot creation
//
// IMPORTANT: Replication slot MUST be created immediately after this returns
// to ensure no WAL changes are lost during snapshot execution
func (s *Snapshotter) Prepare(ctx context.Context, slotName string) (pq.LSN, error) {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Info("[snapshot] preparing", "instanceID", instanceID)

	// Check if snapshot already completed
	existingJob, err := s.loadJob(ctx, slotName)
	if err == nil && existingJob != nil && existingJob.Completed {
		logger.Info("[snapshot] already completed, returning existing LSN")
		return existingJob.SnapshotLSN, nil
	}

	// Setup job (coordinator election, metadata creation, snapshot export)
	job, isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return 0, errors.Wrap(err, "setup job")
	}

	if job == nil {
		// Job already exists (another instance is coordinator)
		// Wait for coordinator to finish setup
		if err := s.waitForCoordinator(ctx, slotName); err != nil {
			return 0, errors.Wrap(err, "wait for coordinator")
		}

		// Load job to get LSN
		job, err = s.loadJob(ctx, slotName)
		if err != nil || job == nil {
			return 0, errors.New("job not found after coordinator setup")
		}
	}

	logger.Info("[snapshot] prepared", "instanceID", instanceID, "lsn", job.SnapshotLSN.String(), "isCoordinator", isCoordinator)
	if isCoordinator {
		logger.Info("[coordinator] snapshot transaction kept OPEN - replication slot must be created NOW")
	}

	return job.SnapshotLSN, nil
}

// Execute performs the actual snapshot data collection
// This should be called AFTER the replication slot is created with the LSN from Prepare()
// Returns when snapshot is complete
func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Info("[snapshot] executing", "instanceID", instanceID)

	// Load job
	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return errors.New("job not found - Prepare() must be called first")
	}

	if job.Completed {
		logger.Info("[snapshot] already completed")
		return nil
	}

	// Execute worker processing (ALL instances work, including coordinator)
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return errors.Wrap(err, "execute worker")
	}

	// Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

// Take performs a complete chunk-based snapshot (Prepare + Execute)
// Deprecated: Use Prepare() and Execute() separately with slot creation in between
// to avoid data loss during snapshot execution
func (s *Snapshotter) Take(ctx context.Context, handler Handler, slotName string) (pq.LSN, error) {
	logger.Warn("[snapshot] Take() should be replaced with Prepare() + CreateSlot() + Execute() pattern")

	lsn, err := s.Prepare(ctx, slotName)
	if err != nil {
		return 0, errors.Wrap(err, "prepare snapshot")
	}

	if err := s.Execute(ctx, handler, slotName); err != nil {
		return 0, errors.Wrap(err, "execute snapshot")
	}

	return lsn, nil
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

	logger.Info("[snapshot] all chunks completed, marking job as complete")

	// Mark job as completed (idempotent - safe for multiple workers)
	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("[snapshot] failed to mark job as completed", "error", err)
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

// decodeColumnData decodes PostgreSQL column data using pgtype
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	if dt, ok := s.typeMap.TypeForOID(dataTypeOID); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
