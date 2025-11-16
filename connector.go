package cdc

import (
	"context"
	goerrors "errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"

	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	stream             replication.Streamer
	prometheusRegistry metric.Registry
	server             http.Server
	cfg                *config.Config
	slot               *slot.Slot
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	timescaleDB        *timescaledb.TimescaleDB
	snapshotter        *snapshot.Snapshotter
	listenerFunc       replication.ListenerFunc
	system             pq.IdentifySystemResult
	snapshotLSN        pq.LSN // LSN from snapshot to continue CDC from
	once               sync.Once
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

//nolint:funlen
func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}
	cfg.Print()

	logger.InitLogger(cfg.Logger.Logger)

	// Snapshot-only mode: minimal setup without CDC components
	if cfg.IsSnapshotOnlyMode() {
		return newSnapshotOnlyConnector(ctx, cfg, listenerFunc)
	}

	// Normal CDC mode: full setup with publication, slot, stream
	// Normal connection for publication setup
	// This uses regular DSN (no replication parameter) to avoid consuming max_wal_senders limit
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	publicationInfo, err := initializePublication(ctx, cfg, conn)
	if err != nil {
		return nil, err
	}
	logger.Info("publication", "info", publicationInfo)

	// Close the setup connection - we don't need it anymore
	conn.Close(ctx)

	m := metric.NewMetric(cfg.Slot.Name)

	// Get tables to snapshot (either from snapshot.tables or publication.tables)
	snapshotTables, err := cfg.GetSnapshotTables(publicationInfo)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	// Create dedicated replication connection for CDC streaming
	// This is the ONLY connection that should use ReplicationDSN()
	// IMPORTANT: IDENTIFY_SYSTEM command requires replication connection
	replConn, err := pq.NewConnection(ctx, cfg.ReplicationDSN())
	if err != nil {
		return nil, errors.Wrap(err, "create replication connection")
	}

	system, err := pq.IdentifySystem(ctx, replConn)
	if err != nil {
		return nil, err
	}
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)

	stream := replication.NewStream(replConn, cfg, m, &system, listenerFunc)

	// Slot needs replication connection for CREATE_REPLICATION_SLOT command
	sl, err := slot.NewSlot(ctx, cfg.ReplicationDSN(), cfg.Slot, m, stream.(slot.XLogUpdater))
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	var tdb *timescaledb.TimescaleDB
	if cfg.ExtensionSupport.EnableTimeScaleDB {
		tdb, err = timescaledb.NewTimescaleDB(ctx, cfg.DSN())
		if err != nil {
			return nil, err
		}
		_, err = tdb.FindHyperTables(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &connector{
		cfg:                &cfg,
		system:             system,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),
		slot:               sl,
		timescaleDB:        tdb,
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}, nil
}

// newSnapshotOnlyConnector creates a minimal connector for snapshot-only mode
// without CDC components (publication, slot, replication stream)
func newSnapshotOnlyConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	// Use a dummy metric name since we don't have a slot
	m := metric.NewMetric("snapshot_only")

	// Get tables to snapshot from snapshot.tables
	snapshotTables, err := cfg.GetSnapshotTables(nil) // nil publicationInfo for snapshot_only mode
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	// Initialize snapshotter with tables from snapshot config
	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	logger.Info("snapshot-only mode enabled", "tables", len(snapshotTables))

	return &connector{
		cfg:                &cfg,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
		// CDC components left nil: system, stream, slot
	}, nil
}

// initializePublication sets up and creates the publication
func initializePublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*publication.Config, error) {
	pub := publication.New(cfg.Publication, conn)
	if err := pub.SetReplicaIdentities(ctx); err != nil {
		return nil, err
	}
	return pub.Create(ctx)
}

// initializeSnapshot creates snapshot if enabled
// tables parameter should come from publicationInfo (not from config) to support both scenarios:
// 1. When user provides tables in config (createIfNotExists: true)
// 2. When user uses existing publication without specifying tables (createIfNotExists: false)
func initializeSnapshot(ctx context.Context, cfg config.Config, tables publication.Tables, m metric.Metric) (*snapshot.Snapshotter, error) {
	if !cfg.Snapshot.Enabled {
		return nil, nil
	}
	return snapshot.New(ctx, cfg.Snapshot, tables, cfg.DSN(), m)
}

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

	// Snapshot-only mode: execute snapshot and exit
	if c.cfg.IsSnapshotOnlyMode() {
		// Check if snapshot already completed (resume capability)
		if !c.shouldTakeSnapshotOnly(ctx) {
			logger.Info("snapshot-only already completed, exiting")
			return
		}

		if err := c.executeSnapshotOnly(ctx); err != nil {
			logger.Error("snapshot-only execution failed", "error", err)
			return
		}
		logger.Info("snapshot-only completed successfully, exiting")
		return
	}

	// Snapshot Pre-phase (optional): Prepare → CreateSlot → Execute
	// This happens BEFORE the normal CDC flow to avoid data loss
	if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
		if err := c.prepareSnapshotAndSlot(ctx); err != nil {
			logger.Error("snapshot preparation failed", "error", err)
			return
		}
	} else {
		// No snapshot: Create slot normally before starting CDC
		logger.Info("creating replication slot for CDC")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			logger.Error("slot creation failed", "error", err)
			return
		}
		logger.Info("slot info", "info", slotInfo)
	}

	// Normal CDC flow (unchanged for backward compatibility)
	c.CaptureSlot(ctx)

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, replication.ErrorSlotInUse) {
			logger.Info("capture failed")
			c.Start(ctx)
			return
		}
		logger.Error("postgres stream open", "error", err)
		return
	}

	logger.Info("slot captured")
	go c.slot.Metrics(ctx)

	if c.timescaleDB != nil {
		go c.timescaleDB.SyncHyperTables(ctx)
	}

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	<-c.cancelCh
	logger.Debug("cancel channel triggered")
}

func (c *connector) shouldTakeSnapshot(ctx context.Context) bool {
	if !c.cfg.Snapshot.Enabled {
		return false
	}

	switch c.cfg.Snapshot.Mode {
	case config.SnapshotModeNever:
		return false
	case config.SnapshotModeInitial:
		job, err := c.snapshotter.LoadJob(ctx, c.cfg.Slot.Name)
		if err != nil {
			logger.Debug("failed to load snapshot job state, will take snapshot", "error", err)
			return true
		}
		return job == nil || !job.Completed
	default:
		logger.Warn("invalid snapshot mode, skipping snapshot", "mode", c.cfg.Snapshot.Mode)
		return false
	}
}

// prepareSnapshotAndSlot handles the snapshot preparation with two-phase approach:
// 1. Prepare: Capture LSN and create metadata
// 2. Create replication slot immediately (to preserve WAL)
// 3. Execute: Collect snapshot data
// This ensures no WAL changes are lost during snapshot execution
func (c *connector) prepareSnapshotAndSlot(ctx context.Context) error {
	return c.retryOperation("snapshot", 3, func(_ int) error {
		// Phase 1: Create replication slot immediately (CRITICAL - preserves WAL)
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			return errors.Wrap(err, "create slot")
		}
		logger.Debug("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

		// Phase 2: Prepare snapshot (capture LSN, create metadata, export snapshot)
		snapshotLSN, err := c.snapshotter.Prepare(ctx, c.cfg.Slot.Name)
		if err != nil {
			return errors.Wrap(err, "prepare snapshot")
		}

		c.snapshotLSN = snapshotLSN
		c.stream.SetSnapshotLSN(snapshotLSN)
		logger.Debug("snapshot prepared, LSN captured", "snapshotLSN", snapshotLSN.String())

		// Phase 3: Execute snapshot (collect data from all chunks)
		// This may fail if coordinator restarts during execution - retry with backoff
		if err := c.executeSnapshotWithRetry(ctx); err != nil {
			// Non-recoverable error
			if c.isSnapshotInvalidationError(err) {
				log.Fatal(err)
			}
			return errors.Wrap(err, "execute snapshot")
		}

		logger.Info("snapshot completed successfully", "snapshotLSN", snapshotLSN.String())
		return nil
	})
}

// executeSnapshotOnly executes snapshot without creating a replication slot
// Used for snapshot_only mode (finite data export without CDC)
// Multi-pod safe: uses consistent slot name for coordinator election
func (c *connector) executeSnapshotOnly(ctx context.Context) error {
	slotName := c.getSnapshotOnlySlotName()

	logger.Info("starting snapshot-only execution", "slotName", slotName)

	// Prepare snapshot (capture LSN, create metadata, export snapshot)
	snapshotLSN, err := c.snapshotter.Prepare(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "prepare snapshot")
	}

	c.snapshotLSN = snapshotLSN
	logger.Info("snapshot prepared", "snapshotLSN", snapshotLSN.String())

	// Execute snapshot (collect data from all chunks)
	// Note: We call Execute directly with the slotName we prepared with,
	// not through executeSnapshotWithRetry which uses c.cfg.Slot.Name
	if err := c.snapshotter.Execute(ctx, c.snapshotHandler, slotName); err != nil {
		return errors.Wrap(err, "execute snapshot")
	}

	logger.Info("snapshot data collection completed", "snapshotLSN", snapshotLSN.String())
	return nil
}

// getSnapshotOnlySlotName returns a consistent slot name for snapshot_only mode
// This ensures multi-pod deployments work together instead of duplicating work
// The slot name is based on the database name to ensure consistency across restarts
func (c *connector) getSnapshotOnlySlotName() string {
	return fmt.Sprintf("snapshot_only_%s", c.cfg.Database)
}

// shouldTakeSnapshotOnly checks if snapshot_only should run
// Returns false if snapshot already completed (resume capability)
func (c *connector) shouldTakeSnapshotOnly(ctx context.Context) bool {
	slotName := c.getSnapshotOnlySlotName()

	job, err := c.snapshotter.LoadJob(ctx, slotName)
	if err != nil {
		logger.Error("failed to load snapshot job, will take snapshot", "error", err)
		return true
	}

	// If job doesn't exist or not completed, take snapshot
	if job == nil || !job.Completed {
		return true
	}

	// Job exists and completed
	logger.Info("snapshot-only already completed, skipping", "slotName", slotName)
	return false
}

// executeSnapshotWithRetry executes snapshot with retry on coordinator failures
// When coordinator restarts, it creates a new snapshot - workers should retry to join the new snapshot
func (c *connector) executeSnapshotWithRetry(ctx context.Context) error {
	const (
		maxRetries        = 5
		initialDelay      = 10 * time.Second
		maxDelay          = 60 * time.Second
		backoffMultiplier = 2
	)

	retryDelay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.snapshotter.Execute(ctx, c.snapshotHandler, c.cfg.Slot.Name)
		if err == nil {
			return nil // Success
		}

		// Check if this is a recoverable error (snapshot invalidation)
		if !c.isSnapshotInvalidationError(err) {
			// Non-recoverable error
			return err
		}

		// Last attempt exhausted
		if attempt >= maxRetries {
			return errors.Wrap(err, "snapshot execution failed after maximum retries")
		}

		// Log and wait before retry
		c.logRetryAttempt(attempt, maxRetries, retryDelay)

		if waitErr := c.waitWithContext(ctx, retryDelay); waitErr != nil {
			return waitErr
		}

		// Exponential backoff
		retryDelay = c.calculateNextDelay(retryDelay, maxDelay, backoffMultiplier)
	}

	return errors.New("snapshot execution failed: unexpected exit from retry loop")
}

// isSnapshotInvalidationError checks if error is due to snapshot invalidation
func (c *connector) isSnapshotInvalidationError(err error) bool {
	if err == nil {
		return false
	}

	// Check for typed error first (preferred)
	if goerrors.Is(err, snapshot.ErrSnapshotInvalidated) {
		return true
	}

	// Fallback to string matching for wrapped errors
	return strings.Contains(err.Error(), "snapshot invalidated")
}

// logRetryAttempt logs snapshot retry attempt with context
func (c *connector) logRetryAttempt(attempt, maxRetries int, delay time.Duration) {
	logger.Warn("[snapshot] snapshot invalidated, coordinator likely restarted",
		"attempt", attempt,
		"maxRetries", maxRetries,
		"waitTime", delay)

	logger.Info("[snapshot] waiting for coordinator to create new snapshot",
		"retryIn", delay)
}

// waitWithContext waits for duration or context cancellation
func (c *connector) waitWithContext(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

// calculateNextDelay calculates next retry delay with exponential backoff
func (c *connector) calculateNextDelay(currentDelay, maxDelay time.Duration, multiplier int) time.Duration {
	nextDelay := currentDelay * time.Duration(multiplier)
	if nextDelay > maxDelay {
		return maxDelay
	}
	return nextDelay
}

// retryOperation executes an operation with retry logic
func (c *connector) retryOperation(operationName string, maxRetries int, operation func(attempt int) error) error {
	var lastErr error
	retryDelay := 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			logger.Info("retrying operation", "operation", operationName, "attempt", attempt, "maxRetries", maxRetries)
		}

		if err := operation(attempt); err != nil {
			lastErr = err
			logger.Warn("operation failed", "operation", operationName, "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("waiting before retry", "retryDelay", retryDelay.String())
				time.Sleep(retryDelay)
			}
			continue
		}

		return nil
	}

	return errors.Wrapf(lastErr, "%s failed after %d retries", operationName, maxRetries)
}

func (c *connector) snapshotHandler(event *format.Snapshot) error {
	c.listenerFunc(&replication.ListenerContext{
		Message: event,
		Ack: func() error {
			return nil // ACK isn't required for snapshot
		},
	})
	return nil
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	// Create a context with timeout for graceful cleanup
	// 30 seconds should be sufficient for closing connections and cleanup operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Debug("[connector] closing connector")

	// Close signal channels
	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	// Close snapshotter connections if still open (fallback for crash/error scenarios)
	// Normal flow: connections are already closed in finalizeSnapshot() when snapshot completes
	if c.snapshotter != nil {
		c.snapshotter.Close(ctx)
	}

	// Close replication slot and stream (nil in snapshot_only mode)
	if c.slot != nil {
		c.slot.Close()
	}
	if c.stream != nil {
		c.stream.Close(ctx)
	}

	// Shutdown HTTP server
	c.server.Shutdown()

	logger.Info("[connector] connector closed successfully")
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func (c *connector) CaptureSlot(ctx context.Context) {
	logger.Info("slot capturing...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		info, err := c.slot.Info(ctx)
		if err != nil {
			continue
		}

		if !info.Active {
			break
		}

		logger.Debug("capture slot", "slotInfo", info)
	}
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
