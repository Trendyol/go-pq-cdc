package cdc

import (
	"context"
	goerrors "errors"
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

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}
	cfg.Print()

	logger.InitLogger(cfg.Logger.Logger)

	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	pub := publication.New(cfg.Publication, conn)
	if err = pub.SetReplicaIdentities(ctx); err != nil {
		return nil, err
	}
	publicationInfo, err := pub.Create(ctx)
	if err != nil {
		return nil, err
	}
	logger.Info("publication", "info", publicationInfo)

	system, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)

	m := metric.NewMetric(cfg.Slot.Name)

	var snapshotter *snapshot.Snapshotter
	if cfg.Snapshot.Enabled {
		snapshotter, err = snapshot.New(ctx, cfg.Snapshot, cfg.Publication.Tables, cfg.DSN(), m)
		if err != nil {
			return nil, err
		}
	}

	stream := replication.NewStream(conn, cfg, m, &system, listenerFunc)

	sl, err := slot.NewSlot(ctx, cfg.DSN(), cfg.Slot, m, stream.(slot.XLogUpdater))
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	tdb, err := timescaledb.NewTimescaleDB(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	_, err = tdb.FindHyperTables(ctx)
	if err != nil {
		return nil, err
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

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

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
	go c.timescaleDB.SyncHyperTables(ctx)

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
	logger.Debug("preparing snapshot and creating slot...")

	var lastErr error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Debug("snapshot attempt", "attempt", attempt, "maxRetries", maxRetries)

		// Phase 1: Prepare snapshot (capture LSN, create metadata, export snapshot)
		snapshotLSN, err := c.snapshotter.Prepare(ctx, c.cfg.Slot.Name)
		if err != nil {
			lastErr = err
			logger.Warn("snapshot prepare failed", "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("retrying snapshot", "retryDelay", "5s")
				time.Sleep(5 * time.Second)
			}
			continue
		}

		c.snapshotLSN = snapshotLSN
		c.stream.SetSnapshotLSN(snapshotLSN)
		logger.Info("snapshot prepared, LSN captured", "snapshotLSN", snapshotLSN.String())

		// CRITICAL: Create replication slot immediately after Prepare()
		// This ensures no WAL changes are lost during snapshot execution
		// Note: slot.Create() is idempotent - if slot exists, it returns existing slot
		logger.Info("creating replication slot to preserve WAL from snapshot LSN")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			lastErr = err
			logger.Warn("slot creation failed", "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("retrying snapshot", "retryDelay", "5s")
				time.Sleep(5 * time.Second)
			}
			continue
		}

		logger.Info("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

		// Phase 2: Execute snapshot (collect data from all chunks)
		if err := c.snapshotter.Execute(ctx, c.snapshotHandler, c.cfg.Slot.Name); err != nil {
			lastErr = err
			logger.Warn("snapshot execute failed", "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("retrying snapshot", "retryDelay", "5s")
				time.Sleep(5 * time.Second)
			}
			continue
		}

		logger.Info("snapshot completed successfully", "snapshotLSN", snapshotLSN.String())
		return nil
	}

	return errors.Wrap(lastErr, "snapshot failed after all retries")
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
	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	c.slot.Close()
	c.stream.Close(context.TODO())
	c.server.Shutdown()
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
