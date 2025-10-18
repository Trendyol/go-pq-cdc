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

	// Create snapshotter with separate state connection only if snapshot is enabled
	var snapshotter *snapshot.Snapshotter
	if cfg.Snapshot.Enabled {
		// Create separate connection for snapshot state (to avoid transaction rollback)
		snapshotStateConn, err := pq.NewConnection(ctx, cfg.DSN())
		if err != nil {
			return nil, errors.Wrap(err, "create state connection")
		}
		snapshotter = snapshot.New(cfg.Snapshot, cfg.Publication.Tables, conn, snapshotStateConn, m)
	}

	stream := replication.NewStream(conn, cfg, m, &system, listenerFunc)

	sl, err := slot.NewSlot(ctx, cfg.DSN(), cfg.Slot, m, stream.(slot.XLogUpdater))
	if err != nil {
		return nil, err
	}

	slotInfo, err := sl.Create(ctx)
	if err != nil {
		return nil, err
	}
	logger.Info("slot info", "info", slotInfo)

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

	if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
		if err := c.takeSnapshotWithRetry(ctx); err != nil {
			logger.Error("snapshot failed after retries", "error", err)
			return
		}
	}

	// Pass snapshot LSN to CDC stream if available
	if c.snapshotLSN > 0 {
		c.stream.SetSnapshotLSN(c.snapshotLSN)
		logger.Info("CDC will continue from snapshot LSN", "lsn", c.snapshotLSN.String())
	}

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

func (c *connector) takeSnapshotWithRetry(ctx context.Context) error {
	logger.Info("taking initial snapshot...")

	var lastErr error
	maxRetries := 3

	for attempt := 1; attempt <= 3; attempt++ {
		logger.Info("snapshot attempt", "attempt", attempt, "maxRetries", maxRetries)

		snapshotLSN, err := c.snapshotter.Take(ctx, c.snapshotHandler, c.cfg.Slot.Name)
		if err == nil {
			c.snapshotLSN = snapshotLSN
			logger.Info("snapshot completed successfully", "snapshotLSN", snapshotLSN.String())
			return nil
		}

		lastErr = err
		logger.Warn("snapshot attempt failed", "attempt", attempt, "error", err)

		// Don't sleep after last attempt
		if attempt < maxRetries {
			logger.Info("retrying snapshot", "retryDelay", "5s")
			time.Sleep(5 * time.Second)
		}
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
