package cdc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/prometheus/client_golang/prometheus"
)

// OnlineConnectorLifecycle is the optional online view of a factory result.
// Connector itself remains unchanged for all existing callers.
type OnlineConnectorLifecycle interface {
	Connector
	WaitUntilCaptureReady(context.Context) error
	WaitUntilSnapshotComplete(context.Context) error
	Done() <-chan struct{}
	Err() error
	Stats() OnlineStats
	Health() OnlineHealth
}

type onlineConnectorAdapter struct {
	backend   *OnlineConnector
	registry  metric.Registry
	server    http.Server
	mu        sync.Mutex
	cancel    context.CancelFunc
	started   bool
	closed    bool
	closeOnce sync.Once
}

var _ OnlineConnectorLifecycle = (*onlineConnectorAdapter)(nil)

func newOnlineConnectorAdapter(backend *OnlineConnector) *onlineConnectorAdapter {
	registry := metric.NewRegistryWithCollectors(backend.MetricsCollector())
	return &onlineConnectorAdapter{backend: backend, registry: registry, server: http.NewServer(backend.cfg, registry, nil)}
}

func (c *onlineConnectorAdapter) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	run, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()
	defer cancel()
	if run.Err() == nil {
		logger.InitLogger(c.backend.cfg.Logger.Logger)
		go c.server.Listen()
	}
	c.run(run)
}

func (c *onlineConnectorAdapter) run(ctx context.Context) {
	if err := c.backend.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("online connector stopped", "error", err)
	}
	c.server.Shutdown()
}

func (c *onlineConnectorAdapter) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		unopened := !c.started
		c.started = true
		cancel := c.cancel
		c.mu.Unlock()
		if unopened {
			ctx, stop := context.WithCancel(context.Background())
			stop()
			c.run(ctx) // Finish readiness/Done without any network setup.
		} else if cancel != nil {
			cancel()
		}
		c.server.Shutdown()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-c.backend.Done():
		case <-cleanup.Done():
			logger.Error("online connector close deadline exceeded", "error", cleanup.Err())
		}
	})
}

func (c *onlineConnectorAdapter) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return context.Canceled
	}
	return nil
}

func (c *onlineConnectorAdapter) WaitUntilReady(ctx context.Context) error {
	return c.WaitUntilCaptureReady(ctx)
}

func (c *onlineConnectorAdapter) WaitUntilCaptureReady(ctx context.Context) error {
	if err := c.closedError(); err != nil {
		return err
	}
	if err := c.backend.WaitUntilCaptureReady(ctx); err != nil {
		return err
	}
	return c.closedError()
}

func (c *onlineConnectorAdapter) WaitUntilSnapshotComplete(ctx context.Context) error {
	if err := c.closedError(); err != nil {
		return err
	}
	if err := c.backend.WaitUntilSnapshotComplete(ctx); err != nil {
		return err
	}
	return c.closedError()
}

func (c *onlineConnectorAdapter) Done() <-chan struct{} { return c.backend.Done() }
func (c *onlineConnectorAdapter) Err() error            { return c.backend.Err() }
func (c *onlineConnectorAdapter) Stats() OnlineStats    { return c.backend.Stats() }
func (c *onlineConnectorAdapter) Health() OnlineHealth {
	health := c.backend.Health()
	if c.closedError() != nil {
		health.Running = false
	}
	return health
}

func (c *onlineConnectorAdapter) GetConfig() *config.Config {
	cfg := c.backend.cfg
	cfg.Publication.Tables = cloneFactoryTables(cfg.Publication.Tables)
	cfg.Snapshot.Tables = cloneFactoryTables(cfg.Snapshot.Tables)
	cfg.Heartbeat.Table.Columns = append([]string(nil), cfg.Heartbeat.Table.Columns...)
	cfg.Publication.Operations = append(publication.Operations(nil), cfg.Publication.Operations...)
	cfg.VisibilityGuard.Replicas = append([]string(nil), cfg.VisibilityGuard.Replicas...)
	if cfg.StatusUpdateInterval != nil {
		interval := *cfg.StatusUpdateInterval
		cfg.StatusUpdateInterval = &interval
	}
	return &cfg
}

func cloneFactoryTables(tables publication.Tables) publication.Tables {
	owned := append(publication.Tables(nil), tables...)
	for i := range owned {
		owned[i].Columns = append([]string(nil), tables[i].Columns...)
	}
	return owned
}

func (c *onlineConnectorAdapter) SetMetricCollectors(collectors ...prometheus.Collector) {
	c.registry.AddMetricCollectors(collectors...)
}
