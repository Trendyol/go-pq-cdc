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

	"github.com/vskurikhin/go-pq-cdc/pq/timescaledb"

	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vskurikhin/go-pq-cdc/config"
	"github.com/vskurikhin/go-pq-cdc/internal/http"
	"github.com/vskurikhin/go-pq-cdc/internal/metric"
	"github.com/vskurikhin/go-pq-cdc/logger"
	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/vskurikhin/go-pq-cdc/pq/publication"
	"github.com/vskurikhin/go-pq-cdc/pq/replication"
	"github.com/vskurikhin/go-pq-cdc/pq/slot"
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
	system             pq.IdentifySystemResult

	once sync.Once
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listeners replication.Listeners) (Connector, error) {
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

	return NewConnector(ctx, cfg, listeners)
}

func NewConnector(ctx context.Context, cfg config.Config, listeners replication.Listeners) (Connector, error) {
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

	stream := replication.NewStream(conn, cfg, m, &system, listeners)

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

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

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
