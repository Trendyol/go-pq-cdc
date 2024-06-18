package cdc

import (
	"context"
	goerrors "errors"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	cfg                *config.Config
	system             pq.IdentifySystemResult
	stream             replication.Streamer
	prometheusRegistry metric.Registry
	server             http.Server
	slot               *slot.Slot

	cancelCh chan os.Signal
	readyCh  chan struct{}
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJson(configFilePath)
	} else {
		cfg, err = config.ReadConfigYaml(configFilePath)
	}
	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}

	cfg.SetDefault()
	cfg.Print()

	logLevel := slog.LevelInfo
	if cfg.DebugMode { // ayri bir config'i olsun
		logLevel = slog.LevelDebug
	}

	// custom logger inject edilebilsin
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})))

	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	pub := publication.New(cfg.Publication, conn)
	publicationInfo, err := pub.Create(ctx)
	if err != nil {
		return nil, err
	}
	slog.Info("publication", "info", publicationInfo)

	system, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}
	slog.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.XLogPos, "database:", system.Database)

	m := metric.NewMetric(cfg.Slot.Name)

	sl, err := slot.NewSlot(ctx, cfg.DSN(), cfg.Slot, m)
	if err != nil {
		return nil, err
	}

	slotInfo, err := sl.Create(ctx)
	if err != nil {
		return nil, err
	}
	slog.Info("slot info", "info", slotInfo)

	stream := replication.NewStream(conn, cfg, m, system, listenerFunc)
	prometheusRegistry := metric.NewRegistry(m)

	return &connector{
		cfg:                &cfg,
		system:             system,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),
		slot:               sl,

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	c.CaptureSlot(ctx)

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, replication.ErrorSlotInUse) {
			slog.Info("capture failed")
			c.Start(ctx)
			return
		}
		slog.Error("postgres stream open", "error", err)
		return
	}

	slog.Info("slot captured")

	go c.slot.Metrics(ctx)

	go c.server.Listen()

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	select {
	case <-c.cancelCh:
		slog.Debug("cancel channel triggered")
	}
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
	slog.Info("slot capturing...")
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

		slog.Debug("capture slot", "slotInfo", info)
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
