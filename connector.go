package cdc

import (
	"context"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	cfg                *config.Config
	system             pq.IdentifySystemResult
	stream             pq.Streamer
	prometheusRegistry metric.Registry
	server             http.Server

	cancelCh chan os.Signal
	readyCh  chan struct{}
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc pq.ListenerFunc) (Connector, error) {
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

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc pq.ListenerFunc) (Connector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}

	cfg.SetDefault()
	cfg.Print()

	logLevel := slog.LevelInfo
	if cfg.DebugMode {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})))

	conn, err := pq.NewConnection(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Publication.DropIfExists {
		if err = pq.DropPublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
	}

	if cfg.Publication.Create {
		if err = pq.CreatePublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
		slog.Info("publication created", "name", cfg.Publication.Name)
	}

	system, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}
	slog.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.XLogPos, "database:", system.Database)

	if cfg.Slot.Create {
		err = pq.CreateReplicationSlot(context.Background(), conn, cfg.Slot.Name)
		if err != nil {
			return nil, err
		}
		slog.Info("slot created", "name", cfg.Slot.Name)
	}

	stream := pq.NewStream(conn, cfg, system, listenerFunc)
	prometheusRegistry := metric.NewRegistry(stream.GetMetric())

	return &connector{
		cfg:                &cfg,
		system:             system,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}, nil
}

func (c *connector) Start(ctx context.Context) {
	err := c.stream.Open(ctx)
	if err != nil {
		slog.Error("postgres stream open", "error", err)
		return
	}

	go c.server.Listen()

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	select {
	case <-c.cancelCh:
		slog.Debug("cancel channel triggered")
	}

	c.Close()
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
	close(c.cancelCh)
	c.stream.Close(context.TODO())
	c.server.Shutdown()
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}
