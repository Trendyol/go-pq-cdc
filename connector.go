package dcpg

import (
	"context"
	"github.com/3n0ugh/dcpg/config"
	"github.com/3n0ugh/dcpg/internal/http"
	"github.com/3n0ugh/dcpg/pq"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

type Connector interface {
	Start(ctx context.Context)
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	cfg              *config.Config
	system           pq.IdentifySystemResult
	stream           pq.Streamer
	metricCollectors []prometheus.Collector
	server           http.Server

	cancelCh chan os.Signal
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc pq.ListenerFunc) (Connector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}

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

	prometheusRegistry := prometheus.NewRegistry()
	stream.GetMetric().Register(prometheusRegistry)

	return &connector{
		cfg:              &cfg,
		system:           system,
		stream:           stream,
		metricCollectors: []prometheus.Collector{},
		server:           http.NewServer(cfg, prometheusRegistry),

		cancelCh: make(chan os.Signal, 1),
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

	select {
	case <-c.cancelCh:
		slog.Debug("cancel channel triggered")
	}

	c.Close()
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
	c.metricCollectors = append(c.metricCollectors, metricCollectors...)
}
