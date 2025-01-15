package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/vskurikhin/go-pq-cdc/config"
	"github.com/vskurikhin/go-pq-cdc/internal/metric"
	"github.com/vskurikhin/go-pq-cdc/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server interface {
	Listen()
	Shutdown()
}

type server struct {
	server    http.Server
	cdcConfig config.Config
	closed    bool
}

func NewServer(cfg config.Config, registry metric.Registry) Server {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.HandlerFor(registry.Prometheus(), promhttp.HandlerOpts{EnableOpenMetrics: true}))

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	if cfg.DebugMode {
		mux.Handle("GET /pprof", pprof.Handler("go-pq-cdc"))
	}

	return &server{
		server: http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.Metric.Port),
			Handler:      mux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
		},
		cdcConfig: cfg,
	}
}

func (s *server) Listen() {
	logger.Info(fmt.Sprintf("server starting on port :%d", s.cdcConfig.Metric.Port))

	err := s.server.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) && s.closed {
			logger.Info("server stopped")
			return
		}
		logger.Error("server cannot start", "port", s.cdcConfig.Metric.Port, "error", err)
	}
}

func (s *server) Shutdown() {
	s.closed = true
	err := s.server.Shutdown(context.Background())
	if err != nil {
		logger.Error("error while api cannot be shutdown", "error", err)
		panic(err)
	}
}
