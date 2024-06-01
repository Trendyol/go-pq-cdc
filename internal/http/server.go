package http

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

type Server interface {
	Listen()
	Shutdown()
}

type server struct {
	server    http.Server
	cdcConfig config.Config
}

func NewServer(cfg config.Config, registry *prometheus.Registry) Server {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
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
	slog.Info(fmt.Sprintf("server starting on port :%d", s.cdcConfig.Metric.Port))

	err := s.server.ListenAndServe()
	if err != nil {
		slog.Error("server cannot start", "port", s.cdcConfig.Metric.Port, "error", err)
		return
	}
	slog.Info("server stopped")
}

func (s *server) Shutdown() {
	err := s.server.Shutdown(context.Background())
	if err != nil {
		slog.Error("error while api cannot be shutdown", "error", err)
		panic(err)
	}
}
