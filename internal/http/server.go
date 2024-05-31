package http

import (
	"context"
	"fmt"
	"github.com/3n0ugh/dcpg/config"
	"log/slog"
	"net/http"
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

func NewServer(cfg config.Config) Server {
	mux := http.NewServeMux()

	// TODO: add metrics

	// TODO: debug mode pprof handler

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
