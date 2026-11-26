package http

import (
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/stretchr/testify/require"
)

func TestShutdownBoundsAnInflightRequest(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	s := &server{server: http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	})}}
	served := make(chan error, 1)
	go func() { served <- s.server.Serve(listener) }()
	t.Cleanup(func() {
		close(release)
		require.NoError(t, s.server.Close())
		require.ErrorIs(t, <-served, http.ErrServerClosed)
	})
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String(), nil)
	require.NoError(t, err)
	clientDone := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: 20 * time.Second}).Do(request)
		if err == nil {
			err = response.Body.Close()
		}
		clientDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the server")
	}
	shutdownDone := make(chan struct{})
	go func() { s.Shutdown(); close(shutdownDone) }()
	select {
	case <-shutdownDone:
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown waited indefinitely for an inflight handler")
	}
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("forced shutdown did not release the client connection")
	}
}

func TestConcurrentShutdownIsIdempotent(t *testing.T) {
	s := &server{}
	done := make(chan struct{}, 2)
	for range 2 {
		go func() { s.Shutdown(); done <- struct{}{} }()
	}
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shutdown did not return")
		}
	}
}
