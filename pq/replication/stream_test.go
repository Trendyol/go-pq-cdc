package replication

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
)

func TestStreamCloseBeforeOpenDoesNotBlock(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {})

	requireCloseReturns(t, stream, "Close blocked before Open started the sink")
}

func TestStreamCloseIsIdempotentBeforeOpen(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {})
	requireCloseReturns(t, stream, "first Close blocked")
	requireCloseReturns(t, stream, "second Close blocked")
}

func TestStreamCloseAfterOpenFailureDoesNotBlock(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {})

	if err := stream.Open(context.Background()); err == nil {
		t.Fatal("expected Open to fail without a connected postgres connection")
	}

	requireCloseReturns(t, stream, "Close blocked after Open failed before starting the sink")
}

func requireCloseReturns(t *testing.T, stream Streamer, msg string) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		stream.Close(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal(msg)
	}
}
