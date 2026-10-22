package replication

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type statusWriteConn struct {
	fe     *pgproto3.Frontend
	writes atomic.Int32
}

func (c *statusWriteConn) Connect(context.Context) error                          { return nil }
func (c *statusWriteConn) IsClosed() bool                                         { return false }
func (c *statusWriteConn) Close(context.Context) error                            { return nil }
func (c *statusWriteConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
func (c *statusWriteConn) Frontend() *pgproto3.Frontend                           { return c.fe }

func (c *statusWriteConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errors.New("not used")
}

type statusWriteCounter struct{ c *statusWriteConn }

func (w statusWriteCounter) Write(p []byte) (int, error) {
	w.c.writes.Add(1)
	return len(p), nil
}

func newStatusStream(interval time.Duration) (*stream, *statusWriteConn) {
	c := &statusWriteConn{}
	c.fe = pgproto3.NewFrontend(strings.NewReader(""), statusWriteCounter{c})

	s := NewStream("", config.Config{StatusUpdateInterval: interval}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = c
	s.lastXLogPos = 1
	return s, c
}

func runStatusUpdateLoop(t *testing.T, s *stream, c *statusWriteConn, d time.Duration) int32 {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.statusUpdateLoop(ctx)
	}()

	time.Sleep(d)
	cancel()
	wg.Wait()

	return c.writes.Load()
}

func TestStatusUpdateLoopSendsOnItsOwnCadence(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(20 * time.Millisecond)
	if got := runStatusUpdateLoop(t, s, c, 200*time.Millisecond); got < 3 {
		t.Fatalf("expected the timer to acknowledge the primary while the stream is busy, got %d updates", got)
	}
}

func TestStatusUpdateLoopDisabledWhenIntervalIsZero(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(0)
	if got := runStatusUpdateLoop(t, s, c, 200*time.Millisecond); got != 0 {
		t.Fatalf("interval 0 must not send updates, got %d", got)
	}
}
