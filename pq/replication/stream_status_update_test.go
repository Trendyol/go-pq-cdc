package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

type statusWriteConn struct {
	fe       *pgproto3.Frontend
	mu       sync.Mutex
	writes   [][]byte
	writeErr error
	lastDL   time.Time
}

func (c *statusWriteConn) Connect(context.Context) error                          { return nil }
func (c *statusWriteConn) IsClosed() bool                                         { return false }
func (c *statusWriteConn) Close(context.Context) error                            { return nil }
func (c *statusWriteConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
func (c *statusWriteConn) Frontend() *pgproto3.Frontend                           { return c.fe }

func (c *statusWriteConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errors.New("not used")
}

func (c *statusWriteConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastDL = t
	return nil
}

func (c *statusWriteConn) setWriteErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeErr = err
}

// snapshot returns the written wire packets and the last write deadline.
func (c *statusWriteConn) snapshot() ([][]byte, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.writes), c.lastDL
}

func (c *statusWriteConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

// statusWriteRecorder is the writer behind the fake's pgproto3.Frontend.
type statusWriteRecorder struct{ c *statusWriteConn }

func (w statusWriteRecorder) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	if w.c.writeErr != nil {
		return 0, w.c.writeErr
	}
	w.c.writes = append(w.c.writes, slices.Clone(p))
	return len(p), nil
}

func newStatusStream(interval time.Duration) (*stream, *statusWriteConn) {
	c := &statusWriteConn{}
	c.fe = pgproto3.NewFrontend(strings.NewReader(""), statusWriteRecorder{c})

	s := NewStream("", config.Config{StatusUpdateInterval: &interval}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = c
	s.lastXLogPos = 100
	s.confirmedXLogPos = 42
	return s, c
}

// decodeStatusUpdate parses the CopyData packet the standby status update is
// encoded into and returns its received, flushed, and applied LSNs.
func decodeStatusUpdate(t *testing.T, wire []byte) (received, flushed, applied uint64) {
	t.Helper()

	require.GreaterOrEqual(t, len(wire), 5+34)
	require.Equal(t, byte('d'), wire[0], "outer message must be CopyData")
	require.Equal(t, len(wire), int(binary.BigEndian.Uint32(wire[1:5]))+1)
	payload := wire[5:]
	require.Equal(t, byte(StandbyStatusUpdateByteID), payload[0])
	return binary.BigEndian.Uint64(payload[1:9]),
		binary.BigEndian.Uint64(payload[9:17]),
		binary.BigEndian.Uint64(payload[17:25])
}

func waitStatusUpdateEnd(t *testing.T, s *stream) {
	t.Helper()
	select {
	case <-s.statusUpdateEnd:
	case <-time.After(2 * time.Second):
		t.Fatal("status update loop did not stop")
	}
}

func TestStatusUpdateLoopSendsLSNsOnItsOwnCadence(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	s.cancel = cancel
	s.startStatusUpdateLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()
	waitStatusUpdateEnd(t, s)

	writes, deadline := c.snapshot()
	require.GreaterOrEqual(t, len(writes), 3, "expected the timer to acknowledge the primary while the stream is busy")
	for _, wire := range writes {
		received, flushed, applied := decodeStatusUpdate(t, wire)
		require.Equal(t, uint64(100), received, "received LSN must be the last WAL position")
		require.Equal(t, uint64(42), flushed, "flushed LSN must be the confirmed position")
		require.Equal(t, uint64(42), applied, "applied LSN must be the confirmed position")
	}
	require.True(t, deadline.IsZero(), "the write deadline must be cleared after the send")
}

func TestStatusUpdateLoopDisabledWhenIntervalIsZero(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(0)
	ctx, cancel := context.WithCancel(t.Context())
	s.cancel = cancel
	s.startStatusUpdateLoop(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()
	waitStatusUpdateEnd(t, s)

	require.Zero(t, c.writeCount())
}

func TestStatusUpdateLoopCancelsStreamOnWriteFailure(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(10 * time.Millisecond)
	c.setWriteErr(errors.New("broken pipe"))
	ctx, cancel := context.WithCancel(t.Context())
	s.cancel = cancel
	s.startStatusUpdateLoop(ctx)

	waitStatusUpdateEnd(t, s)

	require.True(t, s.statusUpdateFailed.Load(), "a failed write must surface as a stream failure")
	require.Error(t, ctx.Err(), "a failed write must cancel the stream")
}

func TestCloseJoinsStatusUpdateLoop(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s, c := newStatusStream(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	s.cancel = cancel
	s.startStatusUpdateLoop(ctx)

	require.Eventually(t, func() bool { return c.writeCount() > 0 }, time.Second, time.Millisecond)
	require.NoError(t, s.Close(t.Context()))

	select {
	case <-s.statusUpdateEnd:
	default:
		t.Fatal("statusUpdateLoop is still running after Close returned")
	}
}
