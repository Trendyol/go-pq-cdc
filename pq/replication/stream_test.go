package replication

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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

// TestStreamCloseIsOrderedWithRejectedOpen covers a startup that the server
// rejects while Close runs on another goroutine, the way an application that
// runs Start in a goroutine closes the connector after WaitUntilReady timed
// out. connector.Start returns after a failed Open without synchronizing with
// anyone, so connMu alone must order Open's use of the replication connection
// against Close's, whichever runs first. Run with -race.
func TestStreamCloseIsOrderedWithRejectedOpen(t *testing.T) {
	var socket countingWriter
	conn := &startupConn{fe: pgproto3.NewFrontend(strings.NewReader(""), &socket)}
	// stream.Connect cannot run against a fake (IDENTIFY_SYSTEM needs a real
	// pgconn result reader), so connect the fake before Open's goroutine starts.
	_ = conn.Connect(context.Background())

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = conn
	s.UpdateConfirmedXLogPos(1) // Close also writes a final status update

	openErr := make(chan error, 1)
	go func() { openErr <- s.Open(context.Background()) }()

	// No wait before Close: synchronizing with the goroutine above would order
	// Open before Close by itself and hide a missing connMu.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-openErr; err == nil {
		t.Fatal("Open must fail: the server rejected START_REPLICATION, or Close closed the connection first")
	}
	if !conn.IsClosed() {
		t.Fatal("Close must close the replication connection whichever ran first")
	}
}

func TestStreamSinkExitStopsProcessor(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	stream.conn = receiveErrorConn{}
	// Simulate Close marking the stream closed before ReceiveMessage unblocks.
	stream.closed.Store(true)
	stream.processStarted.Store(true)

	go stream.process(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		stream.sink(context.Background())
	}()

	select {
	case <-stream.processEnd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("processor did not stop after sink exited")
	}

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sink did not finish")
	}
}

func TestStreamCloseFlushesFinalConfirmedLSN(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	var written bytes.Buffer
	conn := &standbyCaptureConn{
		fe: pgproto3.NewFrontend(strings.NewReader(""), &written),
	}

	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	stream.conn = conn
	stream.UpdateXLogPos(200)
	stream.UpdateConfirmedXLogPos(150)

	stream.Close(context.Background())

	if written.Len() == 0 {
		t.Fatal("expected final standby status update to be written")
	}
	if !bytes.Contains(written.Bytes(), []byte{StandbyStatusUpdateByteID}) {
		t.Fatal("expected standby status update payload")
	}
	if !conn.closed {
		t.Fatal("expected connection to be closed")
	}
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

type receiveErrorConn struct{}

func (receiveErrorConn) Connect(context.Context) error {
	return nil
}

func (receiveErrorConn) IsClosed() bool {
	return false
}

func (receiveErrorConn) Close(context.Context) error {
	return nil
}

func (receiveErrorConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errors.New("receive failed")
}

func (receiveErrorConn) Frontend() *pgproto3.Frontend {
	return nil
}

func (receiveErrorConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	return nil
}

type standbyCaptureConn struct {
	fe     *pgproto3.Frontend
	closed bool
}

func (c *standbyCaptureConn) Connect(context.Context) error {
	return nil
}

func (c *standbyCaptureConn) IsClosed() bool {
	return c.closed
}

func (c *standbyCaptureConn) Close(context.Context) error {
	c.closed = true
	return nil
}

func (c *standbyCaptureConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, errors.New("receive failed")
}

func (c *standbyCaptureConn) Frontend() *pgproto3.Frontend {
	return c.fe
}

func (c *standbyCaptureConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	return nil
}

// startupConn is a pq.Connection fake whose state, like pgconn's, is plain
// fields with no locking of its own. The server rejects START_REPLICATION.
type startupConn struct {
	fe        *pgproto3.Frontend
	connected bool
}

func (c *startupConn) Connect(context.Context) error {
	c.connected = true
	return nil
}

func (c *startupConn) IsClosed() bool {
	return !c.connected
}

func (c *startupConn) Close(context.Context) error {
	c.connected = false
	return nil
}

func (c *startupConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "42704", Message: "replication slot does not exist"}, nil
}

func (c *startupConn) Frontend() *pgproto3.Frontend {
	return c.fe
}

func (c *startupConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	return nil
}

// countingWriter is the socket behind startupConn's frontend.
type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}
