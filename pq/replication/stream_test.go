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
