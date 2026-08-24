package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestMessageBufferFlushStopsWhenContextIsCanceled(t *testing.T) {
	out := make(chan *Message)
	flushed := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	buffer := &messageBuffer{
		pending: &Message{},
		outCh:   out,
		ctx:     ctx,
	}
	go func() {
		buffer.flush()
		close(flushed)
	}()

	cancel()
	select {
	case <-flushed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked flush did not stop after context cancellation")
	}
}

func TestStreamCloseReturnsAfterListenerTimeout(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	entered := make(chan struct{})
	release := make(chan struct{})
	stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(*ListenerContext) {
		close(entered)
		<-release
	}).(*stream)
	stream.conn = receiveErrorConn{}
	stream.processStarted.Store(true)
	processCtx, processCancel := context.WithCancel(context.Background())
	stream.cancel = processCancel

	go stream.process(processCtx)
	stream.messageCH <- &Message{message: &format.Insert{}}
	select {
	case <-entered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := stream.Close(shutdownCtx); err == nil {
		t.Fatal("Close() returned nil despite the listener exceeding the shutdown deadline")
	}

	close(release)
	select {
	case <-stream.processEnd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener process did not finish after it was released")
	}
}

func TestStreamCloseClosesPostgresBeforeWaitingForListener(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	conn := &shutdownAuditConn{closed: make(chan struct{})}
	release := make(chan struct{})
	entered := make(chan struct{})
	stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(*ListenerContext) {
		close(entered)
		<-release
	}).(*stream)
	stream.conn = conn
	stream.processStarted.Store(true)
	processCtx, processCancel := context.WithCancel(context.Background())
	stream.cancel = processCancel

	go stream.process(processCtx)
	stream.messageCH <- &Message{message: &format.Insert{}}
	select {
	case <-entered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close(shutdownCtx) }()

	select {
	case <-conn.closed:
	case <-time.After(10 * time.Millisecond):
		t.Fatal("PostgreSQL connection was not closed before waiting for listener")
	}

	select {
	case err := <-closeDone:
		if err == nil {
			t.Fatal("Close() returned nil despite the blocked listener")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close() did not honor the shutdown timeout")
	}

	close(release)
	select {
	case <-stream.processEnd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener process did not finish after release")
	}
}

func TestListenerReceivesCancellationDuringStreamClose(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	listenerStarted := make(chan struct{})
	listenerStopped := make(chan struct{})
	stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(lCtx *ListenerContext) {
		close(listenerStarted)
		<-lCtx.Context.Done()
		close(listenerStopped)
	}).(*stream)
	stream.conn = &shutdownAuditConn{closed: make(chan struct{})}
	stream.processStarted.Store(true)
	processCtx, processCancel := context.WithCancel(context.Background())
	stream.cancel = processCancel

	go stream.process(processCtx)
	stream.messageCH <- &Message{message: &format.Insert{}}
	select {
	case <-listenerStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener did not start")
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close() returned an unexpected error: %v", err)
	}
	select {
	case <-listenerStopped:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener context was not cancelled")
	}
}

func TestSinkLoopTreatsConnectionTerminationAsCorrupted(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	tests := []struct {
		err  error
		name string
	}{
		{name: "EOF", err: io.EOF},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
		{name: "closed network connection", err: net.ErrClosed},
		{name: "connection reset", err: syscall.ECONNRESET},
		{name: "broken pipe", err: syscall.EPIPE},
		{name: "admin shutdown", err: &pgconn.PgError{Code: postgresAdminShutdown}},
		{name: "crash shutdown", err: &pgconn.PgError{Code: postgresCrashShutdown}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(*ListenerContext) {}).(*stream)
			stream.conn = receiveErrConn{err: tt.err}

			corrupted := stream.sinkLoop(
				context.Background(),
				&messageBuffer{outCh: make(chan *Message, 1)},
				&streamTxBuffer{},
			)
			if !corrupted {
				t.Fatal("expected connection termination while running to be treated as corrupted so the process restarts")
			}
		})
	}
}

func TestSinkLoopDoesNotTreatTerminationAsCorruptedWhenAlreadyClosed(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(*ListenerContext) {}).(*stream)
	stream.conn = receiveErrConn{err: io.ErrUnexpectedEOF}
	stream.closed.Store(true)

	corrupted := stream.sinkLoop(
		context.Background(),
		&messageBuffer{outCh: make(chan *Message, 1)},
		&streamTxBuffer{},
	)
	if corrupted {
		t.Fatal("expected an already-closed stream to stop without being treated as corrupted")
	}
}

func TestSinkPanicsAfterClosingOnUnexpectedDisconnect(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	stream := NewStream("", config.Config{}, metric.NewMetric("test"), func(*ListenerContext) {}).(*stream)
	stream.conn = eofConn{}

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic after unexpected disconnect so the process restarts with a fresh connection")
		}
		if recovered != "corrupted connection" {
			t.Fatalf("unexpected panic value: %v", recovered)
		}
		if !stream.closed.Load() {
			t.Fatal("expected the stream to close before panicking")
		}
	}()

	stream.sink(context.Background())
}

func TestReplicationConnectionTerminationErrors(t *testing.T) {
	tests := []struct {
		err  error
		name string
	}{
		{name: "EOF", err: io.EOF},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF},
		{name: "closed network connection", err: net.ErrClosed},
		{name: "connection reset", err: syscall.ECONNRESET},
		{name: "broken pipe", err: syscall.EPIPE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isReplicationConnectionTerminationError(fmt.Errorf("wrapped: %w", tt.err)) {
				t.Fatalf("%v was not classified as a replication connection termination", tt.err)
			}
		})
	}
}

func TestPostgresShutdownErrorsAreClassifiedSeparately(t *testing.T) {
	for _, code := range []string{postgresAdminShutdown, postgresCrashShutdown} {
		if !isPostgresShutdownError(&pgconn.PgError{Code: code}) {
			t.Errorf("PostgreSQL shutdown code %s was treated as corruption", code)
		}
	}
}

func TestUnrelatedPostgresErrorsAreNotPostgresShutdowns(t *testing.T) {
	if isPostgresShutdownError(&pgconn.PgError{Code: "22012"}) {
		t.Fatal("unrelated PostgreSQL error was treated as a PostgreSQL shutdown")
	}
}

type shutdownAuditConn struct {
	closed chan struct{}
}

func (c *shutdownAuditConn) Close(context.Context) error {
	if c.closed == nil {
		c.closed = make(chan struct{})
	}
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

func (c *shutdownAuditConn) Connect(context.Context) error { return nil }

func (c *shutdownAuditConn) IsClosed() bool { return false }

func (c *shutdownAuditConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, io.EOF
}

func (c *shutdownAuditConn) Frontend() *pgproto3.Frontend { return nil }

func (c *shutdownAuditConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

type receiveErrConn struct {
	err error
}

func (receiveErrConn) Connect(context.Context) error { return nil }

func (receiveErrConn) IsClosed() bool { return false }

func (receiveErrConn) Close(context.Context) error { return nil }

func (c receiveErrConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, c.err
}

func (receiveErrConn) Frontend() *pgproto3.Frontend { return nil }

func (receiveErrConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }

type eofConn struct{}

func (eofConn) Connect(context.Context) error { return nil }

func (eofConn) IsClosed() bool { return false }

func (eofConn) Close(context.Context) error { return nil }

func (eofConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF)
}

func (eofConn) Frontend() *pgproto3.Frontend { return nil }

func (eofConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
