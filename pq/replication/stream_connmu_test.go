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
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// serializeProbeConn is a pq.Connection fake that flags any overlap between a
// ReceiveMessage (the sink's read) and a frontend write (a standby status
// update). The bug this guards against: the read path toggles the socket
// deadline via pgconn's context watcher while an off-goroutine ack writes to the
// same connection, so the write inherits a past deadline and fails with an i/o
// timeout, corrupting the stream and panicking the sink. connMu must keep the
// read and every write mutually exclusive.
type serializeProbeConn struct {
	fe        *pgproto3.Frontend
	keepalive []byte

	active   int32 // in-flight read-or-write ops; must never exceed 1
	overlaps int32
	stop     atomic.Bool
}

// mark records the start of a read or write, flags it if another op is already
// in flight, holds briefly to widen the overlap window, and returns a closure
// that records the end.
func (c *serializeProbeConn) mark() func() {
	if atomic.AddInt32(&c.active, 1) != 1 {
		atomic.AddInt32(&c.overlaps, 1)
	}
	time.Sleep(20 * time.Microsecond)
	return func() { atomic.AddInt32(&c.active, -1) }
}

func (c *serializeProbeConn) Connect(context.Context) error                          { return nil }
func (c *serializeProbeConn) IsClosed() bool                                         { return false }
func (c *serializeProbeConn) Close(context.Context) error                            { return nil }
func (c *serializeProbeConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
func (c *serializeProbeConn) Frontend() *pgproto3.Frontend                           { return c.fe }

func (c *serializeProbeConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	if c.stop.Load() {
		return nil, errors.New("stopped")
	}
	defer c.mark()()
	// A keepalive with ReplyRequested=0 keeps the sink looping without making it
	// send a status update itself, so every write the probe sees comes from the
	// concurrent ackers.
	return &pgproto3.CopyData{Data: c.keepalive}, nil
}

// serializeProbeWriter is the io.Writer behind the fake's frontend; every
// standby status update is written here.
type serializeProbeWriter struct{ c *serializeProbeConn }

func (w serializeProbeWriter) Write(p []byte) (int, error) {
	defer w.c.mark()()
	return len(p), nil
}

// TestStandbyUpdateIsSerializedWithReceive runs the real sink loop against a fake
// connection while four goroutines ack off the sink goroutine — the way the
// consumer's coalescer does. With connMu, the read and the writes never overlap.
func TestStandbyUpdateIsSerializedWithReceive(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	c := &serializeProbeConn{keepalive: make([]byte, 18)}
	c.keepalive[0] = message.PrimaryKeepaliveMessageByteID // 'k'; 17 zero body bytes => ReplyRequested=false
	c.fe = pgproto3.NewFrontend(strings.NewReader(""), serializeProbeWriter{c})

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = c
	s.lastXLogPos = 1 // so sendStandbyStatusUpdate writes a real payload

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sinkLoop(ctx, &messageBuffer{outCh: make(chan *Message, 4096)}, &streamTxBuffer{})
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = s.sendStandbyStatusUpdate(ctx)
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)

	// Stop cleanly: closed=true makes the sink treat the next read error as a
	// normal stop (returns false, no panic). Set closed before stop so it's
	// observed by the time the read errors; cancel unblocks the ackers.
	s.closed.Store(true)
	c.stop.Store(true)
	cancel()
	wg.Wait()

	if got := atomic.LoadInt32(&c.overlaps); got != 0 {
		t.Fatalf("read and standby-update write overlapped %d time(s); connMu did not serialize connection access", got)
	}
}
