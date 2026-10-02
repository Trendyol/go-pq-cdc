package slot

import (
	"context"
	goerrors "errors"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// fakeSlotConn is a pq.Connection whose Exec always fails: Exec on a
// zero-value pgconn is rejected by its status check and yields a closed,
// error-carrying MultiResultReader without panicking. The tests assert on the
// reconnect/retry decisions via the call counters, not on query results.
type fakeSlotConn struct {
	closed       bool
	connectErr   error
	connectCalls int
	execCalls    int
	execHook     func(call int)
}

func (c *fakeSlotConn) Connect(context.Context) error {
	c.connectCalls++
	if c.connectErr != nil {
		return c.connectErr
	}
	c.closed = false
	return nil
}

func (c *fakeSlotConn) IsClosed() bool { return c.closed }

func (c *fakeSlotConn) Close(context.Context) error {
	c.closed = true
	return nil
}

func (c *fakeSlotConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, goerrors.New("unused")
}

func (c *fakeSlotConn) Frontend() *pgproto3.Frontend { return nil }

func (c *fakeSlotConn) Exec(ctx context.Context, sql string) *pgconn.MultiResultReader {
	c.execCalls++
	if c.execHook != nil {
		c.execHook(c.execCalls)
	}
	return (&pgconn.PgConn{}).Exec(ctx, sql)
}

func newTestSlot(conn pq.Connection) *Slot {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	return &Slot{
		cfg:       Config{Name: "test_slot"},
		conn:      conn,
		statusSQL: "SELECT 1",
	}
}

func TestInfoReconnectsClosedConnectionBeforeQuery(t *testing.T) {
	conn := &fakeSlotConn{closed: true}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected query error to surface after pre-reconnect")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 connect (pre-reconnect), got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected exactly 1 exec after pre-reconnect, got %d", conn.execCalls)
	}
}

func TestInfoRetriesOnceAfterConnectionDiesMidQuery(t *testing.T) {
	conn := &fakeSlotConn{}
	// First exec kills the connection mid-query; the retry runs on the
	// reconnected conn (hook cleared) and its error surfaces.
	conn.execHook = func(call int) {
		if call == 1 {
			conn.closed = true
			conn.execHook = nil
		}
	}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected retry error to surface")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 reconnect, got %d", conn.connectCalls)
	}
	if conn.execCalls != 2 {
		t.Fatalf("expected exactly 2 execs (initial + one retry), got %d", conn.execCalls)
	}
}

func TestInfoDoesNotRetryQueryErrorOnLiveConnection(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected query error to surface")
	}
	if conn.connectCalls != 0 {
		t.Fatalf("expected no reconnect for a non-connection error, got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected exactly 1 exec, got %d", conn.execCalls)
	}
}

func TestInfoReturnsErrorWhenReconnectFails(t *testing.T) {
	conn := &fakeSlotConn{connectErr: goerrors.New("dial failed")}
	conn.execHook = func(int) { conn.closed = true }
	s := newTestSlot(conn)

	_, err := s.Info(context.Background())
	if err == nil {
		t.Fatal("expected reconnect error to surface")
	}
	if conn.connectCalls != 1 {
		t.Fatalf("expected 1 reconnect attempt, got %d", conn.connectCalls)
	}
	if conn.execCalls != 1 {
		t.Fatalf("expected no query retry after failed reconnect, got %d execs", conn.execCalls)
	}
}

func TestInfoDuringShutdownDoesNotReconnect(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)
	// Close() flips closed outside the mutex; simulate it landing mid-query.
	conn.execHook = func(int) {
		conn.closed = true
		s.closed.Store(true)
	}

	_, err := s.Info(context.Background())
	if !goerrors.Is(err, ErrorSlotClosed) {
		t.Fatalf("expected ErrorSlotClosed, got %v", err)
	}
	if conn.connectCalls != 0 {
		t.Fatalf("expected no reconnect during shutdown, got %d", conn.connectCalls)
	}
}

func TestInfoOnClosedSlotReturnsSlotClosed(t *testing.T) {
	conn := &fakeSlotConn{}
	s := newTestSlot(conn)
	s.closed.Store(true)

	_, err := s.Info(context.Background())
	if !goerrors.Is(err, ErrorSlotClosed) {
		t.Fatalf("expected ErrorSlotClosed, got %v", err)
	}
	if conn.execCalls != 0 || conn.connectCalls != 0 {
		t.Fatalf("expected no query activity on closed slot, got %d execs, %d connects", conn.execCalls, conn.connectCalls)
	}
}

// A physical / not-yet-reserved slot reports an empty confirmed_flush_lsn.
// It must not blow up with a cryptic "lsn parse: EOF"; the empty column is
// skipped and the logical-type check yields a clear error instead.
func TestDecodeSlotInfoResult_EmptyConfirmedFlushLSN(t *testing.T) {
	result := &pgconn.Result{
		FieldDescriptions: []pgconn.FieldDescription{
			{Name: "slot_name", DataTypeOID: pgtype.TextOID},
			{Name: "slot_type", DataTypeOID: pgtype.TextOID},
			{Name: "restart_lsn", DataTypeOID: pgtype.TextOID},
			{Name: "confirmed_flush_lsn", DataTypeOID: pgtype.TextOID},
		},
		Rows: [][][]byte{{
			[]byte("contents_to_contentmedias_slot"),
			[]byte("physical"),
			[]byte("0/1A2B3C4"),
			[]byte(""), // NULL pg_lsn decoded as empty string
		}},
	}

	info, err := decodeSlotInfoResult(result)
	if err != nil {
		t.Fatalf("expected empty confirmed_flush_lsn to be skipped, got: %v", err)
	}
	if info.ConfirmedFlushLSN != 0 {
		t.Fatalf("expected zero ConfirmedFlushLSN, got: %v", info.ConfirmedFlushLSN)
	}
	if info.Type != Physical {
		t.Fatalf("expected Physical type, got: %v", info.Type)
	}
}
