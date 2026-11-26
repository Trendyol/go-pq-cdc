package slot

import (
	"context"
	goerrors "errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestReplicationSlotCommandsQuoteTheName(t *testing.T) {
	if got := createReplicationSlotCommand("test-slot", false); got != `CREATE_REPLICATION_SLOT "test-slot" LOGICAL pgoutput` {
		t.Fatalf("create: got %s", got)
	}
	if got := createReplicationSlotCommand("test_slot", true); got != `CREATE_REPLICATION_SLOT "test_slot" LOGICAL pgoutput (FAILOVER true)` {
		t.Fatalf("create failover: got %s", got)
	}
	if got := alterReplicationSlotFailoverCommand("test-slot"); got != `ALTER_REPLICATION_SLOT "test-slot" (FAILOVER true)` {
		t.Fatalf("alter: got %s", got)
	}
}

// TestConfigValidateAcceptsAutoLiteral keeps the documented default usable: a
// config file may spell the default as "auto" instead of leaving it empty.
func TestConfigValidateAcceptsAutoLiteral(t *testing.T) {
	cfg := Config{Name: "cdc_slot", SlotActivityCheckerInterval: 3000, ProtoVersion: 2, LogicalMessages: "auto"}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("auto must be accepted as the documented default: %v", err)
	}
}

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

func TestConfigValidateSpill(t *testing.T) {
	validBase := func() Config {
		return Config{
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 1000,
			ProtoVersion:                2,
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:   "disabled zero spill config",
			mutate: func(_ *Config) {},
		},
		{
			name: "enabled valid defaults",
			mutate: func(c *Config) {
				c.Spill = StreamSpillConfig{
					Enabled:        true,
					MaxMemoryBytes: DefaultStreamSpillMaxMemoryBytes,
				}
			},
		},
		{
			name: "enabled with protoVersion unset (0)",
			mutate: func(c *Config) {
				c.ProtoVersion = 0
				c.Spill = StreamSpillConfig{
					Enabled:        true,
					MaxMemoryBytes: 1024,
					MaxDiskBytes:   4096,
					SpillDir:       "/tmp/cdc-spill",
				}
			},
		},
		{
			name: "disabled rejects non-zero maxMemoryBytes",
			mutate: func(c *Config) {
				c.Spill.MaxMemoryBytes = 1024
			},
			wantErr: "slot.spill.maxMemoryBytes requires slot.spill.enabled: true",
		},
		{
			name: "disabled rejects non-zero maxDiskBytes",
			mutate: func(c *Config) {
				c.Spill.MaxDiskBytes = 1024
			},
			wantErr: "slot.spill.maxDiskBytes requires slot.spill.enabled: true",
		},
		{
			name: "disabled rejects non-empty spillDir",
			mutate: func(c *Config) {
				c.Spill.SpillDir = "/tmp/spill"
			},
			wantErr: "slot.spill.spillDir requires slot.spill.enabled: true",
		},
		{
			name: "enabled rejects protoVersion 1",
			mutate: func(c *Config) {
				c.ProtoVersion = 1
				c.Spill = StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1024}
			},
			wantErr: "slot.spill requires slot.protoVersion >= 2",
		},
		{
			name: "enabled rejects zero maxMemoryBytes",
			mutate: func(c *Config) {
				c.Spill = StreamSpillConfig{Enabled: true, MaxMemoryBytes: 0}
			},
			wantErr: "slot.spill.maxMemoryBytes must be greater than 0",
		},
		{
			name: "enabled rejects negative maxMemoryBytes",
			mutate: func(c *Config) {
				c.Spill = StreamSpillConfig{Enabled: true, MaxMemoryBytes: -1}
			},
			wantErr: "slot.spill.maxMemoryBytes must be greater than 0",
		},
		{
			name: "enabled rejects negative maxDiskBytes",
			mutate: func(c *Config) {
				c.Spill = StreamSpillConfig{Enabled: true, MaxMemoryBytes: 1024, MaxDiskBytes: -10}
			},
			wantErr: "slot.spill.maxDiskBytes must not be negative",
		},
		{
			name: "enabled rejects non-zero maxDiskBytes smaller than maxMemoryBytes",
			mutate: func(c *Config) {
				c.Spill = StreamSpillConfig{Enabled: true, MaxMemoryBytes: 4096, MaxDiskBytes: 1024}
			},
			wantErr: "slot.spill.maxDiskBytes must be greater than or equal to slot.spill.maxMemoryBytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validBase()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}
