package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotVisible(t *testing.T) {
	tests := []struct {
		name     string
		snapshot string
		xid      uint32
		visible  bool
	}{
		{name: "below xmin", snapshot: "100:200:150", xid: 50, visible: true},
		{name: "equal to xmin, not in xip", snapshot: "100:200:", xid: 100, visible: true},
		{name: "in xip (in progress)", snapshot: "100:200:150,160", xid: 150, visible: false},
		{name: "between xmin and xmax, not in xip", snapshot: "100:200:150", xid: 160, visible: true},
		{name: "equal to xmax (inside the commit gap)", snapshot: "100:200:", xid: 200, visible: false},
		{name: "above xmax", snapshot: "100:200:", xid: 250, visible: false},
		{name: "empty xip", snapshot: "100:200:", xid: 150, visible: true},
		// xid8 values carry the epoch; only the low 32 bits are compared.
		{name: "epoch 1 snapshot, xid committed", snapshot: "4294967390:4294967496:4294967400", xid: 150, visible: true},
		{name: "epoch 1 snapshot, xid in xip", snapshot: "4294967390:4294967496:4294967400", xid: 104, visible: false},
		{name: "epoch 1 snapshot, xid at xmax", snapshot: "4294967390:4294967496:", xid: 200, visible: false},
		// Wraparound at the 2^32 boundary: xmax just wrapped to 5, an old xid near 2^32 precedes it.
		{name: "wraparound: old xid precedes wrapped xmax", snapshot: "4294967290:4294967301:", xid: 4294967295, visible: true},
		{name: "wraparound: old xid still in xip", snapshot: "4294967290:4294967301:4294967295", xid: 4294967295, visible: false},
		{name: "wraparound: xid just below wrapped xmax", snapshot: "4294967290:4294967301:", xid: 3, visible: true},
		{name: "wraparound: xid at wrapped xmax", snapshot: "4294967290:4294967301:", xid: 5, visible: false},
		{name: "wraparound: xid far ahead is not visible", snapshot: "4294967290:4294967301:", xid: 2_000_000_000, visible: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, err := parseSnapshot(tt.snapshot)
			require.NoError(t, err)
			assert.Equal(t, tt.visible, snap.visible(tt.xid))
		})
	}
}

func TestParseSnapshotMalformed(t *testing.T) {
	for _, s := range []string{"", "garbage", "100:200", "100:200:150:1", "a:200:", "100:b:", "100:200:abc", "100:200:150,"} {
		_, err := parseSnapshot(s)
		assert.Error(t, err, s)
	}
}

// scriptedQuery returns one canned poll row per call; the last row repeats.
type scriptedQuery struct {
	rows  [][]string
	calls atomic.Int32
}

func (q *scriptedQuery) query(_ context.Context, _ string) ([]string, error) {
	n := int(q.calls.Add(1)) - 1
	if n >= len(q.rows) {
		n = len(q.rows) - 1
	}
	return q.rows[n], nil
}

type countingMetric struct {
	metric.Metric
	timeouts  atomic.Int32
	failOpens atomic.Int32
}

func (m *countingMetric) VisibilityTimeoutIncrement()  { m.timeouts.Add(1) }
func (m *countingMetric) VisibilityFailOpenIncrement() { m.failOpens.Add(1) }

func testGuard(query func(context.Context, string) ([]string, error)) (*visibilityGuard, *countingMetric) {
	m := &countingMetric{Metric: metric.NewMetric("test_slot")}
	return &visibilityGuard{
		conn:    &standbyCaptureConn{},
		query:   query,
		metric:  m,
		pollSQL: visibilityPollSQL,
		cfg:     config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailClosed, Timeout: 50 * time.Millisecond, PollInterval: time.Millisecond},
	}, m
}

func TestGuardWaitPollsUntilVisible(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}, {"f", "100:200:"}, {"f", "100:201:"}}}
	g, m := testGuard(q.query)

	require.NoError(t, g.wait(context.Background(), 200))
	assert.Equal(t, int32(3), q.calls.Load())
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestGuardWaitUsesCachedXmin(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}}}
	g, _ := testGuard(q.query)

	require.NoError(t, g.wait(context.Background(), 150)) // poll, caches xmin=100
	require.NoError(t, g.wait(context.Background(), 99))  // below cached xmin: no query
	assert.Equal(t, int32(1), q.calls.Load())

	require.NoError(t, g.wait(context.Background(), 100)) // at xmin: needs a fresh poll
	assert.Equal(t, int32(2), q.calls.Load())
}

func TestGuardWaitNoCacheBeforeFirstPoll(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}}}
	g, _ := testGuard(q.query)

	// Without a snapshot, an xid in the upper half must not "precede" xmin=0.
	require.NoError(t, g.wait(context.Background(), 150))
	assert.Equal(t, int32(1), q.calls.Load())
}

func TestGuardWaitTimeout(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}}}
	g, m := testGuard(q.query)

	err := g.wait(context.Background(), 200)
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	assert.Equal(t, int32(1), m.timeouts.Load())
	assert.Greater(t, q.calls.Load(), int32(1), "must keep polling until the deadline")
}

func TestGuardWaitRecoveryFlipIsAnError(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}, {"t", "100:200:"}}}
	g, m := testGuard(q.query)

	err := g.wait(context.Background(), 200)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestGuardWaitMalformedIsAnError(t *testing.T) {
	for _, row := range [][]string{{"f", "garbage"}, {"maybe", "100:200:"}, {"f"}, {}} {
		q := &scriptedQuery{rows: [][]string{row}}
		g, _ := testGuard(q.query)
		err := g.wait(context.Background(), 150)
		require.Error(t, err, row)
		require.NotErrorIs(t, err, ErrVisibilityTimeout)
	}
}

func TestGuardWaitQueryErrorIsAnError(t *testing.T) {
	boom := errors.New("connection reset")
	g, _ := testGuard(func(context.Context, string) ([]string, error) { return nil, boom })
	require.ErrorIs(t, g.wait(context.Background(), 150), boom)
}

func TestGuardWaitReturnsOnContextCancel(t *testing.T) {
	q := &scriptedQuery{rows: [][]string{{"f", "100:200:"}}}
	g, _ := testGuard(q.query)
	g.cfg.Timeout = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()

	require.ErrorIs(t, g.wait(ctx, 200), context.Canceled)
}

// openScript answers the open-time statements by SQL text.
func openScript(recovery, timeline, walSenderTimeout string, pollErr error, pollRow []string) func(context.Context, string) ([]string, error) {
	return func(_ context.Context, sql string) ([]string, error) {
		switch sql {
		case visibilityOpenSQL:
			if recovery == "t" {
				return nil, &pgconn.PgError{Code: "55000", Message: "recovery is in progress"}
			}
			return []string{recovery, timeline}, nil
		case walSenderTimeoutSQL:
			return []string{walSenderTimeout}, nil
		case visibilityPollSQL:
			if pollErr != nil {
				return nil, pollErr
			}
			return pollRow, nil
		case legacyPollSQL:
			return pollRow, nil
		}
		return nil, errors.New("unexpected statement: " + sql)
	}
}

func TestNewVisibilityGuardOpenChecks(t *testing.T) {
	cfg := config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailClosed, Timeout: 10 * time.Second, PollInterval: 5 * time.Millisecond}
	okRow := []string{"f", "100:200:"}
	undefined := &pgconn.PgError{Code: undefinedFunction, Message: "function pg_current_snapshot() does not exist"}

	tests := []struct {
		query   func(context.Context, string) ([]string, error)
		name    string
		wantErr string
		cfg     config.VisibilityGuardConfig
		legacy  bool
	}{
		{name: "primary on the right timeline", query: openScript("f", "7", "60000", nil, okRow), cfg: cfg},
		{name: "wal_sender_timeout disabled skips the ratio check", query: openScript("f", "7", "0", nil, okRow), cfg: cfg},
		{name: "in recovery", query: openScript("t", "7", "60000", nil, okRow), cfg: cfg, wantErr: "recovery"},
		{name: "timeline mismatch", query: openScript("f", "8", "60000", nil, okRow), cfg: cfg, wantErr: "timeline mismatch"},
		{name: "timeout >= wal_sender_timeout/2", query: openScript("f", "7", "20000", nil, okRow), cfg: cfg, wantErr: "wal_sender_timeout"},
		{name: "timeout just under wal_sender_timeout/2", query: openScript("f", "7", "20001", nil, okRow), cfg: cfg},
		{name: "PG < 13 falls back to txid_current_snapshot", query: openScript("f", "7", "60000", undefined, okRow), cfg: cfg, legacy: true},
		{name: "snapshot probe error", query: openScript("f", "7", "60000", errors.New("boom"), okRow), cfg: cfg, wantErr: "snapshot probe"},
		{name: "snapshot probe malformed", query: openScript("f", "7", "60000", nil, []string{"f", "bad"}), cfg: cfg, wantErr: "snapshot probe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := newVisibilityGuard(context.Background(), &standbyCaptureConn{}, tt.query, tt.cfg, 7, metric.NewMetric("test_slot"))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.legacy, g.pollSQL == legacyPollSQL)
			assert.True(t, g.hasSnapshot, "open probe must prime the snapshot cache")
		})
	}
}
