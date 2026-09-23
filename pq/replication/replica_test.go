package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowFunc adapts a closure to pgx.Row.
type rowFunc func(dest ...any) error

func (f rowFunc) Scan(dest ...any) error { return f(dest...) }

// replicaRow is one canned answer of replicaPollSQL.
type replicaRow struct {
	err      error
	replay   string
	timeline int32
	recovery bool
}

func standby(replay string) replicaRow {
	return replicaRow{recovery: true, replay: replay, timeline: 7}
}

// testSystemID is the session's cluster; the high bit is set so the int8 that
// pg_control_system() returns is negative and the uint64 compare is exercised.
const testSystemID uint64 = 0x8000000000000001

// scriptedReplica answers one row per QueryRow; the last row repeats.
type scriptedReplica struct {
	rows []replicaRow
	// systemID answers replicaSystemSQL; zero means the session's own cluster.
	systemID uint64
	calls    atomic.Int32
	closed   bool
}

func (r *scriptedReplica) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if sql == replicaSystemSQL {
		return rowFunc(func(dest ...any) error {
			id := r.systemID
			if id == 0 {
				id = testSystemID
			}
			*dest[0].(*int64) = int64(id)
			return nil
		})
	}
	n := int(r.calls.Add(1)) - 1
	if n >= len(r.rows) {
		n = len(r.rows) - 1
	}
	row := r.rows[n]
	return rowFunc(func(dest ...any) error {
		if row.err != nil {
			return row.err
		}
		*dest[0].(*bool) = row.recovery
		*dest[1].(*string) = row.replay
		*dest[2].(*int32) = row.timeline
		return nil
	})
}

func (r *scriptedReplica) IsClosed() bool              { return r.closed }
func (r *scriptedReplica) Close(context.Context) error { r.closed = true; return nil }

var fastWait = WaitOptions{Timeout: 30 * time.Millisecond, PollInterval: time.Millisecond}

func TestWaitReplayedIsStrict(t *testing.T) {
	const commit = pq.LSN(0x20)
	tests := []struct {
		name   string
		replay string
		ok     bool
	}{
		{name: "replay behind commit", replay: "0/10", ok: false},
		{name: "replay at commit (commit record not applied yet)", replay: "0/20", ok: false},
		{name: "replay past commit", replay: "0/21", ok: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &scriptedReplica{rows: []replicaRow{standby(tt.replay)}}
			err := WaitReplayed(context.Background(), r, commit, fastWait)
			if tt.ok {
				require.NoError(t, err)
				assert.Equal(t, int32(1), r.calls.Load())
				return
			}
			require.ErrorIs(t, err, ErrVisibilityTimeout)
			assert.Greater(t, r.calls.Load(), int32(1), "must keep polling until the deadline")
		})
	}
}

func TestWaitReplayedPollsUntilPassed(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{standby("0/10"), standby("0/20"), standby("0/21")}}
	require.NoError(t, WaitReplayed(context.Background(), r, 0x20, fastWait))
	assert.Equal(t, int32(3), r.calls.Load())
}

func TestWaitReplayedNotStandby(t *testing.T) {
	for name, row := range map[string]replicaRow{
		"not in recovery": {recovery: false, replay: "", timeline: 7},
		"NULL replay":     {recovery: true, replay: "", timeline: 7},
	} {
		t.Run(name, func(t *testing.T) {
			r := &scriptedReplica{rows: []replicaRow{row}}
			err := WaitReplayed(context.Background(), r, 0x20, fastWait)
			require.ErrorIs(t, err, ErrNotStandby)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			assert.Equal(t, int32(1), r.calls.Load(), "a wrong server is not retried")
		})
	}
}

func TestWaitReplayedQueryErrorIsReturnedUnchanged(t *testing.T) {
	boom := errors.New("connection reset")
	r := &scriptedReplica{rows: []replicaRow{{err: boom}}}
	err := WaitReplayed(context.Background(), r, 0x20, fastWait)
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	assert.Equal(t, int32(1), r.calls.Load(), "the caller owns the connection: no reconnect, no retry")
}

func TestWaitReplayedMalformedReplayIsAnError(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{standby("garbage")}}
	err := WaitReplayed(context.Background(), r, 0x20, fastWait)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotStandby)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
}

// The last poll is cut by the wait's own deadline: that is the timeout, not a
// server answer, and must not escape as a bare context error.
func TestWaitReplayedDeadlineCutPollIsATimeout(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{{err: context.DeadlineExceeded}}}
	err := WaitReplayed(context.Background(), r, 0x20, fastWait)
	require.ErrorIs(t, err, ErrVisibilityTimeout)
}

func TestWaitReplayedRejectsZeroCommitLSN(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{standby("0/21")}}
	require.Error(t, WaitReplayed(context.Background(), r, 0, fastWait))
	assert.Equal(t, int32(0), r.calls.Load())
}

func TestWaitReplayedReturnsOnContextCancel(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{standby("0/10")}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	require.ErrorIs(t, WaitReplayed(ctx, r, 0x20, WaitOptions{Timeout: time.Hour, PollInterval: time.Millisecond}), context.Canceled)
}

func TestWaitReplayedZeroOptionsUseDefaults(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{standby("0/21")}}
	require.NoError(t, WaitReplayed(context.Background(), r, 0x20, WaitOptions{}))
}

// dialer hands out one scripted connection per dial and counts the dials.
type dialer struct {
	err   error
	conns []*scriptedReplica
	dials atomic.Int32
}

func (d *dialer) dial(context.Context) (replicaConn, error) {
	n := int(d.dials.Add(1)) - 1
	if d.err != nil {
		return nil, d.err
	}
	if n >= len(d.conns) {
		n = len(d.conns) - 1
	}
	return d.conns[n], nil
}

func testReplicaGuard(names []string, dialers ...*dialer) (*replicaGuard, *countingMetric) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	m := &countingMetric{Metric: metric.NewMetric("test_slot")}
	g := &replicaGuard{metric: m, timeline: 7, systemID: strconv.FormatUint(testSystemID, 10), cfg: config.VisibilityGuardConfig{
		Enabled: true, FailMode: config.VisibilityFailClosed, Timeout: 30 * time.Millisecond, PollInterval: time.Millisecond,
	}}
	for i, d := range dialers {
		g.replicas = append(g.replicas, &replica{name: names[i], dial: d.dial})
	}
	return g, m
}

func lag(m *countingMetric, replica string) int {
	v, _ := m.lags.Load(replica)
	l, _ := v.(float64)
	return int(l)
}

func TestReplicaGuardOpenChecks(t *testing.T) {
	tests := []struct {
		dial    *dialer
		name    string
		wantErr string
	}{
		{name: "standby on the session timeline", dial: &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10")}}}}},
		{name: "standby on an older timeline (control file lags after failover)", dial: &dialer{conns: []*scriptedReplica{{rows: []replicaRow{{recovery: true, replay: "0/10", timeline: 6}}}}}},
		{name: "standby on a newer timeline", dial: &dialer{conns: []*scriptedReplica{{rows: []replicaRow{{recovery: true, replay: "0/10", timeline: 8}}}}}, wantErr: "timeline 8 is ahead"},
		{name: "primary", dial: &dialer{conns: []*scriptedReplica{{rows: []replicaRow{{recovery: false, timeline: 7}}}}}, wantErr: "not in recovery"},
		{name: "unreachable", dial: &dialer{err: errors.New("connection refused")}, wantErr: "dial: connection refused"},
		{name: "standby of another cluster", dial: &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10")}, systemID: 42}}}, wantErr: "is not the replication session's"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, _ := testReplicaGuard([]string{"standby1:5432"}, tt.dial)
			err := g.open(context.Background())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, err, "replica standby1:5432")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestReplicaGuardWaitsForEveryReplicaUnderOneDeadline(t *testing.T) {
	slow := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10"), standby("0/30")}}}}
	fast := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/30")}}}}
	g, m := testReplicaGuard([]string{"standby1:5432", "standby2:5432"}, slow, fast)

	require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(g.cfg.Timeout)))
	assert.Equal(t, int32(2), slow.conns[0].calls.Load())
	assert.Equal(t, int32(1), fast.conns[0].calls.Load())
	assert.Equal(t, 0, lag(m, "standby1:5432"), "gauge is 0 once applied")
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestReplicaGuardLagGaugeWhileBehind(t *testing.T) {
	d := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10")}}}}
	g, m := testReplicaGuard([]string{"standby1:5432"}, d)

	err := g.wait(context.Background(), 200, 0x20, time.Now().Add(g.cfg.Timeout))
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "replica standby1:5432")
	assert.Equal(t, 0x10, lag(m, "standby1:5432"))
	assert.Equal(t, int32(1), m.timeouts.Load())
}

func TestReplicaGuardReconnectsAfterConnectionError(t *testing.T) {
	broken := &scriptedReplica{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}}
	healthy := &scriptedReplica{rows: []replicaRow{standby("0/30")}}
	d := &dialer{conns: []*scriptedReplica{broken, healthy}}
	g, _ := testReplicaGuard([]string{"standby1:5432"}, d)

	require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(g.cfg.Timeout)))
	assert.Equal(t, int32(2), d.dials.Load(), "a standby restart is routine: redial, do not fail the stream")
	assert.True(t, broken.closed, "the broken connection is dropped")
	assert.Equal(t, int32(1), healthy.calls.Load())
}

func TestReplicaGuardUnreachableUntilDeadlineIsATimeout(t *testing.T) {
	d := &dialer{err: errors.New("connection refused")}
	g, m := testReplicaGuard([]string{"standby1:5432"}, d)

	err := g.wait(context.Background(), 200, 0x20, time.Now().Add(g.cfg.Timeout))
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "connection refused")
	assert.Greater(t, d.dials.Load(), int32(1))
	assert.Equal(t, int32(1), m.timeouts.Load())
}

func TestReplicaGuardServerErrorsAreFatal(t *testing.T) {
	tests := []struct {
		name string
		row  replicaRow
	}{
		{name: "pg error (poll rejected)", row: replicaRow{err: &pgconn.PgError{Code: "42501", Message: "permission denied"}}},
		{name: "promoted mid-wait", row: replicaRow{recovery: false, timeline: 7}},
		{name: "timeline jumps ahead mid-wait", row: replicaRow{recovery: true, replay: "0/30", timeline: 8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/10"), tt.row}}}}
			g, m := testReplicaGuard([]string{"standby1:5432"}, d)

			err := g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Hour))
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			assert.Equal(t, int32(1), d.dials.Load(), "the server answered: no reconnect")
			assert.Equal(t, int32(0), m.timeouts.Load())
		})
	}
}

// A server answer that lands after the shared budget is spent (the primary
// guard used it) stays fatal: failMode open must not dispatch on it.
func TestReplicaGuardServerErrorsAreFatalAfterTheDeadline(t *testing.T) {
	tests := []struct {
		name string
		row  replicaRow
	}{
		{name: "pg error (poll rejected)", row: replicaRow{err: &pgconn.PgError{Code: "42501", Message: "permission denied"}}},
		{name: "promoted", row: replicaRow{recovery: false, timeline: 7}},
		{name: "timeline ahead", row: replicaRow{recovery: true, replay: "0/30", timeline: 8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dialer{conns: []*scriptedReplica{{rows: []replicaRow{tt.row}}}}
			g, m := testReplicaGuard([]string{"standby1:5432"}, d)

			err := g.wait(context.Background(), 200, 0x20, time.Now()) // no budget left
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrVisibilityTimeout)
			assert.Equal(t, int32(0), m.timeouts.Load())
		})
	}
}

func TestWaitReplayedNotStandbyAfterTheDeadline(t *testing.T) {
	r := &scriptedReplica{rows: []replicaRow{{recovery: false}}}
	err := WaitReplayed(context.Background(), r, 0x20, WaitOptions{Timeout: time.Nanosecond, PollInterval: time.Millisecond})
	require.ErrorIs(t, err, ErrNotStandby)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
}

func TestReplicaGuardCloseClosesDialedConnections(t *testing.T) {
	c := &scriptedReplica{rows: []replicaRow{standby("0/30")}}
	g, _ := testReplicaGuard([]string{"standby1:5432", "standby2:5432"}, &dialer{conns: []*scriptedReplica{c}}, &dialer{conns: []*scriptedReplica{{rows: []replicaRow{standby("0/30")}}}})
	require.NoError(t, g.open(context.Background()))
	require.NoError(t, g.close(context.Background()))
	assert.True(t, c.closed)
}

// Gate wiring: primary guard passes at once (xid 200 < xmax 201), then the replica guard runs.

func gatedWithReplica(t *testing.T, mode config.VisibilityFailMode, rows ...replicaRow) (*gatedStream, *scriptedReplica) {
	t.Helper()
	gs := newGatedStream(t, guardCfg(mode), [][]string{{"f", "100:201:"}})
	r := &scriptedReplica{rows: rows}
	g, _ := testReplicaGuard([]string{"standby1:5432"}, &dialer{conns: []*scriptedReplica{r}})
	g.metric = gs.m
	g.cfg = gs.s.config.VisibilityGuard
	gs.s.replicas = g
	return gs, r
}

func TestGateWaitsForReplicasAfterPrimary(t *testing.T) {
	gs, r := gatedWithReplica(t, config.VisibilityFailClosed, standby("0/1"), standby("0/3"))
	require.NoError(t, gs.run(txMsg(200, 2, 2), txMsg(200, 3, 2)))
	assert.Equal(t, []uint32{200, 200}, gs.received)
	assert.Equal(t, int32(2), r.calls.Load(), "replica polled until replay > CommitLSN, once per transaction")
	assert.Equal(t, int32(1), gs.q.calls.Load(), "primary polled once")
}

// A message without a CommitLSN never comes from a decoded BEGIN (snapshot
// events do not pass through the process loop), so with replicas listed the
// gate fails closed rather than skipping the standbys.
func TestGateRejectsZeroCommitLSNWhenReplicasAreListed(t *testing.T) {
	gs, r := gatedWithReplica(t, config.VisibilityFailClosed, standby("0/1"))
	noBegin := &Message{message: &format.Insert{XID: 200, TableName: "books"}, walStart: 1, xid: 200}
	err := gs.run(noBegin)
	require.ErrorIs(t, err, ErrVisibilityGuard)
	require.ErrorContains(t, err, "without a decoded BEGIN")
	assert.Empty(t, gs.received)
	assert.Equal(t, int32(0), r.calls.Load())
}

// Two transactions are told apart by (xid, commitLSN): a repeated xid with a
// new commit record (wraparound) is gated again.
func TestGateGatesAgainWhenCommitLSNChangesUnderTheSameXid(t *testing.T) {
	gs, r := gatedWithReplica(t, config.VisibilityFailClosed, standby("0/3"), standby("0/3"), standby("0/9"))
	first := txMsg(200, 2, 2)
	second := txMsg(200, 8, 8)
	require.NoError(t, gs.run(first, second))
	assert.Equal(t, []uint32{200, 200}, gs.received)
	assert.Equal(t, int32(3), r.calls.Load(), "second transaction polled until replay > its own CommitLSN")
}

// A standby restart answers 57P01 on the open connection before it closes;
// that is redialed like a reset, not treated as a verdict (design R5).
func TestReplicaGuardRedialsAfterAdminShutdown(t *testing.T) {
	stopping := &scriptedReplica{rows: []replicaRow{{err: &pgconn.PgError{Code: postgresAdminShutdown, Message: "terminating connection due to administrator command"}}}}
	back := &scriptedReplica{rows: []replicaRow{standby("0/30")}}
	d := &dialer{conns: []*scriptedReplica{stopping, back}}
	g, m := testReplicaGuard([]string{"standby1:5432"}, d)

	require.NoError(t, g.wait(context.Background(), 200, 0x20, time.Now().Add(g.cfg.Timeout)))
	assert.Equal(t, int32(2), d.dials.Load())
	assert.True(t, stopping.closed)
	assert.Equal(t, int32(0), m.timeouts.Load())
}

// A redial lands on whatever now answers at host:port; a standby of another
// cluster is fatal there exactly as it is at open.
func TestReplicaGuardReconnectToAnotherClusterIsFatal(t *testing.T) {
	broken := &scriptedReplica{rows: []replicaRow{{err: io.ErrUnexpectedEOF}}}
	foreign := &scriptedReplica{rows: []replicaRow{standby("0/30")}, systemID: 42}
	d := &dialer{conns: []*scriptedReplica{broken, foreign}}
	g, m := testReplicaGuard([]string{"standby1:5432"}, d)

	err := g.wait(context.Background(), 200, 0x20, time.Now().Add(time.Hour))
	require.ErrorIs(t, err, errClusterMismatch)
	require.NotErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "replica standby1:5432")
	assert.Equal(t, int32(2), d.dials.Load())
	assert.Equal(t, int32(0), foreign.calls.Load(), "no replay poll on a foreign cluster")
	assert.Equal(t, int32(0), m.timeouts.Load())
}

func TestGateReplicaTimeoutFailClosedStopsProcessing(t *testing.T) {
	gs, _ := gatedWithReplica(t, config.VisibilityFailClosed, standby("0/1"))
	err := gs.run(txMsg(200, 2, 2))
	require.ErrorIs(t, err, ErrVisibilityGuard)
	require.ErrorIs(t, err, ErrVisibilityTimeout)
	require.ErrorContains(t, err, "replica standby1:5432")
	assert.Empty(t, gs.received)
	assert.Equal(t, pq.LSN(0), gs.s.LoadConfirmedXLogPos(), "held message stays un-acked")
	assert.Equal(t, int32(1), gs.m.timeouts.Load())
}

func TestGateReplicaTimeoutFailOpenDispatches(t *testing.T) {
	gs, _ := gatedWithReplica(t, config.VisibilityFailOpen, standby("0/1"))
	require.NoError(t, gs.run(txMsg(200, 2, 2), txMsg(200, 3, 2)))
	assert.Equal(t, []uint32{200, 200}, gs.received)
	assert.Equal(t, int32(1), gs.m.failOpens.Load())
}

func TestGateReplicaNotStandbyIsFatalEvenFailOpen(t *testing.T) {
	gs, _ := gatedWithReplica(t, config.VisibilityFailOpen, replicaRow{recovery: false, timeline: 7})
	err := gs.run(txMsg(200, 2, 2))
	require.ErrorIs(t, err, ErrVisibilityGuard)
	require.ErrorIs(t, err, ErrNotStandby)
	assert.Empty(t, gs.received)
	assert.Equal(t, int32(0), gs.m.failOpens.Load())
}

// Replay cache (design R6): a fully checked poll on the same connection
// certifies lower commits for replayCacheTTL without another query.

func TestReplicaGuardCachedReplayCertifiesLowerCommits(t *testing.T) {
	c := &scriptedReplica{rows: []replicaRow{standby("0/30")}}
	g, _ := testReplicaGuard([]string{"standby1:5432"}, &dialer{conns: []*scriptedReplica{c}})
	g.cfg.Timeout = time.Second

	for _, commit := range []pq.LSN{0x10, 0x20, 0x2F} {
		require.NoError(t, g.wait(context.Background(), 200, commit, time.Now().Add(g.cfg.Timeout)))
	}
	assert.Equal(t, int32(1), c.calls.Load(), "one poll certifies every commit below its replay position")

	err := g.wait(context.Background(), 200, 0x30, time.Now().Add(30*time.Millisecond))
	require.ErrorIs(t, err, ErrVisibilityTimeout, "replay == commitLSN is not applied: strict, like the poll")
	assert.Greater(t, c.calls.Load(), int32(1))
}

func TestReplicaCachedAge(t *testing.T) {
	now := time.Now()
	open := &scriptedReplica{}
	tests := []struct {
		conn       replicaConn
		observedAt time.Time
		name       string
		replayed   pq.LSN
		want       bool
	}{
		{name: "fresh and above", conn: open, replayed: 0x30, observedAt: now.Add(-100 * time.Millisecond), want: true},
		{name: "equal to commit", conn: open, replayed: 0x20, observedAt: now},
		{name: "expired", conn: open, replayed: 0x30, observedAt: now.Add(-replayCacheTTL)},
		{name: "from the future (wall clock stepped back)", conn: open, replayed: 0x30, observedAt: now.Add(time.Millisecond)},
		{name: "no monotonic reading, wall age expired", conn: open, replayed: 0x30, observedAt: now.Add(-2 * replayCacheTTL).Round(0)},
		{name: "never polled", conn: open, replayed: 0x30},
		{name: "no connection", replayed: 0x30, observedAt: now},
		{name: "closed connection", conn: &scriptedReplica{closed: true}, replayed: 0x30, observedAt: now},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &replica{conn: tt.conn, replayed: tt.replayed, observedAt: tt.observedAt}
			_, ok := r.cachedAge(0x20, now)
			assert.Equal(t, tt.want, ok)
		})
	}
}

func TestReplicaGuardCacheDroppedWithTheConnection(t *testing.T) {
	first := &scriptedReplica{rows: []replicaRow{standby("0/30"), {err: io.ErrUnexpectedEOF}}}
	second := &scriptedReplica{rows: []replicaRow{standby("0/40")}}
	d := &dialer{conns: []*scriptedReplica{first, second}}
	g, _ := testReplicaGuard([]string{"standby1:5432"}, d)

	require.NoError(t, g.wait(context.Background(), 200, 0x10, time.Now().Add(time.Second)))
	require.NoError(t, g.wait(context.Background(), 201, 0x35, time.Now().Add(time.Second)))
	assert.Equal(t, int32(2), d.dials.Load())
	assert.Equal(t, pq.LSN(0x40), g.replicas[0].replayed, "cache rebuilt from the new connection only")

	g.replicas[0].conn.(*scriptedReplica).closed = true
	require.NoError(t, g.wait(context.Background(), 202, 0x10, time.Now().Add(time.Second)))
	assert.Equal(t, int32(3), d.dials.Load(), "a closed connection never serves a hit")
}

func TestReplicaGuardRejectedAnswerIsNotCached(t *testing.T) {
	c := &scriptedReplica{rows: []replicaRow{{recovery: true, replay: "0/30", timeline: 8}}}
	g, _ := testReplicaGuard([]string{"standby1:5432"}, &dialer{conns: []*scriptedReplica{c}})

	require.ErrorContains(t, g.wait(context.Background(), 200, 0x10, time.Now().Add(time.Second)), "timeline 8 is ahead")
	assert.Zero(t, g.replicas[0].replayed)
	assert.True(t, g.replicas[0].observedAt.IsZero())
}

func TestReplicaGuardCachesPerReplica(t *testing.T) {
	ahead := &scriptedReplica{rows: []replicaRow{standby("0/30")}}
	behind := &scriptedReplica{rows: []replicaRow{standby("0/15"), standby("0/15"), standby("0/30")}}
	g, _ := testReplicaGuard([]string{"standby1:5432", "standby2:5432"}, &dialer{conns: []*scriptedReplica{ahead}}, &dialer{conns: []*scriptedReplica{behind}})

	require.NoError(t, g.wait(context.Background(), 200, 0x10, time.Now().Add(time.Second)))
	require.NoError(t, g.wait(context.Background(), 201, 0x20, time.Now().Add(time.Second)))
	assert.Equal(t, int32(1), ahead.calls.Load(), "ahead replica served from its cache")
	assert.Equal(t, int32(3), behind.calls.Load(), "behind replica polled until it passes the new commit")
}
