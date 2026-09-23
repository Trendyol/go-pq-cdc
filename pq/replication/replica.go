package replication

import (
	"context"
	goerrors "errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the subset of *pgx.Conn, *pgxpool.Pool, *pgxpool.Conn and pgx.Tx
// that WaitReplayed needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// WaitOptions bounds WaitReplayed. Zero values take the visibility guard
// defaults: 10s timeout, 5ms poll interval doubling to 250ms with jitter.
type WaitOptions struct {
	Timeout      time.Duration
	PollInterval time.Duration
}

// ErrNotStandby reports that the server answering the replay check is not in
// recovery: a primary, a promoted standby, or a pooler that routed to one.
// Never read it as "already applied".
var ErrNotStandby = goerrors.New("server is not in recovery (primary, promoted standby, or pooler routed to one)")

// errReplicaConn marks a replica poll that failed before the server answered;
// the guard reconnects and retries until the deadline.
var errReplicaConn = goerrors.New("replica connection")

// replicaPollSQL is the one statement behind every replay check.
// pg_last_wal_replay_lsn() is NULL on a primary; coalesce keeps the scan simple.
// min_recovery_end_timeline follows the replay timeline on every buffer flush
// (UpdateMinRecoveryPoint) and needs no grant; pg_walfile_name would ERROR here.
// inet_server_addr/port name the backend that answered (empty over a unix
// socket); they are logged so a poll can be tied to a host, never trusted.
const replicaPollSQL = "SELECT pg_is_in_recovery(), coalesce(pg_last_wal_replay_lsn()::text, ''), (pg_control_recovery()).min_recovery_end_timeline, coalesce(host(inet_server_addr()) || ':' || inet_server_port(), '')"

// replicaSystemSQL identifies the cluster behind a listed standby. It runs on
// every new connection: a host:port from another cluster is in recovery and
// answers with a replay position from a history CommitLSN is not part of, which
// passes the replay rule at once and certifies a read that never happened.
const replicaSystemSQL = "SELECT (pg_control_system()).system_identifier"

// replayCacheTTL bounds how long a replica's last replay position certifies
// lower commits without a poll (design R6). Replay only moves backwards across
// a standby restart, which kills the guard's backend; the ttl bounds the case
// where that kill goes unobserved (half-open socket), and the skipped
// in-recovery, timeline and identity checks.
// ponytail: const; a field when a deployment needs N replicas x RTT > 1s.
const replayCacheTTL = time.Second

// errClusterMismatch marks a server answer that puts the standby outside the
// replication session's cluster; fatal in both fail modes.
var errClusterMismatch = goerrors.New("replica cluster mismatch")

// WaitReplayed blocks until the standby behind q has replayed the transaction
// whose commit record starts at commitLSN (ListenerContext.CommitLSN):
// pg_last_wal_replay_lsn() > commitLSN. The comparison is strict because the
// function reports the end of the last replayed record, which equals
// commitLSN right before the commit record itself is applied.
//
// Run it on the connection you will read from, then read in a separate
// statement under READ COMMITTED. It returns ErrNotStandby when q reaches a
// server that is not in recovery and ErrVisibilityTimeout after opts.Timeout.
func WaitReplayed(ctx context.Context, q Querier, commitLSN pq.LSN, opts WaitOptions) error {
	if commitLSN == 0 {
		return goerrors.New("commitLSN is 0 (snapshot event?)")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Millisecond
	}
	poll := func(ctx context.Context) (replicaState, error) { return pollReplica(ctx, q) }
	return waitReplayed(ctx, poll, commitLSN, time.Now().Add(opts.Timeout), opts.PollInterval)
}

// replicaState is one answer of replicaPollSQL from a server in recovery.
type replicaState struct {
	server   string // host:port the backend reports, "" over a unix socket
	replay   pq.LSN
	timeline int32
}

func pollReplica(ctx context.Context, q Querier) (replicaState, error) {
	var inRecovery bool
	var replay string
	var st replicaState
	if err := q.QueryRow(ctx, replicaPollSQL).Scan(&inRecovery, &replay, &st.timeline, &st.server); err != nil {
		return replicaState{}, err
	}
	if !inRecovery || replay == "" {
		return replicaState{}, ErrNotStandby
	}
	lsn, err := pq.ParseLSN(replay)
	if err != nil {
		return replicaState{}, fmt.Errorf("replay position %q: %w", replay, err)
	}
	st.replay = lsn
	return st, nil
}

// waitReplayed polls until the replay position passes commitLSN or deadline.
// Every poll runs under the deadline. An errReplicaConn failure is retried
// after a backoff; any other error is returned as is. Errors are never
// treated as "replayed".
// No cache here: WaitReplayed runs on a consumer's connection, possibly
// pooled, with no stable backend to tie a stored position to. The stream's
// replicaGuard keeps one per connection (replica.cachedAge).
func waitReplayed(ctx context.Context, poll func(context.Context) (replicaState, error), commitLSN pq.LSN, deadline time.Time, delay time.Duration) error {
	for {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		st, err := poll(pollCtx)
		cancel()
		if err == nil && st.replay > commitLSN {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Before the deadline check: the server answered, and that answer is
		// fatal in both fail modes. Classifying it as a timeout because the
		// budget happened to run out would let failMode open dispatch it.
		// A poll cut by this wait's own deadline is the timeout itself, not an
		// answer: the parent's cancellation already returned above.
		if err != nil && !goerrors.Is(err, errReplicaConn) && !goerrors.Is(err, context.DeadlineExceeded) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrVisibilityTimeout, err)
			}
			return fmt.Errorf("%w: replay %s has not passed commit %s", ErrVisibilityTimeout, st.replay, commitLSN)
		}
		var sleep time.Duration
		sleep, delay = backoff(delay, remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

// replicaGuard holds the first message of every transaction until each listed
// standby has replayed its commit record. It runs after the primary guard,
// under the same deadline and failMode. See docs/replica-guard-design.md.
type replicaGuard struct {
	metric   metric.Metric
	systemID string // IDENTIFY_SYSTEM's, unsigned decimal
	replicas []*replica
	cfg      config.VisibilityGuardConfig
	timeline int32
}

// replicaConn is what the guard needs from *pgx.Conn.
type replicaConn interface {
	Querier
	IsClosed() bool
	Close(ctx context.Context) error
}

type replica struct {
	observedAt time.Time // start of the poll that returned replayed; zero = no cache
	dial       func(ctx context.Context) (replicaConn, error)
	conn       replicaConn // nil until dialed; dropped on any connection error
	name       string      // host:port from config; metric label and log key
	server     string      // backend address of the cached poll
	replayed   pq.LSN      // replay position of the last fully checked poll on conn
}

// cachedAge reports whether the last poll on this connection already proves
// commitLSN applied (same strict rule as the poll) and is younger than
// replayCacheTTL. Both the monotonic and the wall-clock age must be in range:
// the monotonic clock stops while the host is suspended, the wall clock can
// step, and a negative age is never fresh.
func (r *replica) cachedAge(commitLSN pq.LSN, now time.Time) (time.Duration, bool) {
	if r.observedAt.IsZero() || r.conn == nil || r.conn.IsClosed() || r.replayed <= commitLSN {
		return 0, false
	}
	age := now.Sub(r.observedAt)
	wall := now.Round(0).Sub(r.observedAt.Round(0))
	if age < 0 || wall < 0 || age >= replayCacheTTL || wall >= replayCacheTTL {
		return 0, false
	}
	return age, true
}

func (r *replica) forget() { r.replayed, r.observedAt, r.server = 0, time.Time{}, "" }

func openReplicaGuard(ctx context.Context, cfg config.Config, system *pq.IdentifySystemResult, m metric.Metric) (*replicaGuard, error) {
	g := &replicaGuard{metric: m, cfg: cfg.VisibilityGuard, timeline: system.Timeline, systemID: system.SystemID}
	for _, hostPort := range cfg.VisibilityGuard.Replicas {
		dsn := cfg.ReplicaDSN(hostPort)
		g.replicas = append(g.replicas, &replica{
			name: hostPort,
			dial: func(ctx context.Context) (replicaConn, error) { return pgx.Connect(ctx, dsn) },
		})
	}
	if err := g.open(ctx); err != nil {
		_ = g.close(ctx)
		return nil, err
	}
	return g, nil
}

// open dials every replica once: each must belong to the replication session's
// cluster (checked by poll on every new connection) and be in recovery on a
// timeline that is not ahead of it, or startup fails.
func (g *replicaGuard) open(ctx context.Context) error {
	for _, r := range g.replicas {
		if _, err := g.poll(ctx, r, 0); err != nil {
			return fmt.Errorf("replica %s: %w", r.name, err)
		}
	}
	return nil
}

// checkSystemID compares the standby's cluster with the replication session's.
// IDENTIFY_SYSTEM prints the identifier unsigned while pg_control_system()
// returns it as int8, so the two are compared as uint64 bits, never as text.
func (g *replicaGuard) checkSystemID(ctx context.Context, r *replica) error {
	want, err := strconv.ParseUint(g.systemID, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: parse replication session system identifier %q: %w", errClusterMismatch, g.systemID, err)
	}
	var got int64
	if err = r.conn.QueryRow(ctx, replicaSystemSQL).Scan(&got); err != nil {
		return err
	}
	if uint64(got) != want {
		return fmt.Errorf("%w: system identifier %d is not the replication session's %d (different cluster?)", errClusterMismatch, uint64(got), want)
	}
	return nil
}

// wait polls the replicas one after another under a shared deadline: while
// one is polled the others catch up, so the cost is about the slowest lag
// plus one round trip per replica.
// Every pass is logged with the replay position and backend address each
// replica answered with, so a consumer-side miss can be tied to the exact
// certification (Debug; Info once the wait is slow).
// ponytail: sequential; poll concurrently if the list grows past a handful.
func (g *replicaGuard) wait(ctx context.Context, xid uint32, commitLSN pq.LSN, deadline time.Time) error {
	start := time.Now()
	var passed strings.Builder
	for i, r := range g.replicas {
		if i > 0 {
			passed.WriteString(", ")
		}
		if age, ok := r.cachedAge(commitLSN, time.Now()); ok {
			fmt.Fprintf(&passed, "%s server=%s replay=%s cached_age_ms=%.1f", r.name, r.server, r.replayed, float64(age.Microseconds())/1000)
			g.metric.VisibilityReplicaCheck(r.name, true)
			continue
		}
		var last replicaState
		poll := func(ctx context.Context) (replicaState, error) {
			st, err := g.poll(ctx, r, commitLSN)
			if err == nil {
				last = st
			}
			return st, err
		}
		if err := waitReplayed(ctx, poll, commitLSN, deadline, g.cfg.PollInterval); err != nil {
			if goerrors.Is(err, ErrVisibilityTimeout) {
				g.metric.VisibilityTimeoutIncrement()
			}
			return fmt.Errorf("replica %s: %w", r.name, err)
		}
		fmt.Fprintf(&passed, "%s server=%s replay=%s", r.name, last.server, last.replay)
		g.metric.VisibilityReplicaCheck(r.name, false)
	}
	waited := time.Since(start)
	args := []any{"xid", xid, "commitLSN", commitLSN.String(), "replicas", passed.String(),
		"started_at", start.UTC().Format(time.RFC3339Nano), "wait_ms", float64(waited.Microseconds()) / 1000}
	if waited >= slowVisibilityWait {
		logger.Info("replica guard slow wait completed", args...)
	} else {
		logger.Debug("replica guard wait completed", args...)
	}
	return nil
}

// poll dials r if needed and runs one replay check. A server that answers
// with something other than a same-cluster standby on an acceptable timeline
// is a fatal guard error; a connection failure drops the connection and is
// retried by waitReplayed. commitLSN only feeds the lag gauge. Only an answer
// that passed every check refreshes the replay cache.
func (g *replicaGuard) poll(ctx context.Context, r *replica, commitLSN pq.LSN) (replicaState, error) {
	if r.conn == nil || r.conn.IsClosed() {
		r.forget()
		conn, err := r.dial(ctx)
		if err != nil {
			return replicaState{}, fmt.Errorf("%w: dial: %w", errReplicaConn, err)
		}
		r.conn = conn
		// Every new connection is checked against the session's cluster: the
		// host:port may be re-provisioned or re-routed between two dials, and a
		// foreign standby passes the replay rule at once.
		if err = g.checkSystemID(ctx, r); err != nil {
			return replicaState{}, g.answerOrDrop(ctx, r, err)
		}
	}
	start := time.Now()
	st, err := pollReplica(ctx, r.conn)
	if err != nil {
		return replicaState{}, g.answerOrDrop(ctx, r, err)
	}
	// <= rather than ==: an idle standby's control file lags after a failover
	// until the next buffer flush or restartpoint. > means it follows a newer
	// primary than the replication session; CommitLSN is not in its history.
	if st.timeline > g.timeline {
		return replicaState{}, fmt.Errorf("timeline %d is ahead of the replication session's %d (followed a newer primary?)", st.timeline, g.timeline)
	}
	var lag pq.LSN
	if commitLSN > st.replay {
		lag = commitLSN - st.replay
	}
	g.metric.SetVisibilityReplicaLag(r.name, float64(lag))
	r.replayed, r.observedAt, r.server = st.replay, start, st.server
	return st, nil
}

// answerOrDrop keeps an error the server answered with (fatal for the guard)
// and turns anything else into a dropped connection that waitReplayed redials.
// A standby shutting down answers too (57P01/57P02, connection_exception
// class 08); that is a restart, not a verdict, and is redialed like a reset.
func (g *replicaGuard) answerOrDrop(ctx context.Context, r *replica, err error) error {
	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		if pgErr.Code != postgresAdminShutdown && pgErr.Code != postgresCrashShutdown && !strings.HasPrefix(pgErr.Code, "08") {
			return err
		}
	} else if goerrors.Is(err, ErrNotStandby) || goerrors.Is(err, errClusterMismatch) {
		return err
	}
	_ = r.conn.Close(ctx)
	r.conn = nil
	r.forget()
	return fmt.Errorf("%w: %w", errReplicaConn, err)
}

func (g *replicaGuard) close(ctx context.Context) error {
	var errs []error
	for _, r := range g.replicas {
		if r.conn != nil && !r.conn.IsClosed() {
			errs = append(errs, r.conn.Close(ctx))
		}
	}
	return goerrors.Join(errs...)
}
