package replication

import (
	"context"
	goerrors "errors"
	"fmt"
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
const replicaPollSQL = "SELECT pg_is_in_recovery(), coalesce(pg_last_wal_replay_lsn()::text, ''), (pg_control_recovery()).min_recovery_end_timeline"

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
	replay   pq.LSN
	timeline int32
}

func pollReplica(ctx context.Context, q Querier) (replicaState, error) {
	var inRecovery bool
	var replay string
	var st replicaState
	if err := q.QueryRow(ctx, replicaPollSQL).Scan(&inRecovery, &replay, &st.timeline); err != nil {
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
//
// ponytail: no replay-position cache. A stored value is only sound if every
// change of backend identity (restart, rewind, pooler swap) is observed before
// it is trusted, and nothing cheaper than this poll observes that.
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
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrVisibilityTimeout, err)
			}
			return fmt.Errorf("%w: replay %s has not passed commit %s", ErrVisibilityTimeout, st.replay, commitLSN)
		}
		if err != nil && !goerrors.Is(err, errReplicaConn) {
			return err
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
	dial func(ctx context.Context) (replicaConn, error)
	conn replicaConn // nil until dialed; dropped on any connection error
	name string      // host:port from config; metric label and log key
}

func openReplicaGuard(ctx context.Context, cfg config.Config, timeline int32, m metric.Metric) (*replicaGuard, error) {
	g := &replicaGuard{metric: m, cfg: cfg.VisibilityGuard, timeline: timeline}
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

// open dials every replica once: each must be in recovery on a timeline that
// is not ahead of the replication session, or startup fails.
func (g *replicaGuard) open(ctx context.Context) error {
	for _, r := range g.replicas {
		if _, err := g.poll(ctx, r, 0); err != nil {
			return fmt.Errorf("replica %s: %w", r.name, err)
		}
	}
	return nil
}

// wait polls the replicas one after another under a shared deadline: while
// one is polled the others catch up, so the cost is about the slowest lag
// plus one round trip per replica.
// ponytail: sequential; poll concurrently if the list grows past a handful.
func (g *replicaGuard) wait(ctx context.Context, commitLSN pq.LSN, deadline time.Time) error {
	start := time.Now()
	for _, r := range g.replicas {
		poll := func(ctx context.Context) (replicaState, error) { return g.poll(ctx, r, commitLSN) }
		if err := waitReplayed(ctx, poll, commitLSN, deadline, g.cfg.PollInterval); err != nil {
			if goerrors.Is(err, ErrVisibilityTimeout) {
				g.metric.VisibilityTimeoutIncrement()
			}
			return fmt.Errorf("replica %s: %w", r.name, err)
		}
	}
	if waited := time.Since(start); waited >= slowVisibilityWait {
		logger.Info("replica guard slow wait completed", "commitLSN", commitLSN.String(), "wait_ms", float64(waited.Microseconds())/1000)
	}
	return nil
}

// poll dials r if needed and runs one replay check. A server that answers
// with something other than a standby on an acceptable timeline is a fatal
// guard error; a connection failure drops the connection and is retried by
// waitReplayed. commitLSN only feeds the lag gauge.
func (g *replicaGuard) poll(ctx context.Context, r *replica, commitLSN pq.LSN) (replicaState, error) {
	if r.conn == nil || r.conn.IsClosed() {
		conn, err := r.dial(ctx)
		if err != nil {
			return replicaState{}, fmt.Errorf("%w: dial: %w", errReplicaConn, err)
		}
		r.conn = conn
	}
	st, err := pollReplica(ctx, r.conn)
	if err != nil {
		var pgErr *pgconn.PgError
		if goerrors.Is(err, ErrNotStandby) || goerrors.As(err, &pgErr) {
			return replicaState{}, err // the server answered: not a standby, or the poll itself is rejected
		}
		_ = r.conn.Close(ctx)
		r.conn = nil
		return replicaState{}, fmt.Errorf("%w: %w", errReplicaConn, err)
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
	return st, nil
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
