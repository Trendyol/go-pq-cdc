package replication

import (
	"context"
	goerrors "errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
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

// errTimelineBlocked marks a poll that passed the replay rule on a replica the
// source reports on another timeline than the replication session's (design
// R19): not replayed yet, retried until the deadline, never fatal.
var errTimelineBlocked = goerrors.New("source reports the replica on another timeline")

// errRoundStopped ends one replica's wait between two polls when a member
// error in the same round needs the source's verdict first (design R16).
var errRoundStopped = goerrors.New("replica wait stopped for a re-resolve")

// errNoTimeLeft reports a ResolveReplicas call that was not made: the gate's
// deadline had already passed.
var errNoTimeLeft = goerrors.New("no time left before the gate deadline")

// errInvalidAnswer marks an answer the core itself rejects (design R14, R17):
// a definite wrong answer, not an outage. It is never retried and never a
// timeout, so failMode cannot turn it into a pass.
var errInvalidAnswer = goerrors.New("invalid answer")

// resolveTimeout bounds every ResolveReplicas call (design R16: at most 10% of
// the default gate budget).
// ponytail: const; a field when a source needs more than 1s to answer.
const resolveTimeout = time.Second

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
// Every poll runs under the deadline. An errReplicaConn or errTimelineBlocked
// failure is retried after a backoff; any other error is returned as is.
// Errors are never treated as "replayed".
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
		if isAnswer(err) {
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

// isAnswer reports whether a poll error is a server verdict, fatal in both
// fail modes: anything but a dropped connection (errReplicaConn), a pass held
// by the source's timeline (errTimelineBlocked) or the wait's own deadline.
// errRoundStopped is not an answer either, but it too ends a wait at once.
func isAnswer(err error) bool {
	return err != nil && !goerrors.Is(err, errReplicaConn) && !goerrors.Is(err, errTimelineBlocked) &&
		!goerrors.Is(err, context.DeadlineExceeded)
}

// isMemberError reports whether a poll error is the replica's own failure,
// which in discovery mode asks the source before it is classified (design
// R16): a dropped connection, any server answer, or a pass held only by R19.
// The wait's own deadline and cancellation are not.
func isMemberError(err error) bool {
	return err != nil && !goerrors.Is(err, context.DeadlineExceeded) && !goerrors.Is(err, context.Canceled)
}

// replicaGuard holds the first message of every transaction until each listed
// standby has replayed its commit record. It runs after the primary guard,
// under the same deadline and failMode. The standbys come from the static
// visibilityGuard.replicas list or, in discovery mode, from
// cfg.ResolveReplicas. See docs/replica-guard-design.md.
//
// The replica set changes only on the goroutine that runs start and wait,
// between two rounds of polls, so no goroutine of a round ever sees it change.
type replicaGuard struct {
	metric metric.Metric
	// resolvedAt is the start of the last successful ResolveReplicas call;
	// zero in static mode.
	resolvedAt time.Time
	// connect returns the dial function of a replica at addr.
	connect  func(addr string) func(ctx context.Context) (replicaConn, error)
	systemID string // IDENTIFY_SYSTEM's, unsigned decimal
	replicas []*replica
	cfg      config.VisibilityGuardConfig
	timeline int32
}

// memberError is a replica's error held back for the source's verdict.
type memberError struct {
	err  error
	name string
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
	name       string      // host:port from config or the source; metric label and log key
	server     string      // backend address of the cached poll
	source     string      // where the source found it (discovery only); logs only
	replayed   pq.LSN      // replay position of the last fully checked poll on conn
	timeline   int32       // as the source last reported it (discovery only); 0 = unknown
}

// cachedAge reports whether the last poll on this connection already proves
// commitLSN applied (same strict rule as the poll) and is younger than
// replayCacheTTL.
func (r *replica) cachedAge(commitLSN pq.LSN, now time.Time) (time.Duration, bool) {
	if r.observedAt.IsZero() || r.conn == nil || r.conn.IsClosed() || r.replayed <= commitLSN {
		return 0, false
	}
	return ageWithin(r.observedAt, now, replayCacheTTL)
}

// ageWithin reports now - at when both the monotonic and the wall-clock age
// are in [0, ttl): the monotonic clock stops while the host is suspended, the
// wall clock can step, and a negative age is never fresh.
func ageWithin(at, now time.Time, ttl time.Duration) (time.Duration, bool) {
	age := now.Sub(at)
	wall := now.Round(0).Sub(at.Round(0))
	if age < 0 || wall < 0 || age >= ttl || wall >= ttl {
		return 0, false
	}
	return age, true
}

func (r *replica) forget() { r.replayed, r.observedAt, r.server = 0, time.Time{}, "" }

func openReplicaGuard(ctx context.Context, cfg config.Config, system *pq.IdentifySystemResult, m metric.Metric) (*replicaGuard, error) {
	g := &replicaGuard{metric: m, cfg: cfg.VisibilityGuard, timeline: system.Timeline, systemID: system.SystemID,
		connect: func(addr string) func(context.Context) (replicaConn, error) {
			dsn := cfg.ReplicaDSN(addr)
			return func(ctx context.Context) (replicaConn, error) { return pgx.Connect(ctx, dsn) }
		},
	}
	if err := g.start(ctx); err != nil {
		_ = g.close(ctx)
		return nil, err
	}
	return g, nil
}

// start builds the replica set and dials and checks every replica (open). In
// discovery mode the source is asked once first and its error fails startup;
// a replica the source reports on another timeline does not (design R18, R19).
func (g *replicaGuard) start(ctx context.Context) error {
	if g.cfg.ResolveReplicas == nil {
		for _, hostPort := range g.cfg.Replicas {
			g.replicas = append(g.replicas, &replica{name: hostPort, dial: g.connect(hostPort)})
		}
		return g.open(ctx)
	}
	members, at, err := g.resolve(ctx, time.Time{})
	if err != nil {
		return fmt.Errorf("resolve replicas: %w", err)
	}
	g.apply(ctx, members, at)
	return g.open(ctx)
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

// wait certifies commitLSN on every replica under the shared deadline. In
// discovery mode a set older than the refresh interval is refreshed first
// (design R17). One round of concurrent polls follows; in discovery mode a
// member error stops it instead of being classified, the source is asked once
// more (design R16), and a second round covers the reconciled set. A set that
// aged past the refresh interval meanwhile does not certify: it is refreshed
// and the round repeated (R17). Every pass is logged with the replay position and backend address each
// replica answered with, so a consumer-side miss can be tied to the exact
// certification (Debug; Info once the wait is slow).
func (g *replicaGuard) wait(ctx context.Context, xid uint32, commitLSN pq.LSN, deadline time.Time) error {
	start := time.Now()
	certified := make(map[*replica]string, len(g.replicas))
	if err := g.certify(ctx, commitLSN, deadline, certified); err != nil {
		if goerrors.Is(err, ErrVisibilityTimeout) {
			g.metric.VisibilityTimeoutIncrement()
		}
		return err
	}
	passed := make([]string, 0, len(g.replicas))
	for _, r := range g.replicas {
		passed = append(passed, certified[r])
	}
	waited := time.Since(start)
	args := []any{"xid", xid, "commitLSN", commitLSN.String(), "replicas", strings.Join(passed, ", "),
		"started_at", start.UTC().Format(time.RFC3339Nano), "wait_ms", float64(waited.Microseconds()) / 1000}
	if waited >= slowVisibilityWait {
		logger.Info("replica guard slow wait completed", args...)
	} else {
		logger.Debug("replica guard wait completed", args...)
	}
	return nil
}

// certify is wait without the bookkeeping. Static mode is one round. In
// discovery mode a pass is the due refresh, a round and, when a member error
// asked for it, the wait's one optional re-resolve and a second round; it
// certifies only if the set is still younger than the refresh interval at its
// end (design R17). Otherwise the mandatory refresh runs and the pass repeats
// on the refreshed set. A member certified earlier in the wait stays certified
// while listed (its replay position only grows) unless R19 now blocks it, so a
// repeated round polls only what the refresh added, each added member through a
// new connection and its full checks. A repeated pass starts with a refresh
// that installed an answer fresh on arrival and ends only once that answer is
// a whole interval old; slower answers back off inside refresh and nothing
// outlives the deadline, so the loop ends in a certification, an error or the
// timeout.
func (g *replicaGuard) certify(ctx context.Context, commitLSN pq.LSN, deadline time.Time, certified map[*replica]string) error {
	if g.cfg.ResolveReplicas == nil {
		_, err := g.round(ctx, commitLSN, deadline, certified, false)
		return err
	}
	ask := true // the wait's one optional re-resolve is unspent
	stale := !g.fresh(time.Now())
	for {
		if stale {
			if err := g.refresh(ctx, deadline); err != nil {
				return err
			}
		}
		asked, err := g.round(ctx, commitLSN, deadline, certified, ask)
		if len(asked) > 0 {
			ask = false
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err = g.reResolve(ctx, deadline, asked); err != nil {
				return err
			}
			_, err = g.round(ctx, commitLSN, deadline, certified, false)
		}
		if err != nil || g.fresh(time.Now()) {
			return err
		}
		logger.Debug("replica guard set aged past the refresh interval during the wait, refreshing before it certifies",
			"refresh_interval", g.refreshInterval().String())
		stale = true
	}
}

// round certifies, concurrently under deadline, every replica certified does
// not already hold, and adds each pass to it, so the cost is about the slowest
// lag plus one round trip. A replica whose cache (design R6) proves commitLSN
// needs no query; one the source reports on another timeline never passes
// (design R19). Each goroutine touches only its own replica.
//
// Without ask, a server answer (fatal) cancels the other polls and wins over a
// timeout, which failMode open would dispatch on. With ask (discovery, the
// wait's optional re-resolve unspent), a member error is not classified: it
// is returned for the source's verdict and stops the other polls between two
// polls, never during one, so their connections and caches survive.
func (g *replicaGuard) round(ctx context.Context, commitLSN pq.LSN, deadline time.Time, certified map[*replica]string, ask bool) ([]memberError, error) {
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	var stopOnce sync.Once
	passed := make([]string, len(g.replicas))
	errs := make([]error, len(g.replicas))
	asked := make([]error, len(g.replicas))
	var wg sync.WaitGroup
	for i, r := range g.replicas {
		blocked := g.timelineBlocks(r)
		if _, ok := certified[r]; ok && !blocked {
			continue
		}
		delete(certified, r)
		if age, ok := r.cachedAge(commitLSN, start); ok && !blocked {
			passed[i] = fmt.Sprintf("%s server=%s replay=%s cached_age_ms=%.1f", r.name, r.server, r.replayed, float64(age.Microseconds())/1000)
			g.metric.VisibilityReplicaCheck(r.name, true)
			continue
		}
		wg.Go(func() {
			var last replicaState
			poll := func(ctx context.Context) (replicaState, error) {
				select {
				case <-stop:
					return replicaState{}, errRoundStopped
				default:
				}
				st, err := g.poll(ctx, r, commitLSN)
				if err == nil {
					last = st
					if st.replay > commitLSN && g.timelineBlocks(r) {
						err = fmt.Errorf("%w: timeline %d, replication session's %d", errTimelineBlocked, r.timeline, g.timeline)
					}
				}
				if ask && isMemberError(err) {
					asked[i] = err
					stopOnce.Do(func() { close(stop) })
					return st, errRoundStopped
				}
				return st, err
			}
			if err := waitReplayed(ctx, poll, commitLSN, deadline, g.cfg.PollInterval); err != nil {
				if goerrors.Is(err, errRoundStopped) {
					return
				}
				errs[i] = fmt.Errorf("replica %s: %w", r.name, err)
				if !goerrors.Is(err, ErrVisibilityTimeout) {
					cancel()
				}
				return
			}
			passed[i] = fmt.Sprintf("%s server=%s replay=%s", r.name, last.server, last.replay)
			g.metric.VisibilityReplicaCheck(r.name, false)
		})
	}
	wg.Wait()
	var held []memberError
	for i, r := range g.replicas {
		if passed[i] != "" {
			certified[r] = passed[i]
		}
		if asked[i] != nil {
			held = append(held, memberError{name: r.name, err: asked[i]})
		}
	}
	return held, worstWaitError(errs)
}

// reResolve spends the wait's one optional re-ask of the source (design R16)
// on the member errors of a round. A replica the answer no longer lists is
// dropped and its error forgotten (a promoted standby is the leader now,
// design D9); every other error is classified exactly as without discovery: a
// server answer is returned (fatal), the rest is retried by the next round. A
// failed re-ask changes nothing, except an answer the core finds invalid: it is
// returned at once, as refresh returns it (design R21), never retried into a
// timeout.
func (g *replicaGuard) reResolve(ctx context.Context, deadline time.Time, asked []memberError) error {
	members, at, err := g.resolve(ctx, deadline)
	if err != nil {
		logger.Warn("replica guard re-resolve failed, replica errors are classified as without discovery", "error", err)
	} else {
		g.apply(ctx, members, at)
	}
	listed := make(map[string]bool, len(members))
	for _, m := range members {
		listed[m.Addr] = true
	}
	var fatal error
	for _, a := range asked {
		if err == nil && !listed[a.name] {
			logger.Info("replica guard dropped a replica the source no longer lists", "replica", a.name, "error", a.err)
			continue
		}
		if isAnswer(a.err) {
			fatal = fmt.Errorf("replica %s: %w", a.name, a.err)
			break
		}
	}
	if goerrors.Is(err, errInvalidAnswer) {
		// The member's own verdict, if any, stays in the error.
		return fmt.Errorf("replica set not re-resolved: %w", goerrors.Join(err, fatal))
	}
	return fatal
}

// fresh reports whether the resolved set is younger than the refresh interval
// by both clocks (design R17).
func (g *replicaGuard) fresh(now time.Time) bool {
	_, ok := ageWithin(g.resolvedAt, now, g.refreshInterval())
	return ok
}

// refresh renews a resolved set older than the refresh interval (design R17).
// It is mandatory: an error the resolver returns, or an answer already older
// than the interval when it arrives (its age counts from the call's start), is
// retried with the poll backoff until a fresh answer is installed or the
// shared deadline passes, so a stale set never certifies; at the deadline it
// is a timeout and failMode applies. A late answer is installed all the same:
// it is the source's newest word on membership. An answer the core finds
// invalid is the exception: it is returned at once, not a timeout.
func (g *replicaGuard) refresh(ctx context.Context, deadline time.Time) error {
	delay := g.cfg.PollInterval
	var cause error
	for attempt := 0; ; attempt++ {
		members, at, err := g.resolve(ctx, deadline)
		if err == nil {
			g.apply(ctx, members, at)
			if g.fresh(time.Now()) {
				return nil
			}
			err = fmt.Errorf("the answer took longer than the refresh interval %s", g.refreshInterval())
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The source answered, and wrongly: retrying until the deadline would
		// end as a timeout, which failMode open dispatches on.
		if goerrors.Is(err, errInvalidAnswer) {
			return fmt.Errorf("replica set not refreshed: %w", err)
		}
		if attempt == 0 {
			logger.Warn("replica guard refresh failed, retrying until the gate deadline", "error", err)
		}
		remaining := time.Until(deadline)
		// An attempt the gate's own deadline cut short says nothing new about
		// the source: report the last one that did.
		if cause == nil || !goerrors.Is(err, errNoTimeLeft) && (remaining > 0 || !goerrors.Is(err, context.DeadlineExceeded)) {
			cause = err
		}
		if remaining <= 0 {
			return fmt.Errorf("%w: replica set older than %s and not refreshed: %w", ErrVisibilityTimeout, g.refreshInterval(), cause)
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

func (g *replicaGuard) refreshInterval() time.Duration {
	if g.cfg.RefreshInterval > 0 {
		return g.cfg.RefreshInterval
	}
	return config.DefaultReplicaRefreshInterval
}

// resolve asks the source once, bounded by resolveTimeout and by deadline
// (zero at startup), and validates the answer. at is the start of the call:
// the answer is no fresher than that.
func (g *replicaGuard) resolve(ctx context.Context, deadline time.Time) ([]config.ReplicaMember, time.Time, error) {
	at := time.Now()
	bound := at.Add(resolveTimeout)
	if !deadline.IsZero() && deadline.Before(bound) {
		if !deadline.After(at) {
			return nil, at, errNoTimeLeft
		}
		bound = deadline
	}
	rctx, cancel := context.WithDeadline(ctx, bound)
	defer cancel()
	members, err := g.cfg.ResolveReplicas(rctx)
	if err != nil {
		return nil, at, err
	}
	// An answer that arrives as its bound expires, or after the caller gave
	// up, may be a half-finished one: it is not an answer.
	if err = rctx.Err(); err != nil {
		return nil, at, fmt.Errorf("answer discarded, resolve ended first: %w", err)
	}
	if err = validateMembers(members); err != nil {
		return nil, at, fmt.Errorf("%w: %w", errInvalidAnswer, err)
	}
	return members, at, nil
}

// validateMembers applies the rules of visibilityGuard.replicas to an answer
// of the source: host:port, each listed once; a timeline is never negative. A
// bad entry fails the whole answer, it is never dropped silently (design R14);
// so does a nil slice, which proves nothing.
func validateMembers(members []config.ReplicaMember) error {
	// A nil slice is not "proved, zero replicas": that is a non-nil empty one.
	if members == nil {
		return goerrors.New("nil slice: an empty answer must be a non-nil empty slice")
	}
	var err error
	seen := make(map[string]bool, len(members))
	for _, m := range members {
		if _, _, e := net.SplitHostPort(m.Addr); e != nil {
			err = goerrors.Join(err, fmt.Errorf("%q must be host:port", m.Addr))
		}
		if seen[m.Addr] {
			err = goerrors.Join(err, fmt.Errorf("%q listed twice", m.Addr))
		}
		seen[m.Addr] = true
		if m.Timeline < 0 {
			err = goerrors.Join(err, fmt.Errorf("%q: negative timeline %d", m.Addr, m.Timeline))
		}
	}
	return err
}

// apply makes members the replica set (design R18). A replica that left is
// closed and its metric series deleted; one that joined is dialled and fully
// checked (R13 identity, recovery, timeline) by its first poll; one that
// stayed keeps its connection and replay cache and takes its new timeline.
// Called between rounds only. The first answer and every change are logged
// with the whole set grouped by source, at Warn when it is empty: the guard
// then gates on the primary only.
func (g *replicaGuard) apply(ctx context.Context, members []config.ReplicaMember, at time.Time) {
	left := make(map[string]*replica, len(g.replicas))
	for _, r := range g.replicas {
		left[r.name] = r
	}
	changed := g.resolvedAt.IsZero()
	next := make([]*replica, 0, len(members))
	var added, removed []string
	for _, m := range members {
		r, ok := left[m.Addr]
		if ok {
			delete(left, m.Addr)
			changed = changed || r.timeline != m.Timeline || r.source != m.Source
		} else {
			r = &replica{name: m.Addr, dial: g.connect(m.Addr)}
			added = append(added, m.Addr)
		}
		r.timeline, r.source = m.Timeline, m.Source
		next = append(next, r)
	}
	for _, r := range g.replicas {
		if left[r.name] == nil {
			continue
		}
		if r.conn != nil && !r.conn.IsClosed() {
			_ = r.conn.Close(ctx)
		}
		g.metric.DeleteVisibilityReplica(r.name)
		removed = append(removed, r.name)
	}
	g.replicas, g.resolvedAt = next, at
	if !changed && len(added) == 0 && len(removed) == 0 {
		return
	}
	args := []any{"replicas", describeMembers(members), "added", added, "removed", removed}
	if len(members) == 0 {
		logger.Warn("replica guard resolved no replicas, gating on the primary only", args...)
		return
	}
	logger.Info("replica guard replica set changed", args...)
}

// describeMembers renders a resolved set grouped by source, in answer order:
// "src-a: h1:5432 (timeline 7), h2:5432 (timeline 7); src-b: h3:5432 (timeline unknown)".
func describeMembers(members []config.ReplicaMember) string {
	var sources []string
	bySource := make(map[string][]string, len(members))
	for _, m := range members {
		if _, ok := bySource[m.Source]; !ok {
			sources = append(sources, m.Source)
		}
		timeline := "unknown"
		if m.Timeline != 0 {
			timeline = strconv.Itoa(int(m.Timeline))
		}
		bySource[m.Source] = append(bySource[m.Source], fmt.Sprintf("%s (timeline %s)", m.Addr, timeline))
	}
	groups := make([]string, 0, len(sources))
	for _, s := range sources {
		group := strings.Join(bySource[s], ", ")
		if s != "" {
			group = s + ": " + group
		}
		groups = append(groups, group)
	}
	return strings.Join(groups, "; ")
}

// timelineBlocks is design R19: a replica the source reports on a known
// timeline other than the replication session's has not replayed yet,
// whatever its replay position says. It holds the gate until the deadline and
// never fails it. 0 (unknown, and always in static mode) turns the rule off.
func (g *replicaGuard) timelineBlocks(r *replica) bool {
	return r.timeline != 0 && r.timeline != g.timeline
}

// worstWaitError picks what a concurrent wait reports: a server answer before
// a timeout, a timeout before the cancellation a failed sibling caused; the
// first listed replica among equals.
func worstWaitError(errs []error) error {
	rank := func(err error) int {
		switch {
		case goerrors.Is(err, ErrVisibilityTimeout):
			return 1
		case goerrors.Is(err, context.Canceled):
			return 0
		}
		return 2
	}
	var worst error
	for _, err := range errs {
		if err != nil && (worst == nil || rank(err) > rank(worst)) {
			worst = err
		}
	}
	return worst
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
