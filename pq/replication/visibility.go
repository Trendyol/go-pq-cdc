package replication

import (
	"context"
	goerrors "errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

// visibilityGuard holds the first message of every transaction until the
// transaction's xid is visible to a fresh snapshot on the primary.
//
// PostgreSQL flushes the commit record (RecordTransactionCommit → XLogFlush)
// before the transaction leaves the proc array (ProcArrayEndTransaction), and
// the logical walsender streams the transaction as soon as the flush wakes it.
// With synchronous replication SyncRepWaitForLSN also runs in between, so the
// window is at least the standby ack round-trip. A consumer that reads the
// primary right after receiving the event can therefore miss the row.
//
// The predicate is client-side and epoch-free (docs/visibility-gate-design.md P1):
// xid is visible iff int32(xid - xmax) < 0 and xid is not in xip, over the
// low 32 bits of pg_current_snapshot() (txid_current_snapshot() on PG < 13).
// The guard connection is bound to the replication session: same host, opened
// after IDENTIFY_SYSTEM, never reconnects; any guard error is a stream error.
type visibilityGuard struct {
	conn   pq.Connection
	query  func(ctx context.Context, sql string) ([]string, error)
	metric metric.Metric
	// pollSQL is the per-poll statement; switched to the txid_* variant on PG < 13.
	pollSQL string
	cfg     config.VisibilityGuardConfig
	// xmin of the last parsed snapshot: any xid that precedes it was already
	// complete when that snapshot was taken and is visible without a query.
	xmin        uint32
	hasSnapshot bool
}

var (
	// ErrVisibilityGuard is the error class for every guard failure that restarts the stream.
	ErrVisibilityGuard = goerrors.New("visibility guard unreachable")
	// ErrVisibilityTimeout reports that a transaction did not become visible within visibilityGuard.timeout.
	ErrVisibilityTimeout = goerrors.New("visibility guard timeout")
)

const (
	// Both checks in one statement: pg_walfile_name ERRORs while in recovery, so
	// a replica (or a pooler routing to one) fails here with a clear message.
	visibilityOpenSQL   = "SELECT pg_is_in_recovery(), ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int"
	walSenderTimeoutSQL = "SELECT setting FROM pg_settings WHERE name = 'wal_sender_timeout'" // milliseconds
	visibilityPollSQL   = "SELECT pg_is_in_recovery(), pg_current_snapshot()::text"           // PostgreSQL 13+
	legacyPollSQL       = "SELECT pg_is_in_recovery(), txid_current_snapshot()::text"         // PostgreSQL < 13
	undefinedFunction   = "42883"
	maxPollInterval     = 250 * time.Millisecond
)

// openVisibilityGuard dials dsn and runs the mandatory open checks.
func openVisibilityGuard(ctx context.Context, dsn string, cfg config.VisibilityGuardConfig, timeline int32, m metric.Metric) (*visibilityGuard, error) {
	conn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, err
	}
	g, err := newVisibilityGuard(ctx, conn, queryRow(conn), cfg, timeline, m)
	if err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return g, nil
}

// newVisibilityGuard verifies the connection is on the primary of the
// replication session's timeline, that the gate timeout fits under
// wal_sender_timeout, and that a snapshot function is available.
func newVisibilityGuard(ctx context.Context, conn pq.Connection, query func(context.Context, string) ([]string, error),
	cfg config.VisibilityGuardConfig, timeline int32, m metric.Metric,
) (*visibilityGuard, error) {
	g := &visibilityGuard{conn: conn, query: query, cfg: cfg, metric: m, pollSQL: visibilityPollSQL}

	row, err := g.query(ctx, visibilityOpenSQL)
	if err != nil {
		return nil, fmt.Errorf("primary check (replica or pooler routing?): %w", err)
	}
	if len(row) != 2 || row[0] != "f" {
		return nil, fmt.Errorf("server is in recovery (replica or pooler routing), pg_is_in_recovery=%q", row)
	}
	if tl, err := strconv.ParseInt(row[1], 10, 32); err != nil || int32(tl) != timeline {
		return nil, fmt.Errorf("timeline mismatch: replication session is on %d, guard connection sees %q", timeline, row[1])
	}

	row, err = g.query(ctx, walSenderTimeoutSQL)
	if err != nil {
		return nil, fmt.Errorf("read wal_sender_timeout: %w", err)
	}
	ms, err := strconv.ParseInt(strings.Join(row, ""), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse wal_sender_timeout %q: %w", row, err)
	}
	if walSenderTimeout := time.Duration(ms) * time.Millisecond; ms > 0 && cfg.Timeout >= walSenderTimeout/2 {
		return nil, fmt.Errorf("visibilityGuard.timeout (%s) must be less than half of wal_sender_timeout (%s): "+
			"keepalive replies stop while the gate blocks", cfg.Timeout, walSenderTimeout)
	}

	if _, err = g.poll(ctx); err != nil {
		var pgErr *pgconn.PgError
		if goerrors.As(err, &pgErr) && pgErr.Code == undefinedFunction {
			g.pollSQL = legacyPollSQL
			_, err = g.poll(ctx)
		}
		if err != nil {
			return nil, fmt.Errorf("snapshot probe: %w", err)
		}
	}
	return g, nil
}

func queryRow(conn pq.Connection) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, sql string) ([]string, error) {
		results, err := conn.Exec(ctx, sql).ReadAll()
		if err != nil {
			return nil, err
		}
		if len(results) != 1 || len(results[0].Rows) != 1 {
			return nil, fmt.Errorf("expected exactly one row from %q", sql)
		}
		row := make([]string, len(results[0].Rows[0]))
		for i, col := range results[0].Rows[0] {
			row[i] = string(col)
		}
		return row, nil
	}
}

// wait blocks until xid is visible to a new snapshot on the primary. It returns
// ErrVisibilityTimeout after cfg.Timeout; any other error means the guard can no
// longer certify visibility and the stream must restart. Errors are never
// treated as "visible".
func (g *visibilityGuard) wait(ctx context.Context, xid uint32) error {
	start := time.Now()
	defer func() { g.metric.ObserveVisibilityWait(time.Since(start)) }()

	if g.hasSnapshot && xidPrecedes(xid, g.xmin) {
		return nil
	}

	deadline := start.Add(g.cfg.Timeout)
	delay := g.cfg.PollInterval
	for {
		snap, err := g.poll(ctx)
		if err != nil {
			return err
		}
		if snap.visible(xid) {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			g.metric.VisibilityTimeoutIncrement()
			return fmt.Errorf("%w: xid %d not visible after %s", ErrVisibilityTimeout, xid, g.cfg.Timeout)
		}
		sleep := delay + rand.N(delay/2+1) //nolint:gosec // jitter only
		if sleep > remaining {
			sleep = remaining
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		if delay *= 2; delay > maxPollInterval {
			delay = maxPollInterval
		}
	}
}

func (g *visibilityGuard) poll(ctx context.Context) (pgSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return pgSnapshot{}, err // shutting down: do not touch the connection Close is about to close
	}
	// A single poll slower than the gate timeout means the guard is broken.
	// pgconn closes the connection when the deadline cuts a query, which is
	// what we want: the guard never reconnects, the stream restarts.
	queryCtx, cancel := context.WithTimeout(ctx, g.cfg.Timeout)
	defer cancel()

	row, err := g.query(queryCtx, g.pollSQL)
	if err != nil {
		return pgSnapshot{}, err
	}
	if len(row) != 2 || (row[0] != "f" && row[0] != "t") {
		return pgSnapshot{}, fmt.Errorf("malformed poll result %q", row)
	}
	if row[0] == "t" {
		return pgSnapshot{}, goerrors.New("primary went into recovery (failover?)")
	}
	snap, err := parseSnapshot(row[1])
	if err != nil {
		return pgSnapshot{}, err
	}
	g.xmin, g.hasSnapshot = snap.xmin, true
	return snap, nil
}

func (g *visibilityGuard) close(ctx context.Context) error {
	return g.conn.Close(ctx)
}

// pgSnapshot is the low 32 bits of a pg_snapshot / txid_snapshot text value.
type pgSnapshot struct {
	xip  []uint32
	xmin uint32
	xmax uint32
}

// parseSnapshot parses "xmin:xmax:xip1,xip2,..." (64-bit values, low 32 bits kept).
func parseSnapshot(s string) (pgSnapshot, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return pgSnapshot{}, fmt.Errorf("malformed snapshot %q", s)
	}
	var snap pgSnapshot
	var err error
	if snap.xmin, err = parseXid(parts[0]); err != nil {
		return pgSnapshot{}, fmt.Errorf("malformed snapshot xmin %q: %w", s, err)
	}
	if snap.xmax, err = parseXid(parts[1]); err != nil {
		return pgSnapshot{}, fmt.Errorf("malformed snapshot xmax %q: %w", s, err)
	}
	if parts[2] != "" {
		for _, x := range strings.Split(parts[2], ",") {
			xid, err := parseXid(x)
			if err != nil {
				return pgSnapshot{}, fmt.Errorf("malformed snapshot xip %q: %w", s, err)
			}
			snap.xip = append(snap.xip, xid)
		}
	}
	return snap, nil
}

func parseXid(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	return uint32(v), err
}

// visible applies PostgreSQL's own rule: everything at or past xmax is
// invisible (xmax = latestCompletedXid + 1, so inside the commit gap xid >= xmax),
// everything below xmax is visible unless it is still in progress (xip).
func (s pgSnapshot) visible(xid uint32) bool {
	if !xidPrecedes(xid, s.xmax) {
		return false
	}
	for _, x := range s.xip {
		if x == xid {
			return false
		}
	}
	return true
}

// xidPrecedes is TransactionIdPrecedes: modular 32-bit compare, valid within 2^31.
func xidPrecedes(a, b uint32) bool {
	return int32(a-b) < 0
}
