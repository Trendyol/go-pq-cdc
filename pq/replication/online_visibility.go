package replication

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
)

// One fresh primary snapshot and identity check per poll. Unlike the legacy
// optional guard, online never uses fail-open, reconnect or a cached-xmin pass.
const onlineVisibilitySQL = `SELECT pg_is_in_recovery(), current_database(), system_identifier::text,
 ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int,
 pg_current_snapshot()::text, current_setting('server_version_num')
 FROM pg_catalog.pg_control_system()`

type onlineVisibilityGate struct {
	conn         pq.Connection
	query        func(context.Context, string) ([]string, error)
	source       OnlineSourceIdentity
	timeout      time.Duration
	pollInterval time.Duration
}

func openOnlineVisibilityGate(ctx context.Context, conn pq.Connection, options OnlineReaderOptions) (*onlineVisibilityGate, error) {
	g := &onlineVisibilityGate{conn: conn, query: queryRow(conn), source: options.Source, timeout: options.Limits.VisibilityTimeout, pollInterval: options.Limits.PollInterval}
	if _, err := g.poll(ctx); err != nil {
		return nil, err
	}
	return g, nil
}
func (g *onlineVisibilityGate) poll(ctx context.Context) (pgSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return pgSnapshot{}, err
	}
	pollCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	row, err := g.query(pollCtx, onlineVisibilitySQL)
	if err != nil {
		return pgSnapshot{}, fmt.Errorf("%w: online primary/identity snapshot query: %w; provision GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO <cdc_role>", ErrVisibilityGuard, err)
	}
	if len(row) != 6 {
		return pgSnapshot{}, fmt.Errorf("%w: malformed online visibility row", ErrVisibilityGuard)
	}
	timeline, err := strconv.ParseInt(row[3], 10, 32)
	if row[0] != "f" || row[1] != g.source.Database || row[2] != g.source.SystemID || err != nil || int32(timeline) != g.source.Timeline {
		return pgSnapshot{}, fmt.Errorf("%w: online primary/source identity changed", ErrVisibilityGuard)
	}
	version, err := strconv.Atoi(row[5])
	if err != nil || version/10000 != 16 {
		return pgSnapshot{}, fmt.Errorf("%w: online POC requires PostgreSQL 16", ErrVisibilityGuard)
	}
	snap, err := parseSnapshot(row[4])
	if err != nil {
		return pgSnapshot{}, fmt.Errorf("%w: %v", ErrVisibilityGuard, err)
	}
	return snap, nil
}
func (g *onlineVisibilityGate) wait(ctx context.Context, xid uint32) error {
	if xid == 0 {
		return fmt.Errorf("%w: zero online transaction XID", ErrVisibilityGuard)
	}
	waitCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	delay := g.pollInterval
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		snap, err := g.poll(waitCtx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if waitCtx.Err() != nil {
				return fmt.Errorf("%w: xid %d: %v", ErrVisibilityTimeout, xid, err)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if snap.visible(xid) {
			return nil
		}
		remaining := g.timeout
		if deadline, ok := waitCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		sleep, next := backoff(delay, remaining)
		delay = next
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-waitCtx.Done():
			timer.Stop()
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("%w: xid %d", ErrVisibilityTimeout, xid)
		case <-timer.C:
		}
	}
}
func (g *onlineVisibilityGate) close(ctx context.Context) error {
	if g.conn == nil {
		return nil
	}
	return g.conn.Close(ctx)
}
