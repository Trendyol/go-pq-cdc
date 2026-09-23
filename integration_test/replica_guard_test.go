package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startStandby streams from the primary and delays the apply of commit records
// by applyDelay (recovery_min_apply_delay), so every event arrives while the
// standby has the row's WAL flushed but the transaction not yet applied. It
// returns the standby as the replica guard lists it (host:port) and a
// superuser pool on it.
func startStandby(ctx context.Context, t *testing.T, name, applyDelay string) (string, *pgxpool.Pool) {
	t.Helper()
	version := os.Getenv(PostgresVersion)
	if version == "" {
		version = defaultVersion
	}
	script := fmt.Sprintf(`pg_basebackup -h primary -U postgres -D "$PGDATA" -R -X stream -C -S %s && chmod 700 "$PGDATA"
exec postgres -c fsync=off -c max_wal_senders=100 -c max_replication_slots=50 -c recovery_min_apply_delay=%s -c primary_conninfo='host=primary user=postgres password=postgres application_name=%s'`, name, applyDelay, name)
	standby, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/postgres:" + version,
			Env:          map[string]string{"PGPASSWORD": "postgres"},
			ExposedPorts: []string{"5432/tcp"},
			Networks:     []string{Network.Name},
			User:         "postgres",
			Entrypoint:   []string{"sh", "-ec"},
			Cmd:          []string{script},
			WaitingFor: wait.ForExec([]string{"pg_isready", "-U", "postgres", "-d", Config.Database}).
				WithStartupTimeout(90 * time.Second),
		},
	})
	require.NoError(t, err, "start standby")
	t.Cleanup(func() {
		_ = standby.Terminate(ctx)
		dropReplicationSlot(ctx, t, name)
	})

	port, err := standby.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	hostPort := fmt.Sprintf("localhost:%d", port.Int())
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", hostPort, Config.Database))
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return hostPort, pool
}

// dropReplicationSlot removes the physical slot pg_basebackup -C created on the
// primary. Left behind, an inactive slot pins WAL for the rest of the suite.
// The walsender needs a moment to notice the standby is gone: until it does,
// the slot is still active and the drop is refused.
func dropReplicationSlot(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	conn, err := newPostgresConn()
	if err != nil {
		t.Logf("drop replication slot %s: %v", name, err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()
	for i := 0; i < 25; i++ {
		if err = pgExec(ctx, conn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", name)); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("drop replication slot %s: %v", name, err)
}

type replicaEvent struct {
	waitErr      error
	dispatchedAt time.Time
	commitLSN    pq.LSN
	id           int32
	visible      bool
}

// runReplicaGuardConnector starts a connector whose handler, on every insert,
// reads the standby and records what it found; the connector is closed with t.
func runReplicaGuardConnector(ctx context.Context, t *testing.T, cdcCfg config.Config, standby *pgxpool.Pool) <-chan replicaEvent {
	t.Helper()
	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

	events := make(chan replicaEvent, 16)
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			ev := replicaEvent{id: ins.Decoded["id"].(int32), dispatchedAt: time.Now(), commitLSN: lCtx.CommitLSN}
			var n int
			ev.visible = standby.QueryRow(ctx, "SELECT count(*) FROM books WHERE id = $1", ev.id).Scan(&n) == nil && n == 1
			ev.waitErr = replication.WaitReplayed(ctx, standby, lCtx.CommitLSN, replication.WaitOptions{Timeout: 500 * time.Millisecond})
			events <- ev
		}
		_ = lCtx.Ack()
	}
	connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		_ = RestoreDB(ctx)
		_ = postgresConn.Close(ctx)
	})
	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	return events
}

// waitStandbyCaughtUp blocks until the standby has replayed everything the
// primary has written so far. The test setup drops and recreates books; while
// that transaction's commit sits in the apply delay the standby holds its
// AccessExclusiveLock, and a read of books there would block (forever, once
// replay is paused). Start every scenario from a caught-up standby.
func waitStandbyCaughtUp(ctx context.Context, t *testing.T, primary, standby *pgxpool.Pool) {
	t.Helper()
	var lsn string
	require.NoError(t, primary.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn))
	deadline := time.Now().Add(15 * time.Second)
	for {
		var caughtUp bool
		require.NoError(t, standby.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", lsn).Scan(&caughtUp))
		if caughtUp {
			return
		}
		require.False(t, time.Now().After(deadline), "standby did not catch up to %s", lsn)
		time.Sleep(100 * time.Millisecond)
	}
}

func nextEvent(t *testing.T, events <-chan replicaEvent, within time.Duration) replicaEvent {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(within):
		t.Fatalf("no event within %s", within)
		return replicaEvent{}
	}
}

// TestReplicaGuard runs against a standby that applies commit records 3s late.
// See docs/replica-guard-design.md (R11).
func TestReplicaGuard(t *testing.T) {
	const applyDelay = 3 * time.Second
	ctx := context.Background()
	standbyHostPort, standby := startStandby(ctx, t, "standby_replica_guard", applyDelay.String())

	primaryCfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	primary, err := pgxpool.New(ctx, primaryCfg.DSNWithoutSSL())
	require.NoError(t, err)
	t.Cleanup(primary.Close)

	t.Run("event is held until the standby applied the commit", func(t *testing.T) {
		cdcCfg := Config
		cdcCfg.Slot.Name = "slot_test_replica_guard_held"
		cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailClosed, Replicas: []string{standbyHostPort}}
		events := runReplicaGuardConnector(ctx, t, cdcCfg, standby)
		waitStandbyCaughtUp(ctx, t, primary, standby)

		committedAt := time.Now()
		_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (97001, 'held')")
		require.NoError(t, err)

		ev := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(97001), ev.id)
		assert.True(t, ev.visible, "the row must be readable on the standby when the handler runs")
		assert.NoError(t, ev.waitErr, "WaitReplayed passes at once on a standby the guard already waited for")
		held := ev.dispatchedAt.Sub(committedAt)
		assert.GreaterOrEqual(t, held, applyDelay-500*time.Millisecond, "dispatch must wait for the apply delay, held %s", held)
		assert.Less(t, held, 10*time.Second, "held %s", held)
	})

	t.Run("old events bypass replica waits until CDC catches up", func(t *testing.T) {
		cdcCfg := Config
		cdcCfg.Slot.Name = "slot_test_replica_guard_bypass"
		cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{
			Enabled:  true,
			FailMode: config.VisibilityFailClosed,
			Replicas: []string{standbyHostPort},
			ReplicaBypass: config.ReplicaBypassConfig{
				Enabled:        true,
				MaxEventAge:    time.Second,
				ResumeEventAge: 200 * time.Millisecond,
			},
		}
		events := runReplicaGuardConnector(ctx, t, cdcCfg, standby)
		waitStandbyCaughtUp(ctx, t, primary, standby)

		firstCommittedAt := time.Now()
		_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (97003, 'guarded')")
		require.NoError(t, err)
		time.Sleep(1500 * time.Millisecond)
		secondCommittedAt := time.Now()
		_, err = primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (97004, 'bypassed')")
		require.NoError(t, err)

		first := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(97003), first.id)
		assert.True(t, first.visible, "fresh event must use the replica guard")
		assert.GreaterOrEqual(t, first.dispatchedAt.Sub(firstCommittedAt), applyDelay-500*time.Millisecond)

		second := nextEvent(t, events, 5*time.Second)
		assert.Equal(t, int32(97004), second.id)
		assert.False(t, second.visible, "event aged behind the first wait must bypass replica visibility")
		assert.Less(t, second.dispatchedAt.Sub(secondCommittedAt), applyDelay-500*time.Millisecond)
		assert.ErrorIs(t, second.waitErr, replication.ErrVisibilityTimeout)

		require.NoError(t, replication.WaitReplayed(ctx, standby, second.commitLSN, replication.WaitOptions{Timeout: 15 * time.Second}))
		thirdCommittedAt := time.Now()
		_, err = primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (97005, 'guarded-again')")
		require.NoError(t, err)

		third := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(97005), third.id)
		assert.True(t, third.visible, "fresh event must leave catch-up mode and restore replica visibility")
		assert.GreaterOrEqual(t, third.dispatchedAt.Sub(thirdCommittedAt), applyDelay-500*time.Millisecond)
	})

	t.Run("failMode open dispatches after the timeout while the standby is paused", func(t *testing.T) {
		const timeout = 2 * time.Second
		cdcCfg := Config
		cdcCfg.Slot.Name = "slot_test_replica_guard_paused"
		cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailOpen, Timeout: timeout, Replicas: []string{standbyHostPort}}
		events := runReplicaGuardConnector(ctx, t, cdcCfg, standby)
		waitStandbyCaughtUp(ctx, t, primary, standby)

		_, err := standby.Exec(ctx, "SELECT pg_wal_replay_pause()")
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = standby.Exec(ctx, "SELECT pg_wal_replay_resume()") })

		committedAt := time.Now()
		_, err = primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (97002, 'paused')")
		require.NoError(t, err)

		ev := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(97002), ev.id)
		assert.False(t, ev.visible, "replay is paused: the row cannot be on the standby")
		assert.ErrorIs(t, ev.waitErr, replication.ErrVisibilityTimeout, "WaitReplayed sees the same lag")
		held := ev.dispatchedAt.Sub(committedAt)
		assert.GreaterOrEqual(t, held, timeout-200*time.Millisecond, "fail-open waits the full timeout first, held %s", held)

		_, err = standby.Exec(ctx, "SELECT pg_wal_replay_resume()")
		require.NoError(t, err)
		assert.NoError(t, replication.WaitReplayed(ctx, standby, ev.commitLSN, replication.WaitOptions{Timeout: 15 * time.Second}))
		var n int
		require.NoError(t, standby.QueryRow(ctx, "SELECT count(*) FROM books WHERE id = 97002").Scan(&n))
		assert.Equal(t, 1, n, "after WaitReplayed the row is readable on the same connection pool")
	})

	t.Run("a primary listed as replica is rejected at startup", func(t *testing.T) {
		cdcCfg := Config
		cdcCfg.Slot.Name = "slot_test_replica_guard_primary"
		cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, Replicas: []string{fmt.Sprintf("%s:%d", Config.Host, Config.Port)}}
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)
		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))
		connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) { _ = lCtx.Ack() })
		require.NoError(t, err)
		t.Cleanup(func() {
			connector.Close()
			_ = RestoreDB(ctx)
			_ = postgresConn.Close(ctx)
		})
		go connector.Start(ctx)
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		assert.Error(t, connector.WaitUntilReady(waitCtx), "stream must not become ready when a listed replica is not in recovery")
	})
}
