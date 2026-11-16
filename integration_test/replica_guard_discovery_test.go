package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplicaGuardDiscovery changes the resolved replica set while the stream
// runs (docs/replica-guard-design.md R16–R18): the standby applies commit
// records 3s late, so an event waits for it only while the resolver lists it.
func TestReplicaGuardDiscovery(t *testing.T) {
	const applyDelay = 3 * time.Second
	const refreshInterval = 200 * time.Millisecond
	ctx := context.Background()
	standbyHostPort, standby := startStandby(ctx, t, "standby_replica_discovery", applyDelay.String())

	primaryCfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	primary, err := pgxpool.New(ctx, primaryCfg.DSNWithoutSSL())
	require.NoError(t, err)
	t.Cleanup(primary.Close)

	var resolved atomic.Pointer[[]config.ReplicaMember]
	// No member is the explicit non-nil empty slice: nil is not "zero replicas".
	setMembers := func(members ...config.ReplicaMember) {
		members = append([]config.ReplicaMember{}, members...)
		resolved.Store(&members)
	}
	var calls atomic.Int32
	listed := config.ReplicaMember{Addr: standbyHostPort, Source: "fake"}
	setMembers(listed)

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_guard_discovery"
	cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{
		Enabled: true, FailMode: config.VisibilityFailClosed, RefreshInterval: refreshInterval,
		ResolveReplicas: func(context.Context) ([]config.ReplicaMember, error) {
			calls.Add(1)
			return *resolved.Load(), nil
		},
	}
	events := runReplicaGuardConnector(ctx, t, cdcCfg, standby)
	waitStandbyCaughtUp(ctx, t, primary, standby)

	t.Run("listed: held until the standby applied the commit", func(t *testing.T) {
		committedAt := time.Now()
		_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (98001, 'listed')")
		require.NoError(t, err)
		ev := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(98001), ev.id)
		assert.True(t, ev.visible, "the row must be readable on the standby when the handler runs")
		assert.GreaterOrEqual(t, ev.dispatchedAt.Sub(committedAt), applyDelay-500*time.Millisecond)
	})

	t.Run("dropped from the set: primary-only, not held", func(t *testing.T) {
		setMembers()
		time.Sleep(2 * refreshInterval)
		committedAt := time.Now()
		_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (98002, 'dropped')")
		require.NoError(t, err)
		ev := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(98002), ev.id)
		assert.False(t, ev.visible, "dispatched before the standby applied it: the standby is no longer gated")
		assert.Less(t, ev.dispatchedAt.Sub(committedAt), applyDelay-time.Second)
	})

	t.Run("re-added: held again", func(t *testing.T) {
		setMembers(listed)
		time.Sleep(2 * refreshInterval)
		committedAt := time.Now()
		_, err := primary.Exec(ctx, "INSERT INTO books (id, name) VALUES (98003, 're-added')")
		require.NoError(t, err)
		ev := nextEvent(t, events, 20*time.Second)
		assert.Equal(t, int32(98003), ev.id)
		assert.True(t, ev.visible)
		assert.GreaterOrEqual(t, ev.dispatchedAt.Sub(committedAt), applyDelay-500*time.Millisecond)
	})

	assert.GreaterOrEqual(t, calls.Load(), int32(3), "startup plus a refresh per change")
}
