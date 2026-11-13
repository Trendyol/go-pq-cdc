package config

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resolveNothing(context.Context) ([]ReplicaMember, error) { return nil, nil }

func TestVisibilityGuardResolveReplicasConfig(t *testing.T) {
	enabled := func() VisibilityGuardConfig {
		return VisibilityGuardConfig{Enabled: true, FailMode: VisibilityFailClosed, Timeout: time.Second, PollInterval: time.Millisecond}
	}

	t.Run("requires the primary guard", func(t *testing.T) {
		cfg := VisibilityGuardConfig{ResolveReplicas: resolveNothing}
		require.ErrorContains(t, cfg.Validate(), "visibilityGuard.ResolveReplicas requires visibilityGuard.enabled: true")
	})

	t.Run("mutually exclusive with replicas", func(t *testing.T) {
		cfg := enabled()
		cfg.ResolveReplicas, cfg.Replicas = resolveNothing, []string{"standby1:5432"}
		require.ErrorContains(t, cfg.Validate(), "visibilityGuard.replicas and visibilityGuard.ResolveReplicas are mutually exclusive")
	})

	t.Run("negative refresh interval", func(t *testing.T) {
		cfg := enabled()
		cfg.ResolveReplicas, cfg.RefreshInterval = resolveNothing, -time.Second
		require.ErrorContains(t, cfg.Validate(), "visibilityGuard.RefreshInterval must not be negative")
	})

	t.Run("refresh interval defaults only with a resolver", func(t *testing.T) {
		withHook := Config{VisibilityGuard: VisibilityGuardConfig{Enabled: true, ResolveReplicas: resolveNothing}}
		withHook.SetDefault()
		assert.Equal(t, DefaultReplicaRefreshInterval, withHook.VisibilityGuard.RefreshInterval)
		require.NoError(t, withHook.VisibilityGuard.Validate())

		explicit := Config{VisibilityGuard: VisibilityGuardConfig{Enabled: true, ResolveReplicas: resolveNothing, RefreshInterval: 3 * time.Second}}
		explicit.SetDefault()
		assert.Equal(t, 3*time.Second, explicit.VisibilityGuard.RefreshInterval)

		static := Config{VisibilityGuard: VisibilityGuardConfig{Enabled: true, Replicas: []string{"standby1:5432"}}}
		static.SetDefault()
		assert.Zero(t, static.VisibilityGuard.RefreshInterval)
	})

	// Config.Print marshals the whole config and drops a marshal error.
	t.Run("a func field never reaches JSON", func(t *testing.T) {
		cfg := Config{VisibilityGuard: VisibilityGuardConfig{Enabled: true, ResolveReplicas: resolveNothing, RefreshInterval: time.Second}}
		b, err := json.Marshal(cfg)
		require.NoError(t, err)
		assert.NotContains(t, string(b), "ResolveReplicas")
		assert.NotContains(t, string(b), "RefreshInterval")
	})
}
