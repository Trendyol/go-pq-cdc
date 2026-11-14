package cdc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/dnsdiscovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyVisibilityDiscoveryBuildsDNSResolver drives the YAML path up to the
// wiring seam (no PostgreSQL, no DNS server): the options the resolver is built
// with and the refresh interval the guard gets must be the file's, with the
// port taken from the main connection when the block has none.
func TestApplyVisibilityDiscoveryBuildsDNSResolver(t *testing.T) {
	var got dnsdiscovery.Options
	hook := func(context.Context) ([]config.ReplicaMember, error) {
		return []config.ReplicaMember{{Addr: "10.0.0.2:6432", Source: "replica.a"}}, nil
	}
	old := newDNSResolver
	newDNSResolver = func(o dnsdiscovery.Options) func(context.Context) ([]config.ReplicaMember, error) {
		got = o
		return hook
	}
	t.Cleanup(func() { newDNSResolver = old })

	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `port: 6432
visibilityGuard:
  enabled: true
  discovery:
    dns:
      refreshInterval: 3s
      nameserver: 10.0.0.1:8600
      names:
        - replica.a.service.consul
        - replica.b.service.consul
`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	cfg, err := config.ReadConfigYAML(path)
	require.NoError(t, err)
	cfg.SetDefault()
	require.NoError(t, cfg.VisibilityGuard.Validate())

	applyVisibilityDiscovery(&cfg)
	require.NotNil(t, cfg.VisibilityGuard.ResolveReplicas)
	assert.Equal(t, 3*time.Second, cfg.VisibilityGuard.RefreshInterval)
	assert.Equal(t, dnsdiscovery.Options{
		Nameserver: "10.0.0.1:8600",
		Names:      []string{"replica.a.service.consul", "replica.b.service.consul"},
		Port:       6432,
	}, got)

	members, err := cfg.VisibilityGuard.ResolveReplicas(context.Background())
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, "10.0.0.2:6432", members[0].Addr)
}

func TestApplyVisibilityDiscoveryLeavesStaticConfigAlone(t *testing.T) {
	cfg := config.Config{VisibilityGuard: config.VisibilityGuardConfig{Enabled: true, Replicas: []string{"standby1:5432"}}}
	cfg.SetDefault()
	require.NoError(t, cfg.VisibilityGuard.Validate())

	applyVisibilityDiscovery(&cfg)
	assert.Nil(t, cfg.VisibilityGuard.ResolveReplicas)
	assert.Zero(t, cfg.VisibilityGuard.RefreshInterval)
	assert.Equal(t, []string{"standby1:5432"}, cfg.VisibilityGuard.Replicas)
}
