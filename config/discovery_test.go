package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discoveryGuard(d *DNSDiscoveryConfig) VisibilityGuardConfig {
	return VisibilityGuardConfig{
		Enabled: true, FailMode: VisibilityFailClosed, Timeout: time.Second, PollInterval: time.Millisecond,
		Discovery: VisibilityDiscoveryConfig{DNS: d},
	}
}

func dnsBlock(names ...string) *DNSDiscoveryConfig {
	return &DNSDiscoveryConfig{Names: names, Port: 5432, RefreshInterval: 10 * time.Second}
}

func TestDNSDiscoveryValidation(t *testing.T) {
	with := func(mutate func(*VisibilityGuardConfig)) VisibilityGuardConfig {
		c := discoveryGuard(dnsBlock("replica.a.service.consul"))
		mutate(&c)
		return c
	}
	block := func(mutate func(*DNSDiscoveryConfig)) VisibilityGuardConfig {
		d := dnsBlock("replica.a.service.consul")
		mutate(d)
		return discoveryGuard(d)
	}
	cases := []struct {
		name    string
		wantErr string
		cfg     VisibilityGuardConfig
	}{
		{name: "valid block", cfg: discoveryGuard(dnsBlock("replica.a.service.consul"))},
		{name: "valid: two regions, trailing dot, underscore, nameserver", cfg: block(func(d *DNSDiscoveryConfig) {
			d.Names = []string{"replica.scope.service.services.region-a.example.internal.", "replica._scope.service.services.region-b.example.internal"}
			d.Nameserver = "10.0.0.1:8600"
		})},
		{name: "valid ipv6 nameserver", cfg: block(func(d *DNSDiscoveryConfig) { d.Nameserver = "[2001:db8::1]:8600" })},
		{name: "requires enabled", cfg: with(func(c *VisibilityGuardConfig) { c.Enabled = false }), wantErr: "visibilityGuard.discovery.dns requires visibilityGuard.enabled: true"},
		{name: "exclusive with replicas", cfg: with(func(c *VisibilityGuardConfig) { c.Replicas = []string{"standby1:5432"} }), wantErr: "visibilityGuard.discovery.dns and visibilityGuard.replicas are mutually exclusive"},
		{name: "exclusive with the Go hook", cfg: with(func(c *VisibilityGuardConfig) { c.ResolveReplicas = resolveNothing }), wantErr: "visibilityGuard.discovery.dns and visibilityGuard.ResolveReplicas are mutually exclusive"},
		{name: "exclusive with top-level RefreshInterval", cfg: with(func(c *VisibilityGuardConfig) { c.RefreshInterval = time.Second }), wantErr: "visibilityGuard.discovery.dns and visibilityGuard.RefreshInterval are mutually exclusive"},
		{name: "no names", cfg: discoveryGuard(dnsBlock()), wantErr: "visibilityGuard.discovery.dns.names must list at least one name"},
		{name: "empty name", cfg: discoveryGuard(dnsBlock("")), wantErr: `"" is not a valid DNS name`},
		{name: "only a dot", cfg: discoveryGuard(dnsBlock(".")), wantErr: `"." is not a valid DNS name`},
		{name: "ip literal", cfg: discoveryGuard(dnsBlock("10.0.0.1")), wantErr: `"10.0.0.1" is not a valid DNS name`},
		{name: "url", cfg: discoveryGuard(dnsBlock("http://replica.a")), wantErr: `"http://replica.a" is not a valid DNS name`},
		{name: "host and port", cfg: discoveryGuard(dnsBlock("replica.a:5432")), wantErr: `"replica.a:5432" is not a valid DNS name`},
		{name: "space", cfg: discoveryGuard(dnsBlock("replica a")), wantErr: `"replica a" is not a valid DNS name`},
		{name: "empty label", cfg: discoveryGuard(dnsBlock("replica..a")), wantErr: `"replica..a" is not a valid DNS name`},
		{name: "leading hyphen", cfg: discoveryGuard(dnsBlock("-replica.a")), wantErr: `"-replica.a" is not a valid DNS name`},
		{name: "label longer than 63", cfg: discoveryGuard(dnsBlock(strings.Repeat("a", 64) + ".b")), wantErr: "is not a valid DNS name"},
		{name: "duplicate name", cfg: discoveryGuard(dnsBlock("replica.a", "replica.a")), wantErr: `"replica.a" listed twice`},
		{name: "duplicate name differing in case", cfg: discoveryGuard(dnsBlock("Replica.A", "replica.a")), wantErr: `"replica.a" listed twice`},
		{name: "duplicate name differing in the trailing dot", cfg: discoveryGuard(dnsBlock("replica.a.", "replica.a")), wantErr: `"replica.a" listed twice`},
		{name: "port zero", cfg: block(func(d *DNSDiscoveryConfig) { d.Port = 0 }), wantErr: "visibilityGuard.discovery.dns.port must be in 1-65535"},
		{name: "port too large", cfg: block(func(d *DNSDiscoveryConfig) { d.Port = 65536 }), wantErr: "visibilityGuard.discovery.dns.port must be in 1-65535"},
		{name: "nameserver is a hostname", cfg: block(func(d *DNSDiscoveryConfig) { d.Nameserver = "consul.service.internal:8600" }), wantErr: "must be a literal ip:port"},
		{name: "nameserver without a port", cfg: block(func(d *DNSDiscoveryConfig) { d.Nameserver = "10.0.0.1" }), wantErr: "must be a literal ip:port"},
		{name: "negative refresh interval", cfg: block(func(d *DNSDiscoveryConfig) { d.RefreshInterval = -time.Second }), wantErr: "visibilityGuard.discovery.dns.refreshInterval must be greater than 0"},
		{name: "zero refresh interval without defaults", cfg: block(func(d *DNSDiscoveryConfig) { d.RefreshInterval = 0 }), wantErr: "visibilityGuard.discovery.dns.refreshInterval must be greater than 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestDNSDiscoveryDefaults(t *testing.T) {
	t.Run("refresh interval and the main connection's port", func(t *testing.T) {
		cfg := Config{Port: 6432, VisibilityGuard: discoveryGuard(&DNSDiscoveryConfig{Names: []string{"replica.a.service.consul"}})}
		cfg.SetDefault()
		d := cfg.VisibilityGuard.Discovery.DNS
		require.NotNil(t, d)
		assert.Equal(t, DefaultReplicaRefreshInterval, d.RefreshInterval)
		assert.Equal(t, 6432, d.Port)
		assert.Zero(t, cfg.VisibilityGuard.RefreshInterval)
		require.NoError(t, cfg.VisibilityGuard.Validate())
	})
	t.Run("the main port itself defaults to 5432", func(t *testing.T) {
		cfg := Config{VisibilityGuard: discoveryGuard(&DNSDiscoveryConfig{Names: []string{"replica.a.service.consul"}})}
		cfg.SetDefault()
		assert.Equal(t, 5432, cfg.VisibilityGuard.Discovery.DNS.Port)
	})
	t.Run("explicit values are kept", func(t *testing.T) {
		cfg := Config{Port: 6432, VisibilityGuard: discoveryGuard(&DNSDiscoveryConfig{Names: []string{"replica.a.service.consul"}, Port: 5433, RefreshInterval: 3 * time.Second})}
		cfg.SetDefault()
		d := cfg.VisibilityGuard.Discovery.DNS
		assert.Equal(t, 5433, d.Port)
		assert.Equal(t, 3*time.Second, d.RefreshInterval)
	})
}

func TestDNSDiscoveryDecoding(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	yamlBody := `visibilityGuard:
  enabled: true
  discovery:
    dns:
      refreshInterval: 3s
      port: 5433
      nameserver: 10.0.0.1:8600
      names:
        - replica.a.service.consul
        - replica.b.service.consul
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(yamlBody), 0o600))
	jsonPath := filepath.Join(dir, "config.json")
	// encoding/json reads time.Duration as nanoseconds, like every other duration field.
	jsonBody := `{"visibilityGuard":{"enabled":true,"discovery":{"dns":{"refreshInterval":3000000000,"port":5433,"nameserver":"10.0.0.1:8600","names":["replica.a.service.consul","replica.b.service.consul"]}}}}`
	require.NoError(t, os.WriteFile(jsonPath, []byte(jsonBody), 0o600))

	check := func(t *testing.T, cfg Config, err error) {
		t.Helper()
		require.NoError(t, err)
		require.True(t, cfg.VisibilityGuard.Enabled)
		d := cfg.VisibilityGuard.Discovery.DNS
		require.NotNil(t, d)
		assert.Equal(t, 3*time.Second, d.RefreshInterval)
		assert.Equal(t, 5433, d.Port)
		assert.Equal(t, "10.0.0.1:8600", d.Nameserver)
		assert.Equal(t, []string{"replica.a.service.consul", "replica.b.service.consul"}, d.Names)

		cfg.SetDefault()
		require.NoError(t, cfg.VisibilityGuard.Validate())
	}
	t.Run("yaml", func(t *testing.T) {
		cfg, err := ReadConfigYAML(yamlPath)
		check(t, cfg, err)
	})
	t.Run("json", func(t *testing.T) {
		cfg, err := ReadConfigJSON(jsonPath)
		check(t, cfg, err)
	})
}
