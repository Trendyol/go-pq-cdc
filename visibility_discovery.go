package cdc

import (
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/dnsdiscovery"
)

// newDNSResolver is dnsdiscovery.Replicas; the wiring test replaces it to see
// the options the YAML block produces without a DNS server.
var newDNSResolver = dnsdiscovery.Replicas

// applyVisibilityDiscovery builds the ResolveReplicas hook from
// visibilityGuard.discovery.dns. NewConnector calls it once, after Validate and
// after the logger is installed (the resolver warns at construction), so the
// block and the hook are never both set during validation. The replica guard
// then runs the same hook a Go caller would set.
func applyVisibilityDiscovery(cfg *config.Config) {
	d := cfg.VisibilityGuard.Discovery.DNS
	if d == nil {
		return
	}
	cfg.VisibilityGuard.ResolveReplicas = newDNSResolver(dnsdiscovery.Options{
		Nameserver: d.Nameserver,
		Names:      d.Names,
		Port:       d.Port,
	})
	cfg.VisibilityGuard.RefreshInterval = d.RefreshInterval
}
