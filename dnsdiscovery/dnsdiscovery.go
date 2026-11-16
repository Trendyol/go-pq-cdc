// Package dnsdiscovery resolves the standbys the replica visibility guard has
// to gate from role-tagged service DNS names, for example the Consul name
// replica.<scope>.service.services.<region>.<domain>, one name per region. See
// docs/replica-guard-design.md R32-R40.
//
// Each name is one source. A name answers with the addresses of the standbys
// the service registry currently reports as passing; every address is combined
// with one configured port, because an A or AAAA record carries none. The
// resolver is stdlib only and reports Timeline 0 (unknown): DNS does not say
// which timeline a standby is on. A deployment that can supply timelines sets
// its own config.VisibilityGuardConfig ResolveReplicas hook.
package dnsdiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
)

// Options configures Replicas.
type Options struct {
	// Nameserver is an optional literal ip:port (a host name is rejected).
	// When set, every query goes to it over TCP, which is meant for a Consul
	// agent's DNS port and removes any forwarder from the path. When empty the
	// system resolver configuration is used.
	Nameserver string
	// Names are the DNS names to look up, one source each.
	Names []string
	// Port is the PostgreSQL port every address is combined with.
	Port int
}

// lookupFunc resolves host for network "ip4", "ip6" or "ip" (both families, no
// filter). It is the seam the unit tests replace.
type lookupFunc func(ctx context.Context, network, host string) ([]netip.Addr, error)

// Replicas returns a config.VisibilityGuardConfig.ResolveReplicas hook that
// reports every address behind the names as a standby, with Source set to the
// name and Timeline 0.
//
// Per name, inside the call's context, one A, one AAAA and one unfiltered
// cross-check lookup run concurrently and the name is made absolute so a search
// list can never answer for it. A lookup error other than "not found" fails the
// name; one family being absent is fine only when the other returned records.
// An address only the cross-check returns (an IPv4-mapped AAAA record, which the
// AAAA lookup drops) fails the name rather than leaving it out of the set. An empty answer
// (neither family has a record) fails the name as well: it is never read as
// "zero replicas", because a name that lists nobody cannot be told apart from a
// registry that lost its entries. Names are looked up concurrently and the
// union is all-or-nothing: any failing name fails the call, and the same
// host:port under two names is an error rather than a merge. An address
// repeated inside one answer is collapsed.
//
// Invalid options (no name, a port outside 1-65535, a nameserver that is not a
// literal ip:port) make every call fail: nothing is proven. When Nameserver is
// empty a Warn is logged once, here, about the silent answer cut a forwarder
// can cause.
func Replicas(opts Options) func(ctx context.Context) ([]config.ReplicaMember, error) {
	r := &resolver{names: append([]string(nil), opts.Names...), port: strconv.Itoa(opts.Port)}
	switch {
	case len(opts.Names) == 0:
		r.invalid = errors.New("dnsdiscovery: no name configured, no cluster is proven")
	case opts.Port < 1 || opts.Port > 65535:
		r.invalid = fmt.Errorf("dnsdiscovery: port %d is not in 1-65535", opts.Port)
	case opts.Nameserver != "":
		if _, err := netip.ParseAddrPort(opts.Nameserver); err != nil {
			r.invalid = fmt.Errorf("dnsdiscovery: nameserver %q must be a literal ip:port: %w", opts.Nameserver, err)
		}
	}
	if r.invalid == nil {
		r.lookup = newNetResolver(opts.Nameserver).lookup
	}
	if opts.Nameserver == "" {
		logger.Warn("dnsdiscovery: no nameserver is set, answers come from the system resolver; Consul cuts a UDP answer to 3 records (udp_answer_limit) when a forwarder queries without EDNS, so a standby can be missing silently. Set a nameserver (a Consul agent DNS port) or confirm the path returns complete answers")
	}
	return r.resolve
}

type resolver struct {
	invalid error
	lookup  lookupFunc
	port    string
	names   []string
}

// netResolver adapts net.Resolver to lookupFunc.
type netResolver struct{ *net.Resolver }

func (n netResolver) lookup(ctx context.Context, network, host string) ([]netip.Addr, error) {
	// ponytail: the stdlib resolver is trusted for message integrity: it accepts a TCP answer that still has the truncation flag and can return the records parsed before a malformed tail; the upgrade is a message-level DNS client that rejects both.
	return n.LookupNetIP(ctx, network, host)
}

// newNetResolver builds the pure-Go resolver (EDNS0, no cgo) that reports every
// server error instead of the first success. With a nameserver, every query
// goes to it over TCP: Go treats a net.Conn that is not a net.PacketConn as a
// stream and frames the query accordingly.
func newNetResolver(nameserver string) netResolver {
	r := &net.Resolver{PreferGo: true, StrictErrors: true}
	if nameserver != "" {
		r.Dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", nameserver)
		}
	}
	return netResolver{r}
}

type sourceResult struct {
	err     error
	members []config.ReplicaMember
}

func (r *resolver) resolve(ctx context.Context) ([]config.ReplicaMember, error) {
	if r.invalid != nil {
		return nil, r.invalid
	}
	results := make([]sourceResult, len(r.names))
	var wg sync.WaitGroup
	for i, name := range r.names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].members, results[i].err = r.source(ctx, name)
		}()
	}
	wg.Wait()

	var errs []error
	for _, res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	members := make([]config.ReplicaMember, 0)
	owner := make(map[string]string)
	for _, res := range results {
		for _, m := range res.members {
			if first, dup := owner[m.Addr]; dup {
				return nil, fmt.Errorf("dnsdiscovery: %s is listed under %s and under %s, two names resolved to the same standby", m.Addr, first, m.Source)
			}
			owner[m.Addr] = m.Source
		}
		members = append(members, res.members...)
	}
	return members, nil
}

// source resolves one name: the A, the AAAA and a cross-check lookup run
// concurrently. LookupNetIP("ip6") silently drops an AAAA record that holds an
// IPv4-mapped address (::ffff:a.b.c.d), so a set built from the two family
// lookups alone could be a subset of the answer. The "ip" lookup has no family
// filter: every address it returns, unmapped, must be in the union of the other
// two, otherwise the name fails and names the address nobody explained. A
// standby published only as a mapped AAAA therefore fails the name instead of
// vanishing from the gated set.
func (r *resolver) source(ctx context.Context, name string) ([]config.ReplicaMember, error) {
	// A trailing dot makes the name absolute: the resolver's search list and
	// ndots rule are skipped, so another name can never answer for it.
	fqdn := strings.TrimSuffix(name, ".") + "."

	families := [3]string{"ip4", "ip6", "ip"}
	var (
		answers [3][]netip.Addr
		errs    [3]error
		wg      sync.WaitGroup
	)
	for i, network := range families {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answers[i], errs[i] = r.lookup(ctx, network, fqdn)
		}()
	}
	wg.Wait()

	var failures []error
	for i, err := range errs {
		if err == nil || isNotFound(err) || (families[i] == "ip6" && isFilteredOut(err)) {
			continue
		}
		failures = append(failures, fmt.Errorf("dnsdiscovery name %s: %s lookup: %w", name, families[i], err))
	}
	if len(failures) > 0 {
		return nil, errors.Join(failures...)
	}

	// Unmap so 10.0.0.1 and ::ffff:10.0.0.1 are one address, which is what the
	// duplicate check across names compares.
	members := make([]config.ReplicaMember, 0, len(answers[0])+len(answers[1]))
	seen := make(map[netip.Addr]bool)
	for _, family := range answers[:2] {
		for _, a := range family {
			a = a.Unmap()
			if seen[a] {
				continue
			}
			seen[a] = true
			members = append(members, config.ReplicaMember{
				Addr:   net.JoinHostPort(a.String(), r.port),
				Source: name,
			})
		}
	}
	// The unfiltered lookup returns every record the family lookups do, so
	// "not found" from it while a family returned records is a disagreement,
	// not a pass.
	if familyRecords := len(answers[0]) + len(answers[1]); familyRecords > 0 && len(answers[2]) == 0 {
		return nil, fmt.Errorf("dnsdiscovery name %s: the unfiltered lookup found no record while the ip4 or ip6 lookup returned %d; the cross-check disagrees with the family lookups", name, familyRecords)
	}
	for _, a := range answers[2] {
		if !seen[a.Unmap()] {
			return nil, fmt.Errorf("dnsdiscovery name %s: the unfiltered lookup returned %s, which the ip4 and ip6 lookups did not report (an IPv4-mapped AAAA record is dropped by the ip6 lookup); the set would be incomplete", name, a)
		}
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("dnsdiscovery name %s: no A or AAAA record; an empty answer is a source failure, not zero replicas", name)
	}
	return members, nil
}

// isFilteredOut reports the stdlib's "no suitable address found": the answer had
// records but the family filter dropped every one, which is what an ip6 lookup
// reports when all AAAA records are IPv4-mapped. It is not a failure by itself:
// the unfiltered lookup in source accounts for those records.
func isFilteredOut(err error) bool {
	var addrErr *net.AddrError
	return errors.As(err, &addrErr) && addrErr.Err == "no suitable address found"
}

// isNotFound reports a definite "this name has no record of this type"
// (NXDOMAIN, or NOERROR without data). Everything else, a timeout, SERVFAIL,
// a refused or reset connection, says nothing about the name.
func isNotFound(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}
