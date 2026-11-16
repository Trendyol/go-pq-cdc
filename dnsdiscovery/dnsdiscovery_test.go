package dnsdiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logRecord struct {
	level string
	msg   string
}

// captureLogger records what the package logs; dnsdiscovery logs through the
// shared logger package like the core does.
type captureLogger struct {
	records []logRecord
	mu      sync.Mutex
}

func (l *captureLogger) add(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{level: level, msg: msg})
}

func (l *captureLogger) Debug(msg string, _ ...any) { l.add("debug", msg) }
func (l *captureLogger) Info(msg string, _ ...any)  { l.add("info", msg) }
func (l *captureLogger) Warn(msg string, _ ...any)  { l.add("warn", msg) }
func (l *captureLogger) Error(msg string, _ ...any) { l.add("error", msg) }

func (l *captureLogger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = nil
}

func (l *captureLogger) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if r.level == "warn" {
			out = append(out, r.msg)
		}
	}
	return out
}

var logs = &captureLogger{}

func TestMain(m *testing.M) {
	logger.InitLogger(logs)
	m.Run()
}

var errNotFound = &net.DNSError{Err: "no such host", IsNotFound: true}

type answer struct {
	err   error
	addrs []netip.Addr
}

// fakeLookup answers by (network, host) and records every call.
type fakeLookup struct {
	answers map[string]answer
	calls   []string
	mu      sync.Mutex
}

func newFakeLookup(answers map[string]answer) *fakeLookup { return &fakeLookup{answers: answers} }

func (f *fakeLookup) lookup(_ context.Context, network, host string) ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, network+" "+host)
	a, ok := f.answers[network+" "+host]
	if !ok && network == "ip" {
		// Without an explicit answer the unfiltered lookup returns what the
		// family lookups return, as the resolver does; "ip" is set explicitly
		// where a test needs it to differ.
		var all []netip.Addr
		for _, fam := range []string{"ip4 ", "ip6 "} {
			all = append(all, f.answers[fam+host].addrs...)
		}
		if len(all) == 0 {
			return nil, errNotFound
		}
		return all, nil
	}
	if !ok {
		return nil, errNotFound
	}
	return a.addrs, a.err
}

func addrs(s ...string) []netip.Addr {
	out := make([]netip.Addr, len(s))
	for i, a := range s {
		out[i] = netip.MustParseAddr(a)
	}
	return out
}

// resolverWith builds the resolve function over a fake lookup, without the
// network.
func resolverWith(f *fakeLookup, port int, names ...string) *resolver {
	return &resolver{names: names, port: fmt.Sprint(port), lookup: f.lookup}
}

func addrsOf(t *testing.T, r *resolver) []string {
	t.Helper()
	members, err := r.resolve(context.Background())
	require.NoError(t, err)
	out := make([]string, len(members))
	for i, m := range members {
		out[i] = m.Addr
	}
	sort.Strings(out)
	return out
}

func TestMembersCarryPortSourceAndNoTimeline(t *testing.T) {
	f := newFakeLookup(map[string]answer{
		"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
		"ip4 replica.b.": {addrs: addrs("10.0.1.1")},
	})
	members, err := resolverWith(f, 6432, "replica.a", "replica.b").resolve(context.Background())
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "10.0.0.1:6432", members[0].Addr)
	assert.Equal(t, "replica.a", members[0].Source, "Source is the configured name, not the absolute one")
	assert.Equal(t, "10.0.1.1:6432", members[1].Addr)
	assert.Equal(t, "replica.b", members[1].Source)
	for _, m := range members {
		assert.Zero(t, m.Timeline, "DNS carries no timeline")
	}
}

func TestIPv6AddressIsBracketed(t *testing.T) {
	f := newFakeLookup(map[string]answer{"ip6 replica.a.": {addrs: addrs("2001:db8::7")}})
	assert.Equal(t, []string{"[2001:db8::7]:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
}

func TestNameIsLookedUpAbsolute(t *testing.T) {
	f := newFakeLookup(map[string]answer{"ip4 replica.a.": {addrs: addrs("10.0.0.1")}})
	// A name given with and without the trailing dot is queried the same,
	// always with exactly one dot, so a search list cannot answer for it.
	assert.Equal(t, []string{"10.0.0.1:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
	assert.Equal(t, []string{"10.0.0.1:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a.")))
	for _, c := range f.calls {
		assert.Equal(t, "replica.a.", strings.SplitN(c, " ", 2)[1])
	}
}

func TestBothFamiliesAreLookedUpAndMerged(t *testing.T) {
	f := newFakeLookup(map[string]answer{
		"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
		"ip6 replica.a.": {addrs: addrs("2001:db8::1")},
	})
	assert.Equal(t, []string{"10.0.0.1:5432", "[2001:db8::1]:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
	assert.ElementsMatch(t, []string{"ip4 replica.a.", "ip6 replica.a.", "ip replica.a."}, f.calls, "the unfiltered cross-check lookup runs too")
}

func TestOneFamilyAbsentIsFineWhenTheOtherAnswered(t *testing.T) {
	// ip6 is not in the table: the fake answers "not found" for it.
	f := newFakeLookup(map[string]answer{"ip4 replica.a.": {addrs: addrs("10.0.0.1")}})
	assert.Equal(t, []string{"10.0.0.1:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
}

func TestOtherLookupErrorFailsTheSourceEvenWhenTheOtherFamilyAnswered(t *testing.T) {
	for name, failure := range map[string]error{
		"server failure": &net.DNSError{Err: "server misbehaving", IsTemporary: true},
		"timeout":        &net.DNSError{Err: "i/o timeout", IsTimeout: true, IsTemporary: true},
		"refused":        &net.OpError{Op: "dial", Err: errors.New("connection refused")},
		"plain":          errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			for _, failing := range []string{"ip4", "ip6"} {
				other := map[string]string{"ip4": "ip6", "ip6": "ip4"}[failing]
				f := newFakeLookup(map[string]answer{
					failing + " replica.a.": {err: failure},
					other + " replica.a.":   {addrs: addrs(map[string]string{"ip4": "10.0.0.1", "ip6": "2001:db8::1"}[other])},
				})
				members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
				require.Error(t, err, "%s failing, %s answering", failing, other)
				assert.Nil(t, members)
				assert.Contains(t, err.Error(), "replica.a")
				assert.Contains(t, err.Error(), failing)
			}
		})
	}
}

func TestEmptyAnswerIsASourceFailure(t *testing.T) {
	for name, answers := range map[string]map[string]answer{
		"both families not found": {},
		"both families empty":     {"ip4 replica.a.": {}, "ip6 replica.a.": {}},
	} {
		t.Run(name, func(t *testing.T) {
			members, err := resolverWith(newFakeLookup(answers), 5432, "replica.a").resolve(context.Background())
			require.ErrorContains(t, err, "no A or AAAA record")
			assert.Nil(t, members, "an empty answer must never read as zero replicas")
		})
	}
}

func TestMoreThanThreeRecordsAreAllReported(t *testing.T) {
	// Consul's silent cut to 3 records is the forwarder's doing; the resolver
	// itself must not truncate.
	f := newFakeLookup(map[string]answer{"ip4 replica.a.": {addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5")}})
	assert.Equal(t, []string{"10.0.0.1:5432", "10.0.0.2:5432", "10.0.0.3:5432", "10.0.0.4:5432", "10.0.0.5:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
}

func TestRepeatedAddressInsideOneNameIsCollapsed(t *testing.T) {
	f := newFakeLookup(map[string]answer{"ip4 replica.a.": {addrs: addrs("10.0.0.1", "10.0.0.2", "10.0.0.1")}})
	assert.Equal(t, []string{"10.0.0.1:5432", "10.0.0.2:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
}

func TestSameAddressUnderTwoNamesIsAnError(t *testing.T) {
	f := newFakeLookup(map[string]answer{
		"ip4 replica.a.": {addrs: addrs("10.0.0.1", "10.0.0.2")},
		"ip4 replica.b.": {addrs: addrs("10.0.0.2")},
	})
	members, err := resolverWith(f, 5432, "replica.a", "replica.b").resolve(context.Background())
	require.ErrorContains(t, err, "10.0.0.2:5432")
	require.ErrorContains(t, err, "replica.a")
	require.ErrorContains(t, err, "replica.b")
	assert.Nil(t, members, "two names resolving to the same standby is never merged")
}

func TestOneFailingNameFailsTheWholeResolve(t *testing.T) {
	f := newFakeLookup(map[string]answer{
		"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
		"ip4 replica.b.": {err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
	})
	members, err := resolverWith(f, 5432, "replica.a", "replica.b").resolve(context.Background())
	require.ErrorContains(t, err, "replica.b")
	assert.Nil(t, members, "never a partial set")
}

func TestNamesAreLookedUpConcurrently(t *testing.T) {
	// Every lookup waits until both names have entered; a sequential resolver
	// never gets the second name in and the context ends the wait.
	var entered sync.WaitGroup
	entered.Add(2)
	release := make(chan struct{})
	go func() { entered.Wait(); close(release) }()
	var once sync.Map
	lookup := func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		if network == "ip6" {
			return nil, errNotFound
		}
		if _, dup := once.LoadOrStore(host, true); !dup {
			entered.Done()
		}
		select {
		case <-release:
			return addrs(map[string]string{"replica.a.": "10.0.0.1", "replica.b.": "10.0.0.2"}[host]), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := &resolver{names: []string{"replica.a", "replica.b"}, port: "5432", lookup: lookup}
	members, err := r.resolve(ctx)
	require.NoError(t, err)
	assert.Len(t, members, 2)
}

func TestInvalidOptionsFailEveryResolve(t *testing.T) {
	for name, opts := range map[string]Options{
		"no name":                  {Port: 5432},
		"port zero":                {Names: []string{"replica.a"}},
		"port too large":           {Names: []string{"replica.a"}, Port: 70000},
		"nameserver is a hostname": {Names: []string{"replica.a"}, Port: 5432, Nameserver: "consul.service.internal:8600"},
		"nameserver without port":  {Names: []string{"replica.a"}, Port: 5432, Nameserver: "10.0.0.1"},
	} {
		t.Run(name, func(t *testing.T) {
			members, err := Replicas(opts)(context.Background())
			require.Error(t, err)
			assert.Nil(t, members)
		})
	}
}

func TestWarnsOnceAtStartupWhenNoNameserverIsSet(t *testing.T) {
	logs.reset()
	resolve := Replicas(Options{Names: []string{"replica.a"}, Port: 5432})
	warnings := logs.warnings()
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "udp_answer_limit")
	assert.Contains(t, warnings[0], "nameserver")

	// Resolving, even with an error, never repeats the warning.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = resolve(ctx)
	_, _ = resolve(ctx)
	assert.Len(t, logs.warnings(), 1)

	logs.reset()
	Replicas(Options{Names: []string{"replica.a"}, Port: 5432, Nameserver: "10.0.0.1:8600"})
	assert.Empty(t, logs.warnings(), "a configured nameserver removes the risk the warning names")
}

func TestNameserverIsDialledOverTCPWhateverTheResolverAsks(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan struct{})
	go func() {
		if c, aErr := ln.Accept(); aErr == nil {
			close(accepted)
			_ = c.Close()
		}
	}()

	r := newNetResolver(ln.Addr().String())
	// The resolver asks for a UDP packet conn to the address it read from
	// resolv.conf; both must be ignored.
	conn, err := r.Dial(context.Background(), "udp", "192.0.2.1:53")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	assert.Equal(t, ln.Addr().String(), conn.RemoteAddr().String())
	_, isPacketConn := conn.(net.PacketConn)
	assert.False(t, isPacketConn, "a packet conn would make the resolver speak UDP")
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("the nameserver was never connected to over TCP")
	}

	assert.Nil(t, newNetResolver("").Dial, "without a nameserver the system configuration is used")
}

// The tests below drive the real net.Resolver against a minimal DNS server on
// TCP only, so they pass only if the query really goes to the nameserver over
// TCP, once per family, for the absolute name.

func TestResolverEndToEndOverTCP(t *testing.T) {
	srv := newDNSServer(t, func(name string, qtype uint16) (int, []netip.Addr) {
		if name != "replica.test.consul." {
			return rcodeNXDomain, nil
		}
		if qtype == typeA {
			return rcodeOK, addrs("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4")
		}
		return rcodeOK, addrs("2001:db8::9")
	})
	resolve := Replicas(Options{Names: []string{"replica.test.consul"}, Port: 5432, Nameserver: srv.addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members, err := resolve(ctx)
	require.NoError(t, err)
	got := make([]string, len(members))
	for i, m := range members {
		got[i] = m.Addr
	}
	assert.ElementsMatch(t, []string{"10.0.0.1:5432", "10.0.0.2:5432", "10.0.0.3:5432", "10.0.0.4:5432", "[2001:db8::9]:5432"}, got)
	// ip4 asks A, ip6 asks AAAA, the unfiltered cross-check asks both.
	assert.ElementsMatch(t, []string{"replica.test.consul. A", "replica.test.consul. AAAA", "replica.test.consul. A", "replica.test.consul. AAAA"}, srv.queries())
}

func TestResolverEndToEndFamilyOutcomes(t *testing.T) {
	cases := []struct {
		a, aaaa int
		wantErr string
	}{
		{a: rcodeOK, aaaa: rcodeNXDomain},
		{a: rcodeNXDomain, aaaa: rcodeOK},
		{a: rcodeOK, aaaa: rcodeServFail, wantErr: "ip6 lookup"},
		{a: rcodeServFail, aaaa: rcodeOK, wantErr: "ip4 lookup"},
		{a: rcodeNXDomain, aaaa: rcodeNXDomain, wantErr: "no A or AAAA record"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("a=%d,aaaa=%d", tc.a, tc.aaaa), func(t *testing.T) {
			srv := newDNSServer(t, func(_ string, qtype uint16) (int, []netip.Addr) {
				rcode, ip := tc.a, "10.0.0.1"
				if qtype == typeAAAA {
					rcode, ip = tc.aaaa, "2001:db8::1"
				}
				if rcode != rcodeOK {
					return rcode, nil
				}
				return rcodeOK, addrs(ip)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			members, err := Replicas(Options{Names: []string{"replica.test."}, Port: 5432, Nameserver: srv.addr})(ctx)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, members)
				return
			}
			require.NoError(t, err)
			assert.Len(t, members, 1)
		})
	}
}

// Go's ip6 lookup drops an AAAA record that holds an IPv4-mapped address
// (::ffff:a.b.c.d) without an error, so a set built from the ip4 and ip6
// lookups alone silently lacks that standby. The unfiltered lookup must catch it.
func TestResolverEndToEndNeverDropsAnIPv4MappedAAAA(t *testing.T) {
	cases := map[string]struct {
		a, aaaa []string
		want    []string // the members; empty means the name must fail
		named   string   // the address the error must name
	}{
		"A plus a mapped AAAA":           {a: []string{"10.0.0.1"}, aaaa: []string{"::ffff:10.0.0.2"}, named: "::ffff:10.0.0.2"},
		"genuine AAAA plus mapped AAAA":  {aaaa: []string{"2001:db8::1", "::ffff:10.0.0.2"}, named: "::ffff:10.0.0.2"},
		"only a mapped AAAA":             {aaaa: []string{"::ffff:10.0.0.2"}, named: "::ffff:10.0.0.2"},
		"A and genuine AAAA and mapped":  {a: []string{"10.0.0.1"}, aaaa: []string{"2001:db8::1", "::ffff:10.0.0.2"}, named: "::ffff:10.0.0.2"},
		"mapped AAAA repeats an A":       {a: []string{"10.0.0.1"}, aaaa: []string{"::ffff:10.0.0.1"}, want: []string{"10.0.0.1:5432"}},
		"mapped AAAA repeats an A, more": {a: []string{"10.0.0.1", "10.0.0.2"}, aaaa: []string{"2001:db8::1", "::ffff:10.0.0.2"}, want: []string{"10.0.0.1:5432", "10.0.0.2:5432", "[2001:db8::1]:5432"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newDNSServer(t, func(_ string, qtype uint16) (int, []netip.Addr) {
				ips := tc.a
				if qtype == typeAAAA {
					ips = tc.aaaa
				}
				if len(ips) == 0 {
					return rcodeNXDomain, nil
				}
				return rcodeOK, addrs(ips...)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			members, err := Replicas(Options{Names: []string{"replica.test."}, Port: 5432, Nameserver: srv.addr})(ctx)
			if tc.named != "" {
				require.ErrorContains(t, err, tc.named)
				assert.Nil(t, members, "a set that lacks a standby of the answer must never be returned")
				return
			}
			require.NoError(t, err)
			got := make([]string, len(members))
			for i, m := range members {
				got[i] = m.Addr
			}
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestCrossCheckLookup(t *testing.T) {
	t.Run("an address only the unfiltered lookup returned fails the name", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
			"ip replica.a.":  {addrs: addrs("10.0.0.1", "10.0.0.9")},
		})
		members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
		require.ErrorContains(t, err, "10.0.0.9")
		assert.Nil(t, members)
	})
	t.Run("a mapped form of a reported address is explained", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
			"ip replica.a.":  {addrs: addrs("10.0.0.1", "::ffff:10.0.0.1")},
		})
		assert.Equal(t, []string{"10.0.0.1:5432"}, addrsOf(t, resolverWith(f, 5432, "replica.a")))
	})
	t.Run("members are unmapped so a duplicate across names is still found", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
			"ip4 replica.b.": {addrs: addrs("::ffff:10.0.0.1")},
		})
		_, err := resolverWith(f, 5432, "replica.a", "replica.b").resolve(context.Background())
		require.ErrorContains(t, err, "10.0.0.1:5432")
	})
	t.Run("an error of the unfiltered lookup fails the name", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
			"ip replica.a.":  {err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}},
		})
		members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
		require.ErrorContains(t, err, "ip lookup")
		assert.Nil(t, members)
	})
	t.Run("not found from the unfiltered lookup fails the name when ip4 returned records", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip4 replica.a.": {addrs: addrs("10.0.0.1")},
			"ip replica.a.":  {err: errNotFound},
		})
		members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
		require.ErrorContains(t, err, "the unfiltered lookup found no record while the ip4 or ip6 lookup returned 1")
		assert.Nil(t, members)
	})
	t.Run("not found from the unfiltered lookup fails the name when ip6 returned records", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{
			"ip6 replica.a.": {addrs: addrs("2001:db8::7")},
			"ip replica.a.":  {err: errNotFound},
		})
		members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
		require.ErrorContains(t, err, "the unfiltered lookup found no record")
		assert.Nil(t, members)
	})
	t.Run("not found from every lookup is an empty source failure", func(t *testing.T) {
		f := newFakeLookup(map[string]answer{})
		members, err := resolverWith(f, 5432, "replica.a").resolve(context.Background())
		require.ErrorContains(t, err, "no A or AAAA record")
		assert.Nil(t, members)
	})
}
