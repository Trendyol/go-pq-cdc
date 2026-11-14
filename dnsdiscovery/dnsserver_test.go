package dnsdiscovery

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	typeA    uint16 = 1
	typeAAAA uint16 = 28

	rcodeOK       = 0
	rcodeServFail = 2
	rcodeNXDomain = 3
)

// dnsServer is a minimal DNS server on TCP only (no UDP listener at all): it
// answers one question per message, with the records the handler returns. It
// exists so the tests can run the real net.Resolver without a DNS library.
type dnsServer struct {
	addr string
	seen []string
	mu   sync.Mutex
}

func newDNSServer(t *testing.T, handle func(name string, qtype uint16) (rcode int, ips []netip.Addr)) *dnsServer {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	s := &dnsServer{addr: ln.Addr().String()}
	go func() {
		for {
			c, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			go s.serve(c, handle)
		}
	}()
	return s
}

func (s *dnsServer) queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func (s *dnsServer) serve(c net.Conn, handle func(string, uint16) (int, []netip.Addr)) {
	defer c.Close()
	for {
		var size [2]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		msg := make([]byte, binary.BigEndian.Uint16(size[:]))
		if _, err := io.ReadFull(c, msg); err != nil {
			return
		}
		name, qtype, end := parseQuestion(msg)

		s.mu.Lock()
		s.seen = append(s.seen, name+" "+map[uint16]string{typeA: "A", typeAAAA: "AAAA"}[qtype])
		s.mu.Unlock()

		rcode, ips := handle(name, qtype)
		resp := make([]byte, 12, 512)
		copy(resp, msg[:2])                                        // ID
		binary.BigEndian.PutUint16(resp[2:], 0x8180|uint16(rcode)) // QR, RD, RA, RCODE
		binary.BigEndian.PutUint16(resp[4:], 1)                    // QDCOUNT
		binary.BigEndian.PutUint16(resp[6:], uint16(len(ips)))     // ANCOUNT
		resp = append(resp, msg[12:end]...)                        // the question
		for _, ip := range ips {
			raw := ip.AsSlice()
			resp = append(resp, 0xC0, 0x0C) // pointer to the question name
			resp = binary.BigEndian.AppendUint16(resp, qtype)
			resp = binary.BigEndian.AppendUint16(resp, 1) // IN
			resp = binary.BigEndian.AppendUint32(resp, 0) // TTL
			resp = binary.BigEndian.AppendUint16(resp, uint16(len(raw)))
			resp = append(resp, raw...)
		}
		out := binary.BigEndian.AppendUint16(nil, uint16(len(resp)))
		if _, err := c.Write(append(out, resp...)); err != nil {
			return
		}
	}
}

// parseQuestion returns the question's absolute name, its type and the offset
// just past it.
func parseQuestion(msg []byte) (string, uint16, int) {
	var labels []string
	i := 12
	for msg[i] != 0 {
		n := int(msg[i])
		labels = append(labels, string(msg[i+1:i+1+n]))
		i += 1 + n
	}
	i++ // the root label
	qtype := binary.BigEndian.Uint16(msg[i:])
	return strings.Join(labels, ".") + ".", qtype, i + 4
}
