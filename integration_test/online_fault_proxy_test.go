package integration

import (
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type onlineWireFault string

const (
	faultReadRows      onlineWireFault = "rows"
	faultCommitHeld    onlineWireFault = "commit-held"
	faultCommitReply   onlineWireFault = "commit-reply"
	faultBeforeCommit  onlineWireFault = "before-commit"
	faultRollbackReply onlineWireFault = "rollback-reply"
)

// Controlled real-wire cuts. Backend COMMIT completion means PostgreSQL has
// committed, even though that completion/ReadyForQuery never reaches the client.
func onlineFaultProxy(t *testing.T, dsn string, fault onlineWireFault) (string, <-chan struct{}) {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	address := u.Host
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	u.Host = listener.Addr().String()
	stop := make(chan struct{})
	fired := make(chan struct{})
	var once sync.Once
	var consumed atomic.Bool
	done := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		defer close(done)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				backend, err := net.Dial("tcp", address)
				if err != nil {
					return
				}
				defer backend.Close()
				closed := make(chan struct{})
				var closeOnce sync.Once
				closePair := func() { closeOnce.Do(func() { close(closed) }); client.Close(); backend.Close() }
				watcherDone := make(chan struct{})
				go func() {
					defer close(watcherDone)
					select {
					case <-stop:
					case <-closed:
					}
					closePair()
				}()
				defer func() { closePair(); <-watcherDone }()
				// Negotiate optional PostgreSQL SSL request; isolated image responds N.
				startup := func() ([]byte, error) {
					var header [4]byte
					if _, e := io.ReadFull(client, header[:]); e != nil {
						return nil, e
					}
					n := binary.BigEndian.Uint32(header[:])
					if n < 8 || n > 1<<20 {
						return nil, io.ErrUnexpectedEOF
					}
					b := make([]byte, n)
					copy(b, header[:])
					_, e := io.ReadFull(client, b[4:])
					return b, e
				}
				first, e := startup()
				if e != nil {
					return
				}
				if _, e = backend.Write(first); e != nil {
					return
				}
				if binary.BigEndian.Uint32(first[4:8]) == 80877103 {
					var answer [1]byte
					if _, e = io.ReadFull(backend, answer[:]); e != nil {
						return
					}
					client.Write(answer[:])
					if answer[0] != 'N' {
						return
					}
					first, e = startup()
					if e != nil {
						return
					}
					if _, e = backend.Write(first); e != nil {
						return
					}
				}
				var mu sync.Mutex
				query := ""
				completion := false
				selectedRows := 0
				backendDone := make(chan struct{})
				go func() {
					defer close(backendDone)
					defer closePair()
					for {
						tag, body, e := readOnlineFrame(backend)
						if e != nil {
							return
						}
						mu.Lock()
						q := query
						complete := completion
						if tag == 'D' && strings.Contains(q, "SELECT * FROM") {
							selectedRows++
						}
						rows := selectedRows
						mu.Unlock()
						cut := fault == faultReadRows && tag == 'D' && rows == 2 || (fault == faultCommitReply || fault == faultCommitHeld) && complete && tag == 'C' && string(body) == "COMMIT\x00" || fault == faultRollbackReply && tag == 'C' && string(body) == "ROLLBACK\x00"
						if cut && consumed.CompareAndSwap(false, true) {
							once.Do(func() { close(fired) })
							if fault == faultRollbackReply || fault == faultCommitHeld {
								select {
								case <-stop:
								case <-closed:
								}
							}
							return
						}
						if e = writeOnlineFrame(client, tag, body); e != nil {
							return
						}
					}
				}()
				defer func() { closePair(); <-backendDone }()
				for {
					tag, body, e := readOnlineFrame(client)
					if e != nil {
						return
					}
					mu.Lock()
					if tag == 'Q' {
						query = strings.TrimSuffix(string(body), "\x00")
					} else if tag == 'P' {
						if n := strings.IndexByte(string(body), 0); n >= 0 {
							query = strings.TrimSuffix(string(body[n+1:]), "\x00")
						}
					}
					if strings.Contains(query, "UPDATE cdc_online.windows SET status='completed'") {
						completion = true
					}
					q := query
					complete := completion
					mu.Unlock()
					if fault == faultBeforeCommit && complete && tag == 'Q' && strings.EqualFold(q, "commit") && consumed.CompareAndSwap(false, true) {
						once.Do(func() { close(fired) })
						return
					}
					if e = writeOnlineFrame(backend, tag, body); e != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { close(stop); listener.Close(); <-done; wg.Wait() })
	return u.String(), fired
}
func readOnlineFrame(conn net.Conn) (byte, []byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(conn, h[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n < 4 || n > 64<<20 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	body := make([]byte, n-4)
	_, err := io.ReadFull(conn, body)
	return h[0], body, err
}
func writeOnlineFrame(conn net.Conn, tag byte, body []byte) error {
	var h [5]byte
	h[0] = tag
	binary.BigEndian.PutUint32(h[1:], uint32(len(body)+4))
	if _, err := conn.Write(h[:]); err != nil {
		return err
	}
	_, err := conn.Write(body)
	return err
}
