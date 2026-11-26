package integration

import (
	"context"
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

type onlineFaultControl struct {
	fault       onlineWireFault
	stop, fired chan struct{}
	once        sync.Once
	consumed    atomic.Bool
}
type onlineFaultQuery struct {
	mu           sync.Mutex
	query        string
	completion   bool
	selectedRows int
}

// Controlled real-wire cuts preserve COMMIT completion on the server even when
// CommandComplete/ReadyForQuery is deliberately withheld from the client.
func onlineFaultProxy(t *testing.T, dsn string, fault onlineWireFault) (string, <-chan struct{}) {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	address := u.Host
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	u.Host = listener.Addr().String()
	control := &onlineFaultControl{fault: fault, stop: make(chan struct{}), fired: make(chan struct{})}
	done := make(chan struct{})
	var owners sync.WaitGroup
	go func() {
		defer close(done)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			owners.Add(1)
			go func() { defer owners.Done(); onlineFaultConnection(t.Context(), client, address, control) }()
		}
	}()
	t.Cleanup(func() { close(control.stop); _ = listener.Close(); <-done; owners.Wait() })
	return u.String(), control.fired
}
func onlineFaultConnection(ctx context.Context, client net.Conn, address string, control *onlineFaultControl) {
	defer client.Close()
	backend, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return
	}
	defer backend.Close()
	closed := make(chan struct{})
	var once sync.Once
	closePair := func() { once.Do(func() { close(closed) }); _ = client.Close(); _ = backend.Close() }
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-control.stop:
		case <-closed:
		case <-ctx.Done():
		}
		closePair()
	}()
	defer func() { closePair(); <-watcherDone }()
	if err = onlineFaultStartup(client, backend); err != nil {
		return
	}
	query := &onlineFaultQuery{}
	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		defer closePair()
		onlineFaultBackend(backend, client, query, control, closed)
	}()
	defer func() { closePair(); <-backendDone }()
	onlineFaultFrontend(client, backend, query, control)
}
func onlineFaultStartup(client, backend net.Conn) error {
	first, err := onlineStartupPacket(client)
	if err != nil {
		return err
	}
	if _, err = backend.Write(first); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(first[4:8]) != 80877103 {
		return nil
	}
	var answer [1]byte
	if _, err = io.ReadFull(backend, answer[:]); err != nil {
		return err
	}
	if _, err = client.Write(answer[:]); err != nil {
		return err
	}
	if answer[0] != 'N' {
		return io.ErrUnexpectedEOF
	}
	first, err = onlineStartupPacket(client)
	if err != nil {
		return err
	}
	_, err = backend.Write(first)
	return err
}
func onlineStartupPacket(client net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(client, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 8 || n > 1<<20 {
		return nil, io.ErrUnexpectedEOF
	}
	packet := make([]byte, n)
	copy(packet, header[:])
	_, err := io.ReadFull(client, packet[4:])
	return packet, err
}
func onlineFaultBackend(backend, client net.Conn, query *onlineFaultQuery, control *onlineFaultControl, closed <-chan struct{}) {
	for {
		tag, body, err := readOnlineFrame(backend)
		if err != nil {
			return
		}
		query.mu.Lock()
		q, complete := query.query, query.completion
		if tag == 'D' && strings.Contains(q, "SELECT * FROM") {
			query.selectedRows++
		}
		rows := query.selectedRows
		query.mu.Unlock()
		cut := control.fault == faultReadRows && tag == 'D' && rows == 2 || (control.fault == faultCommitReply || control.fault == faultCommitHeld) && complete && tag == 'C' && string(body) == "COMMIT\x00" || control.fault == faultRollbackReply && tag == 'C' && string(body) == "ROLLBACK\x00"
		if cut && control.consumed.CompareAndSwap(false, true) {
			control.once.Do(func() { close(control.fired) })
			if control.fault == faultRollbackReply || control.fault == faultCommitHeld {
				select {
				case <-control.stop:
				case <-closed:
				}
			}
			return
		}
		if err = writeOnlineFrame(client, tag, body); err != nil {
			return
		}
	}
}
func onlineFaultFrontend(client, backend net.Conn, query *onlineFaultQuery, control *onlineFaultControl) {
	for {
		tag, body, err := readOnlineFrame(client)
		if err != nil {
			return
		}
		query.mu.Lock()
		switch tag {
		case 'Q':
			query.query = strings.TrimSuffix(string(body), "\x00")
		case 'P':
			if n := strings.IndexByte(string(body), 0); n >= 0 {
				query.query = strings.TrimSuffix(string(body[n+1:]), "\x00")
			}
		}
		if strings.Contains(query.query, "UPDATE cdc_online.windows SET status='completed'") {
			query.completion = true
		}
		q, complete := query.query, query.completion
		query.mu.Unlock()
		if control.fault == faultBeforeCommit && complete && tag == 'Q' && strings.EqualFold(q, "commit") && control.consumed.CompareAndSwap(false, true) {
			control.once.Do(func() { close(control.fired) })
			return
		}
		if err = writeOnlineFrame(backend, tag, body); err != nil {
			return
		}
	}
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
	if n, err := conn.Write(h[:]); err != nil {
		return err
	} else if n != len(h) {
		return io.ErrShortWrite
	}
	n, err := conn.Write(body)
	if err == nil && n != len(body) {
		return io.ErrShortWrite
	}
	return err
}
