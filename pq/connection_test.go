package pq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestConnectionConnectAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	conn := NewConnectionTemplate("postgres://test@127.0.0.1:1/test?sslmode=disable")
	start := time.Now()
	err := conn.Connect(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 750*time.Millisecond)
	require.True(t, conn.IsClosed())
}

func TestConnectionConnectCancelsRetryWait(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var attempts atomic.Int32
	rejected := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			startup, readErr := pgproto3.NewBackend(conn, conn).ReceiveStartupMessage()
			_ = conn.Close()
			if readErr != nil {
				continue
			}
			if _, ok := startup.(*pgproto3.StartupMessage); !ok {
				continue // Do not count pgconn's asynchronous CancelRequest.
			}
			if attempts.Add(1) == 1 {
				close(rejected)
			}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-serverDone
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn := NewConnectionTemplate(fmt.Sprintf("postgres://test@%s/test?sslmode=disable", listener.Addr()))
	result := make(chan error, 1)
	go func() { result <- conn.Connect(ctx) }()
	select {
	case <-rejected:
	case <-time.After(2 * time.Second):
		t.Fatal("connection attempt did not reach the server")
	}
	// Allow the rejected attempt to reach the one-second retry wait.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(750 * time.Millisecond):
		t.Fatal("connection retry wait did not observe cancellation")
	}
	require.EqualValues(t, 1, attempts.Load())
	require.True(t, conn.IsClosed())
}

func TestConnectionClosesSocketWhenPingFails(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	serverResult := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		serverResult <- rejectConnectionPing(pgproto3.NewBackend(conn, conn))
	}()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn := NewConnectionTemplate(fmt.Sprintf("postgres://test@%s/test?sslmode=disable", listener.Addr()))
	result := make(chan error, 1)
	go func() { result <- conn.Connect(ctx) }()
	select {
	case err := <-serverResult:
		require.NoError(t, err)
	case <-time.After(750 * time.Millisecond):
		t.Fatal("connection with a failed Ping was not closed before retrying")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(750 * time.Millisecond):
		t.Fatal("connection retry did not stop")
	}
}

func rejectConnectionPing(backend *pgproto3.Backend) error {
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return err
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return err
	}
	if _, err := backend.Receive(); err != nil { // Ping query.
		return err
	}
	backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "query canceled"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return err
	}
	message, err := backend.Receive()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, ok := message.(*pgproto3.Terminate); !ok {
		return fmt.Errorf("expected Terminate after failed Ping, got %T", message)
	}
	return nil
}
