package replication

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestOnlineReaderStartupDeadline(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
		close(accepted)
	}()
	options := onlineOptions()
	options.ReplicationDSN = "postgres://test:test@" + listener.Addr().String() + "/test?sslmode=disable"
	options.SQLDSN = options.ReplicationDSN
	begin := time.Now()
	_, err = StartOnlineReaderWithStartupTimeout(context.Background(), options, 50*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup did not fail at the independent deadline: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("startup exceeded independent deadline: %s", elapsed)
	}
	// A busy scheduler can consume this small budget before TCP acceptance;
	// that is still a correctly bounded connect, not an authentication success.
	// Close/join the fixture accept owner instead of requiring an unproven stage.
	_ = listener.Close()
	if conn := <-accepted; conn != nil {
		_ = conn.Close()
	}
	if _, err = StartOnlineReaderWithStartupTimeout(context.Background(), options, 0); err == nil {
		t.Fatal("zero startup bound accepted")
	}
}
