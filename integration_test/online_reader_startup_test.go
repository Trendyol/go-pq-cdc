package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// startupQueryProxy holds one frontend Query before forwarding it to the real
// PG16 server. SSL is disabled in this isolated fixture so query framing is
// observable; no payload/credential messages are logged or retained.
func startupQueryProxy(t *testing.T, dsn, query string) (string, <-chan struct{}) {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	backendAddress := u.Host
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	u.Host = listener.Addr().String()
	params := u.Query()
	params.Set("sslmode", "disable")
	u.RawQuery = params.Encode()
	intercepted := make(chan struct{})
	var interceptedOnce sync.Once
	stop := make(chan struct{})
	acceptedStopped := make(chan struct{})
	var owners sync.WaitGroup
	go func() {
		defer close(acceptedStopped)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			owners.Add(1)
			go func() {
				defer owners.Done()
				defer client.Close()
				backend, err := net.DialTimeout("tcp", backendAddress, time.Second)
				if err != nil {
					return
				}
				defer backend.Close()
				paired := make(chan struct{})
				var pairedOnce sync.Once
				closePair := func() { pairedOnce.Do(func() { close(paired) }) }
				watchStopped := make(chan struct{})
				go func() {
					defer close(watchStopped)
					select {
					case <-stop:
					case <-paired:
					}
					_ = client.Close()
					_ = backend.Close()
				}()
				backendStopped := make(chan struct{})
				go func() { defer close(backendStopped); _, _ = io.Copy(client, backend); closePair() }()
				defer func() { closePair(); _ = backend.Close(); _ = client.Close(); <-backendStopped; <-watchStopped }()
				// The startup packet has a length word but no type byte.
				var size [4]byte
				if _, err = io.ReadFull(client, size[:]); err != nil {
					return
				}
				length := binary.BigEndian.Uint32(size[:])
				if length < 8 || length > 1<<20 {
					return
				}
				startup := make([]byte, length)
				copy(startup, size[:])
				if _, err = io.ReadFull(client, startup[4:]); err != nil {
					return
				}
				if query == "SQL_CONNECT" {
					interceptedOnce.Do(func() { close(intercepted) })
					select {
					case <-stop:
					case <-paired:
					}
					return
				}
				if _, err = backend.Write(startup); err != nil {
					return
				}
				for {
					var header [5]byte
					if _, err = io.ReadFull(client, header[:]); err != nil {
						return
					}
					length = binary.BigEndian.Uint32(header[1:])
					if length < 4 || length > 1<<20 {
						return
					}
					body := make([]byte, length-4)
					if _, err = io.ReadFull(client, body); err != nil {
						return
					}
					if header[0] == 'Q' && strings.Contains(string(body), query) {
						interceptedOnce.Do(func() { close(intercepted) })
						select {
						case <-stop:
						case <-paired:
						}
						return
					}
					if _, err = backend.Write(header[:]); err != nil {
						return
					}
					if _, err = backend.Write(body); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { close(stop); _ = listener.Close(); <-acceptedStopped; owners.Wait() })
	return u.String(), intercepted
}

func readerStartupFixture(t *testing.T) (onlinePGFixture, replication.OnlineReaderOptions) {
	t.Helper()
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE startup_rows(id bigint PRIMARY KEY,value text); ALTER TABLE startup_rows REPLICA IDENTITY FULL; CREATE PUBLICATION startup_pub FOR TABLE startup_rows")
	var start string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT lsn::text FROM pg_create_logical_replication_slot('startup_slot','pgoutput')").Scan(&start))
	lsn, err := pq.ParseLSN(start)
	require.NoError(t, err)
	replicationDSN := source.DSN + "&replication=database"
	sourceID, err := online.IdentifySource(context.Background(), replicationDSN)
	require.NoError(t, err)
	return source, replication.OnlineReaderOptions{ReplicationDSN: replicationDSN, SQLDSN: source.DSN, Publication: "startup_pub", Slot: "startup_slot", StartLSN: lsn, ProtoVersion: 1, Source: sourceID, Limits: replication.OnlineReaderLimits{TransactionBytes: 4096, QueueBytes: 8192, FeedbackInterval: 50 * time.Millisecond, VisibilityTimeout: 5 * time.Second, PollInterval: 5 * time.Millisecond}}
}
func TestOnlineReaderStartupStages(t *testing.T) {
	for _, stage := range []string{"IDENTIFY_SYSTEM", "SQL_CONNECT", "pg_catalog.pg_control_system()", "START_REPLICATION"} {
		t.Run(stage, func(t *testing.T) {
			_, opts := readerStartupFixture(t)
			dsn := opts.ReplicationDSN
			if stage == "pg_catalog.pg_control_system()" || stage == "SQL_CONNECT" {
				dsn = opts.SQLDSN
			}
			proxy, held := startupQueryProxy(t, dsn, stage)
			if stage == "pg_catalog.pg_control_system()" || stage == "SQL_CONNECT" {
				opts.SQLDSN = proxy
			} else {
				opts.ReplicationDSN = proxy
			}
			began := time.Now()
			reader, err := replication.StartOnlineReaderWithStartupTimeout(context.Background(), opts, 300*time.Millisecond)
			if reader != nil {
				defer reader.Close(context.Background())
			}
			require.Error(t, err)
			require.Less(t, time.Since(began), 2*time.Second, "startup negotiation deadline")
			select {
			case <-held:
			default:
				t.Fatalf("requested startup stage was not intercepted: %s: %v", stage, err)
			}
			t.Logf("bounded real PG16 startup stage %s: %v", stage, err)
		})
	}
}
func TestOnlineReaderStartupLifecycle(t *testing.T) {
	source, opts := readerStartupFixture(t)
	run, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := replication.StartOnlineReaderWithStartupTimeout(run, opts, 500*time.Millisecond)
	require.NoError(t, err)
	defer reader.Close(context.Background())
	select {
	case <-reader.Done():
		t.Fatalf("successful owner died with startup cleanup: %v", reader.Err())
	case <-time.After(1200 * time.Millisecond):
	}
	conn, err := pgx.Connect(context.Background(), source.DSN)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	onlineExec(t, conn, "INSERT INTO startup_rows VALUES(1,'after startup expiry')")
	envelope, err := reader.Next(run)
	require.NoError(t, err)
	require.Len(t, envelope.Changes, 1)
	cancel()
	select {
	case <-reader.Done():
		require.ErrorIs(t, reader.Err(), context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("lifetime cancellation did not stop reader")
	}
	t.Log(fmt.Sprintf("startup expired without canceling successful source owner; delivered end=%s and honored lifetime cancellation", envelope.EndLSN))
}
