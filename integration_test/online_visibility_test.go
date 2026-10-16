package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// physicalApplyGate is explicit PG16 synchronous-replication instrumentation:
// receive and flush real primary WAL, but withhold apply acknowledgement until
// Release. It is not a replaying standby or an HA qualification fixture.
func physicalApplyGate(t *testing.T, dsn, start string) (*atomic.Bool, <-chan error) {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("replication", "true")
	q.Set("application_name", "online_visibility_gate")
	u.RawQuery = q.Encode()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := pgconn.Connect(ctx, u.String())
	require.NoError(t, err)
	conn.Frontend().Send(&pgproto3.Query{String: "START_REPLICATION PHYSICAL " + start})
	require.NoError(t, conn.Frontend().Flush())
	raw, err := conn.ReceiveMessage(ctx)
	require.NoError(t, err)
	require.IsType(t, &pgproto3.CopyBothResponse{}, raw)
	release := &atomic.Bool{}
	result := make(chan error, 1)
	go func() {
		defer conn.Close(context.Background())
		var received uint64
		lastFeedback := time.Time{}
		for {
			if ctx.Err() != nil {
				result <- ctx.Err()
				return
			}
			if time.Since(lastFeedback) > 20*time.Millisecond {
				b := make([]byte, 34)
				b[0] = 'r'
				binary.BigEndian.PutUint64(b[1:], received)
				binary.BigEndian.PutUint64(b[9:], received)
				if release.Load() {
					binary.BigEndian.PutUint64(b[17:], received)
				}
				binary.BigEndian.PutUint64(b[25:], uint64(time.Now().UnixMicro()-946684800000000))
				_ = conn.Conn().SetWriteDeadline(time.Now().Add(time.Second))
				conn.Frontend().Send(&pgproto3.CopyData{Data: b})
				e := conn.Frontend().Flush()
				_ = conn.Conn().SetWriteDeadline(time.Time{})
				if e != nil {
					result <- e
					return
				}
				lastFeedback = time.Now()
			}
			read, stop := context.WithTimeout(ctx, 20*time.Millisecond)
			msg, e := conn.ReceiveMessage(read)
			stop()
			if e != nil {
				if pgconn.Timeout(e) {
					continue
				}
				result <- e
				return
			}
			if copy, ok := msg.(*pgproto3.CopyData); ok && len(copy.Data) > 0 {
				if copy.Data[0] == 'w' && len(copy.Data) >= 25 {
					end := binary.BigEndian.Uint64(copy.Data[1:]) + uint64(len(copy.Data)-25)
					if end > received {
						received = end
					}
				}
			} else if errorResponse, ok := msg.(*pgproto3.ErrorResponse); ok {
				result <- pgconn.ErrorResponseToPgError(errorResponse)
				return
			}
		}
	}()
	return release, result
}

func TestOnlineVisibilityBarrier(t *testing.T) {
	t.Run("connector-durable-restart", testOnlineRootVisibility)
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	writer := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE visible(id bigint PRIMARY KEY,value text); ALTER TABLE visible REPLICA IDENTITY FULL; CREATE PUBLICATION visibility_pub FOR TABLE visible")
	var start string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT lsn::text FROM pg_create_logical_replication_slot('visibility_slot','pgoutput')").Scan(&start))
	lsn, err := pq.ParseLSN(start)
	require.NoError(t, err)
	replicationDSN := strings.Replace(source.DSN, "sslmode=disable", "sslmode=disable&replication=database", 1)
	identity, err := online.IdentifySource(context.Background(), replicationDSN)
	require.NoError(t, err)
	opts := replication.OnlineReaderOptions{ReplicationDSN: replicationDSN, SQLDSN: source.DSN, Publication: "visibility_pub", Slot: "visibility_slot", StartLSN: lsn, ProtoVersion: 1, Source: identity, Limits: replication.OnlineReaderLimits{TransactionBytes: 4096, QueueBytes: 8192, FeedbackInterval: 50 * time.Millisecond, VisibilityTimeout: 15 * time.Second, PollInterval: 5 * time.Millisecond}}
	reader, err := replication.StartOnlineReader(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close(context.Background()) })
	code, _, err := source.Container.Exec(context.Background(), []string{"sh", "-c", `printf '\nhost replication postgres all scram-sha-256\n' >> "$PGDATA/pg_hba.conf"`})
	require.NoError(t, err)
	require.Zero(t, code)
	onlineExec(t, conn, "SELECT pg_reload_conf()")
	release, physicalResult := physicalApplyGate(t, source.DSN, start)
	defer release.Store(true)
	onlineExec(t, conn, "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (online_visibility_gate)'")
	onlineExec(t, conn, "SELECT pg_reload_conf()")
	onlineExec(t, conn, "SET synchronous_commit=off; INSERT INTO visible VALUES(99,'physical warmup')")
	onlineEventually(t, func() bool {
		var n int
		err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_replication WHERE application_name='online_visibility_gate' AND sync_state='sync'").Scan(&n)
		return err == nil && n == 1
	}, 5*time.Second, 10*time.Millisecond)
	warmup, err := reader.Next(context.Background())
	require.NoError(t, err)
	require.Len(t, warmup.Changes, 1)
	// Reset restart position only after this fixture warmup has been durably
	// observed; the subsequent held transaction must remain beyond that frontier.
	opts.StartLSN = warmup.EndLSN
	onlineExec(t, writer, "BEGIN; SET LOCAL synchronous_commit=remote_apply; INSERT INTO visible VALUES(1,'held')")
	var heldXID uint32
	require.NoError(t, writer.QueryRow(context.Background(), "SELECT pg_current_xact_id()::text::bigint").Scan(&heldXID))
	commitDone := make(chan error, 1)
	go func() { _, e := writer.Exec(context.Background(), "COMMIT"); commitDone <- e }()
	onlineEventually(t, func() bool {
		var waiting bool
		err := conn.QueryRow(context.Background(), "SELECT wait_event='SyncRep' FROM pg_stat_activity WHERE pid=$1", writer.PgConn().PID()).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 5*time.Millisecond, "held COMMIT must be in SyncRep")
	onlineExec(t, conn, "SET synchronous_commit=off; INSERT INTO visible VALUES(2,'independently visible')")
	var count int
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM visible WHERE id=2").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM visible WHERE id=1").Scan(&count))
	require.Zero(t, count)
	// BufferedBytes proves the committed logical envelope was decoded while the
	// source XID is still invisible. Next must not release it or the later XID.
	onlineEventually(t, func() bool { return reader.BufferedBytes() > 0 }, 5*time.Second, 5*time.Millisecond)
	nextCtx, cancelNext := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { _, e := reader.Next(nextCtx); returned <- e }()
	select {
	case e := <-returned:
		t.Fatalf("released invisible transaction: %v", e)
	case <-time.After(200 * time.Millisecond):
	}
	cancelNext()
	require.Error(t, <-returned)
	require.NoError(t, reader.Close(context.Background()))
	// Restart from the same durable checkpoint before releasing SQL visibility.
	reader, err = replication.StartOnlineReader(context.Background(), opts)
	require.NoError(t, err)
	envelopes := make(chan *replication.OnlineCommittedEnvelope, 2)
	gateErrors := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			e, err := reader.Next(context.Background())
			if err != nil {
				gateErrors <- err
				return
			}
			envelopes <- e
		}
	}()
	select {
	case e := <-envelopes:
		t.Fatalf("restart bypassed held xid: %d", e.XID)
	case err := <-gateErrors:
		t.Fatal(err)
	case <-time.After(200 * time.Millisecond):
	}
	release.Store(true)
	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case err := <-physicalResult:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("remote apply release timeout")
	}
	for i := 0; i < 2; i++ {
		select {
		case e := <-envelopes:
			if i == 0 {
				require.Equal(t, heldXID, e.XID)
			}
			require.NoError(t, reader.AdvanceSafeACK(e.EndLSN))
		case err := <-gateErrors:
			t.Fatal(err)
		case <-time.After(5 * time.Second):
			t.Fatal("visibility release timeout")
		}
	}
	require.NoError(t, reader.Close(context.Background()))
	t.Log(fmt.Sprintf("PG16 real COMMIT-before-SQL-visibility proven for xid=%d, later visible xid held behind it, including reader restart", heldXID))
}

func testOnlineRootVisibility(t *testing.T) {
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	writer := source.Connect(t)
	handlerSQL := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, "CREATE TABLE root_visible(id bigint PRIMARY KEY,value text)")
	provisionOnline(t, conn, "root_visible")
	cfg := onlineConfig(t, source, "root_visible")
	cfg.Snapshot.Online.QueryTimeout = 10 * time.Second
	cfg.Snapshot.Online.DeliveryTimeout = 30 * time.Second
	child, lines, input := startOnlineChild(t, cfg, dest.DSN, "visibility-root")
	select {
	case line := <-lines:
		require.Equal(t, "ONLINE-BARRIER", line)
	case <-time.After(10 * time.Second):
		t.Fatal("capture-only child barrier")
	}
	var checkpoint, binding string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT source_lsn::text,slot_binding::text FROM cdc_online.jobs").Scan(&checkpoint, &binding))
	oracle := startCDCOracle(t, source, cfg.Publication.Name, binding, false, "root_visible")
	code, _, err := source.Container.Exec(context.Background(), []string{"sh", "-c", `printf '\nhost replication postgres all scram-sha-256\n' >> "$PGDATA/pg_hba.conf"`})
	require.NoError(t, err)
	require.Zero(t, code)
	onlineExec(t, conn, "SELECT pg_reload_conf()")
	var physicalStart string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&physicalStart))
	release, physicalResult := physicalApplyGate(t, source.DSN, physicalStart)
	defer release.Store(true)
	onlineExec(t, conn, "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (online_visibility_gate)'")
	onlineExec(t, conn, "SELECT pg_reload_conf()")
	onlineExec(t, conn, "SET synchronous_commit=off; UPDATE cdc_online.jobs SET status=status")
	onlineEventually(t, func() bool {
		var n int
		err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_replication WHERE application_name='online_visibility_gate' AND sync_state='sync'").Scan(&n)
		return err == nil && n == 1
	}, 5*time.Second, 10*time.Millisecond)
	onlineExec(t, writer, "BEGIN; SET LOCAL synchronous_commit=remote_apply; INSERT INTO root_visible VALUES(1,'held')")
	committed := make(chan error, 1)
	go func() { _, err := writer.Exec(context.Background(), "COMMIT"); committed <- err }()
	onlineEventually(t, func() bool {
		var held bool
		err := conn.QueryRow(context.Background(), "SELECT wait_event='SyncRep' FROM pg_stat_activity WHERE pid=$1", writer.PgConn().PID()).Scan(&held)
		return err == nil && held
	}, 5*time.Second, 10*time.Millisecond)
	onlineExec(t, conn, "INSERT INTO root_visible VALUES(2,'later visible')")
	var visible int
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM root_visible").Scan(&visible))
	require.Equal(t, 1, visible)
	_, err = input.Write([]byte("continue\n"))
	require.NoError(t, err)
	onlineEventually(t, func() bool {
		var n int
		err := conn.QueryRow(context.Background(), "SELECT count(*) FROM cdc_online.windows WHERE low_emitted").Scan(&n)
		return err == nil && n == 1
	}, 5*time.Second, 10*time.Millisecond)
	assertHeld := func() {
		var actual string
		var unsafe bool
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT j.source_lsn::text,s.confirmed_flush_lsn>j.source_lsn FROM cdc_online.jobs j JOIN pg_replication_slots s ON s.slot_name=j.slot_name").Scan(&actual, &unsafe))
		require.Equal(t, checkpoint, actual)
		require.False(t, unsafe)
		var n int
		require.NoError(t, target.Conn.QueryRow(context.Background(), "SELECT count(*) FROM online_event_ledger").Scan(&n))
		require.Zero(t, n, "invisible/later transaction escaped delivery")
	}
	assertHeld()
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait())
	r := startOnline(t, cfg, func(ctx context.Context, event online.Event) error {
		var n int
		err := handlerSQL.QueryRow(ctx, "SELECT count(*) FROM root_visible WHERE id=$1", event.Key).Scan(&n)
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("invisible row delivered")
		}
		return target.Apply(ctx, event, onlineTargetOK)
	})
	waitOnlineCapture(t, r)
	onlineEventually(t, func() bool { return r.Connector.Stats().BufferedEncodedBytes > 0 }, 5*time.Second, 10*time.Millisecond)
	assertHeld()
	release.Store(true)
	select {
	case err := <-committed:
		require.NoError(t, err)
	case err := <-physicalResult:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("physical apply release")
	}
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	compareOnlineTable(t, conn, target, "root_visible")
	oracle.assert(t, target, 2)
	var generation int
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT generation FROM cdc_online.jobs").Scan(&generation))
	require.Equal(t, 2, generation)
}
