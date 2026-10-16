package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Dedicated test-process mode, dispatched before TestMain's legacy containers.
// Pipes carry one bounded barrier line, never row payloads or credentials.
func runOnlineHelper() int {
	var cfg config.Config
	if err := json.Unmarshal([]byte(os.Getenv("CDC_ONLINE_CONFIG")), &cfg); err != nil {
		return 2
	}
	conn, err := pgx.Connect(context.Background(), os.Getenv("CDC_ONLINE_TARGET"))
	if err != nil {
		return 2
	}
	defer conn.Close(context.Background())
	target := &onlineTarget{Conn: conn}
	mode := os.Getenv("CDC_ONLINE_HELPER")
	reads := 0
	barrier := func(ctx context.Context) error {
		fmt.Fprintln(os.Stdout, "ONLINE-BARRIER")
		done := make(chan error, 1)
		go func() { input := bufio.NewReaderSize(os.Stdin, 64); _, err := input.ReadString('\n'); done <- err }()
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	handler := func(ctx context.Context, e online.Event) (result error) {
		if mode == "delayed-request" && e.Operation == online.Read && reads == 0 {
			data, err := json.Marshal(e)
			if err != nil {
				return err
			}
			tx, err := conn.Begin(ctx)
			if err != nil {
				return err
			}
			defer func() { result = errors.Join(result, online.RollbackBounded(tx)) }()
			if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "INSERT INTO online_pending_read VALUES($1::jsonb)", string(data)); err != nil {
				return err
			}
			if err = tx.Commit(ctx); err != nil {
				return err
			}
			return barrier(ctx)
		}
		if e.Operation == online.Read && mode == "before-read" && reads == 0 {
			if err := barrier(ctx); err != nil {
				return err
			}
		}
		if err := target.Apply(ctx, e, onlineTargetOK); err != nil {
			return err
		}
		if e.Operation == online.Read {
			reads++
			if mode == "after-read" && reads == 1 || mode == "after-last-read" && reads == 2 || mode == "after-third-read" && reads == 3 {
				if err := barrier(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	}
	observer := func(ctx context.Context, o online.Observation) error {
		match := mode == "after-low" && o.Phase == online.LOWRecorded || mode == "observed-select" && o.Phase == online.ReadResult || mode == "after-confirmed" && o.Phase == online.ACKAdvanced || mode == "visibility-root" && o.Phase == online.CaptureStarted
		if !match {
			return nil
		}
		if mode == "after-confirmed" {
			sql, err := pgx.Connect(ctx, cfg.DSN())
			if err != nil {
				return err
			}
			defer sql.Close(context.Background())
			for {
				var confirmed bool
				err = sql.QueryRow(ctx, "SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name=$2", o.EndLSN.String(), cfg.Slot.Name).Scan(&confirmed)
				if err != nil {
					return err
				}
				if confirmed {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
		return barrier(ctx)
	}
	connector, err := cdc.NewOnlineConnector(context.Background(), cfg, handler, cdc.WithOnlineObserver(observer))
	if err != nil {
		return 2
	}
	if err = connector.Run(context.Background()); err != nil {
		return 1
	}
	return 0
}

func startOnlineChild(t *testing.T, cfg config.Config, targetDSN, mode string) (*exec.Cmd, <-chan string, io.WriteCloser) {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "CDC_ONLINE_HELPER="+mode, "CDC_ONLINE_CONFIG="+string(data), "CDC_ONLINE_TARGET="+targetDSN)
	output, err := cmd.StdoutPipe()
	require.NoError(t, err)
	input, err := cmd.StdinPipe()
	require.NoError(t, err)
	// stderr is bounded and drained; credentials are never printed by this helper.
	cmd.Stderr = io.Discard
	require.NoError(t, cmd.Start())
	lines := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		scanner.Buffer(make([]byte, 64), 1024)
		if scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	t.Cleanup(func() {
		_ = input.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd, lines, input
}

func TestOnlineInitialRecovery(t *testing.T) {
	t.Run("delayed-old-READ", testOnlineDelayedREAD)
	for _, point := range []string{"after-low", "after-select", "observed-select", "before-read", "after-read", "after-last-read", "metadata-before-reply", "after-confirmed", "after-completed-window"} {
		t.Run(point, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			dest := newOnlinePGFixture(t)
			conn := source.Connect(t)
			target := newOnlineTarget(t, dest)
			onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'one'),(2,'two'),(3,'three'),(4,'four')")
			provisionOnline(t, conn, "items")
			cfg := onlineConfig(t, source, "items")
			mode := point
			if point == "after-completed-window" {
				mode = "after-third-read"
			}
			if point == "after-select" {
				installHighBarrier(t, conn)
				mode = "none"
			}
			var committed <-chan struct{}
			if point == "metadata-before-reply" {
				proxy, fired := onlineFaultProxy(t, source.DSN, faultCommitHeld)
				parsed, err := pgx.ParseConfig(proxy)
				require.NoError(t, err)
				cfg.Host = parsed.Host
				cfg.Port = int(parsed.Port)
				committed = fired
				mode = "none"
			}
			child, lines, _ := startOnlineChild(t, cfg, dest.DSN, mode)
			if point == "after-select" {
				waitHighBarrier(t, conn)
			} else if point == "metadata-before-reply" {
				select {
				case <-committed:
				case <-time.After(10 * time.Second):
					t.Fatal("committed reply barrier timeout")
				}
			} else {
				select {
				case line := <-lines:
					require.Equal(t, "ONLINE-BARRIER", line)
				case <-time.After(10 * time.Second):
					t.Fatal("child barrier timeout")
				}
			}
			var attempt string
			var checkpoint string
			var cursor *int64
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT attempt_id::text,j.source_lsn::text,p.cursor_key FROM cdc_online.windows w JOIN cdc_online.jobs j USING(job_id) JOIN cdc_online.table_progress p USING(job_id,table_schema,table_name) WHERE w.generation=1 ORDER BY w.low_end DESC LIMIT 1").Scan(&attempt, &checkpoint, &cursor))
			if point == "after-completed-window" || point == "metadata-before-reply" || point == "after-confirmed" {
				require.NotNil(t, cursor)
				require.Equal(t, int64(2), *cursor)
			} else {
				require.Nil(t, cursor)
			}
			if point == "after-confirmed" {
				var confirmed bool
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn >= $1::pg_lsn FROM pg_replication_slots WHERE slot_name='online_slot'", checkpoint).Scan(&confirmed))
				require.True(t, confirmed)
				var durable int
				require.NoError(t, target.Conn.QueryRow(context.Background(), "SELECT count(*) FROM online_projection WHERE NOT deleted AND row_key IN (1,2)").Scan(&durable))
				require.Equal(t, 2, durable)
			}
			var binding string
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT slot_binding::text FROM cdc_online.jobs").Scan(&binding))
			require.NoError(t, child.Process.Kill())
			require.Error(t, child.Wait()) // hard kill, no Close
			if point == "after-select" {
				onlineExec(t, conn, "SELECT pg_advisory_unlock(8791)")
			}
			// Slot consistency can wait on an open HIGH transaction. Start the
			// oracle only after releasing that fence, before every business write.
			oracle := startCDCOracle(t, source, cfg.Publication.Name, binding, false, "items")
			cfg = onlineConfig(t, source, "items")
			onlineExec(t, conn, "UPDATE items SET value='newer' WHERE id=1; DELETE FROM items WHERE id=2; INSERT INTO items VALUES(2,'reinsert')")
			r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
			waitOnlineCapture(t, r)
			waitOnlineSnapshot(t, r)
			waitOnlineFence(t, conn)
			r.Cancel()
			require.ErrorIs(t, onlineResult(t, r), context.Canceled)
			compareOnlineTable(t, conn, target, "items")
			oracle.assert(t, target, 3)
			var abandoned string
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT status FROM cdc_online.windows WHERE attempt_id=$1::uuid", attempt).Scan(&abandoned))
			if point == "metadata-before-reply" || point == "after-confirmed" {
				require.Equal(t, "completed", abandoned)
			} else {
				require.Equal(t, "abandoned", abandoned)
			}
			var generation int
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT generation FROM cdc_online.jobs").Scan(&generation))
			require.Equal(t, 2, generation)
			var oldReadCount int
			require.NoError(t, target.Conn.QueryRow(context.Background(), "SELECT count(*) FROM online_event_ledger").Scan(&oldReadCount))
			require.GreaterOrEqual(t, oldReadCount, 5)
		})
	}
	t.Run("lost-sink-reply", func(t *testing.T) {
		source := newOnlinePGFixture(t)
		dest := newOnlinePGFixture(t)
		conn := source.Connect(t)
		target := newOnlineTarget(t, dest)
		onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text); INSERT INTO items VALUES(1,'one'),(2,'two')")
		provisionOnline(t, conn, "items")
		cfg := onlineConfig(t, source, "items")
		r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetAfterCommit) })
		waitOnlineCapture(t, r)
		require.ErrorIs(t, onlineResult(t, r), errOnlineTargetFault)
		onlineExec(t, conn, "DELETE FROM items WHERE id=1")
		r = startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
		waitOnlineSnapshot(t, r)
		waitOnlineFence(t, conn)
		r.Cancel()
		require.ErrorIs(t, onlineResult(t, r), context.Canceled)
		compareOnlineTable(t, conn, target, "items")
	})
}

func testOnlineDelayedREAD(t *testing.T) {
	for _, mutation := range []string{"update", "delete", "delete-reinsert"} {
		t.Run(mutation, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			dest := newOnlinePGFixture(t)
			conn := source.Connect(t)
			target := newOnlineTarget(t, dest)
			lateTarget := &onlineTarget{Conn: dest.Connect(t)}
			onlineExec(t, conn, "CREATE TABLE delayed(id bigint PRIMARY KEY,value text); INSERT INTO delayed VALUES(1,'old'),(2,'stable')")
			provisionOnline(t, conn, "delayed")
			cfg := onlineConfig(t, source, "delayed")
			onlineExec(t, target.Conn, "CREATE TABLE online_pending_read(event jsonb NOT NULL)")
			first, lines, _ := startOnlineChild(t, cfg, dest.DSN, "delayed-request")
			select {
			case line := <-lines:
				require.Equal(t, "ONLINE-BARRIER", line)
			case <-time.After(10 * time.Second):
				t.Fatal("actual child READ request timeout")
			}
			var encoded string
			require.NoError(t, target.Conn.QueryRow(context.Background(), "SELECT event::text FROM online_pending_read").Scan(&encoded))
			var old online.Event
			decoder := json.NewDecoder(bytes.NewBufferString(encoded))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&old))
			require.Equal(t, online.Read, old.Operation)
			require.Equal(t, int64(1), old.Key)
			var binding string
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT slot_binding::text FROM cdc_online.jobs").Scan(&binding))
			oracle := startCDCOracle(t, source, cfg.Publication.Name, binding, false, "delayed")
			request, cancelRequest := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancelRequest()
			ready, release := make(chan struct{}), make(chan struct{})
			lateResult := make(chan error, 1)
			go func() { lateResult <- lateTarget.ApplyDelayed(request, old, ready, release) }()
			select {
			case <-ready:
			case <-request.Done():
				t.Fatal("old request did not reach independent target barrier")
			}
			require.NoError(t, first.Process.Kill())
			require.Error(t, first.Wait())
			count := 1
			switch mutation {
			case "update":
				onlineExec(t, conn, "UPDATE delayed SET value='newer' WHERE id=1")
			case "delete":
				onlineExec(t, conn, "DELETE FROM delayed WHERE id=1")
			case "delete-reinsert":
				onlineExec(t, conn, "DELETE FROM delayed WHERE id=1; INSERT INTO delayed VALUES(1,'reinsert')")
				count = 2
			}
			second := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
			waitOnlineSnapshot(t, second)
			waitOnlineFence(t, conn)
			second.Cancel()
			require.ErrorIs(t, onlineResult(t, second), context.Canceled)
			before, deleted, token := target.Image(t, old.Table, old.Key)
			var checkpoint string
			var cursor *int64
			var generation int64
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT j.source_lsn::text,p.cursor_key,j.generation FROM cdc_online.jobs j JOIN cdc_online.table_progress p USING(job_id)").Scan(&checkpoint, &cursor, &generation))
			require.Equal(t, int64(2), generation)
			close(release)
			require.NoError(t, <-lateResult)
			after, deletedAfter, tokenAfter := target.Image(t, old.Table, old.Key)
			require.Equal(t, before, after)
			require.Equal(t, deleted, deletedAfter)
			require.Equal(t, token, tokenAfter, "late READ changed newer token/tombstone")
			var checkpointAfter string
			var cursorAfter *int64
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT j.source_lsn::text,p.cursor_key FROM cdc_online.jobs j JOIN cdc_online.table_progress p USING(job_id)").Scan(&checkpointAfter, &cursorAfter))
			require.Equal(t, checkpoint, checkpointAfter)
			require.Equal(t, cursor, cursorAfter)
			compareOnlineTable(t, conn, target, "delayed")
			oracle.assert(t, target, count)
		})
	}
}
