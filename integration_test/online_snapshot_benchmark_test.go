package integration

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialSnapshot10MComparison(t *testing.T) {
	if os.Getenv("CDC_ONLINE_10M_BENCH") != "1" {
		t.Skip("opt in with CDC_ONLINE_10M_BENCH=1 to run the 10m-row online-vs-legacy snapshot comparison")
	}
	const rows int64 = 10_000_000
	const chunkRows = 5000
	for _, strategy := range []config.SnapshotStrategy{config.SnapshotStrategyConsistent, config.SnapshotStrategyOnline} {
		t.Run(string(strategy), func(t *testing.T) { runOnlineSnapshot10MComparison(t, strategy, rows, chunkRows) })
	}
}

func runOnlineSnapshot10MComparison(t *testing.T, strategy config.SnapshotStrategy, rows int64, chunkRows int) {
	runtime.GC()
	source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
	conn := source.Connect(t)
	writer := source.Connect(t)
	onlineExec(t, conn, "CREATE EXTENSION pg_stat_statements")
	onlineExec(t, conn, "CREATE TABLE workload(id bigint PRIMARY KEY,value bigint)")
	onlineExec(t, conn, "INSERT INTO workload SELECT i, i * 2 FROM generate_series(1,$1) i", rows)
	provisionOnline(t, conn, "workload")
	cfg := onlineConfig(t, source, "workload")
	cfg.Snapshot.Strategy = strategy
	cfg.Snapshot.ChunkSize = int64(chunkRows)
	cfg.Snapshot.Online.QueryTimeout = 2 * time.Minute
	cfg.Snapshot.Online.DeliveryTimeout = 2 * time.Minute
	cfg.Snapshot.Online.ChunkBytes = 8 << 20
	cfg.Snapshot.Online.TransactionBytes = 16 << 20
	cfg.Snapshot.Online.QueueBytes = 32 << 20
	onlineExec(t, conn, "CREATE PUBLICATION online_pub FOR TABLE workload WITH (publish='insert,update,delete,truncate')")

	var readRows atomic.Int64
	var readChecksum atomic.Int64
	var cdcRows atomic.Int64
	var cdcChecksum atomic.Int64
	var peakHeap uint64
	var peakRSS, peakWAL, peakLag int64
	var peakFeedbackAgeSeconds float64
	sample := func() {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		peakHeap = max(peakHeap, memory.HeapAlloc)
		rss, err := processRSS()
		require.NoError(t, err)
		peakRSS = max(peakRSS, rss)
		var wal, lag int64
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)),0)::bigint,COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)),0)::bigint FROM pg_replication_slots WHERE slot_name=$1`, cfg.Slot.Name).Scan(&wal, &lag))
		peakWAL = max(peakWAL, wal)
		peakLag = max(peakLag, lag)
		var feedbackAge float64
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(max(extract(epoch FROM clock_timestamp()-reply_time)),0)::float8 FROM pg_stat_replication WHERE pid=(SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1)`, cfg.Slot.Name).Scan(&feedbackAge))
		peakFeedbackAgeSeconds = max(peakFeedbackAgeSeconds, feedbackAge)
	}

	const requestedUpdatesPerSecond = 120
	const updateDuration = 60 * time.Second
	var writes atomic.Int64
	writeDone := make(chan error, 1)
	startWrites := func() time.Time {
		started := time.Now()
		go func() {
			ticker := time.NewTicker(time.Second / requestedUpdatesPerSecond)
			defer ticker.Stop()
			deadline := time.NewTimer(updateDuration)
			defer deadline.Stop()
			for {
				select {
				case <-deadline.C:
					writeDone <- nil
					return
				case <-ticker.C:
					n := writes.Load() + 1
					call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, err := writer.Exec(call, "UPDATE workload SET value=$1 WHERE id=$2", n, (n%rows)+1)
					cancel()
					if err != nil {
						writeDone <- err
						return
					}
					writes.Add(1)
				}
			}
		}()
		return started
	}

	var resultErr error
	var stats online.Stats
	var legacy cdc.Connector
	var writeStarted time.Time
	snapshotStarted := make(chan struct{})
	var snapshotOnce sync.Once
	noteSnapshotStarted := func() { snapshotOnce.Do(func() { close(snapshotStarted) }) }
	started := time.Now()

	if strategy == config.SnapshotStrategyOnline {
		require.NoError(t, online.EnsureWatermarkPublication(context.Background(), conn, cfg.Publication.Name, true))
		r := startOnline(t, cfg, func(ctx context.Context, event online.Event) error {
			if event.Table == (online.Table{Schema: "public", Name: "workload"}) && event.Operation == online.Read {
				readRows.Add(1)
				readChecksum.Add(event.Key)
			} else if event.Table == (online.Table{Schema: "public", Name: "workload"}) {
				cdcRows.Add(1)
				cdcChecksum.Add(event.Key)
			}

			return nil
		})
		waitOnlineCapture(t, r)
		writeStarted = startWrites()
		for deadline := time.Now().Add(5 * time.Minute); ; {
			sample()
			require.NoError(t, r.Connector.Err())
			var complete bool
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM cdc_online.jobs WHERE status='snapshot_complete')").Scan(&complete))
			if complete {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("online snapshot completion deadline")
			}
			time.Sleep(500 * time.Millisecond)
		}
		snapshotDuration := time.Since(started)
		require.NoError(t, <-writeDone)
		drainStart := time.Now()
		for deadline := time.Now().Add(3 * time.Minute); cdcRows.Load() < writes.Load(); {
			sample()
			require.NoError(t, r.Connector.Err())
			if time.Now().After(deadline) {
				t.Fatalf("CDC drain deadline: delivered %d/%d updates", cdcRows.Load(), writes.Load())
			}
			time.Sleep(500 * time.Millisecond)
		}
		waitOnlineSnapshot(t, r)
		stats = r.Connector.Stats()
		r.Cancel()
		resultErr = onlineResult(t, r)
		require.ErrorIs(t, resultErr, context.Canceled)
		writeBenchmarkReport(t, conn, source, strategy, rows, chunkRows, started, snapshotDuration, writeStarted, time.Since(drainStart), &readRows, &readChecksum, &cdcRows, &cdcChecksum, &writes, peakHeap, peakRSS, peakWAL, peakLag, peakFeedbackAgeSeconds, stats)
		return
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg.Metric.Port = listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	done := make(chan error, 1)
	legacy, err = cdc.NewConnector(context.Background(), cfg, func(l *replication.ListenerContext) {
		switch message := l.Message.(type) {
		case *format.Snapshot:
			switch message.EventType {
			case format.SnapshotEventTypeData:
				noteSnapshotStarted()
				key, err := online.IntegerKey(message.Data, "id")
				if err != nil {
					done <- err
					return
				}
				readRows.Add(1)
				readChecksum.Add(key)
			case format.SnapshotEventTypeEnd:
				select {
				case done <- nil:
				default:
				}
			}
		case *format.Insert:
			cdcRows.Add(1)
			key, err := online.IntegerKey(message.Decoded, "id")
			if err == nil {
				cdcChecksum.Add(key)
			}
		case *format.Update:
			cdcRows.Add(1)
			key, err := online.IntegerKey(message.NewDecoded, "id")
			if err == nil {
				cdcChecksum.Add(key)
			}
		case *format.Delete:
			cdcRows.Add(1)
			key, err := online.IntegerKey(message.OldDecoded, "id")
			if err == nil {
				cdcChecksum.Add(key)
			}
		}
		if l.Ack != nil {
			_ = l.Ack()
		}
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go legacy.Start(ctx)
	select {
	case <-snapshotStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("legacy exported snapshot READ readiness")
	}
	writeStarted = startWrites()
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			goto legacySnapshotComplete
		default:
		}
		sample()
		time.Sleep(500 * time.Millisecond)
	}
legacySnapshotComplete:
	snapshotDuration := time.Since(started)
	require.NoError(t, <-writeDone)
	for readRows.Load() < rows {
		sample()
		time.Sleep(500 * time.Millisecond)
	}
	drainStart := time.Now()
	for deadline := time.Now().Add(3 * time.Minute); cdcRows.Load() < writes.Load(); {
		sample()
		if time.Now().After(deadline) {
			t.Fatalf("CDC drain deadline: delivered %d/%d updates", cdcRows.Load(), writes.Load())
		}
		time.Sleep(500 * time.Millisecond)
	}
	legacy.Close()
	writeBenchmarkReport(t, conn, source, strategy, rows, chunkRows, started, snapshotDuration, writeStarted, time.Since(drainStart), &readRows, &readChecksum, &cdcRows, &cdcChecksum, &writes, peakHeap, peakRSS, peakWAL, peakLag, peakFeedbackAgeSeconds, stats)
}

func writeBenchmarkReport(t *testing.T, conn anyQueryRow, source onlinePGFixture, strategy config.SnapshotStrategy, rows int64, chunkRows int, started time.Time, snapshotDuration time.Duration, writeStarted time.Time, drainDuration time.Duration, readRows, readChecksum, cdcRows, cdcChecksum, writes *atomic.Int64, peakHeap uint64, peakRSS, peakWAL, peakLag int64, peakFeedbackAgeSeconds float64, stats online.Stats) {
	t.Helper()
	sampleDuration := time.Since(started)
	if writes.Load() == 0 || strategy == config.SnapshotStrategyConsistent {
		require.Equal(t, rows, readRows.Load())
		require.Equal(t, rows*(rows+1)/2, readChecksum.Load())
	}
	require.LessOrEqual(t, readRows.Load(), rows)
	require.GreaterOrEqual(t, cdcRows.Load(), writes.Load(), "all steady-rate updates should drain")
	require.GreaterOrEqual(t, readRows.Load()+cdcRows.Load(), rows, "reads plus CDC updates should cover the table under the unique-key update workload")
	var readMS float64
	var readQueries int64
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(max(max_exec_time),0),COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query ILIKE 'SELECT%workload%' AND query NOT ILIKE '%to_jsonb%' AND query NOT ILIKE '%count(%' AND query NOT ILIKE '%max(%' AND query NOT ILIKE '%min(%'`).Scan(&readMS, &readQueries))
	writeDuration := snapshotDuration.Seconds()
	if !writeStarted.IsZero() && writes.Load() > 0 {
		writeDuration = 60
	}
	report := map[string]any{"strategy": strategy, "rows": rows, "chunkRows": chunkRows, "durationSeconds": sampleDuration.Seconds(), "snapshotDurationSeconds": snapshotDuration.Seconds(), "drainSeconds": drainDuration.Seconds(), "snapshotRows": readRows.Load(), "keyChecksum": readChecksum.Load(), "cdcKeyChecksum": cdcChecksum.Load(), "rowsPerSecond": float64(readRows.Load()) / snapshotDuration.Seconds(), "requestedUpdatesPerSecond": 120, "requestedUpdateDurationSeconds": 60, "sourceTransactionsPerSecond": float64(writes.Load()) / writeDuration, "writes": writes.Load(), "cdcDeliveries": cdcRows.Load(), "peakWholeProcessHeapBytes": peakHeap, "peakWholeProcessRSSBytes": peakRSS, "peakMainSlotRetainedWALBytes": peakWAL, "peakConfirmedLagBytes": peakLag, "peakFeedbackAgeSeconds": peakFeedbackAgeSeconds, "maxSourceReadExecutionMS": readMS, "sourceReadQueries": readQueries, "onlineStats": stats, "sourceContainer": source.Container.GetContainerID()}
	data, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll("../specs/verifications/online-10m-snapshot-benchmark/evidence", 0o755))
	require.NoError(t, os.WriteFile("../specs/verifications/online-10m-snapshot-benchmark/evidence/"+string(strategy)+".json", append(data, '\n'), 0o600))
	t.Log("SNAPSHOT_10M " + string(data))
}

type anyQueryRow interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
