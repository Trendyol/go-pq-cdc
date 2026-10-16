package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialCapacity(t *testing.T) {
	if os.Getenv("CDC_ONLINE_CAPACITY") != "1" {
		t.Skip("opt in with CDC_ONLINE_CAPACITY=1; explicit opt-in never skips")
	}
	t.Run("live-full-queue-feedback-cancellation", testOnlineLiveQueue)
	for _, strategy := range []config.SnapshotStrategy{config.SnapshotStrategyConsistent, config.SnapshotStrategyOnline} {
		t.Run(string(strategy), func(t *testing.T) { runOnlineCapacity(t, strategy) })
	}
}
func testOnlineLiveQueue(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE saturated(id bigint PRIMARY KEY,value text); INSERT INTO saturated VALUES(1,'one')")
	provisionOnline(t, conn, "saturated")
	cfg := onlineConfig(t, source, "saturated")
	cfg.Snapshot.Online.TransactionBytes = 4096
	cfg.Snapshot.Online.QueueBytes = 8192
	cfg.Snapshot.Online.DeliveryTimeout = 10 * time.Second
	parked := make(chan struct{})
	var once sync.Once
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		once.Do(func() { close(parked) })
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("sink stall")
	}
	for i := 2; i < 25; i++ {
		onlineExec(t, conn, "INSERT INTO saturated VALUES($1,repeat('x',1024))", i)
	}
	onlineEventually(t, func() bool {
		return r.Connector.Stats().BufferedEncodedBytes > cfg.Snapshot.Online.QueueBytes-cfg.Snapshot.Online.TransactionBytes
	}, 5*time.Second, 10*time.Millisecond, "reserved transaction cannot fit: full queue receive pause")
	before := r.Connector.Stats()
	time.Sleep(2200 * time.Millisecond)
	after := r.Connector.Stats()
	require.GreaterOrEqual(t, after.FeedbackWrites, before.FeedbackWrites+2)
	require.Equal(t, before.SafeACK, after.SafeACK, "stalled sink advanced ACK")
	require.LessOrEqual(t, after.PeakQueueAndAssemblyEncodedBytes, int64(8192))
	started := time.Now()
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	require.Less(t, time.Since(started), 5*time.Second)
}
func runOnlineCapacity(t *testing.T, strategy config.SnapshotStrategy) {
	runtime.GC()
	source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	writer := source.Connect(t)
	target := newOnlineTarget(t, dest)
	targetProbe := dest.Connect(t)
	onlineExec(t, conn, `CREATE EXTENSION pg_stat_statements; CREATE TABLE workload(id bigint PRIMARY KEY,value bigint,payload text); INSERT INTO workload SELECT i,0,repeat(md5(i::text),32) FROM generate_series(1,10000) i`)
	provisionOnline(t, conn, "workload")
	cfg := onlineConfig(t, source, "workload")
	cfg.Snapshot.Strategy = strategy
	cfg.Snapshot.ChunkSize = 100
	cfg.Snapshot.Online.DeliveryTimeout = 10 * time.Second
	onlineExec(t, conn, "CREATE PUBLICATION online_pub FOR TABLE workload WITH (publish='insert,update,delete,truncate')")
	benchmarkCtx, stopBenchmark := context.WithCancel(context.Background())
	defer stopBenchmark()
	var delivered, targetPeak atomic.Int64
	var legacyMu sync.Mutex
	var lastXID uint32
	var ordinal uint32
	var legacyChunk, legacyPeak int64
	var legacyReads int
	snapshotStarted := make(chan struct{})
	var snapshotOnce sync.Once
	sinkErrors := make(chan error, 1)
	apply := func(ctx context.Context, event online.Event) error {
		start := time.Now()
		err := target.Apply(ctx, event, onlineTargetOK)
		elapsed := time.Since(start).Microseconds()
		for old := targetPeak.Load(); elapsed > old && !targetPeak.CompareAndSwap(old, elapsed); old = targetPeak.Load() {
		}
		if err == nil {
			delivered.Add(1)
		}
		return err
	}
	binding := "legacy-capacity-binding"
	var r onlineRun
	var legacy cdc.Connector
	if strategy == config.SnapshotStrategyOnline {
		require.NoError(t, online.EnsureWatermarkPublication(context.Background(), conn, cfg.Publication.Name, true))
		r = startOnline(t, cfg, apply)
		waitOnlineCapture(t, r)
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT slot_binding::text FROM cdc_online.jobs").Scan(&binding))
	} else {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Metric.Port = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		legacy, err = cdc.NewConnector(context.Background(), cfg, func(l *replication.ListenerContext) {
			legacyMu.Lock()
			defer legacyMu.Unlock()
			event := online.Event{Table: online.Table{Schema: "public", Name: "workload"}, CommitLSN: l.CommitLSN}
			var image map[string]any
			switch m := l.Message.(type) {
			case *format.Snapshot:
				if m.EventType != format.SnapshotEventTypeData {
					return
				}
				snapshotOnce.Do(func() { close(snapshotStarted) })
				event.Operation = online.Read
				image = m.Data
				event.CommitLSN = 0
				event.Order = online.OrderToken{EndLSN: 1, Phase: 1}
			case *format.Insert:
				event.Operation = online.Insert
				image = m.Decoded
			case *format.Update:
				event.Operation = online.Update
				image = m.NewDecoded
			case *format.Delete:
				event.Operation = online.Delete
				image = m.OldDecoded
			default:
				if l.Ack != nil {
					_ = l.Ack()
				}
				return
			}
			key, err := online.IntegerKey(image, "id")
			if err != nil {
				select {
				case sinkErrors <- err:
				default:
				}
				return
			}
			event.Key = key
			if event.Operation != online.Delete {
				event.Image = image
			}
			if event.Operation == online.Read {
				event.ID = online.READEventID("legacy-job", "legacy-attempt", event.Table, key)
				legacyChunk += int64(len(image["payload"].(string)) + len(fmt.Sprint(image["id"])) + len(fmt.Sprint(image["value"])))
				legacyReads++
				if legacyReads%100 == 0 {
					legacyPeak = max(legacyPeak, legacyChunk)
					legacyChunk = 0
				}
			} else {
				if lastXID != l.Xid {
					lastXID = l.Xid
					ordinal = 0
				}
				event.Order = online.OrderToken{EndLSN: l.CommitLSN, Ordinal: ordinal}
				event.ID = online.CDCEventID(binding, l.CommitLSN, ordinal)
				ordinal++
			}
			// Legacy snapshot ListenerContext intentionally has no Context. Use
			// the fixture's cancellable lifetime, without changing legacy APIs.
			parent := l.Context
			if parent == nil {
				parent = benchmarkCtx
			}
			call, cancel := context.WithTimeout(parent, 10*time.Second)
			err = apply(call, event)
			cancel()
			if err != nil {
				select {
				case sinkErrors <- err:
				default:
				}
				return
			}
			if l.Ack != nil {
				_ = l.Ack()
			}
		})
		require.NoError(t, err)
		defer func() { stopBenchmark(); legacy.Close() }()
	}
	oracle := startCDCOracle(t, source, cfg.Publication.Name, binding, strategy == config.SnapshotStrategyConsistent, "workload")
	if legacy != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go legacy.Start(ctx)
		// The consistent strategy retains WAL in an inactive slot while it
		// reads its exported T0 snapshot; active CopyBoth starts only afterward.
		// First READ proves T0 was captured, so all subsequent writes are CDC.
		select {
		case <-snapshotStarted:
		case <-time.After(15 * time.Second):
			t.Fatal("legacy exported snapshot READ readiness")
		}
	}
	var writes atomic.Int64
	stopWrites := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second / 120)
		defer ticker.Stop()
		for {
			select {
			case <-stopWrites:
				writeDone <- nil
				return
			case <-ticker.C:
				n := writes.Load() + 1
				call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := writer.Exec(call, "UPDATE workload SET value=$1 WHERE id=$2", n, (n%10000)+1)
				cancel()
				if err != nil {
					writeDone <- err
					return
				}
				writes.Add(1)
			}
		}
	}()
	var stopOnce sync.Once
	t.Cleanup(func() { stopOnce.Do(func() { close(stopWrites) }) })
	started := time.Now()
	var peakHeap uint64
	var peakRSS, peakWAL, peakLag int64
	var peakFeedback float64
	progress := false
	sample := func() {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		peakHeap = max(peakHeap, memory.HeapAlloc)
		rss, err := processRSS()
		require.NoError(t, err)
		peakRSS = max(peakRSS, rss)
		var wal, lag int64
		var age *float64
		require.NoError(t, conn.QueryRow(context.Background(), `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint,pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name=$1`, cfg.Slot.Name).Scan(&wal, &lag))
		peakWAL = max(peakWAL, wal)
		peakLag = max(peakLag, lag)
		require.Less(t, wal, int64(128<<20), "WAL safety budget")
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT max(extract(epoch FROM clock_timestamp()-reply_time))::float8 FROM pg_stat_replication WHERE pid=(SELECT active_pid FROM pg_replication_slots WHERE slot_name=$1)", cfg.Slot.Name).Scan(&age))
		if age != nil {
			peakFeedback = max(peakFeedback, *age)
		}
		if strategy == config.SnapshotStrategyOnline {
			var qualified bool
			require.NoError(t, conn.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM cdc_online.windows WHERE status='completed')>=2 AND j.status='active' AND s.confirmed_flush_lsn>(SELECT min(low_end) FROM cdc_online.windows WHERE status='completed') FROM cdc_online.jobs j JOIN pg_replication_slots s ON s.slot_name=j.slot_name`).Scan(&qualified))
			progress = progress || qualified
			require.NoError(t, r.Connector.Err())
		}
		select {
		case err := <-sinkErrors:
			t.Fatal(err)
		default:
		}
	}
	for time.Since(started) < 60*time.Second {
		time.Sleep(500 * time.Millisecond)
		sample()
	}
	duration := time.Since(started)
	stopOnce.Do(func() { close(stopWrites) })
	require.NoError(t, <-writeDone)
	rate := float64(writes.Load()) / duration.Seconds()
	require.GreaterOrEqual(t, rate, 100.0, "qualifying achieved source rate")
	// A final business fence is captured by the independent CDC oracle as well.
	onlineExec(t, conn, "UPDATE workload SET value=$1 WHERE id=1", -writes.Load()-1)
	drainStart := time.Now()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		sample()
		var count int
		require.NoError(t, targetProbe.QueryRow(context.Background(), "SELECT count(*) FROM online_event_ledger WHERE event->>'Operation'<>'READ'").Scan(&count))
		var rows int
		require.NoError(t, targetProbe.QueryRow(context.Background(), "SELECT count(*) FROM online_projection WHERE NOT deleted").Scan(&rows))
		if count == int(writes.Load()+1) && rows == 10000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capacity backlog drain deadline")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if strategy == config.SnapshotStrategyOnline {
		waitOnlineSnapshot(t, r)
		waitOnlineFence(t, conn)
		r.Cancel()
		require.ErrorIs(t, onlineResult(t, r), context.Canceled)
		require.True(t, progress, "two windows + confirmation before snapshot complete under writes")
	} else {
		legacy.Close()
	}
	compareOnlineTable(t, conn, target, "workload")
	oracle.assert(t, target, int(writes.Load()+1))
	oracle.cancel()
	require.NoError(t, oracle.reader.Close(context.Background()))
	<-oracle.done
	oracle.mu.Lock()
	oracle.events = nil
	oracle.mu.Unlock()
	var readMS float64
	var readQueries int64
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT COALESCE(max(max_exec_time),0),COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query ILIKE 'SELECT%workload%WHERE%' AND query NOT ILIKE '%to_jsonb%' AND query NOT ILIKE '%count(%' AND query NOT ILIKE '%max(%' AND query NOT ILIKE '%min(%'`).Scan(&readMS, &readQueries))
	require.Greater(t, readQueries, int64(0))
	require.Greater(t, readMS, 0.0)
	stats := online.Stats{}
	if strategy == config.SnapshotStrategyOnline {
		stats = r.Connector.Stats()
		require.Greater(t, stats.PeakChunkRawBytes, int64(0))
		require.LessOrEqual(t, stats.PeakChunkRawBytes, cfg.Snapshot.Online.ChunkBytes)
		require.Greater(t, stats.PeakTransactionEncodedBytes, int64(0))
		require.LessOrEqual(t, stats.PeakTransactionEncodedBytes, cfg.Snapshot.Online.TransactionBytes)
		require.Greater(t, stats.PeakQueueAndAssemblyEncodedBytes, int64(0))
		require.LessOrEqual(t, stats.PeakQueueAndAssemblyEncodedBytes, cfg.Snapshot.Online.QueueBytes)
		require.Greater(t, stats.ReadQueries, int64(1))
		require.Greater(t, stats.PeakReadLatency, time.Duration(0))
	}
	report := map[string]any{"strategy": strategy, "seed": 20261014, "rows": 10000, "payloadBytes": 1024, "chunkRows": 100, "durationSeconds": duration.Seconds(), "sourceTransactionsPerSecond": rate, "writes": writes.Load(), "deliveries": delivered.Load(), "peakWholeProcessHeapBytes": peakHeap, "peakWholeProcessRSSBytes": peakRSS, "peakMainSlotRetainedWALBytes": peakWAL, "peakConfirmedLagBytes": peakLag, "peakFeedbackAgeSeconds": peakFeedback, "peakTargetMicroseconds": targetPeak.Load(), "maxSourceReadExecutionMS": readMS, "sourceReadQueries": readQueries, "drainSeconds": time.Since(drainStart).Seconds(), "onlineStats": stats, "legacyDurableReadBatchRawBytes": legacyPeak, "sourceContainer": source.Container.GetContainerID(), "targetContainer": dest.Container.GetContainerID()}
	data, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile("../specs/verifications/e01-evidence/capacity-current-"+string(strategy)+".json", append(data, '\n'), 0600))
	t.Log("CAPACITY " + string(data))
}
func processRSS() (int64, error) {
	output, err := exec.Command("/bin/ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	return n * 1024, err
}
