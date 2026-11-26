package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

// TestOnlineInitialSnapshot10MHeadToHead compares legacy consistent and online
// snapshot strategies under a fair end condition. Both connectors consume the
// SAME transaction stream from one shared source concurrently, each with its
// own slot and publication. The workload is a fixed-rate steady update stream
// that ends with one sentinel transaction. Delivery is ordered per relation,
// so an arm that delivers the sentinel has delivered everything before it;
// each arm stops exactly at that same logical CDC message. The writer records
// WAL flush LSNs before and after the sentinel commit and both arms must
// report a sentinel commit LSN inside that window, proving the barrier is the
// same physical transaction for both connectors.
func TestOnlineInitialSnapshot10MHeadToHead(t *testing.T) {
	if os.Getenv("CDC_ONLINE_10M_BENCH") != "1" {
		t.Skip("opt in with CDC_ONLINE_10M_BENCH=1 to run the head-to-head sentinel-barrier benchmark")
	}
	rows := int64(10_000_000)
	if override := os.Getenv("CDC_H2H_ROWS"); override != "" {
		parsed, err := strconv.ParseInt(override, 10, 64)
		require.NoError(t, err)
		rows = parsed
	}
	const chunkRows = 5000
	const requestedUpdatesPerSecond = 1000
	// Timer-driven sentinel: writers push at the steady rate for a fixed
	// duration T, then commit one sentinel transaction that stops both arms.
	// Timing budget from measurements on this machine: legacy snapshot lands
	// in seconds, so with T=5m legacy consumes CDC live for ~T. Online drains
	// this input at roughly 1/3 of 1000/s, so its snapshot completes at ~3T,
	// after writers stop — its entire CDC backlog is consumed as part of the
	// snapshot, leaving only the sentinel as post-snapshot CDC. The deadline
	// 3T + 30m covers the online drain plus the sentinel.
	updateDuration := 5 * time.Minute
	if override := os.Getenv("CDC_H2H_SECONDS"); override != "" {
		parsed, err := strconv.ParseInt(override, 10, 64)
		require.NoError(t, err)
		updateDuration = time.Duration(parsed) * time.Second
	}
	sentinelID := rows
	const sentinelValue int64 = 9_000_000_007
	workloadTable := online.Table{Schema: "public", Name: "workload"}

	runtime.GC()
	source := newOnlinePGFixtureDurability(t, true, "shared_preload_libraries=pg_stat_statements")
	setup := source.Connect(t)
	writer := source.Connect(t)
	sampleConn := source.Connect(t)
	watchConn := source.Connect(t)
	onlineExec(t, setup, "CREATE EXTENSION pg_stat_statements")
	onlineExec(t, setup, "CREATE TABLE workload(id bigint PRIMARY KEY,value bigint)")
	onlineExec(t, setup, "INSERT INTO workload SELECT i, i * 2 FROM generate_series(1,$1) i", rows)
	provisionOnline(t, setup, "workload")

	legacyCfg := onlineConfig(t, source, "workload")
	legacyCfg.Snapshot.Strategy = config.SnapshotStrategyConsistent
	legacyCfg.Snapshot.ChunkSize = int64(chunkRows)
	legacyCfg.Slot.Name = "bench_legacy_slot"
	legacyCfg.Publication.Name = "bench_legacy_pub"
	legacyCfg.Metric.Port = h2hFreePort(t)

	onlineCfg := onlineConfig(t, source, "workload")
	onlineCfg.Snapshot.Strategy = config.SnapshotStrategyOnline
	onlineCfg.Snapshot.ChunkSize = int64(chunkRows)
	onlineCfg.Snapshot.Online.QueryTimeout = 2 * time.Minute
	onlineCfg.Snapshot.Online.DeliveryTimeout = 2 * time.Minute
	onlineCfg.Snapshot.Online.ChunkBytes = 8 << 20
	onlineCfg.Snapshot.Online.TransactionBytes = 16 << 20
	onlineCfg.Snapshot.Online.QueueBytes = 32 << 20
	onlineCfg.Slot.Name = "bench_online_slot"
	onlineCfg.Publication.Name = "bench_online_pub"
	onlineCfg.Metric.Port = h2hFreePort(t)
	onlineExec(t, setup, "CREATE PUBLICATION bench_online_pub FOR TABLE workload WITH (publish='insert,update,delete,truncate')")
	require.NoError(t, online.EnsureWatermarkPublication(context.Background(), setup, onlineCfg.Publication.Name, true))

	legacyState := new(h2hArm)
	onlineState := new(h2hArm)
	var legacyFatal atomic.Value

	legacyReady := make(chan struct{})
	var legacyOnce sync.Once
	noteLegacyReady := func() { legacyOnce.Do(func() { close(legacyReady) }) }

	legacy, err := cdc.NewConnector(context.Background(), legacyCfg, func(l *replication.ListenerContext) {
		switch message := l.Message.(type) {
		case *format.Snapshot:
			switch message.EventType {
			case format.SnapshotEventTypeData:
				noteLegacyReady()
				key, keyErr := online.IntegerKey(message.Data, "id")
				if keyErr != nil {
					legacyState.noteError(keyErr)
					return
				}
				legacyState.reads.Add(1)
				legacyState.readChecksum.Add(key)
			case format.SnapshotEventTypeEnd:
				legacyState.noteSnapshotComplete()
			}
		case *format.Insert:
			legacyState.cdcDelivered.Add(1)
			legacyState.noteError(fmt.Errorf("unexpected insert on workload table"))
		case *format.Update:
			legacyState.cdcDelivered.Add(1)
			key, keyErr := online.IntegerKey(message.NewDecoded, "id")
			value, valueErr := online.IntegerKey(message.NewDecoded, "value")
			if keyErr != nil || valueErr != nil {
				legacyState.noteError(keyErr)
				legacyState.noteError(valueErr)
				return
			}
			if key == sentinelID && value == sentinelValue {
				legacyState.sentinels.Add(1)
				legacyState.sentinelCommitLSN.Store(uint64(l.CommitLSN))
			}
		case *format.Delete:
			legacyState.cdcDelivered.Add(1)
		}
		if l.Ack != nil {
			_ = l.Ack()
		}
	})
	require.NoError(t, err)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				legacyFatal.Store(fmt.Sprint(recovered))
			}
		}()
		legacy.Start(context.Background())
	}()

	onlineRun := startOnline(t, onlineCfg, func(ctx context.Context, event online.Event) error {
		if event.Table != workloadTable {
			return nil
		}
		if event.Operation == online.Read {
			onlineState.reads.Add(1)
			onlineState.readChecksum.Add(event.Key)
			return nil
		}
		onlineState.cdcDelivered.Add(1)
		if event.Key == sentinelID {
			value, valueErr := online.IntegerKey(event.Image, "value")
			if valueErr != nil {
				onlineState.noteError(valueErr)
				return nil
			}
			if value == sentinelValue {
				onlineState.sentinels.Add(1)
				onlineState.sentinelCommitLSN.Store(uint64(event.CommitLSN))
			}
		}
		return nil
	})
	waitOnlineCapture(t, onlineRun)
	select {
	case <-legacyReady:
	case <-time.After(60 * time.Second):
		t.Fatal("legacy connector exported-snapshot readiness")
	}

	type h2hWrites struct {
		count      int64
		preLSN     pq.LSN
		postLSN    pq.LSN
		sentinelAt time.Time
		err        error
	}
	writeDone := make(chan h2hWrites, 1)
	var writesPreview atomic.Int64
	writersStart := time.Now()
	go func() {
		updateCount := int64(requestedUpdatesPerSecond) * int64(updateDuration/time.Second)
		interval := time.Second / requestedUpdatesPerSecond
		var writes int64
		for seq := int64(1); seq <= updateCount; seq++ {
			if sleep := time.Until(writersStart.Add(time.Duration(seq-1) * interval)); sleep > 0 {
				time.Sleep(sleep)
			}
			call, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, execErr := writer.Exec(call, "UPDATE workload SET value=$1 WHERE id=$2", seq, (seq%rows)+1)
			cancel()
			if execErr != nil {
				writeDone <- h2hWrites{count: writes, err: execErr}
				return
			}
			writes = seq
			writesPreview.Store(seq)
		}
		var pre, post string
		if queryErr := writer.QueryRow(context.Background(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&pre); queryErr != nil {
			writeDone <- h2hWrites{count: writes, err: queryErr}
			return
		}
		if _, execErr := writer.Exec(context.Background(), "UPDATE workload SET value=$1 WHERE id=$2", sentinelValue, sentinelID); execErr != nil {
			writeDone <- h2hWrites{count: writes, err: execErr}
			return
		}
		if queryErr := writer.QueryRow(context.Background(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&post); queryErr != nil {
			writeDone <- h2hWrites{count: writes, err: queryErr}
			return
		}
		preLSN, preErr := pq.ParseLSN(pre)
		postLSN, postErr := pq.ParseLSN(post)
		if preErr != nil {
			writeDone <- h2hWrites{count: writes, err: preErr}
			return
		}
		if postErr != nil {
			writeDone <- h2hWrites{count: writes, err: postErr}
			return
		}
		writeDone <- h2hWrites{count: writes, preLSN: preLSN, postLSN: postLSN, sentinelAt: time.Now()}
	}()

	legacyStop := make(chan time.Time, 1)
	onlineStop := make(chan time.Time, 1)
	go func() {
		for {
			if legacyState.snapshotDone.Load() && legacyState.sentinels.Load() > 0 {
				legacyStop <- time.Now()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for {
			call, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var complete bool
			queryErr := watchConn.QueryRow(call, "SELECT EXISTS(SELECT 1 FROM cdc_online.jobs WHERE status='snapshot_complete')").Scan(&complete)
			cancel()
			if queryErr != nil {
				onlineState.noteError(queryErr)
				time.Sleep(1 * time.Second)
				continue
			}
			if complete {
				onlineState.noteSnapshotComplete()
				if onlineState.sentinels.Load() > 0 {
					onlineStop <- time.Now()
					return
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	type h2hSlot struct {
		walBytes        int64
		confirmedLag    int64
		feedbackAgeSecs float64
	}
	var legacySlot, onlineSlot h2hSlot
	var peakHeap uint64
	var peakRSS int64
	sample := func() {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		peakHeap = max(peakHeap, memory.HeapAlloc)
		rss, rssErr := processRSS()
		require.NoError(t, rssErr)
		peakRSS = max(peakRSS, rss)
		for _, item := range []struct {
			slotName string
			observed *h2hSlot
		}{{legacyCfg.Slot.Name, &legacySlot}, {onlineCfg.Slot.Name, &onlineSlot}} {
			var wal, lag int64
			var age float64
			call, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			queryErr := sampleConn.QueryRow(call, `SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn),0)::bigint,COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),s.confirmed_flush_lsn),0)::bigint,COALESCE(extract(epoch FROM clock_timestamp()-r.reply_time),0)::float8 FROM pg_replication_slots s LEFT JOIN pg_stat_replication r ON r.pid=s.active_pid WHERE s.slot_name=$1`, item.slotName).Scan(&wal, &lag, &age)
			cancel()
			if queryErr != nil {
				require.NoError(t, queryErr)
			}
			item.observed.walBytes = max(item.observed.walBytes, wal)
			item.observed.confirmedLag = max(item.observed.confirmedLag, lag)
			item.observed.feedbackAgeSecs = max(item.observed.feedbackAgeSecs, age)
		}
	}

	var legacyDoneAt, onlineDoneAt time.Time
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	deadline := time.After(3*updateDuration + 30*time.Minute)
	lastProgress := time.Now()
	for legacyDoneAt.IsZero() || onlineDoneAt.IsZero() {
		select {
		case legacyDoneAt = <-legacyStop:
			legacy.Close()
			t.Logf("legacy arm stopped at sentinel after %v", time.Since(writersStart))
		case onlineDoneAt = <-onlineStop:
			t.Logf("online arm stopped at sentinel after %v", time.Since(writersStart))
		case <-ticker.C:
			sample()
			require.NoError(t, onlineRun.Connector.Err())
			require.Empty(t, legacyFatal.Load())
			if time.Since(lastProgress) >= 30*time.Second {
				lastProgress = time.Now()
				var doneWindows int64
				call, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				queryErr := watchConn.QueryRow(call, "SELECT count(*) FROM cdc_online.windows WHERE status='completed'").Scan(&doneWindows)
				cancel()
				if queryErr != nil {
					require.NoError(t, queryErr)
				}
				t.Logf("progress: online windows completed=%d/%d online reads=%d online cdc=%d/%d (snapshot done=%v sentinels=%d) legacy cdc=%d", doneWindows, rows/int64(chunkRows), onlineState.reads.Load(), onlineState.cdcDelivered.Load(), writesPreview.Load(), onlineState.snapshotDone.Load(), onlineState.sentinels.Load(), legacyState.cdcDelivered.Load())
			}
		case <-deadline:
			t.Fatalf("head-to-head deadline: legacy stopped=%v (sentinels=%d, snapshot done=%v) online stopped=%v (sentinels=%d)", !legacyDoneAt.IsZero(), legacyState.sentinels.Load(), legacyState.snapshotDone.Load(), !onlineDoneAt.IsZero(), onlineState.sentinels.Load())
		}
	}
	sample()

	writes := <-writeDone
	require.NoError(t, writes.err)
	require.Positive(t, writes.count)

	stats := onlineRun.Connector.Stats()
	onlineRun.Cancel()
	require.ErrorIs(t, onlineResult(t, onlineRun), context.Canceled)

	// Sentinel barrier proof: exactly one sentinel event per arm, each sentinel
	// commit LSN inside the writer-recorded window. The sentinel is the final
	// transaction of the shared stream, so both arms stopped on the same
	// logical CDC message. Per-relation delivery is ordered, therefore each
	// arm delivered the full prefix up to that point.
	require.Equal(t, int64(1), legacyState.sentinels.Load(), "legacy must deliver the sentinel exactly once")
	require.Equal(t, int64(1), onlineState.sentinels.Load(), "online must deliver the sentinel exactly once")
	require.NotZero(t, legacyState.sentinelCommitLSN.Load(), "legacy sentinel commit LSN")
	require.NotZero(t, onlineState.sentinelCommitLSN.Load(), "online sentinel commit LSN")
	require.Equal(t, legacyState.sentinelCommitLSN.Load(), onlineState.sentinelCommitLSN.Load(), "both arms must stop on the same sentinel commit LSN")
	require.GreaterOrEqual(t, legacyState.sentinelCommitLSN.Load(), uint64(writes.preLSN), "legacy sentinel commit LSN >= writer pre LSN")
	require.LessOrEqual(t, legacyState.sentinelCommitLSN.Load(), uint64(writes.postLSN), "legacy sentinel commit LSN <= writer post LSN")
	require.GreaterOrEqual(t, onlineState.sentinelCommitLSN.Load(), uint64(writes.preLSN), "online sentinel commit LSN >= writer pre LSN")
	require.LessOrEqual(t, onlineState.sentinelCommitLSN.Load(), uint64(writes.postLSN), "online sentinel commit LSN <= writer post LSN")

	// Delivery accounting: the sentinel is the last transaction, so each arm
	// must have delivered every steady update plus the sentinel as CDC.
	require.Equal(t, writes.count+1, legacyState.cdcDelivered.Load(), "legacy CDC deliveries must equal writes+sentinel")
	require.Equal(t, writes.count+1, onlineState.cdcDelivered.Load(), "online CDC deliveries must equal writes+sentinel")

	// Strategy-specific expectations.
	require.Empty(t, legacyState.errorValue(), "legacy handler errors")
	require.Equal(t, rows, legacyState.reads.Load())
	require.Equal(t, rows*(rows+1)/2, legacyState.readChecksum.Load())
	require.Empty(t, onlineState.errorValue(), "online handler errors")
	require.LessOrEqual(t, onlineState.reads.Load(), rows, "online reads cannot exceed table cardinality")
	require.GreaterOrEqual(t, onlineState.reads.Load()+onlineState.cdcDelivered.Load(), rows, "reads plus CDC updates must cover the table")

	var maxReadMS float64
	var readQueries int64
	require.NoError(t, sampleConn.QueryRow(context.Background(), `SELECT COALESCE(max(max_exec_time),0),COALESCE(sum(calls),0)::bigint FROM pg_stat_statements WHERE query ILIKE 'SELECT%workload%' AND query NOT ILIKE '%to_jsonb%' AND query NOT ILIKE '%count(%' AND query NOT ILIKE '%max(%' AND query NOT ILIKE '%min(%'`).Scan(&maxReadMS, &readQueries))

	legacySnapAt, legacySnapOK := legacyState.snapshotTime()
	onlineSnapAt, onlineSnapOK := onlineState.snapshotTime()
	require.True(t, legacySnapOK)
	require.True(t, onlineSnapOK)
	// Run-length guarantee that is achievable at this drain rate: legacy must
	// spend a long live CDC phase between its snapshot and the sentinel, and
	// online must have been consuming the live stream throughout its snapshot
	// (its completion lands after writers stop, so the sentinel is its only
	// post-snapshot CDC message).
	require.Greater(t, writes.sentinelAt.Sub(legacySnapAt), updateDuration/2, "legacy must consume live CDC for at least half the writer duration after its snapshot ends")
	t.Logf("timing: legacy snapshot done at %vs, writers stopped and sentinel committed at %vs, online snapshot done at %vs (onlineSnapshotBeforeSentinel=%v, online post-snapshot CDC beyond sentinel: none by stream construction)", legacySnapAt.Sub(writersStart).Seconds(), writes.sentinelAt.Sub(writersStart).Seconds(), onlineSnapAt.Sub(writersStart).Seconds(), onlineSnapAt.Before(writes.sentinelAt))
	report := map[string]any{
		"rows":                              rows,
		"chunkRows":                         chunkRows,
		"requestedUpdatesPerSecond":         requestedUpdatesPerSecond,
		"writersDurationSeconds":            updateDuration.Seconds(),
		"actualSourceTransactionsPerSecond": float64(writes.count) / updateDuration.Seconds(),
		"sentinelCommitSeconds":             writes.sentinelAt.Sub(writersStart).Seconds(),
		"onlineSnapshotBeforeSentinel":      onlineSnapAt.Before(writes.sentinelAt),
		"legacyLiveCDCSeconds":              writes.sentinelAt.Sub(legacySnapAt).Seconds(),
		"writes":                            writes.count,
		"sentinel": map[string]any{
			"id":              sentinelID,
			"value":           sentinelValue,
			"writerPreLSN":    writes.preLSN.String(),
			"writerPostLSN":   writes.postLSN.String(),
			"legacyCommitLSN": pq.LSN(legacyState.sentinelCommitLSN.Load()).String(),
			"onlineCommitLSN": pq.LSN(onlineState.sentinelCommitLSN.Load()).String(),
			"sameCommitLSN":   legacyState.sentinelCommitLSN.Load() == onlineState.sentinelCommitLSN.Load(),
		},
		"legacy": map[string]any{
			"stopAtSentinelSeconds":  legacyDoneAt.Sub(writersStart).Seconds(),
			"snapshotDoneSeconds":    legacySnapAt.Sub(writersStart).Seconds(),
			"snapshotRows":           legacyState.reads.Load(),
			"snapshotKeyChecksum":    legacyState.readChecksum.Load(),
			"cdcDeliveries":          legacyState.cdcDelivered.Load(),
			"peakSlotRetainedWAL":    legacySlot.walBytes,
			"peakConfirmedLagBytes":  legacySlot.confirmedLag,
			"peakFeedbackAgeSeconds": legacySlot.feedbackAgeSecs,
			"slotName":               legacyCfg.Slot.Name,
		},
		"online": map[string]any{
			"stopAtSentinelSeconds":  onlineDoneAt.Sub(writersStart).Seconds(),
			"snapshotDoneSeconds":    onlineSnapAt.Sub(writersStart).Seconds(),
			"snapshotRows":           onlineState.reads.Load(),
			"snapshotKeyChecksum":    onlineState.readChecksum.Load(),
			"cdcDeliveries":          onlineState.cdcDelivered.Load(),
			"peakSlotRetainedWAL":    onlineSlot.walBytes,
			"peakConfirmedLagBytes":  onlineSlot.confirmedLag,
			"peakFeedbackAgeSeconds": onlineSlot.feedbackAgeSecs,
			"slotName":               onlineCfg.Slot.Name,
			"onlineStats":            stats,
		},
		"ratioOnlineOverLegacyStopAtSentinel": (onlineDoneAt.Sub(writersStart).Seconds()) / legacyDoneAt.Sub(writersStart).Seconds(),
		"sharedProcessPeakHeapBytes":          peakHeap,
		"sharedProcessPeakRSSBytes":           peakRSS,
		"maxSourceReadExecutionMS":            maxReadMS,
		"sourceReadQueries":                   readQueries,
		"sourceContainer":                     source.Container.GetContainerID(),
	}
	data, marshalErr := json.Marshal(report)
	require.NoError(t, marshalErr)
	require.NoError(t, os.MkdirAll("../specs/verifications/online-10m-snapshot-benchmark/evidence", 0o755))
	require.NoError(t, os.WriteFile("../specs/verifications/online-10m-snapshot-benchmark/evidence/h2h.json", append(data, '\n'), 0o600))
	t.Log("SNAPSHOT_H2H " + string(data))
}

type h2hArm struct {
	reads             atomic.Int64
	readChecksum      atomic.Int64
	cdcDelivered      atomic.Int64
	snapshotDone      atomic.Bool
	sentinels         atomic.Int64
	sentinelCommitLSN atomic.Uint64
	mu                sync.Mutex
	snapshotAt        time.Time
	handlerError      atomic.Value
}

func (a *h2hArm) noteSnapshotComplete() {
	a.mu.Lock()
	if a.snapshotAt.IsZero() {
		a.snapshotAt = time.Now()
	}
	a.mu.Unlock()
	a.snapshotDone.Store(true)
}

func (a *h2hArm) snapshotTime() (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snapshotAt, !a.snapshotAt.IsZero()
}

func (a *h2hArm) noteError(err error) {
	if err != nil {
		a.handlerError.CompareAndSwap(nil, err.Error())
	}
}

func (a *h2hArm) errorValue() string {
	if value := a.handlerError.Load(); value != nil {
		return value.(string)
	}
	return ""
}

func h2hFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}
