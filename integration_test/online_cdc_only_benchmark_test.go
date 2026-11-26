package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
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

// TestOnlineCDCOnlyHeadToHead isolates the CDC capture paths. No snapshot of
// real data runs in either arm:
//
//   - legacy: snapshot.enabled=false, the genuine library CDC-only path
//     (create slot -> stream.Open -> processLoop). Stop condition is the
//     sentinel alone.
//   - online: the online strategy requires snapshot.enabled+initial, so its
//     snapshot is made vacuous instead: the connector starts against an EMPTY
//     workload table and the writers only begin after the online snapshot has
//     already completed. The online arm is therefore streaming pure CDC for
//     the whole measured window.
//
// Writers then INSERT at 1000 tx/s for a fixed duration (default 2 minutes,
// CDC_H2H_SECONDS) and commit one final sentinel INSERT. Both arms share one
// source with separate slots/publications and stop on that same sentinel;
// equality of the two arms' sentinel commit LSNs inside the writer-recorded
// window proves the same physical barrier. INSERTs (not UPDATEs) generate the
// stream because the table is empty and a zero-row UPDATE emits no WAL.
//
// This measures raw CDC-capture throughput: legacy's buffered processLoop
// versus the online coordinator's single select loop, with snapshot cost
// removed from both sides.
func TestOnlineCDCOnlyHeadToHead(t *testing.T) {
	if os.Getenv("CDC_ONLINE_10M_BENCH") != "1" {
		t.Skip("opt in with CDC_ONLINE_10M_BENCH=1 to run the CDC-only sentinel-barrier benchmark")
	}
	const requestedUpdatesPerSecond = 1000
	updateDuration := 2 * time.Minute
	if override := os.Getenv("CDC_H2H_SECONDS"); override != "" {
		parsed, err := strconv.ParseInt(override, 10, 64)
		require.NoError(t, err)
		updateDuration = time.Duration(parsed) * time.Second
	}
	const sentinelID int64 = 1_000_000_001
	const sentinelValue int64 = 9_000_000_007
	workloadTable := online.Table{Schema: "public", Name: "workload"}

	runtime.GC()
	source := newOnlinePGFixtureDurability(t, true)
	setup := source.Connect(t)
	writer := source.Connect(t)
	sampleConn := source.Connect(t)
	watchConn := source.Connect(t)
	onlineExec(t, setup, "CREATE TABLE workload(id bigint PRIMARY KEY,value bigint)")
	// Deliberately left empty: the online snapshot completes with zero READs
	// and the whole INSERT stream below is pure CDC for both arms.
	provisionOnline(t, setup, "workload")

	legacyCfg := onlineConfig(t, source, "workload")
	legacyCfg.Snapshot.Strategy = config.SnapshotStrategyConsistent
	legacyCfg.Snapshot.Enabled = false
	legacyCfg.Slot.Name = "bench_legacy_slot"
	legacyCfg.Publication.Name = "bench_legacy_pub"
	legacyCfg.Metric.Port = h2hFreePort(t)

	onlineCfg := onlineConfig(t, source, "workload")
	onlineCfg.Snapshot.Strategy = config.SnapshotStrategyOnline
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

	legacy, err := cdc.NewConnector(context.Background(), legacyCfg, func(l *replication.ListenerContext) {
		switch message := l.Message.(type) {
		case *format.Snapshot:
			legacyState.noteError(fmt.Errorf("snapshot event in CDC-only benchmark"))
		case *format.Insert:
			legacyState.cdcDelivered.Add(1)
			key, keyErr := online.IntegerKey(message.Decoded, "id")
			value, valueErr := online.IntegerKey(message.Decoded, "value")
			if keyErr != nil || valueErr != nil {
				legacyState.noteError(keyErr)
				legacyState.noteError(valueErr)
				return
			}
			if key == sentinelID && value == sentinelValue {
				legacyState.sentinels.Add(1)
				legacyState.sentinelCommitLSN.Store(uint64(l.CommitLSN))
			}
		case *format.Update, *format.Delete:
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

	// Readiness: legacy must have its stream open (slot active) and online
	// must have completed its vacuous snapshot, so both arms are in pure CDC
	// mode before the first measured INSERT.
	require.Eventually(t, func() bool {
		var active bool
		call, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		queryErr := watchConn.QueryRow(call, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", legacyCfg.Slot.Name).Scan(&active)
		cancel()
		return queryErr == nil && active
	}, 60*time.Second, 100*time.Millisecond, "legacy slot must become active (CDC stream open)")
	waitOnlineCapture(t, onlineRun)
	waitOnlineSnapshot(t, onlineRun)
	onlineState.noteSnapshotComplete()
	require.Zero(t, onlineState.reads.Load(), "online snapshot must deliver zero READs on the empty table")

	type cdcWrites struct {
		count      int64
		preLSN     pq.LSN
		postLSN    pq.LSN
		sentinelAt time.Time
		err        error
	}
	writeDone := make(chan cdcWrites, 1)
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
			_, execErr := writer.Exec(call, "INSERT INTO workload(id,value) VALUES($1,$2)", seq, seq*3)
			cancel()
			if execErr != nil {
				writeDone <- cdcWrites{count: writes, err: execErr}
				return
			}
			writes = seq
			writesPreview.Store(seq)
		}
		var pre, post string
		if queryErr := writer.QueryRow(context.Background(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&pre); queryErr != nil {
			writeDone <- cdcWrites{count: writes, err: queryErr}
			return
		}
		if _, execErr := writer.Exec(context.Background(), "INSERT INTO workload(id,value) VALUES($1,$2)", sentinelID, sentinelValue); execErr != nil {
			writeDone <- cdcWrites{count: writes, err: execErr}
			return
		}
		if queryErr := writer.QueryRow(context.Background(), "SELECT pg_current_wal_flush_lsn()::text").Scan(&post); queryErr != nil {
			writeDone <- cdcWrites{count: writes, err: queryErr}
			return
		}
		preLSN, preErr := pq.ParseLSN(pre)
		postLSN, postErr := pq.ParseLSN(post)
		if preErr != nil {
			writeDone <- cdcWrites{count: writes, err: preErr}
			return
		}
		if postErr != nil {
			writeDone <- cdcWrites{count: writes, err: postErr}
			return
		}
		writeDone <- cdcWrites{count: writes, preLSN: preLSN, postLSN: postLSN, sentinelAt: time.Now()}
	}()

	legacyStop := make(chan time.Time, 1)
	onlineStop := make(chan time.Time, 1)
	go func() {
		for {
			if legacyState.sentinels.Load() > 0 {
				legacyStop <- time.Now()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for {
			if onlineState.sentinels.Load() > 0 {
				onlineStop <- time.Now()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	type cdcSlot struct {
		walBytes        int64
		confirmedLag    int64
		feedbackAgeSecs float64
	}
	var legacySlot, onlineSlot cdcSlot
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
			observed *cdcSlot
		}{{legacyCfg.Slot.Name, &legacySlot}, {onlineCfg.Slot.Name, &onlineSlot}} {
			var wal, lag int64
			var age float64
			call, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			queryErr := sampleConn.QueryRow(call, `SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn),0)::bigint,COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),s.confirmed_flush_lsn),0)::bigint,COALESCE(extract(epoch FROM clock_timestamp()-r.reply_time),0)::float8 FROM pg_replication_slots s LEFT JOIN pg_stat_replication r ON r.pid=s.active_pid WHERE s.slot_name=$1`, item.slotName).Scan(&wal, &lag, &age)
			cancel()
			require.NoError(t, queryErr)
			item.observed.walBytes = max(item.observed.walBytes, wal)
			item.observed.confirmedLag = max(item.observed.confirmedLag, lag)
			item.observed.feedbackAgeSecs = max(item.observed.feedbackAgeSecs, age)
		}
	}

	var legacyDoneAt, onlineDoneAt time.Time
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	deadlineDuration := 2*updateDuration + 15*time.Minute
	if override := os.Getenv("CDC_H2H_DEADLINE_MIN"); override != "" {
		parsed, err := strconv.Atoi(override)
		require.NoError(t, err)
		deadlineDuration = time.Duration(parsed) * time.Minute
	}
	deadline := time.After(deadlineDuration)
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
				t.Logf("progress: online cdc=%d/%d sentinels=%d legacy cdc=%d/%d sentinels=%d", onlineState.cdcDelivered.Load(), writesPreview.Load(), onlineState.sentinels.Load(), legacyState.cdcDelivered.Load(), writesPreview.Load(), legacyState.sentinels.Load())
			}
		case <-deadline:
			t.Fatalf("cdc-only deadline: legacy stopped=%v (sentinels=%d) online stopped=%v (sentinels=%d)", !legacyDoneAt.IsZero(), legacyState.sentinels.Load(), !onlineDoneAt.IsZero(), onlineState.sentinels.Load())
		}
	}
	sample()

	writes := <-writeDone
	require.NoError(t, writes.err)
	require.Positive(t, writes.count)

	stats := onlineRun.Connector.Stats()
	onlineRun.Cancel()
	require.ErrorIs(t, onlineResult(t, onlineRun), context.Canceled)

	require.Equal(t, int64(1), legacyState.sentinels.Load(), "legacy must deliver the sentinel exactly once")
	require.Equal(t, int64(1), onlineState.sentinels.Load(), "online must deliver the sentinel exactly once")
	require.NotZero(t, legacyState.sentinelCommitLSN.Load(), "legacy sentinel commit LSN")
	require.NotZero(t, onlineState.sentinelCommitLSN.Load(), "online sentinel commit LSN")
	require.Equal(t, legacyState.sentinelCommitLSN.Load(), onlineState.sentinelCommitLSN.Load(), "both arms must stop on the same sentinel commit LSN")
	require.GreaterOrEqual(t, legacyState.sentinelCommitLSN.Load(), uint64(writes.preLSN), "legacy sentinel commit LSN >= writer pre LSN")
	require.LessOrEqual(t, legacyState.sentinelCommitLSN.Load(), uint64(writes.postLSN), "legacy sentinel commit LSN <= writer post LSN")

	require.Equal(t, writes.count+1, legacyState.cdcDelivered.Load(), "legacy CDC deliveries must equal writes+sentinel")
	require.Equal(t, writes.count+1, onlineState.cdcDelivered.Load(), "online CDC deliveries must equal writes+sentinel")
	require.Zero(t, legacyState.reads.Load(), "legacy must deliver no snapshot rows")
	require.Zero(t, onlineState.reads.Load(), "online must deliver no snapshot rows")
	require.Empty(t, legacyState.errorValue(), "legacy handler errors")
	require.Empty(t, onlineState.errorValue(), "online handler errors")

	legacySeconds := legacyDoneAt.Sub(writersStart).Seconds()
	onlineSeconds := onlineDoneAt.Sub(writersStart).Seconds()
	t.Logf("timing: writers ran %vs, sentinel committed at %vs, legacy stopped at %vs, online stopped at %vs", updateDuration.Seconds(), writes.sentinelAt.Sub(writersStart).Seconds(), legacySeconds, onlineSeconds)
	report := map[string]any{
		"mode":                              "cdc-only",
		"requestedUpdatesPerSecond":         requestedUpdatesPerSecond,
		"writersDurationSeconds":            updateDuration.Seconds(),
		"actualSourceTransactionsPerSecond": float64(writes.count) / updateDuration.Seconds(),
		"sentinelCommitSeconds":             writes.sentinelAt.Sub(writersStart).Seconds(),
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
			"stopAtSentinelSeconds":  legacySeconds,
			"cdcDeliveries":          legacyState.cdcDelivered.Load(),
			"cdcDeliveriesPerSecond": float64(legacyState.cdcDelivered.Load()) / legacySeconds,
			"peakSlotRetainedWAL":    legacySlot.walBytes,
			"peakConfirmedLagBytes":  legacySlot.confirmedLag,
			"peakFeedbackAgeSeconds": legacySlot.feedbackAgeSecs,
			"slotName":               legacyCfg.Slot.Name,
		},
		"online": map[string]any{
			"stopAtSentinelSeconds":  onlineSeconds,
			"cdcDeliveries":          onlineState.cdcDelivered.Load(),
			"cdcDeliveriesPerSecond": float64(onlineState.cdcDelivered.Load()) / onlineSeconds,
			"peakSlotRetainedWAL":    onlineSlot.walBytes,
			"peakConfirmedLagBytes":  onlineSlot.confirmedLag,
			"peakFeedbackAgeSeconds": onlineSlot.feedbackAgeSecs,
			"slotName":               onlineCfg.Slot.Name,
			"onlineStats":            stats,
		},
		"ratioOnlineOverLegacyStopAtSentinel": onlineSeconds / legacySeconds,
		"sharedProcessPeakHeapBytes":          peakHeap,
		"sharedProcessPeakRSSBytes":           peakRSS,
		"sourceContainer":                     source.Container.GetContainerID(),
	}
	data, marshalErr := json.Marshal(report)
	require.NoError(t, marshalErr)
	require.NoError(t, os.MkdirAll("../specs/verifications/online-10m-snapshot-benchmark/evidence", 0o755))
	require.NoError(t, os.WriteFile("../specs/verifications/online-10m-snapshot-benchmark/evidence/cdc-only.json", append(data, '\n'), 0o600))
	t.Log("CDC_ONLY " + string(data))
}
