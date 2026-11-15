package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineInitialNaturalOverlapCDCOrderLogs(t *testing.T) {
	if os.Getenv("RUN_LONG_ONLINE_STRESS") != "1" {
		t.Skip("set RUN_LONG_ONLINE_STRESS=1 to run the natural-overlap 20k-update online snapshot order stress test")
	}

	const initialRows = 100000
	const chunkSize = 100
	const updateRPS = 1000
	const updateCount = 20000
	const updateDuration = 20 * time.Second

	type updateLogEntry struct {
		Seq      int    `json:"seq"`
		TxID     uint64 `json:"txid"`
		Key      int64  `json:"key"`
		CommitNS int64  `json:"commitUnixNano"`
	}
	type cdcLogEntry struct {
		Index     int    `json:"index"`
		Seq       int    `json:"seq"`
		Key       int64  `json:"key"`
		CommitLSN string `json:"commitLsn"`
		EndLSN    string `json:"endLsn"`
		Ordinal   uint32 `json:"ordinal"`
	}

	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	cdcConn := source.Connect(t)
	onlineExec(t, conn, "CREATE TABLE items(id bigint PRIMARY KEY,value text)")
	onlineExec(t, conn, "INSERT INTO items SELECT id, 'snapshot-' || id::text FROM generate_series(1,$1) AS id", initialRows)
	provisionOnline(t, conn, "items")

	cfg := onlineConfig(t, source, "items")
	cfg.Snapshot.ChunkSize = chunkSize
	cfg.Snapshot.Online.QueryTimeout = updateDuration + time.Minute
	cfg.Snapshot.Online.DeliveryTimeout = updateDuration + time.Minute

	artifactDir := filepath.Join("testdata", "tmp", t.Name())
	require.NoError(t, os.RemoveAll(artifactDir))
	require.NoError(t, os.MkdirAll(artifactDir, 0o755))
	actualPath := filepath.Join(artifactDir, "actual-updates.jsonl")
	cdcPath := filepath.Join(artifactDir, "cdc-output.jsonl")

	actualFile, err := os.Create(actualPath)
	require.NoError(t, err)
	defer actualFile.Close()
	actualWriter := bufio.NewWriterSize(actualFile, 1<<20)
	defer actualWriter.Flush()

	var mu sync.Mutex
	var delivered []online.Event
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		mu.Lock()
		delivered = append(delivered, e)
		mu.Unlock()
		return nil
	})
	waitOnlineCapture(t, r)

	ctx := context.Background()
	updateInterval := time.Second / updateRPS
	updatesStarted := time.Now()
	for seq := 1; seq <= updateCount; seq++ {
		if sleep := time.Until(updatesStarted.Add(time.Duration(seq-1) * updateInterval)); sleep > 0 {
			time.Sleep(sleep)
		}
		key := (int64(seq-1) % int64(initialRows)) + 1
		tx, err := cdcConn.Begin(ctx)
		require.NoError(t, err)
		var txID uint64
		require.NoError(t, tx.QueryRow(ctx, "SELECT txid_current()").Scan(&txID))
		_, err = tx.Exec(ctx, "UPDATE items SET value='cdc-update-' || $2::bigint::text WHERE id=$1", key, int64(seq))
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		entry := updateLogEntry{Seq: seq, TxID: txID, Key: key, CommitNS: time.Now().UnixNano()}
		data, err := json.Marshal(entry)
		require.NoError(t, err)
		_, err = actualWriter.Write(append(data, '\n'))
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, time.Since(updatesStarted), updateDuration, "CDC update workload must run for the full configured duration")
	require.NoError(t, actualWriter.Flush())
	require.NoError(t, actualFile.Sync())

	lastProgressLog := time.Now()
	for {
		mu.Lock()
		count := 0
		for _, event := range delivered {
			if event.Table == (online.Table{Schema: "public", Name: "items"}) && event.Operation == online.Update && event.Key >= 1 && event.Key <= int64(initialRows) {
				count++
			}
		}
		mu.Unlock()
		if count >= updateCount {
			break
		}
		if time.Since(lastProgressLog) >= 10*time.Second {
			t.Logf("waiting for CDC connector output: delivered %d/%d updates", count, updateCount)
			lastProgressLog = time.Now()
		}
		time.Sleep(10 * time.Millisecond)
	}

	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)

	mu.Lock()
	events := append([]online.Event(nil), delivered...)
	mu.Unlock()

	cdcFile, err := os.Create(cdcPath)
	require.NoError(t, err)
	defer cdcFile.Close()
	cdcWriter := bufio.NewWriterSize(cdcFile, 1<<20)
	actualBySeq := make(map[int]updateLogEntry, updateCount)
	for seq := 1; seq <= updateCount; seq++ {
		key := (int64(seq-1) % int64(initialRows)) + 1
		actualBySeq[seq] = updateLogEntry{Seq: seq, Key: key}
	}
	lastSeq := 0
	cdcUpdates := 0
	for _, event := range events {
		if event.Table != (online.Table{Schema: "public", Name: "items"}) || event.Operation != online.Update || event.Key < 1 || event.Key > int64(initialRows) {
			continue
		}
		value, ok := event.Image["value"].(string)
		require.True(t, ok, "CDC update for key %d has non-string value %#v", event.Key, event.Image["value"])
		var seq int
		_, err = fmt.Sscanf(value, "cdc-update-%d", &seq)
		require.NoError(t, err, "CDC update value %q does not include the update sequence", value)
		cdcUpdates++
		entry := cdcLogEntry{Index: cdcUpdates, Seq: seq, Key: event.Key, CommitLSN: event.CommitLSN.String(), EndLSN: event.Order.EndLSN.String(), Ordinal: event.Order.Ordinal}
		data, err := json.Marshal(entry)
		require.NoError(t, err)
		_, err = cdcWriter.Write(append(data, '\n'))
		require.NoError(t, err)

		expected, ok := actualBySeq[seq]
		require.True(t, ok, "CDC emitted unknown update sequence %d", seq)
		require.Equal(t, expected.Key, event.Key, "CDC key mismatch for update sequence %d", seq)
		require.Greater(t, seq, lastSeq, "CDC event out of order at output index %d: got seq %d after seq %d; logs: actual=%s cdc=%s", cdcUpdates, seq, lastSeq, actualPath, cdcPath)
		lastSeq = seq
	}
	require.NoError(t, cdcWriter.Flush())
	require.NoError(t, cdcFile.Sync())
	require.Equal(t, updateCount, cdcUpdates, "CDC output log must contain every actual update exactly once; logs: actual=%s cdc=%s", actualPath, cdcPath)
	t.Logf("wrote online snapshot order logs: actual=%s cdc=%s", actualPath, cdcPath)
}
