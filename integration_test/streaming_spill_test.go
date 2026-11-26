package integration

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countWALFiles(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".wal") {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

func fetchMetricFloat(metricName string) (float64, error) {
	raw, err := fetchMetrics(metricName)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(raw, 64)
}

func cleanupSlotAndPub(ctx context.Context, cfg config.Config) {
	conn, err := newPostgresConn()
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	_ = pgExec(ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cfg.Publication.Name))
	_ = pgExec(ctx, conn, fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')",
		cfg.Slot.Name, cfg.Slot.Name,
	))
}

func newTestPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg := config.Config{
		Host:     Config.Host,
		Port:     Config.Port,
		Username: "postgres",
		Password: "postgres",
		Database: Config.Database,
	}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestStreamingSpillLargeTransactionCommit(t *testing.T) {
	const (
		rowCount = 2000
		slotName = "slot_test_spill_large_commit"
	)
	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	spillDir := t.TempDir()
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2
	cdcCfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1024,
		SpillDir:       spillDir,
	}

	pgConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg))

	var received atomic.Int64
	msgCh := make(chan *replication.ListenerContext, rowCount+10)
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) {
		if _, ok := lCtx.Message.(*format.Insert); ok {
			received.Add(1)
			msgCh <- lCtx
		}
		_ = lCtx.Ack()
	})
	require.NoError(t, err)

	pool := newTestPool(ctx, t)
	t.Cleanup(func() {
		connector.Close()
		cleanupSlotAndPub(ctx, cdcCfg)
		_ = RestoreDB(ctx)
		_ = pgConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", i+100000, fmt.Sprintf("spill-book-%d", i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))

	deadline := time.After(30 * time.Second)
	for i := 0; i < rowCount; i++ {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d inserts, got %d", rowCount, i)
		case lCtx := <-msgCh:
			ins := lCtx.Message.(*format.Insert)
			assert.Equal(t, int32(i+100000), ins.Decoded["id"])
			assert.Equal(t, fmt.Sprintf("spill-book-%d", i), ins.Decoded["name"])
			assert.NotZero(t, lCtx.Xid)
			assert.NotZero(t, lCtx.CommitLSN)
		}
	}

	assert.Equal(t, int64(rowCount), received.Load())
	require.Eventually(t, func() bool {
		return countWALFiles(t, spillDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "spill files must be cleaned up after commit")

	spillTxTotal, err := fetchMetricFloat("go_pq_cdc_streaming_spill_transactions_total")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, spillTxTotal, float64(1))
}

func TestStreamingSpillRollbackCleansUp(t *testing.T) {
	const (
		rowCount = 2000
		slotName = "slot_test_spill_rollback"
	)
	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	spillDir := t.TempDir()
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2
	cdcCfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1024,
		SpillDir:       spillDir,
	}

	pgConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg))

	msgCh := make(chan *format.Insert, 100)
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			msgCh <- ins
		}
		_ = lCtx.Ack()
	})
	require.NoError(t, err)

	pool := newTestPool(ctx, t)
	t.Cleanup(func() {
		connector.Close()
		cleanupSlotAndPub(ctx, cdcCfg)
		_ = RestoreDB(ctx)
		_ = pgConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	abortBefore, _ := fetchMetricFloat("go_pq_cdc_streaming_aborted_transactions_total")

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", i+110000, fmt.Sprintf("rb-%d", i))
		require.NoError(t, err)
	}
	// Ensure PostgreSQL streamed and spilled the in-progress TX before ROLLBACK
	// so TransactionIdDidAbort cannot short-circuit before disk spill occurs.
	require.Eventually(t, func() bool {
		spilledBytes, err := fetchMetricFloat("go_pq_cdc_streaming_spilled_bytes")
		return err == nil && spilledBytes > 0
	}, 15*time.Second, 50*time.Millisecond, "expected in-progress transaction to spill to disk before rollback")
	require.NoError(t, tx.Rollback(ctx))

	require.Eventually(t, func() bool {
		abortAfter, err := fetchMetricFloat("go_pq_cdc_streaming_aborted_transactions_total")
		return err == nil && abortAfter >= abortBefore+1
	}, 15*time.Second, 100*time.Millisecond)

	select {
	case ins := <-msgCh:
		t.Fatalf("unexpected row from rolled-back transaction: %v", ins.Decoded)
	case <-time.After(1 * time.Second):
	}

	require.Eventually(t, func() bool {
		return countWALFiles(t, spillDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "spill files must be cleaned up after rollback")

	spillTxTotal, err := fetchMetricFloat("go_pq_cdc_streaming_spill_transactions_total")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, spillTxTotal, float64(1))

	// Subsequent transaction commits cleanly.
	_, err = pool.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", 119999, "after-rollback")
	require.NoError(t, err)

	select {
	case ins := <-msgCh:
		assert.Equal(t, int32(119999), ins.Decoded["id"])
		assert.Equal(t, "after-rollback", ins.Decoded["name"])
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for post-rollback commit")
	}
}

func TestStreamingSpillSubTransactionRollback(t *testing.T) {
	const (
		keptBefore   = 500
		rolledBack   = 1000
		keptAfter    = 500
		slotName     = "slot_test_spill_subtx_rollback"
		beforeBase   = 120000
		rollbackBase = 121000
		afterBase    = 123000
	)
	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	spillDir := t.TempDir()
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2
	cdcCfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1024,
		SpillDir:       spillDir,
	}

	pgConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg))

	msgCh := make(chan *format.Insert, keptBefore+rolledBack+keptAfter)
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			msgCh <- ins
		}
		_ = lCtx.Ack()
	})
	require.NoError(t, err)

	pool := newTestPool(ctx, t)
	t.Cleanup(func() {
		connector.Close()
		cleanupSlotAndPub(ctx, cdcCfg)
		_ = RestoreDB(ctx)
		_ = pgConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < keptBefore; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", beforeBase+i, fmt.Sprintf("kept-before-%d", i))
		require.NoError(t, err)
	}
	_, err = tx.Exec(ctx, "SAVEPOINT sp")
	require.NoError(t, err)
	for i := 0; i < rolledBack; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", rollbackBase+i, fmt.Sprintf("sp-drop-%d", i))
		require.NoError(t, err)
	}
	// Ensure PostgreSQL streamed and spilled the savepoint rows before rolling
	// back so on-disk sub-transaction truncation is exercised deterministically.
	require.Eventually(t, func() bool {
		spilledBytes, err := fetchMetricFloat("go_pq_cdc_streaming_spilled_bytes")
		return err == nil && spilledBytes > 0
	}, 15*time.Second, 50*time.Millisecond, "expected savepoint rows to spill to disk before rollback to savepoint")
	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp")
	require.NoError(t, err)
	for i := 0; i < keptAfter; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", afterBase+i, fmt.Sprintf("kept-after-%d", i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))

	want := keptBefore + keptAfter
	got := make(map[int32]struct{}, want)
	deadline := time.After(30 * time.Second)
	for len(got) < want {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d inserts, got %d", want, len(got))
		case ins := <-msgCh:
			id := ins.Decoded["id"].(int32)
			if id >= rollbackBase && id < rollbackBase+rolledBack {
				t.Fatalf("rolled-back savepoint row %d was delivered", id)
			}
			got[id] = struct{}{}
		}
	}

	select {
	case ins := <-msgCh:
		t.Fatalf("unexpected extra row: %v", ins.Decoded)
	case <-time.After(1 * time.Second):
	}
	require.Eventually(t, func() bool {
		return countWALFiles(t, spillDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "spill files must be cleaned up after sub-TX rollback commit")

	spillTxTotal, err := fetchMetricFloat("go_pq_cdc_streaming_spill_transactions_total")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, spillTxTotal, float64(1))
}

func TestStreamingSpillInterleavedAndDDL(t *testing.T) {
	const (
		txABeforeRows = 1000
		txBRows       = 5
		txAAfterRows  = 500
		slotName      = "slot_test_spill_interleaved_ddl"
	)
	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	spillDir := t.TempDir()
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2
	cdcCfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1024,
		SpillDir:       spillDir,
	}

	pgConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg))

	msgCh := make(chan *format.Insert, txABeforeRows+txBRows+txAAfterRows+10)
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			msgCh <- ins
		}
		_ = lCtx.Ack()
	})
	require.NoError(t, err)

	pool := newTestPool(ctx, t)
	t.Cleanup(func() {
		connector.Close()
		cleanupSlotAndPub(ctx, cdcCfg)
		_ = RestoreDB(ctx)
		_ = pgConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	// TX-A starts and inserts enough rows to stream & spill.
	txA, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < txABeforeRows; i++ {
		_, err = txA.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", 130000+i, fmt.Sprintf("a-v1-%d", i))
		require.NoError(t, err)
	}

	// Interleaved small TX-B commits before TX-A runs ALTER TABLE (which takes ACCESS EXCLUSIVE lock).
	for i := 0; i < txBRows; i++ {
		_, err = pool.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", 140000+i, fmt.Sprintf("b-%d", i))
		require.NoError(t, err)
	}

	// Mid-TX DDL inside TX-A followed by v2 inserts.
	_, err = txA.Exec(ctx, "ALTER TABLE books ADD COLUMN author TEXT NOT NULL DEFAULT 'unknown'")
	require.NoError(t, err)
	for i := 0; i < txAAfterRows; i++ {
		_, err = txA.Exec(ctx, "INSERT INTO books (id, name, author) VALUES ($1, $2, $3)", 150000+i, fmt.Sprintf("a-v2-%d", i), "ada")
		require.NoError(t, err)
	}
	require.NoError(t, txA.Commit(ctx))

	totalExpected := txABeforeRows + txBRows + txAAfterRows
	got := make(map[int32]*format.Insert, totalExpected)
	deadline := time.After(30 * time.Second)
	for len(got) < totalExpected {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d inserts, got %d", totalExpected, len(got))
		case ins := <-msgCh:
			id := ins.Decoded["id"].(int32)
			got[id] = ins
		}
	}

	for i := 0; i < txBRows; i++ {
		ins := got[int32(140000+i)]
		require.NotNil(t, ins)
		assert.Equal(t, fmt.Sprintf("b-%d", i), ins.Decoded["name"])
	}
	for i := 0; i < txABeforeRows; i++ {
		ins := got[int32(130000+i)]
		require.NotNil(t, ins)
		assert.Equal(t, fmt.Sprintf("a-v1-%d", i), ins.Decoded["name"])
		_, hasAuthor := ins.Decoded["author"]
		assert.False(t, hasAuthor, "pre-DDL row must decode with v1 schema")
	}
	for i := 0; i < txAAfterRows; i++ {
		ins := got[int32(150000+i)]
		require.NotNil(t, ins)
		assert.Equal(t, fmt.Sprintf("a-v2-%d", i), ins.Decoded["name"])
		assert.Equal(t, "ada", ins.Decoded["author"])
	}
	require.Eventually(t, func() bool {
		return countWALFiles(t, spillDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "spill files must be cleaned up after interleaved DDL commit")

	spillTxTotal, err := fetchMetricFloat("go_pq_cdc_streaming_spill_transactions_total")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, spillTxTotal, float64(1))
}

func TestStreamingSpillDiskLimitFailsClosed(t *testing.T) {
	const (
		rowCount = 1000
		slotName = "slot_test_spill_disk_limit"
	)
	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	spillDir := t.TempDir()
	readyFile := filepath.Join(t.TempDir(), "ready")
	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2

	pgConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg))

	pool := newTestPool(ctx, t)
	t.Cleanup(func() {
		cleanupSlotAndPub(ctx, cdcCfg)
		_ = RestoreDB(ctx)
		_ = pgConn.Close(ctx)
	})

	var cmdOut bytes.Buffer
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamingSpillDiskLimitSubprocess$")
	cmd.Stdout = &cmdOut
	cmd.Stderr = &cmdOut
	cmd.Env = append(os.Environ(),
		"CDC_INTEGRATION_SUBPROCESS=1",
		"CDC_PG_HOST="+Config.Host,
		"CDC_PG_PORT="+strconv.Itoa(Config.Port),
		"CDC_SLOT_NAME="+slotName,
		"CDC_SPILL_DIR="+spillDir,
		"CDC_READY_FILE="+readyFile,
	)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 15*time.Second, 50*time.Millisecond, "subprocess connector failed to become ready")

	_, confirmedInitial, err := readSlotLSNs(ctx, pgConn, slotName)
	require.NoError(t, err)

	// Baseline small TX so confirmed_flush_lsn advances before the failing TX.
	_, err = pool.Exec(ctx, "INSERT INTO books (id, name) VALUES (1, 'baseline')")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, confirmedBefore, err := readSlotLSNs(ctx, pgConn, slotName)
		return err == nil && confirmedBefore != "" && confirmedBefore != confirmedInitial
	}, 10*time.Second, 100*time.Millisecond)

	// Large TX exceeding MaxDiskBytes=1024 causes fail-closed panic in subprocess.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", 160000+i, fmt.Sprintf("fail-closed-%d", i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))

	waitErr := cmd.Wait()
	require.Error(t, waitErr, "subprocess must crash fail-closed when MaxDiskBytes is exceeded")
	outStr := cmdOut.String()
	assert.Contains(t, outStr, "stream spill disk limit exceeded")
	assert.Contains(t, outStr, "corrupted connection")
	require.Eventually(t, func() bool {
		return countWALFiles(t, spillDir) == 0
	}, 5*time.Second, 50*time.Millisecond, "spill files must be removed when sink exits")

	_, confirmedAfterFail, err := readSlotLSNs(ctx, pgConn, slotName)
	require.NoError(t, err)
	confirmedFailLSN, err := pq.ParseLSN(confirmedAfterFail)
	require.NoError(t, err)

	// Restart with unlimited disk budget on the same slot and verify zero loss.
	cdcCfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1024,
		SpillDir:       spillDir,
	}
	gotIDs := make(map[int32]struct{}, rowCount+1)
	type redeliveredRow struct {
		id        int32
		commitLSN pq.LSN
	}
	doneCh := make(chan redeliveredRow, rowCount+10)
	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			doneCh <- redeliveredRow{
				id:        ins.Decoded["id"].(int32),
				commitLSN: lCtx.CommitLSN,
			}
		}
		_ = lCtx.Ack()
	})
	require.NoError(t, err)
	t.Cleanup(connector.Close)

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	var largeTxCommitLSN pq.LSN
	deadline := time.After(30 * time.Second)
	for {
		allPresent := true
		for i := 0; i < rowCount; i++ {
			if _, ok := gotIDs[int32(160000+i)]; !ok {
				allPresent = false
				break
			}
		}
		if allPresent {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for redelivery after fail-closed restart: got %d/%d rows", len(gotIDs), rowCount)
		case row := <-doneCh:
			gotIDs[row.id] = struct{}{}
			if row.id >= 160000 && largeTxCommitLSN == 0 {
				largeTxCommitLSN = row.commitLSN
			}
		}
	}
	require.NotZero(t, largeTxCommitLSN)
	assert.Less(t, confirmedFailLSN, largeTxCommitLSN, "confirmed_flush_lsn must stay strictly below failed transaction commit LSN")
}

func TestStreamingSpillDiskLimitSubprocess(t *testing.T) {
	if os.Getenv("CDC_INTEGRATION_SUBPROCESS") != "1" {
		t.Skip("subprocess helper")
	}

	cfg, err := config.ReadConfigYAML("./test_connector.yaml")
	require.NoError(t, err)
	cfg.SetDefault()

	ctx := context.Background()
	port, err := strconv.Atoi(os.Getenv("CDC_PG_PORT"))
	require.NoError(t, err)
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	freePort := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	cfg.Host = os.Getenv("CDC_PG_HOST")
	cfg.Port = port
	cfg.Metric.Port = freePort
	statusInterval := 200 * time.Millisecond
	cfg.StatusUpdateInterval = &statusInterval
	cfg.Slot.Name = os.Getenv("CDC_SLOT_NAME")
	cfg.Slot.ProtoVersion = 2
	cfg.Slot.Spill = slot.StreamSpillConfig{
		Enabled:        true,
		MaxMemoryBytes: 1,
		MaxDiskBytes:   1024,
		SpillDir:       os.Getenv("CDC_SPILL_DIR"),
	}

	connector, err := cdc.NewConnector(ctx, cfg, func(lCtx *replication.ListenerContext) {
		_ = lCtx.Ack()
	})
	require.NoError(t, err)

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	require.NoError(t, os.WriteFile(os.Getenv("CDC_READY_FILE"), []byte("ok"), 0o600))
	select {}
}
