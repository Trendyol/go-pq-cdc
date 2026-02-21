package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestStreamingTransactionRollback verifies that when a large streaming
// transaction is explicitly rolled back, NO messages are delivered to the
// consumer. PostgreSQL sends StreamAbort for explicit rollbacks and the
// streamTxBuffer must discard all accumulated messages for that XID.
func TestStreamingTransactionRollback(t *testing.T) {
	const (
		rowCount = 500
		slotName = "slot_test_stream_rollback"
	)

	ctx := context.Background()

	// Force PostgreSQL to stream in-progress transactions.
	lowerLogicalDecodingWorkMem(ctx, t)

	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	msgCh := make(chan any, 10)
	handler := func(lCtx *replication.ListenerContext) {
		switch lCtx.Message.(type) {
		case *format.Insert:
			msgCh <- lCtx.Message
		}
		_ = lCtx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		_ = RestoreDB(ctx)
		pool.Close()
		_ = postgresConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	assert.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	// --- Insert many rows then ROLLBACK -----------------------------------
	tx, err := pool.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for i := 0; i < rowCount; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", i+60000, fmt.Sprintf("rollback-book-%d", i))
		if !assert.NoError(t, err) {
			_ = tx.Rollback(ctx)
			t.FailNow()
		}
	}
	assert.NoError(t, tx.Rollback(ctx))

	// --- No messages should arrive ----------------------------------------
	select {
	case msg := <-msgCh:
		t.Fatalf("unexpected message received after streaming rollback: %v", msg)
	case <-time.After(3 * time.Second):
		// success – no messages delivered
	}
}

// TestStreamingRollbackThenCommit verifies that a rolled-back streaming
// transaction does not pollute a subsequent committed transaction.
//
// Timeline:
//
//	TX-A (streamed): BEGIN → many INSERTs → ROLLBACK   → no messages
//	TX-B (streamed): BEGIN → many INSERTs → COMMIT     → all messages delivered
//
// This ensures that streamTxBuffer correctly discards TX-A's messages on
// StreamAbort and independently delivers TX-B's messages on StreamCommit.
func TestStreamingRollbackThenCommit(t *testing.T) {
	const (
		rowCount = 500
		slotName = "slot_test_stream_rollback_commit"
	)

	ctx := context.Background()

	lowerLogicalDecodingWorkMem(ctx, t)

	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	var received atomic.Int64
	msgCh := make(chan *format.Insert, rowCount+10)
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			received.Add(1)
			msgCh <- ins
		}
		_ = lCtx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		_ = RestoreDB(ctx)
		pool.Close()
		_ = postgresConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	assert.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	// --- TX-A: large transaction → ROLLBACK -------------------------------
	txA, err := pool.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for i := 0; i < rowCount; i++ {
		_, err = txA.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", i+70000, fmt.Sprintf("rollback-%d", i))
		assert.NoError(t, err)
	}
	assert.NoError(t, txA.Rollback(ctx))

	// --- TX-B: large transaction → COMMIT ---------------------------------
	txB, err := pool.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for i := 0; i < rowCount; i++ {
		_, err = txB.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", i+80000, fmt.Sprintf("commit-%d", i))
		assert.NoError(t, err)
	}
	assert.NoError(t, txB.Commit(ctx))

	// --- Only TX-B's messages should arrive -------------------------------
	deadline := time.After(15 * time.Second)
	for received.Load() < int64(rowCount) {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d insert messages from committed tx, got %d", rowCount, received.Load())
		case <-msgCh:
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Wait a bit more to ensure no extra messages from the rolled-back tx leak through
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(rowCount), received.Load(),
		"expected exactly %d messages from committed tx, but got %d (rolled-back tx may have leaked)", rowCount, received.Load())
}
