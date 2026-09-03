package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestVisibilityGuardRowIsVisibleWhenDispatched enables the visibility guard
// and, from inside the handler, reads the primary with a fresh snapshot: every
// delivered insert must already be visible. Covers the guard open checks
// (primary, timeline, wal_sender_timeout, snapshot function) and both the
// regular and the streamed (proto v2, low logical_decoding_work_mem) paths
// against a real server.
func TestVisibilityGuardRowIsVisibleWhenDispatched(t *testing.T) {
	const (
		smallRows = 20
		largeRows = 500
		slotName  = "slot_test_visibility_guard"
		smallBase = 95000
		largeBase = 96000
	)

	ctx := context.Background()
	lowerLogicalDecodingWorkMem(ctx, t)

	cdcCfg := Config
	cdcCfg.Slot.Name = slotName
	cdcCfg.Slot.ProtoVersion = 2
	cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailClosed}

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	type seen struct {
		id      int32
		visible bool
	}
	seenCh := make(chan seen, smallRows+largeRows)
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			id := ins.Decoded["id"].(int32)
			var n int
			err := pool.QueryRow(ctx, "SELECT count(*) FROM books WHERE id = $1", id).Scan(&n)
			seenCh <- seen{id: id, visible: err == nil && n == 1}
		}
		_ = lCtx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
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

	// Many small transactions (regular path) …
	for i := 0; i < smallRows; i++ {
		_, err = pool.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", smallBase+i, fmt.Sprintf("small-%d", i))
		assert.NoError(t, err)
	}
	// … and one large streamed transaction.
	tx, err := pool.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for i := 0; i < largeRows; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", largeBase+i, fmt.Sprintf("large-%d", i))
		assert.NoError(t, err)
	}
	assert.NoError(t, tx.Commit(ctx))

	got := make(map[int32]struct{}, smallRows+largeRows)
	deadline := time.After(20 * time.Second)
	for len(got) < smallRows+largeRows {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d inserts, got %d", smallRows+largeRows, len(got))
		case s := <-seenCh:
			if !s.visible {
				t.Fatalf("row %d was dispatched before it was visible on the primary", s.id)
			}
			got[s.id] = struct{}{}
		}
	}
}

// TestVisibilityGuardRejectsTimeoutAboveHalfWalSenderTimeout: the guard refuses
// to start when the gate could outlast the walsender's keepalive budget.
func TestVisibilityGuardRejectsTimeoutAboveHalfWalSenderTimeout(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_visibility_guard_reject"
	cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, Timeout: time.Hour}

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, func(lCtx *replication.ListenerContext) { _ = lCtx.Ack() })
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() {
		connector.Close()
		_ = RestoreDB(ctx)
		_ = postgresConn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	assert.Error(t, connector.WaitUntilReady(waitCtx), "stream must not become ready with a rejected guard config")
}
