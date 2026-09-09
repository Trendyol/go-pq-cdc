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

// TestStreamingSubTransactionRollback verifies that ROLLBACK TO SAVEPOINT inside
// a streamed transaction drops only the savepoint's rows. PostgreSQL sends
// STREAM ABORT with SubXid != Xid for it; treating that as a whole-transaction
// abort used to lose every row of the transaction.
func TestStreamingSubTransactionRollback(t *testing.T) {
	const (
		keptRows     = 500
		rolledBack   = 500
		afterRows    = 5
		slotName     = "slot_test_stream_subtx_rollback"
		keptBase     = 90000
		rollbackBase = 91000
		afterBase    = 92000
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

	msgCh := make(chan *format.Insert, keptRows+rolledBack+afterRows)
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
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

	tx, err := pool.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	for i := 0; i < keptRows; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", keptBase+i, fmt.Sprintf("kept-%d", i))
		assert.NoError(t, err)
	}
	_, err = tx.Exec(ctx, "SAVEPOINT sp")
	assert.NoError(t, err)
	for i := 0; i < rolledBack; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", rollbackBase+i, fmt.Sprintf("rolled-back-%d", i))
		assert.NoError(t, err)
	}
	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp")
	assert.NoError(t, err)
	for i := 0; i < afterRows; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, $2)", afterBase+i, fmt.Sprintf("after-%d", i))
		assert.NoError(t, err)
	}
	assert.NoError(t, tx.Commit(ctx))

	want := keptRows + afterRows
	got := make(map[int32]struct{}, want)
	deadline := time.After(15 * time.Second)
	for len(got) < want {
		select {
		case <-deadline:
			t.Fatalf("timeout: expected %d insert messages, got %d", want, len(got))
		case ins := <-msgCh:
			id := ins.Decoded["id"].(int32)
			if id >= rollbackBase && id < rollbackBase+rolledBack {
				t.Fatalf("row %d from the rolled-back savepoint was delivered", id)
			}
			got[id] = struct{}{}
		}
	}

	// Nothing else may leak through after the expected rows.
	select {
	case ins := <-msgCh:
		t.Fatalf("unexpected extra insert delivered: id=%v", ins.Decoded["id"])
	case <-time.After(time.Second):
	}
}
