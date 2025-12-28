package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestConcurrentTxOrdering verifies that when two concurrent transactions commit
// out-of-order (B commits before A although A began first) the CDC stream emits
// B's changes first, followed by A's.
func TestConcurrentTxOrdering(t *testing.T) {
	ctx := context.Background()
	slot := "slot_test_concurrent_order"

	// ---------- connector prep -------------------------------------------
	cdcCfg := Config
	cdcCfg.Slot.Name = slot

	pgConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NoError(t, SetupTestDB(ctx, pgConn, cdcCfg)) {
		t.FailNow()
	}

	msgCh := make(chan *format.Insert, 5)
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			msgCh <- ins
		}
		_ = lCtx.Ack()
	}

	connector, _ := cdc.NewConnector(ctx, cdcCfg, handler)
	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	assert.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, _ := pgxpool.New(ctx, cfg.DSNWithoutSSL())

	//---------------- start TxA (long) -------------------------------------
	txA, _ := pool.Begin(ctx)
	_, _ = txA.Exec(ctx, "INSERT INTO books(id,name) VALUES(600,'A')")

	//---------------- TxB (short) commit first -----------------------------
	txB, _ := pool.Begin(ctx)
	_, _ = txB.Exec(ctx, "INSERT INTO books(id,name) VALUES(601,'B')")
	_ = txB.Commit(ctx)

	// simulate some gap
	time.Sleep(200 * time.Millisecond)

	//---------------- commit TxA ------------------------------------------
	_ = txA.Commit(ctx)

	//----------- collect two messages ------------------------------------
	var first, second *format.Insert
	select {
	case first = <-msgCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting first message")
	}
	select {
	case second = <-msgCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting second message")
	}

	// Expect B(id 601) comes before A(id 600)
	assert.Equal(t, int32(601), first.Decoded["id"])
	assert.Equal(t, int32(600), second.Decoded["id"])

	// -------- validate confirmed_flush_lsn & restart_lsn -----------------
	// Give connector a moment to send ACK and Postgres to advance slot
	time.Sleep(500 * time.Millisecond)
	var restartLSN, confirmedLSN string
	row := pool.QueryRow(ctx, "SELECT restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=$1", slot)
	err = row.Scan(&restartLSN, &confirmedLSN)
	assert.NoError(t, err)
	assert.NotEmpty(t, confirmedLSN)
	// When no other transaction is in-progress, restart_lsn should equal confirmed
	// restart_lsn is the oldest LSN that might still be needed; it is <= confirmed_flush_lsn
	confirmed, _ := pq.ParseLSN(confirmedLSN)
	restart, _ := pq.ParseLSN(restartLSN)
	assert.LessOrEqual(t, restart, confirmed)

	connector.Close()
}
