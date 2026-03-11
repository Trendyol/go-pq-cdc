package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestLargeTransactionalRollback verifies that no messages are emitted when a large
// transaction is rolled back. The CDC stream must stay silent because PostgreSQL
// logical replication sends data only after COMMIT.
func TestLargeTransactionalRollback(t *testing.T) {
	const (
		rowCount = 5000
		slotName = "slot_test_large_tx_rollback"
	)

	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = slotName

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
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
		})

		go connector.Start(ctx)
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		assert.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)
		for i := 0; i < rowCount; i++ {
			_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, 'temp')", i+20000)
			assert.NoError(t, err)
		}
		assert.NoError(t, tx.Rollback(ctx))

		select {
		case <-msgCh:
			t.Fatalf("unexpected message received after rollback")
		case <-time.After(2 * time.Second):
		}
	})
}
