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
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		msgCh := make(chan any, 10)
		handler := func(lCtx *replication.ListenerContext) {
			if _, ok := lCtx.Message.(*format.Insert); ok {
				msgCh <- lCtx.Message
			}
			_ = lCtx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
		require.NoError(t, err)

		cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
		pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
		require.NoError(t, err)

		t.Cleanup(func() {
			connector.Close()
			_ = RestoreDB(ctx)
			pool.Close()
		})

		go connector.Start(ctx)
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		for i := 0; i < rowCount; i++ {
			_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, 'temp')", i+20000)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Rollback(ctx))

		select {
		case <-msgCh:
			t.Fatalf("unexpected message received after rollback")
		case <-time.After(2 * time.Second):
		}
	})
}
