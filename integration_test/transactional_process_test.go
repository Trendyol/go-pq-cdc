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
	"github.com/stretchr/testify/require"
)

func TestTransactionalProcess(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_transactional_process"

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)

		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		messageCh := make(chan any, 500)
		handlerFunc := func(ctx *replication.ListenerContext) {
			switch msg := ctx.Message.(type) {
			case *format.Insert, *format.Delete, *format.Update:
				messageCh <- msg
			}
			_ = ctx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err)

		cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
		pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
		require.NoError(t, err)

		t.Cleanup(func() {
			connector.Close()
			err = RestoreDB(ctx)
			require.NoError(t, err)

			pool.Close()
		})

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		t.Run("Start transactional operation and commit. Then check the messages and metrics", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (12, 'j*va is best')")
			require.NoError(t, err)

			_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 12")
			require.NoError(t, err)

			err = tx.Commit(ctx)
			require.NoError(t, err)

			insertMessage := <-messageCh
			assert.Equal(t, map[string]any{"id": int32(12), "name": "j*va is best"}, insertMessage.(*format.Insert).Decoded)
			updateMessage := <-messageCh
			assert.Equal(t, map[string]any{"id": int32(12), "name": "go is best"}, updateMessage.(*format.Update).NewDecoded)

			updateMetric, _ := fetchUpdateOpMetric()
			insertMetric, _ := fetchInsertOpMetric()
			deleteMetric, _ := fetchDeleteOpMetric()
			assert.Equal(t, 1, updateMetric)
			assert.Equal(t, 1, insertMetric)
			assert.Equal(t, 0, deleteMetric)
		})

		t.Run("Start transactional operation and rollback. Then Delete book which id is 12. Then check the messages and metrics", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (13, 'j*va is best')")
			require.NoError(t, err)

			_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 13")
			require.NoError(t, err)

			err = tx.Rollback(ctx)
			require.NoError(t, err)

			_, err = pool.Exec(ctx, "DELETE FROM books WHERE id = 12")
			require.NoError(t, err)

			deleteMessage := <-messageCh
			assert.Equal(t, int32(12), deleteMessage.(*format.Delete).OldDecoded["id"])

			updateMetric, _ := fetchUpdateOpMetric()
			insertMetric, _ := fetchInsertOpMetric()
			deleteMetric, _ := fetchDeleteOpMetric()
			assert.Equal(t, 1, updateMetric)
			assert.Equal(t, 1, insertMetric)
			assert.Equal(t, 1, deleteMetric)
		})
	})
}
