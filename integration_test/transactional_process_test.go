package integration

import (
	"context"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTransactionalProcess(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_transactional_process"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx pq.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	assert.NoError(t, err)

	cfg := config.Config{Host: Config.Host, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	assert.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		err = RestoreDB(ctx)
		assert.NoError(t, err)

		pool.Close()
	})

	go func() {
		connector.Start(ctx)
	}()

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = connector.WaitUntilReady(waitCtx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	cancel()

	t.Run("Start transactional operation and commit. Then check the messages and metrics", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (12, 'j*va is best')")
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 12")
		assert.NoError(t, err)

		err = tx.Commit(ctx)
		assert.NoError(t, err)

		insertMessage := <-messageCh
		assert.Equal(t, map[string]any{"id": int32(12), "name": "j*va is best"}, insertMessage.(*format.Insert).Decoded)
		updateMessage := <-messageCh
		assert.Equal(t, map[string]any{"id": int32(12), "name": "go is best"}, updateMessage.(*format.Update).NewDecoded)

		updateMetric, _ := fetchUpdateOpMetric()
		insertMetric, _ := fetchInsertOpMetric()
		deleteMetric, _ := fetchDeleteOpMetric()
		assert.True(t, updateMetric == 1)
		assert.True(t, insertMetric == 1)
		assert.True(t, deleteMetric == 0)
	})

	t.Run("Start transactional operation and rollback. Then Delete book which id is 12. Then check the messages and metrics", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES (13, 'j*va is best')")
		assert.NoError(t, err)

		_, err = tx.Exec(ctx, "UPDATE books SET name = 'go is best' WHERE id = 13")
		assert.NoError(t, err)

		err = tx.Rollback(ctx)
		assert.NoError(t, err)

		_, err = pool.Exec(ctx, "DELETE FROM books WHERE id = 12")
		assert.NoError(t, err)

		deleteMessage := <-messageCh
		assert.Equal(t, int32(12), deleteMessage.(*format.Delete).OldDecoded["id"])

		updateMetric, _ := fetchUpdateOpMetric()
		insertMetric, _ := fetchInsertOpMetric()
		deleteMetric, _ := fetchDeleteOpMetric()
		assert.True(t, updateMetric == 1)
		assert.True(t, insertMetric == 1)
		assert.True(t, deleteMetric == 1)
	})
}
