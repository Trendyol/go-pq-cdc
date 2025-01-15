package integration

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	cdc "github.com/vskurikhin/go-pq-cdc"
	"github.com/vskurikhin/go-pq-cdc/config"
	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/vskurikhin/go-pq-cdc/pq/message/format"
	"github.com/vskurikhin/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var _ replication.Listeners = (*listenersContainerTransactional)(nil)

type listenersContainerTransactional struct {
	messageCh chan any
}

func (l *listenersContainerTransactional) SendLSNHookFunc() replication.SendLSNHookFunc {
	return func(pq.LSN) {
	}
}

func (l *listenersContainerTransactional) ListenerFunc() replication.ListenerFunc {
	return func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			l.messageCh <- msg
		}
		_ = ctx.Ack()
	}
}

func (l *listenersContainerTransactional) SinkHookFunc() replication.SinkHookFunc {
	return func(xLogData *replication.XLogData) {
	}
}

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
	lc := &listenersContainerTransactional{
		messageCh: messageCh,
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, lc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	cfg := config.Config{Host: Config.Host, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		err = RestoreDB(ctx)
		assert.NoError(t, err)

		pool.Close()
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
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
