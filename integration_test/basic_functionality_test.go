package integration

import (
	"context"
	"fmt"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBasicFunctionality(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_basic_functionality"

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
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
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

	t.Run("Insert 10 book to table. Then check messages and metric", func(t *testing.T) {
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			assert.NoError(t, err)
		}

		for i := range 10 {
			m := <-messageCh
			assert.Equal(t, books[i].Map(), m.(*format.Insert).Decoded)
		}

		metric, _ := fetchInsertOpMetric()
		assert.True(t, metric == 10)
	})

	t.Run("Update 5 book on table. Then check messages and metric", func(t *testing.T) {
		books := CreateBooks(5)
		for i, b := range books {
			b.ID = i + 1
			books[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, books[i].Map(), m.(*format.Update).NewDecoded)
		}

		metric, _ := fetchUpdateOpMetric()
		assert.True(t, metric == 5)
	})

	t.Run("Delete 5 book from table. Then check messages and metric", func(t *testing.T) {
		for i := range 5 {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("DELETE FROM books WHERE id = %d", i+1))
			assert.NoError(t, err)
		}

		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, int32(i+1), m.(*format.Delete).OldDecoded["id"])
		}

		metric, _ := fetchDeleteOpMetric()
		assert.True(t, metric == 5)
	})
}
