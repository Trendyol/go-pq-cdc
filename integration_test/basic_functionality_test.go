package integration

import (
	"context"
	"fmt"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestBasicFunctionality(t *testing.T) {
	// Run CDC with create publisher and slot option
	ctx := context.Background()

	messageCh := make(chan any, 500)
	handlerFunc := func(ctx pq.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert, *format.Delete, *format.Update:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, Config, handlerFunc)
	if err != nil {
		t.Fatal(err)
	}

	postgresConn, err := newPostgresConn()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		// Stop CDC - Restore to snapshot
		connector.Close()
		if err = RestoreDB(ctx); err != nil {
			log.Fatal(err)
		}
	})

	go func() {
		connector.Start(ctx)
	}()

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err = connector.WaitUntilReady(waitCtx); err != nil {
		t.Fatal(err)
	}
	cancel()

	t.Run("Insert 10 book to table. Then check messages and metric", func(t *testing.T) {
		// Insert 10 book to table
		books := CreateBooks(10)
		for _, b := range books {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO books(id, name) VALUES(%d, '%s')", b.ID, b.Name))
			if err != nil {
				t.Fatal(err)
			}
		}

		// Check messages
		for i := range 10 {
			m := <-messageCh
			assert.Equal(t, books[i].Map(), m.(*format.Insert).Decoded)
		}

		// Check metric
		metric, _ := fetchInsertOpMetric()
		assert.True(t, metric == 10)
	})

	t.Run("Update 5 book on table. Then check messages and metric", func(t *testing.T) {
		// Update 5 book
		books := CreateBooks(5)
		for i, b := range books {
			b.ID = i + 1
			books[i] = b
			err = pgExec(ctx, postgresConn, fmt.Sprintf("UPDATE books SET name = '%s' WHERE id = %d", b.Name, b.ID))
			if err != nil {
				t.Fatal(err)
			}
		}

		// Check messages
		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, books[i].Map(), m.(*format.Update).NewDecoded)
		}

		// Check metric
		metric, _ := fetchUpdateOpMetric()
		assert.True(t, metric == 5)
	})

	t.Run("Delete 5 book from table. Then check messages and metric", func(t *testing.T) {
		// Delete 5 book
		for i := range 5 {
			err = pgExec(ctx, postgresConn, fmt.Sprintf("DELETE FROM books WHERE id = %d", i+1))
			if err != nil {
				t.Fatal(err)
			}
		}

		// Check messages
		for i := range 5 {
			m := <-messageCh
			assert.Equal(t, int32(i+1), m.(*format.Delete).OldDecoded["id"])
		}

		// Check metric
		metric, _ := fetchDeleteOpMetric()
		assert.True(t, metric == 5)
	})
}
