package integration

import (
	"context"
	"runtime"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestLargeTransactionalCommit ensures that the CDC stream keeps only O(1) messages in memory
// even when a transaction touches a very large number of rows. We do a single transaction
// inserting 10_000 rows and verify that all messages are delivered while memory stays reasonable.
// NOTE: We do not assert exact memory usage (which is environment-dependent) but we check
// that the allocated heap does not grow proportionally with the number of rows.
func TestLargeTransactionalCommit(t *testing.T) {
	const (
		rowCount      = 10000
		slotName      = "slot_test_large_tx_commit"
		memoryUpperMB = 80 // heuristic upper bound
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

		messageCh := make(chan any, rowCount+10)
		handlerFunc := func(ctx *replication.ListenerContext) {
			switch msg := ctx.Message.(type) {
			case *format.Insert:
				messageCh <- msg
			}
			_ = ctx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
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

		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		tx, err := pool.Begin(ctx)
		assert.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			_, err = tx.Exec(ctx, "INSERT INTO books (id, name) VALUES ($1, 'bulk tx')", i+10000)
			if !assert.NoError(t, err) {
				_ = tx.Rollback(ctx)
				t.FailNow()
			}
		}
		assert.NoError(t, tx.Commit(ctx))

		received := 0
		deadline := time.After(10 * time.Second)
		for received < rowCount {
			select {
			case <-deadline:
				t.Fatalf("timeout waiting for %d messages, got %d", rowCount, received)
			case <-time.After(50 * time.Millisecond):
			case <-func() <-chan struct{} {
				ch := make(chan struct{})
				go func() {
					for len(messageCh) > 0 {
						<-messageCh
						received++
					}
					close(ch)
				}()
				return ch
			}():
			}
		}

		assert.Equal(t, rowCount, received, "unexpected number of insert messages")

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		allocMB := (memAfter.Alloc - memBefore.Alloc) / (1024 * 1024)
		assert.Less(t, allocMB, uint64(memoryUpperMB), "memory usage grew too much: %d MB", allocMB)
	})
}
