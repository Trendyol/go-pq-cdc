package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyProtocol(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_copy_protocol"

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)

		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		messageCh := make(chan *replication.ListenerContext)
		totalCounter := atomic.Int64{}
		handlerFunc := func(ctx *replication.ListenerContext) {
			switch ctx.Message.(type) {
			case *format.Insert, *format.Delete, *format.Update:
				totalCounter.Add(1)
				messageCh <- ctx
			}
		}

		cdc2Cfg := cdcCfg
		cdc2Cfg.Metric.Port = 8085
		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err)

		connector2, err := cdc.NewConnector(ctx, cdc2Cfg, handlerFunc)
		require.NoError(t, err)

		cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
		pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
		require.NoError(t, err)

		t.Cleanup(func() {
			pool.Close()
			connector.Close()
			connector2.Close()
			assert.NoError(t, RestoreDB(ctx))
		})

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		go connector2.Start(ctx)

		t.Run("Insert 30 book to table with Copy protocol. Then stop the consumer after 16th message processed", func(t *testing.T) {
			entries := make([][]any, 30)
			books := CreateBooks(30)

			for i, user := range books {
				entries[i] = []any{user.ID, user.Name}
			}

			_, err = pool.CopyFrom(
				ctx,
				pgx.Identifier{"books"},
				[]string{"id", "name"},
				pgx.CopyFromRows(entries),
			)
			if err != nil {
				t.Errorf("error copying into %s table: %v", "books", err)
			}

			for {
				m := <-messageCh
				if v, ok := m.Message.(*format.Insert); ok {
					if v.Decoded["id"].(int32) == 16 {
						connector.Close()
						break
					}
				}

				assert.NoError(t, m.Ack())
			}
		})

		t.Run("Run CDC again. Then check message count after all messages consumed", func(t *testing.T) {
			waitCtx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			require.NoError(t, connector2.WaitUntilReady(waitCtx))
			cancel()

			for {
				m := <-messageCh
				if v, ok := m.Message.(*format.Insert); ok {
					if v.Decoded["id"].(int32) == 30 {
						break
					}
				}
			}
		})
	})
}
