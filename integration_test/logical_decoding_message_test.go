package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	logicalMessagePrefix       = "go_pq_cdc_test"
	logicalMessageNonTxContent = "non-transactional message payload"
	logicalMessageTxContent    = "transactional message payload"
	logicalMessageBookName     = "logical message book"

	// emitMessageQuery calls pg_logical_emit_message. Its argument order
	// (transactional, prefix, message) has been the same since the function was
	// introduced; PostgreSQL 14 added a trailing flush parameter that defaults
	// to false, so the three-argument form works on every supported server.
	// The explicit casts give pgx the parameter types it cannot infer (the
	// function has both text and bytea overloads).
	emitMessageQuery = "SELECT pg_logical_emit_message($1::boolean, $2::text, $3::text)"
)

// delivered is one listener callback: the decoded message plus the commit
// position the library reported for it.
type delivered struct {
	message   any
	commitLSN pq.LSN
}

// TestLogicalDecodingMessage verifies that a message emitted outside of any
// transaction reaches the listener before the transaction that follows it,
// carrying its prefix, content and the LSN of its own WAL record.
//
// go test -race -p=1 -v -run=TestLogicalDecodingMessage
func TestLogicalDecodingMessage(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_logical_decoding_message"

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
			t.FailNow()
		}

		dropSlot(ctx, postgresConn, cdcCfg.Slot.Name)

		deliveredCh := make(chan delivered, 500)
		handlerFunc := func(ctx *replication.ListenerContext) {
			deliveredCh <- delivered{message: ctx.Message, commitLSN: ctx.CommitLSN}
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
			pool.Close()
			assert.NoError(t, RestoreDB(ctx))
			assert.NoError(t, postgresConn.Close(ctx))
		})

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
			cancel()
			t.FailNow()
		}
		cancel()

		t.Run("Non-transactional message is delivered before the next transaction", func(t *testing.T) {
			// Emitted outside any transaction: pgoutput delivers it standalone,
			// with no BEGIN/COMMIT around it.
			emitLSN := emitMessage(t, ctx, pool, logicalMessagePrefix, logicalMessageNonTxContent)

			// A transaction right after it must not swallow the message.
			emitMessageInTransaction(t, ctx, pool, logicalMessagePrefix, logicalMessageTxContent)

			// 1) The non-transactional message arrives first, immediately —
			// not held back or dropped by the following BEGIN.
			got := receiveMessage(t, deliveredCh)
			nonTx, ok := got.message.(*format.LogicalDecodingMessage)
			require.True(t, ok, "expected *format.LogicalDecodingMessage, got %T", got.message)
			assert.Equal(t, logicalMessagePrefix, nonTx.Prefix)
			assert.Equal(t, logicalMessageNonTxContent, string(nonTx.Content))
			assert.Zero(t, nonTx.Flags&1, "message must not have the transactional flag")
			// The wire carries the LSN of the message's own WAL record, which is
			// exactly what pg_logical_emit_message returns. An LSN that is too low
			// would restart the consumer behind the message, so pin the value.
			assert.Equal(t, emitLSN, nonTx.LSN)
			// It belongs to no transaction, so it reports no commit position.
			assert.Zero(t, got.commitLSN, "a non-transactional message reports no commit LSN")

			// 2) The transaction's insert follows. Relation messages are internal
			// bookkeeping; anything else arriving here is an ordering failure and
			// must surface as one instead of a timeout further down.
			var insertMsg *format.Insert
			for insertMsg == nil {
				switch m := receiveMessage(t, deliveredCh).message.(type) {
				case *format.Insert:
					insertMsg = m
				case *format.Relation:
				default:
					require.FailNowf(t, "unexpected message before the insert", "got %T", m)
				}
			}
			assert.Equal(t, logicalMessageBookName, insertMsg.Decoded["name"])

			// 3) The transactional message is delivered with its transaction.
			got = receiveMessage(t, deliveredCh)
			txMsg, ok := got.message.(*format.LogicalDecodingMessage)
			require.True(t, ok, "expected *format.LogicalDecodingMessage, got %T", got.message)
			assert.Equal(t, logicalMessagePrefix, txMsg.Prefix)
			assert.Equal(t, logicalMessageTxContent, string(txMsg.Content))
			assert.Equal(t, uint8(1), txMsg.Flags&1, "message must have the transactional flag")
			assert.NotZero(t, txMsg.LSN)
		})
	})
}

// TestLogicalDecodingMessageDisabled verifies that slot.logicalMessages: off
// keeps the messages option off START_REPLICATION: the same traffic the enabled
// test decodes must produce row changes only.
//
// go test -race -p=1 -v -run=TestLogicalDecodingMessageDisabled
func TestLogicalDecodingMessageDisabled(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_logical_decoding_message_disabled"
	cdcCfg.Slot.ProtoVersion = 2 // the default; messages depend on the server version, not this one
	cdcCfg.Slot.LogicalMessages = slot.LogicalMessagesOff

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))
	dropSlot(ctx, postgresConn, cdcCfg.Slot.Name)

	deliveredCh := make(chan delivered, 10)
	handlerFunc := func(ctx *replication.ListenerContext) {
		deliveredCh <- delivered{message: ctx.Message, commitLSN: ctx.CommitLSN}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	cfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	pool, err := pgxpool.New(ctx, cfg.DSNWithoutSSL())
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		pool.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(waitCtx))

	// The same traffic as the enabled test: a standalone message, then a
	// transaction carrying a row change and a message.
	emitMessage(t, ctx, pool, logicalMessagePrefix, logicalMessageNonTxContent)
	emitMessageInTransaction(t, ctx, pool, logicalMessagePrefix, logicalMessageTxContent)

	// The row change proves the stream processed that traffic. Nothing else may
	// arrive: not the standalone message that precedes it, and not the message
	// inside the transaction.
	var inserts int
	for inserts == 0 {
		switch m := receiveMessage(t, deliveredCh).message.(type) {
		case *format.Insert:
			inserts++
		case *format.Relation:
		default:
			require.FailNowf(t, "only row changes may arrive", "got %T", m)
		}
	}

	drain := time.After(time.Second)
	for {
		select {
		case d := <-deliveredCh:
			require.FailNowf(t, "slot.logicalMessages is off, so no logical message may be delivered", "got %T", d.message)
		case <-drain:
			return
		}
	}
}

// emitMessage emits a logical decoding message outside of any transaction and
// returns the LSN PostgreSQL assigned to it.
func emitMessage(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prefix, content string) pq.LSN {
	t.Helper()

	var lsnStr string
	require.NoError(t, pool.QueryRow(ctx, emitMessageQuery, false, prefix, content).Scan(&lsnStr))

	lsn, err := pq.ParseLSN(lsnStr)
	require.NoError(t, err)
	return lsn
}

// emitMessageInTransaction inserts a book and emits a transactional logical
// decoding message in one transaction, so pgoutput delivers it wrapped in
// BEGIN/COMMIT.
func emitMessageInTransaction(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prefix, content string) {
	t.Helper()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO books(id, name) VALUES ($1, $2)", 1, logicalMessageBookName)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, emitMessageQuery, true, prefix, content)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

func receiveMessage(t *testing.T, ch <-chan delivered) delivered {
	t.Helper()

	select {
	case d := <-ch:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for replication message")
		return delivered{}
	}
}

// dropSlot removes a leftover slot so a test starts from a known position: an
// earlier run (same container, -count>1) replays its unacked WAL, and these
// tests assert on the first message they receive.
func dropSlot(ctx context.Context, conn pq.Connection, name string) {
	_ = pgExec(ctx, conn, fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')",
		name, name))
}
