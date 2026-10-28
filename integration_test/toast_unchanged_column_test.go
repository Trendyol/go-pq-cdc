package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateUnchangedToastColumn mirrors example/toast-unchanged-column. An unchanged TOASTed
// column of an UPDATE is never reported as NULL: it is copied from the old tuple only when the
// old tuple carries its real value ('O', or a key column), and otherwise it is absent from
// NewDecoded and listed in UnchangedToastColumns.
func TestUpdateUnchangedToastColumn(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_toast_unchanged_column"
	cdcCfg.Publication.Tables = publication.Tables{
		{Name: "toast_docs", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityDefault},
	}

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)

		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))
		// EXTERNAL keeps the 5000-char payload out of line; the default (EXTENDED) would compress
		// a run of 'x' inline, and then no 'u' would be sent.
		require.NoError(t, pgExec(ctx, postgresConn, `
			DROP TABLE IF EXISTS toast_docs;
			CREATE TABLE toast_docs (id int PRIMARY KEY, title text, payload text);
			ALTER TABLE toast_docs ALTER COLUMN payload SET STORAGE EXTERNAL;
		`))

		messageCh := make(chan any, 500)
		handlerFunc := func(ctx *replication.ListenerContext) {
			switch msg := ctx.Message.(type) {
			case *format.Insert, *format.Update:
				messageCh <- msg
			}
			_ = ctx.Ack()
		}

		connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
		require.NoError(t, err)

		t.Cleanup(func() {
			connector.Close()
			assert.NoError(t, pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS toast_docs"))
			assert.NoError(t, RestoreDB(ctx))
			assert.NoError(t, postgresConn.Close(ctx))
		})

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		nextMessage := func(t *testing.T) any {
			t.Helper()
			select {
			case m := <-messageCh:
				return m
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for message")
				return nil
			}
		}
		nextUpdate := func(t *testing.T) *format.Update {
			t.Helper()
			upd, ok := nextMessage(t).(*format.Update)
			require.True(t, ok, "expected *format.Update")
			return upd
		}

		t.Run("Insert row with out-of-line payload", func(t *testing.T) {
			payload := strings.Repeat("x", 5000)
			require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf("INSERT INTO toast_docs(id, title, payload) VALUES(1, 'a', '%s')", payload)))
			require.IsType(t, &format.Insert{}, nextMessage(t))
		})

		t.Run("Non-key UPDATE with N old tuple: payload is listed as unchanged", func(t *testing.T) {
			require.NoError(t, pgExec(ctx, postgresConn, "UPDATE toast_docs SET title = 'b' WHERE id = 1"))

			upd := nextUpdate(t)
			assert.Equal(t, uint8(format.UpdateTupleTypeNew), upd.OldTupleType)
			assert.NotContains(t, upd.NewDecoded, "payload")
			assert.Equal(t, []string{"payload"}, upd.UnchangedToastColumns)
		})

		t.Run("Primary-key UPDATE with K old tuple: payload is listed as unchanged", func(t *testing.T) {
			require.NoError(t, pgExec(ctx, postgresConn, "UPDATE toast_docs SET id = 2 WHERE id = 1"))

			upd := nextUpdate(t)
			assert.Equal(t, uint8(format.UpdateTupleTypeKey), upd.OldTupleType)
			assert.NotContains(t, upd.NewDecoded, "payload")
			assert.Equal(t, int32(2), upd.NewDecoded["id"])
			assert.Equal(t, []string{"payload"}, upd.UnchangedToastColumns)
		})

		t.Run("Primary-key UPDATE with REPLICA IDENTITY FULL: payload is carried from O old tuple", func(t *testing.T) {
			require.NoError(t, pgExec(ctx, postgresConn, "ALTER TABLE toast_docs REPLICA IDENTITY FULL; UPDATE toast_docs SET id = 3 WHERE id = 2"))

			upd := nextUpdate(t)
			assert.Equal(t, uint8(format.UpdateTupleTypeOld), upd.OldTupleType)
			payload, ok := upd.NewDecoded["payload"].(string)
			require.True(t, ok, "payload should be a string")
			assert.Len(t, payload, 5000)
			assert.Empty(t, upd.UnchangedToastColumns)
		})
	})
}
