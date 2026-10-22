package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatusUpdateAdvancesSlotWhileStreamIsBusy covers the backlog case: a
// slow consumer keeps the sink fed end to end, so the stream never idles.
// Postgres only requests a reply when it has nothing to send, and the idle
// path never runs, so without statusUpdateInterval confirmed_flush_lsn stays
// frozen for the whole drain. Every consumer restart then replays the same WAL,
// and once the walsender waits long enough Postgres terminates it at
// wal_sender_timeout, which the connector treats as a corrupted connection.
func TestStatusUpdateAdvancesSlotWhileStreamIsBusy(t *testing.T) {
	ctx := context.Background()

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_status_update_busy"
	cdcCfg.Publication.Tables = append(publication.Tables(nil), Config.Publication.Tables...)
	cdcCfg.StatusUpdateInterval = 200 * time.Millisecond

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		var consumed atomic.Int64
		connector, err := cdc.NewConnector(ctx, cdcCfg, func(l *replication.ListenerContext) {
			consumed.Add(1)
			time.Sleep(50 * time.Microsecond) // slow consumer: the sink blocks on backpressure
			_ = l.Ack()
		})
		require.NoError(t, err)
		t.Cleanup(connector.Close)

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		_, initialConfirmed, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
		require.NoError(t, err)
		require.NotEmpty(t, initialConfirmed)

		// Inserted after the slot exists, so it is all backlog: ~15s of work at
		// the consumer's pace (300k messages × 50µs), long enough that the
		// stream stays busy for the whole observation window.
		require.NoError(t, pgExec(ctx, postgresConn,
			`INSERT INTO books (name) SELECT 'book-no-'||g FROM generate_series(1, 300000) g`))

		time.Sleep(8 * time.Second)

		require.Greater(t, consumed.Load(), int64(0), "no CDC message consumed")

		_, finalConfirmed, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
		require.NoError(t, err)
		assert.NotEqual(t, initialConfirmed, finalConfirmed,
			"confirmed_flush_lsn did not advance while the stream was busy: the primary was never acknowledged")
	})
}
