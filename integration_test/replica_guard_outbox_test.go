package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// outboxEvent is what the handler saw on the standby it read at dispatch.
type outboxEvent struct {
	err       error
	standby   string
	replay    string
	commitLSN pq.LSN
	id        int32
	visible   bool
}

// TestReplicaGuardOutboxTwoStandbys is the production shape: one transaction
// writes an outbox row (published) and a business row (not published); the
// handler reads the business row from a listed standby, alternating between
// two of them, one applying commit records late. Many writers commit
// concurrently so the gate runs against interleaved transactions. Every event
// must find its business row on the standby it was routed to.
func TestReplicaGuardOutboxTwoStandbys(t *testing.T) {
	const (
		writers     = 8
		txPerWriter = 25
		total       = writers * txPerWriter
	)
	ctx := context.Background()
	fastHostPort, fast := startStandby(ctx, t, "standby_outbox_fast", "0")
	slowHostPort, slow := startStandby(ctx, t, "standby_outbox_slow", "300ms")
	names := []string{fastHostPort, slowHostPort}
	standbys := []*pgxpool.Pool{fast, slow}

	primaryCfg := config.Config{Host: Config.Host, Port: Config.Port, Username: "postgres", Password: "postgres", Database: Config.Database}
	primary, err := pgxpool.New(ctx, primaryCfg.DSNWithoutSSL())
	require.NoError(t, err)
	t.Cleanup(primary.Close)
	_, err = primary.Exec(ctx, `DROP TABLE IF EXISTS outbox, rule_definition;
		CREATE TABLE rule_definition (id int PRIMARY KEY, name text NOT NULL);
		CREATE TABLE outbox (id int PRIMARY KEY, definition_id int NOT NULL)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = primary.Exec(ctx, "DROP TABLE IF EXISTS outbox, rule_definition") })

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_replica_guard_outbox"
	cdcCfg.Publication.Tables = []publication.Table{{Name: "outbox", ReplicaIdentity: publication.ReplicaIdentityFull}}
	cdcCfg.VisibilityGuard = config.VisibilityGuardConfig{Enabled: true, FailMode: config.VisibilityFailClosed, Replicas: names}

	events := make(chan outboxEvent, total)
	var next atomic.Int32
	handler := func(lCtx *replication.ListenerContext) {
		if ins, ok := lCtx.Message.(*format.Insert); ok {
			i := int(next.Add(1)-1) % len(standbys)
			ev := outboxEvent{id: ins.Decoded["definition_id"].(int32), standby: names[i], commitLSN: lCtx.CommitLSN}
			var n int
			ev.err = standbys[i].QueryRow(ctx, "SELECT count(*), pg_last_wal_replay_lsn()::text FROM rule_definition WHERE id = $1", ev.id).Scan(&n, &ev.replay)
			ev.visible = n == 1
			events <- ev
		}
		_ = lCtx.Ack()
	}
	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))
	connector, err := cdc.NewConnector(ctx, cdcCfg, handler)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		_ = RestoreDB(ctx)
		_ = postgresConn.Close(ctx)
	})
	go connector.Start(ctx)
	readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))
	waitStandbyCaughtUp(ctx, t, primary, fast)
	waitStandbyCaughtUp(ctx, t, primary, slow)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range txPerWriter {
				id := w*txPerWriter + i + 1
				tx, err := primary.Begin(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = tx.Exec(ctx, "INSERT INTO rule_definition (id, name) VALUES ($1, $2)", id, fmt.Sprintf("rule-%d", id)); err == nil {
					_, err = tx.Exec(ctx, "INSERT INTO outbox (id, definition_id) VALUES ($1, $1)", id)
				}
				if err != nil {
					_ = tx.Rollback(ctx)
					t.Error(err)
					return
				}
				if err = tx.Commit(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := make(map[int32]outboxEvent, total)
	reads := make(map[string]int, len(names))
	deadline := time.After(90 * time.Second)
	for len(seen) < total {
		select {
		case ev := <-events:
			seen[ev.id] = ev
			reads[ev.standby]++
			require.NoError(t, ev.err, "id %d on %s", ev.id, ev.standby)
			assert.Truef(t, ev.visible, "id %d dispatched with CommitLSN %s but not readable on %s (replay %s)", ev.id, ev.commitLSN, ev.standby, ev.replay)
		case <-deadline:
			t.Fatalf("received %d of %d events", len(seen), total)
		}
	}
	for _, name := range names {
		assert.Positive(t, reads[name], "every standby must have been read")
	}
}
