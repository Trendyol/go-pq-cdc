package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

// Independent protocol reader records exact committed CDC, not inferred final
// state or handler counts. It never ACKs volatile oracle history. Its separate
// fixture slot retains WAL through the test and is not a runtime outbox.
type onlineCDCOracle struct {
	mu     sync.Mutex
	events map[string]online.Event
	err    error
	cancel context.CancelFunc
	reader *replication.OnlineReader
	done   chan struct{}
}

func startCDCOracle(t *testing.T, source onlinePGFixture, publication, binding string, legacy bool, tables ...string) *onlineCDCOracle {
	t.Helper()
	conn := source.Connect(t)
	startup, stopStartup := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopStartup()
	slot := "oracle_" + fmt.Sprintf("%x", time.Now().UnixNano())
	var start string
	require.NoError(t, conn.QueryRow(startup, "SELECT lsn::text FROM pg_create_logical_replication_slot($1,'pgoutput')", slot).Scan(&start))
	lsn, err := pq.ParseLSN(start)
	require.NoError(t, err)
	dsn := source.DSN + "&replication=database"
	identity, err := online.IdentifySource(startup, dsn)
	require.NoError(t, err)
	var schemas []online.TableSchema
	for _, table := range tables {
		s, err := online.InspectTable(startup, conn, online.Table{Schema: "public", Name: table})
		require.NoError(t, err)
		schemas = append(schemas, s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := replication.StartOnlineReaderWithStartupTimeout(ctx, replication.OnlineReaderOptions{ReplicationDSN: dsn, SQLDSN: source.DSN, Publication: publication, Slot: slot, StartLSN: lsn, ProtoVersion: 1, Source: identity, Limits: replication.OnlineReaderLimits{TransactionBytes: 16 << 20, QueueBytes: 32 << 20, FeedbackInterval: time.Second, VisibilityTimeout: 5 * time.Second, PollInterval: 5 * time.Millisecond}}, 5*time.Second)
	require.NoError(t, err)
	oracle := &onlineCDCOracle{events: map[string]online.Event{}, cancel: cancel, reader: reader, done: make(chan struct{})}
	go func() {
		defer close(oracle.done)
		for {
			env, err := reader.Next(ctx)
			if err != nil {
				if ctx.Err() == nil {
					oracle.mu.Lock()
					oracle.err = err
					oracle.mu.Unlock()
				}
				return
			}
			for _, change := range env.Changes {
				_, control, err := online.DecodeControl(change)
				if err != nil {
					oracle.mu.Lock()
					oracle.err = err
					oracle.mu.Unlock()
					return
				}
				if control {
					continue
				}
				event, err := online.BusinessEvent(change, env, schemas, binding)
				if err != nil {
					oracle.mu.Lock()
					oracle.err = err
					oracle.mu.Unlock()
					return
				}
				if legacy {
					event.ID = online.CDCEventID(binding, env.CommitLSN, change.Ordinal)
					event.Order.EndLSN = env.CommitLSN
				}
				oracle.mu.Lock()
				if len(oracle.events) >= 20000 {
					oracle.err = fmt.Errorf("oracle fixed event budget exceeded")
					oracle.mu.Unlock()
					return
				}
				oracle.events[event.ID] = event
				oracle.mu.Unlock()
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, reader.Close(ctx))
		select {
		case <-oracle.done:
		case <-ctx.Done():
			t.Error("oracle close deadline")
		}
	})
	return oracle
}
func (o *onlineCDCOracle) assert(t *testing.T, target *onlineTarget, count int) {
	t.Helper()
	onlineEventually(t, func() bool { o.mu.Lock(); defer o.mu.Unlock(); return o.err != nil || len(o.events) == count }, 15*time.Second, 10*time.Millisecond, "all exact source CDC envelopes")
	o.mu.Lock()
	require.NoError(t, o.err)
	expected := make(map[string]online.Event, len(o.events))
	for id, event := range o.events {
		expected[id] = event
	}
	o.mu.Unlock()
	rows, err := target.Conn.Query(context.Background(), "SELECT event_id,event::text FROM online_event_ledger WHERE event->>'Operation'<>'READ'")
	require.NoError(t, err)
	seen := 0
	for rows.Next() {
		var id, data string
		require.NoError(t, rows.Scan(&id, &data))
		want, ok := expected[id]
		require.True(t, ok, "unexpected CDC identity %s", id)
		encoded, err := json.Marshal(want)
		require.NoError(t, err)
		require.True(t, losslessJSONEqual(string(encoded), data), "CDC identity/image/order mismatch %s", id)
		seen++
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, count, seen, "complete durable CDC identity coverage")
}
