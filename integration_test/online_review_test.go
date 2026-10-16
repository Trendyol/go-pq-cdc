package integration

import (
	"context"
	"encoding/json"
	"fmt"
	cdc "github.com/Trendyol/go-pq-cdc"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineLosslessImages(t *testing.T) {
	require.False(t, losslessJSONEqual(`{"n":9007199254740993}`, `{"n":9007199254740992}`), "negative precision oracle")
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	target := newOnlineTarget(t, dest)
	onlineExec(t, conn, `CREATE TABLE precise(id bigint PRIMARY KEY,payload jsonb,j json,f double precision); INSERT INTO precise VALUES(1,'{"n":9007199254740993}','{"n":9223372036854775807}','Infinity');`)
	provisionOnline(t, conn, "precise")
	cfg := onlineConfig(t, source, "precise")
	r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
	waitOnlineSnapshot(t, r)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	compareOnlineTable(t, conn, target, "precise")
	image, _, _ := target.Image(t, online.Table{Schema: "public", Name: "precise"}, 1)
	require.Equal(t, json.Number("9007199254740993"), image["payload"].(map[string]any)["n"])
	require.Equal(t, "Infinity", image["f"])
	// CDC committed response loss, then identical event replay under a new run.
	r = startOnline(t, cfg, func(ctx context.Context, e online.Event) error {
		if e.Operation == online.Update {
			return target.Apply(ctx, e, onlineTargetAfterCommit)
		}
		return target.Apply(ctx, e, onlineTargetOK)
	})
	waitOnlineCapture(t, r)
	onlineExec(t, conn, `UPDATE precise SET payload='{"n":9007199254740995}',f='NaN' WHERE id=1`)
	require.ErrorIs(t, onlineResult(t, r), errOnlineTargetFault)
	onlineEventually(t, func() bool {
		var inactive bool
		err := conn.QueryRow(context.Background(), "SELECT NOT active FROM pg_replication_slots WHERE slot_name='online_slot'").Scan(&inactive)
		return err == nil && inactive
	}, 5*time.Second, 5*time.Millisecond)
	r = startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
	waitOnlineCapture(t, r)
	waitOnlineFence(t, conn)
	onlineExec(t, conn, `INSERT INTO precise VALUES(2,'{"n":9007199254740997}','null','-Infinity')`)
	waitOnlineFence(t, conn)
	r.Cancel()
	require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	compareOnlineTable(t, conn, target, "precise")
	var updates int
	require.NoError(t, target.Conn.QueryRow(context.Background(), `SELECT count(*) FROM online_event_ledger WHERE event->>'Operation'='UPDATE'`).Scan(&updates))
	require.Equal(t, 1, updates, "replayed CDC retains original identity")
}
func TestOnlinePermanentMetadata(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	for _, table := range []string{"schema_version", "windows", "table_progress"} {
		onlineExec(t, conn, "BEGIN")
		if table == "table_progress" {
			onlineExec(t, conn, "ALTER TABLE cdc_online.windows SET UNLOGGED")
		}
		onlineExec(t, conn, "ALTER TABLE cdc_online."+table+" SET UNLOGGED")
		require.Error(t, online.ValidateProvisioned(context.Background(), conn))
		onlineExec(t, conn, "ROLLBACK")
	}
	require.NoError(t, online.ValidateProvisioned(context.Background(), conn))
}

func TestOnlineObserverLifecycle(t *testing.T) {
	for _, phase := range []online.ObservationPhase{online.LOWRecorded, online.CompletionCommitted, online.ACKAdvanced} {
		t.Run(string(phase), func(t *testing.T) {
			source := newOnlinePGFixture(t)
			conn := source.Connect(t)
			onlineExec(t, conn, "CREATE TABLE observed(id bigint PRIMARY KEY,value text); INSERT INTO observed VALUES(1,'one'),(2,'two')")
			provisionOnline(t, conn, "observed")
			cfg := onlineConfig(t, source, "observed")
			sentinel := fmt.Errorf("observer failure at %s", phase)
			reads := 0
			r := startOnline(t, cfg, func(context.Context, online.Event) error { reads++; return nil }, cdc.WithOnlineObserver(func(_ context.Context, o online.Observation) error {
				if o.Phase == phase {
					return sentinel
				}
				return nil
			}))
			require.ErrorIs(t, onlineResult(t, r), sentinel)
			var cursor *int64
			var checkpoint, low, high string
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT p.cursor_key,j.source_lsn::text,w.low_end::text,COALESCE(w.high_end,'0/0'::pg_lsn)::text FROM cdc_online.jobs j JOIN cdc_online.table_progress p USING(job_id) JOIN cdc_online.windows w USING(job_id,table_schema,table_name)").Scan(&cursor, &checkpoint, &low, &high))
			if phase == online.LOWRecorded {
				require.Nil(t, cursor)
				require.Equal(t, low, checkpoint)
				require.Zero(t, reads)
			} else {
				require.NotNil(t, cursor)
				require.Equal(t, int64(2), *cursor)
				require.Equal(t, high, checkpoint)
				require.Equal(t, 2, reads)
				if phase == online.CompletionCommitted {
					require.Less(t, uint64(r.Connector.Stats().SafeACK), parseOnlineLSN(t, high))
				} else {
					require.Equal(t, parseOnlineLSN(t, high), uint64(r.Connector.Stats().SafeACK))
				}
			}
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		source := newOnlinePGFixture(t)
		conn := source.Connect(t)
		onlineExec(t, conn, "CREATE TABLE observed(id bigint PRIMARY KEY)")
		provisionOnline(t, conn, "observed")
		cfg := onlineConfig(t, source, "observed")
		park := make(chan struct{})
		r := startOnline(t, cfg, func(context.Context, online.Event) error { return nil }, cdc.WithOnlineObserver(func(ctx context.Context, o online.Observation) error {
			if o.Phase == online.ReadStarted {
				close(park)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}))
		select {
		case <-park:
		case <-time.After(10 * time.Second):
			t.Fatal("observer did not park")
		}
		r.Cancel()
		require.ErrorIs(t, onlineResult(t, r), context.Canceled)
	})
}

func TestOnlineCatalogCancellation(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := online.ValidatePublication(ctx, conn, "canceled")
	require.ErrorIs(t, err, context.Canceled)
	_, err = online.InspectTable(ctx, conn, online.Table{Schema: "public", Name: "canceled"})
	require.ErrorIs(t, err, context.Canceled)
}
