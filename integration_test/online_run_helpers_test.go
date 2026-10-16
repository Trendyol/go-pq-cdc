package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func onlineConfig(t *testing.T, fixture onlinePGFixture, tables ...string) config.Config {
	t.Helper()
	parsed, err := pgx.ParseConfig(fixture.DSN)
	require.NoError(t, err)
	cfg := config.Config{Host: parsed.Host, Port: int(parsed.Port), Username: parsed.User, Password: parsed.Password, Database: parsed.Database, Publication: publication.Config{Name: "online_pub", CreateIfNotExists: true, Operations: publication.Operations{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}}, Slot: slot.Config{Name: "online_slot", ProtoVersion: 1, CreateIfNotExists: true}, Snapshot: config.SnapshotConfig{Enabled: true, Mode: config.SnapshotModeInitial, Strategy: config.SnapshotStrategyOnline, ChunkSize: 2, Online: config.OnlineSnapshotConfig{QueryTimeout: 3 * time.Second, DeliveryTimeout: 3 * time.Second, CheckpointInterval: 20 * time.Millisecond, MarkerInterval: 50 * time.Millisecond}}}
	for _, name := range tables {
		cfg.Publication.Tables = append(cfg.Publication.Tables, publication.Table{Schema: "public", Name: name, ReplicaIdentity: "FULL"})
	}
	cfg.SetDefault()
	return cfg
}
func provisionOnline(t *testing.T, conn *pgx.Conn, tables ...string) {
	t.Helper()
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	for _, name := range tables {
		onlineExec(t, conn, "ALTER TABLE "+pgx.Identifier{"public", name}.Sanitize()+" REPLICA IDENTITY FULL")
	}
}

type onlineRun struct {
	Connector *cdc.OnlineConnector
	Cancel    context.CancelFunc
	Result    chan error
}

func startOnline(t *testing.T, cfg config.Config, handler cdc.OnlineHandler, opts ...cdc.OnlineConnectorOption) onlineRun {
	t.Helper()
	c, err := cdc.NewOnlineConnector(context.Background(), cfg, handler, opts...)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-c.Done():
		case <-time.After(10 * time.Second):
			t.Error("online Run cleanup timeout")
		}
	})
	return onlineRun{c, cancel, result}
}
func waitOnlineCapture(t *testing.T, r onlineRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, r.Connector.WaitUntilCaptureReady(ctx))
}
func waitOnlineSnapshot(t *testing.T, r onlineRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, r.Connector.WaitUntilSnapshotComplete(ctx))
}
func waitOnlineFence(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	var fence string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&fence))
	onlineExec(t, conn, "UPDATE cdc_online.watermarks SET phase='HEARTBEAT',attempt_id=NULL,marker_id=$1::uuid", onlineID(t))
	onlineEventually(t, func() bool {
		var passed bool
		err := conn.QueryRow(context.Background(), "SELECT source_lsn >= $1::pg_lsn FROM cdc_online.jobs", fence).Scan(&passed)
		return err == nil && passed
	}, 10*time.Second, 10*time.Millisecond, "final delivery checkpoint fence %s", fence)
}
func compareOnlineTable(t *testing.T, source *pgx.Conn, target *onlineTarget, table string) {
	t.Helper()
	qualified := pgx.Identifier{"public", table}.Sanitize()
	fields, err := source.Query(context.Background(), "SELECT * FROM "+qualified+" LIMIT 0")
	require.NoError(t, err)
	names := map[string]bool{}
	for _, field := range fields.FieldDescriptions() {
		names[field.Name] = true
	}
	fields.Close()
	require.NoError(t, fields.Err())
	// PostgreSQL resolves an unqualified name as a column before a whole-row
	// alias. Avoid alias/column collisions instead of comparing a scalar/null.
	alias := "_cdc_online_oracle_row"
	for names[alias] {
		alias += "_"
	}
	quoted := pgx.Identifier{alias}.Sanitize()
	rows, err := source.Query(context.Background(), "SELECT "+quoted+".id::bigint,to_jsonb("+quoted+")::text FROM "+qualified+" AS "+quoted+" ORDER BY "+quoted+".id")
	require.NoError(t, err)
	expected := map[int64]json.RawMessage{}
	for rows.Next() {
		var key int64
		var data string
		require.NoError(t, rows.Scan(&key, &data))
		expected[key] = json.RawMessage(data)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	actual, err := target.Conn.Query(context.Background(), "SELECT row_key,image::text FROM online_projection WHERE table_schema='public' AND table_name=$1 AND NOT deleted ORDER BY row_key", table)
	require.NoError(t, err)
	seen := 0
	for actual.Next() {
		var key int64
		var image string
		require.NoError(t, actual.Scan(&key, &image))
		want, ok := expected[key]
		require.True(t, ok, "unexpected target key %d", key)
		require.True(t, losslessJSONEqual(string(want), image), "exact full image mismatch key %d: source=%s target=%s", key, want, image)
		seen++
	}
	require.NoError(t, actual.Err())
	actual.Close()
	require.Equal(t, len(expected), seen, "exact final-fence row count")
}
func installHighBarrier(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	onlineExec(t, conn, `CREATE FUNCTION online_hold_high() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='HIGH' THEN PERFORM pg_advisory_xact_lock(8791); END IF; RETURN NEW; END $$; CREATE TRIGGER online_hold_high BEFORE UPDATE ON cdc_online.watermarks FOR EACH ROW EXECUTE FUNCTION online_hold_high();`)
	onlineExec(t, conn, "SELECT pg_advisory_lock(8791)")
}
func waitHighBarrier(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	onlineEventually(t, func() bool {
		var n int
		err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'UPDATE cdc_online.watermarks%'").Scan(&n)
		return err == nil && n > 0
	}, 10*time.Second, 5*time.Millisecond, "HIGH must wait after SELECT")
}
func onlineResult(t *testing.T, r onlineRun) error {
	t.Helper()
	select {
	case err := <-r.Result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("terminal error timeout")
		return fmt.Errorf("timeout")
	}
}

// Unlike testify's asynchronous polling, this never leaves a SQL condition
// goroutine using a pgx connection after a deadline or fixture cleanup.
func onlineEventually(t *testing.T, condition func() bool, timeout, interval time.Duration, message ...any) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("condition deadline exceeded: %v", message)
}

func losslessJSONEqual(a, b string) bool {
	decode := func(text string) (any, error) {
		var value any
		decoder := json.NewDecoder(bytes.NewBufferString(text))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("oracle input must contain exactly one JSON value")
		}
		return normalizeJSONNumbers(value), nil
	}
	left, err := decode(a)
	if err != nil {
		return false
	}
	right, err := decode(b)
	return err == nil && reflect.DeepEqual(left, right)
}

// Keep normalized numbers dynamically distinct from JSON strings. An ordinary
// string such as "1/2" must never compare equal to the JSON number 0.5.
type normalizedJSONNumber string

func normalizeJSONNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		if rational, ok := new(big.Rat).SetString(v.String()); ok {
			return normalizedJSONNumber(rational.RatString())
		}
		return v
	case []any:
		for i, element := range v {
			v[i] = normalizeJSONNumbers(element)
		}
	case map[string]any:
		for key, element := range v {
			v[key] = normalizeJSONNumbers(element)
		}
	}
	return value
}
