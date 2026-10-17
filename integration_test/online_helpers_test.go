package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Independent clusters keep target WAL out of source measurements. The image's
// PGDATA volume survives connection/process/container restarts within a test;
// cleanup terminates the fixture only after its assertions finish.
type onlinePGFixture struct {
	Container testcontainers.Container
	DSN       string
	// ServerVersionNum is probed from this cluster, not inferred from its tag.
	ServerVersionNum int
}

func newOnlinePGFixture(t *testing.T) onlinePGFixture { return newOnlinePGFixtureDurability(t, true) }

// The false setting is exclusively a negative control for metadata durability.
func newOnlinePGFixtureDurability(t *testing.T, fsyncEnabled bool, settings ...string) onlinePGFixture {
	t.Helper()
	ctx := context.Background()
	command := []string{"postgres", "-c", fmt.Sprintf("fsync=%t", fsyncEnabled), "-c", "synchronous_commit=on", "-c", "full_page_writes=on", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"}
	for _, setting := range settings {
		command = append(command, "-c", setting)
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: postgresImage(), Env: map[string]string{"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "online_poc"}, ExposedPorts: []string{"5432/tcp"},
		Cmd:        command,
		WaitingFor: wait.ForExec([]string{"pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "online_poc"}).WithStartupTimeout(60 * time.Second),
	}, Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	fixture := onlinePGFixture{Container: container, DSN: fmt.Sprintf("postgres://postgres:postgres@%s/online_poc?sslmode=disable", net.JoinHostPort(host, port.Port()))}
	conn := fixture.Connect(t)
	var fsync, sync, server, wal string
	require.NoError(t, conn.QueryRow(ctx, "SELECT current_setting('fsync'),current_setting('synchronous_commit'),current_setting('server_version_num')::int,version(),current_setting('wal_level')").Scan(&fsync, &sync, &fixture.ServerVersionNum, &server, &wal))
	expectedFsync := "off"
	if fsyncEnabled {
		expectedFsync = "on"
	}
	require.Equal(t, expectedFsync, fsync)
	require.Equal(t, "on", sync)
	expectedMajor, err := strconv.Atoi(strings.SplitN(postgresImageVersion(), ".", 2)[0])
	if err != nil {
		expectedMajor, err = strconv.Atoi(strings.SplitN(postgresImageVersion(), "-", 2)[0])
	}
	require.NoError(t, err, "fixture image tag must identify its major")
	require.Equal(t, expectedMajor, fixture.ServerVersionNum/10000, "image/server major mismatch")
	require.Equal(t, "logical", wal)
	imageEvidence, err := postgresImageEvidence(ctx, container)
	require.NoError(t, err)
	t.Logf("online fixture: image=%s %s server_version_num=%d server=%s effective_online_protocol=1 fsync=%s synchronous_commit=%s wal_level=%s", postgresImage(), imageEvidence, fixture.ServerVersionNum, server, fsync, sync, wal)
	return fixture
}
func (f onlinePGFixture) Connect(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), f.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}
func onlineID(t *testing.T) string {
	t.Helper()
	id, err := online.NewID()
	require.NoError(t, err)
	return id
}
func onlineExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

var errOnlineTargetFault = errors.New("injected durable target fault")

type onlineTargetFault int

const (
	onlineTargetOK onlineTargetFault = iota
	onlineTargetBeforeCommit
	onlineTargetAfterCommit
)

type onlineTarget struct{ Conn *pgx.Conn }

func newOnlineTarget(t *testing.T, f onlinePGFixture) *onlineTarget {
	conn := f.Connect(t)
	onlineExec(t, conn, `CREATE TABLE IF NOT EXISTS online_event_ledger (event_id text PRIMARY KEY,event_hash text NOT NULL,event jsonb NOT NULL);
 CREATE TABLE IF NOT EXISTS online_projection (
  table_schema text NOT NULL,table_name text NOT NULL,row_key bigint NOT NULL,image jsonb,deleted boolean NOT NULL,
  order_lsn pg_lsn NOT NULL,phase smallint NOT NULL,ordinal bigint NOT NULL,
  PRIMARY KEY(table_schema,table_name,row_key));`)
	return &onlineTarget{Conn: conn}
}
func (target *onlineTarget) Apply(ctx context.Context, event online.Event, fault onlineTargetFault) (err error) {
	if err := event.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	image, err := json.Marshal(event.Image)
	if err != nil {
		return err
	}
	tx, err := target.Conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, online.RollbackBounded(tx)) }()
	if _, err = tx.Exec(ctx, `SET LOCAL synchronous_commit=on`); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO online_event_ledger(event_id,event_hash,event) VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`, event.ID, digest, string(data))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var existing string
		if err = tx.QueryRow(ctx, `SELECT event_hash FROM online_event_ledger WHERE event_id=$1`, event.ID).Scan(&existing); err != nil {
			return err
		}
		if existing != digest {
			return fmt.Errorf("event identity reused with different content")
		}
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO online_projection(table_schema,table_name,row_key,image,deleted,order_lsn,phase,ordinal)
   VALUES($1,$2,$3,$4::jsonb,$5,$6::pg_lsn,$7,$8)
   ON CONFLICT(table_schema,table_name,row_key) DO UPDATE SET image=EXCLUDED.image,deleted=EXCLUDED.deleted,order_lsn=EXCLUDED.order_lsn,phase=EXCLUDED.phase,ordinal=EXCLUDED.ordinal
   WHERE (online_projection.order_lsn,online_projection.phase,online_projection.ordinal)<(EXCLUDED.order_lsn,EXCLUDED.phase,EXCLUDED.ordinal)`,
			event.Table.Schema, event.Table.Name, event.Key, string(image), event.Operation == online.Delete, event.Order.EndLSN.String(), int16(event.Order.Phase), int64(event.Order.Ordinal))
		if err != nil {
			return err
		}
	}
	if fault == onlineTargetBeforeCommit {
		return errOnlineTargetFault
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if fault == onlineTargetAfterCommit {
		return errOnlineTargetFault
	}
	return nil
}

// ApplyDelayed holds an old request before its target transaction begins. The
// newer request uses a different connection; releasing this one must not
// overwrite that newer version or tombstone.
func (target *onlineTarget) ApplyDelayed(ctx context.Context, event online.Event, ready chan<- struct{}, release <-chan struct{}) error {
	select {
	case ready <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-release:
		return target.Apply(ctx, event, onlineTargetOK)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (target *onlineTarget) Image(t *testing.T, table online.Table, key int64) (map[string]any, bool, online.OrderToken) {
	t.Helper()
	var data []byte
	var deleted bool
	var lsn string
	var phase int16
	var ordinal int64
	require.NoError(t, target.Conn.QueryRow(context.Background(), `SELECT image,deleted,order_lsn::text,phase,ordinal FROM online_projection WHERE table_schema=$1 AND table_name=$2 AND row_key=$3`, table.Schema, table.Name, key).Scan(&data, &deleted, &lsn, &phase, &ordinal))
	var image map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&image))
	end, err := pq.ParseLSN(lsn)
	require.NoError(t, err)
	return image, deleted, online.OrderToken{EndLSN: end, Phase: uint8(phase), Ordinal: uint32(ordinal)}
}
