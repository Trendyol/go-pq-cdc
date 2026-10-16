package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestOnlineSelectFailures(t *testing.T) {
	for _, fault := range []string{"rows.Err", "SELECT timeout"} {
		t.Run(fault, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			conn := source.Connect(t)
			onlineExec(t, conn, "CREATE TABLE fault_rows(id bigint PRIMARY KEY,value text); INSERT INTO fault_rows VALUES(1,'one'),(2,'two')")
			provisionOnline(t, conn, "fault_rows")
			cfg := onlineConfig(t, source, "fault_rows")
			var opts []cdc.OnlineConnectorOption
			var held <-chan struct{}
			if fault == "rows.Err" {
				proxy, fired := onlineFaultProxy(t, source.DSN, faultReadRows)
				held = fired
				parsed, err := pgx.ParseConfig(proxy)
				require.NoError(t, err)
				cfg.Host = parsed.Host
				cfg.Port = int(parsed.Port)
			} else {
				parked, release := make(chan struct{}), make(chan struct{})
				cfg.Snapshot.Online.QueryTimeout = 250 * time.Millisecond
				opts = append(opts, cdc.WithOnlineObserver(func(ctx context.Context, o online.Observation) error {
					if o.Phase == online.ReadStarted {
						close(parked)
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}))
				held = parked
				// Acquire after read-start observation, after all schema checks, so this is
				// SELECT's AccessShareLock wait, not a marker/catalog timeout.
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				r := startOnline(t, cfg, func(context.Context, online.Event) error { t.Error("partial READ escaped"); return nil }, opts...)
				select {
				case <-parked:
				case <-time.After(10 * time.Second):
					t.Fatal("read entry timeout")
				}
				onlineExec(t, conn, "BEGIN; LOCK fault_rows IN ACCESS EXCLUSIVE MODE")
				close(release)
				err := onlineResult(t, r)
				require.ErrorIs(t, err, online.ErrRead)
				onlineExec(t, conn, "ROLLBACK")
				assertReadFailed(t, conn)
				return
			}
			reads := 0
			r := startOnline(t, cfg, func(context.Context, online.Event) error { reads++; return nil }, opts...)
			require.ErrorIs(t, onlineResult(t, r), online.ErrRead)
			select {
			case <-held:
			default:
				t.Fatal("no actual result-stream fault")
			}
			require.Zero(t, reads)
			assertReadFailed(t, conn)
			// Fresh connection/attempt reconstructs the entire discarded partial page.
			cfg = onlineConfig(t, source, "fault_rows")
			r = startOnline(t, cfg, func(context.Context, online.Event) error { return nil })
			waitOnlineSnapshot(t, r)
			r.Cancel()
			require.ErrorIs(t, onlineResult(t, r), context.Canceled)
		})
	}
}
func assertReadFailed(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	var cursor *int64
	var complete, unsafe bool
	var status, checkpoint, low string
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT p.cursor_key,p.complete,w.status,j.source_lsn::text,w.low_end::text,s.confirmed_flush_lsn>j.source_lsn FROM cdc_online.table_progress p JOIN cdc_online.jobs j USING(job_id) JOIN cdc_online.windows w USING(job_id,table_schema,table_name) JOIN pg_replication_slots s ON s.slot_name=j.slot_name ORDER BY w.generation DESC LIMIT 1`).Scan(&cursor, &complete, &status, &checkpoint, &low, &unsafe))
	require.Nil(t, cursor)
	require.False(t, complete)
	require.Equal(t, "abandoned", status)
	require.Equal(t, low, checkpoint)
	require.False(t, unsafe)
}

func TestOnlineMetadataCommitResponse(t *testing.T) {
	for _, fault := range []onlineWireFault{faultCommitReply, faultBeforeCommit} {
		t.Run(string(fault), func(t *testing.T) {
			source := newOnlinePGFixture(t)
			dest := newOnlinePGFixture(t)
			conn := source.Connect(t)
			target := newOnlineTarget(t, dest)
			onlineExec(t, conn, "CREATE TABLE commit_rows(id bigint PRIMARY KEY,value text); INSERT INTO commit_rows VALUES(1,'one'),(2,'two'),(3,'three')")
			provisionOnline(t, conn, "commit_rows")
			cfg := onlineConfig(t, source, "commit_rows")
			proxy, fired := onlineFaultProxy(t, source.DSN, fault)
			parsed, err := pgx.ParseConfig(proxy)
			require.NoError(t, err)
			cfg.Host = parsed.Host
			cfg.Port = int(parsed.Port)
			r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
			require.ErrorIs(t, onlineResult(t, r), online.ErrOwnerLost)
			select {
			case <-fired:
			default:
				t.Fatal("completion COMMIT was not cut")
			}
			var id, status, checkpoint, low, high string
			var cursor *int64
			require.NoError(t, conn.QueryRow(context.Background(), `SELECT w.attempt_id::text,w.status,j.source_lsn::text,w.low_end::text,w.high_end::text,p.cursor_key FROM cdc_online.windows w JOIN cdc_online.jobs j USING(job_id) JOIN cdc_online.table_progress p USING(job_id,table_schema,table_name)`).Scan(&id, &status, &checkpoint, &low, &high, &cursor))
			if fault == faultCommitReply {
				require.Equal(t, "completed", status)
				require.Equal(t, high, checkpoint)
				require.NotNil(t, cursor)
				require.Equal(t, int64(2), *cursor)
			} else {
				require.Equal(t, "high", status)
				require.Equal(t, low, checkpoint)
				require.Nil(t, cursor)
			}
			// No in-process ACK of the ambiguous completion, even in the committed case.
			require.LessOrEqual(t, uint64(r.Connector.Stats().SafeACK), parseOnlineLSN(t, low))
			cfg = onlineConfig(t, source, "commit_rows")
			r = startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
			waitOnlineSnapshot(t, r)
			waitOnlineFence(t, conn)
			r.Cancel()
			require.ErrorIs(t, onlineResult(t, r), context.Canceled)
			compareOnlineTable(t, conn, target, "commit_rows")
			var preserved string
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT status FROM cdc_online.windows WHERE attempt_id=$1::uuid", id).Scan(&preserved))
			if fault == faultCommitReply {
				require.Equal(t, "completed", preserved)
			} else {
				require.Equal(t, "abandoned", preserved)
			}
		})
	}
}
func TestOnlineRollbackResponseLoss(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	proxy, fired := onlineFaultProxy(t, source.DSN, faultRollbackReply)
	owner, err := online.OpenStoreWithStartupTimeout(context.Background(), proxy, "rollback_owner", time.Second)
	require.NoError(t, err)
	started := time.Now()
	_, err = owner.LoadJob(context.Background())
	require.Error(t, err)
	require.Less(t, time.Since(started), 3*time.Second)
	require.ErrorIs(t, owner.Err(), online.ErrOwnerLost)
	select {
	case <-fired:
	default:
		t.Fatal("rollback reply not held")
	}
	require.NoError(t, owner.Close(context.Background()))
	dest := newOnlinePGFixture(t)
	target := newOnlineTarget(t, dest)
	targetProxy, fired := onlineFaultProxy(t, dest.DSN, faultRollbackReply)
	raw, err := pgx.Connect(context.Background(), targetProxy)
	require.NoError(t, err)
	faultTarget := &onlineTarget{Conn: raw}
	started = time.Now()
	err = faultTarget.Apply(context.Background(), online.Event{ID: onlineID(t), Table: online.Table{Schema: "public", Name: "x"}, Key: 1, Operation: online.Read, Image: map[string]any{"id": 1}, Order: online.OrderToken{EndLSN: 100, Phase: 1}}, onlineTargetBeforeCommit)
	require.ErrorIs(t, err, errOnlineTargetFault)
	require.Less(t, time.Since(started), 3*time.Second)
	require.True(t, raw.IsClosed())
	select {
	case <-fired:
	default:
		t.Fatal("target rollback reply not held")
	}
	var count int
	require.NoError(t, target.Conn.QueryRow(context.Background(), "SELECT count(*) FROM online_event_ledger").Scan(&count))
	require.Zero(t, count)
}
func TestOnlineWireBudget(t *testing.T) {
	for _, path := range []string{"READ", "CDC"} {
		t.Run(path, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			conn := source.Connect(t)
			onlineExec(t, conn, "CREATE TABLE wire_rows(id bigint PRIMARY KEY,value text)")
			provisionOnline(t, conn, "wire_rows")
			cfg := onlineConfig(t, source, "wire_rows")
			cfg.Snapshot.Online.ChunkBytes = 1024
			cfg.Snapshot.Online.TransactionBytes = 4096
			cfg.Snapshot.Online.QueueBytes = 8192
			if path == "READ" {
				onlineExec(t, conn, "INSERT INTO wire_rows VALUES(1,repeat('x',131072))")
			}
			count := 0
			r := startOnline(t, cfg, func(context.Context, online.Event) error { count++; return nil })
			if path == "CDC" {
				waitOnlineSnapshot(t, r)
				onlineExec(t, conn, "INSERT INTO wire_rows VALUES(1,repeat('x',131072))")
			}
			require.Error(t, onlineResult(t, r))
			require.Zero(t, count)
			require.LessOrEqual(t, r.Connector.Stats().PeakTransactionEncodedBytes, int64(4096))
		})
	}
}

func parseOnlineLSN(t *testing.T, value string) uint64 {
	t.Helper()
	lsn, err := pq.ParseLSN(value)
	require.NoError(t, err)
	return uint64(lsn)
}
