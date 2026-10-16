package integration

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestOnlineTargetContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := newOnlinePGFixture(t)
	fixture := newOnlinePGFixture(t)
	require.NotEqual(t, source.Container.GetContainerID(), fixture.Container.GetContainerID())
	target := newOnlineTarget(t, fixture)
	onlineExec(t, target.Conn, `SET synchronous_commit=off; CREATE FUNCTION public.require_target_sync() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') <> 'on' THEN RAISE EXCEPTION 'target commit must be synchronous'; END IF; RETURN NEW; END $$; CREATE TRIGGER target_sync BEFORE INSERT ON online_event_ledger FOR EACH ROW EXECUTE FUNCTION public.require_target_sync()`)
	table := online.Table{Schema: "public", Name: "items"}
	event := online.Event{ID: "first", Table: table, Key: -1, Operation: online.Insert, Image: map[string]any{"id": -1, "name": "first", "nullable": nil, "extra": "complete"}, CommitLSN: 99, Order: online.OrderToken{EndLSN: 100}}
	require.ErrorIs(t, target.Apply(ctx, event, onlineTargetBeforeCommit), errOnlineTargetFault)
	var ledger, projection int
	require.NoError(t, target.Conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM online_event_ledger),(SELECT count(*) FROM online_projection)`).Scan(&ledger, &projection))
	require.Zero(t, ledger)
	require.Zero(t, projection)
	require.ErrorIs(t, target.Apply(ctx, event, onlineTargetAfterCommit), errOnlineTargetFault)
	require.NoError(t, target.Apply(ctx, event, onlineTargetOK))
	image, deleted, token := target.Image(t, table, -1)
	require.False(t, deleted)
	require.Equal(t, event.Order, token)
	require.Equal(t, map[string]any{"id": json.Number("-1"), "name": "first", "nullable": nil, "extra": "complete"}, image)
	bad := event
	bad.Image = map[string]any{"id": -1}
	require.ErrorContains(t, target.Apply(ctx, bad, onlineTargetOK), "identity reused")
	read := event
	read.ID = "read"
	read.Operation = online.Read
	read.CommitLSN = 0
	read.Order.Phase = 1
	read.Image = map[string]any{"id": -1, "name": "read", "nullable": nil}
	require.NoError(t, target.Apply(ctx, read, onlineTargetOK))
	old := read
	old.ID = "late-old-read"
	delayed := newOnlineTarget(t, fixture)
	ready, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- delayed.ApplyDelayed(ctx, old, ready, release) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("delayed target request did not start")
	}
	newer := event
	newer.ID = "update"
	newer.Operation = online.Update
	newer.Order = online.OrderToken{EndLSN: 101, Ordinal: 2}
	newer.Image = map[string]any{"id": -1, "name": "new", "nullable": nil}
	require.NoError(t, target.Apply(ctx, newer, onlineTargetOK))
	close(release)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("delayed target request did not complete")
	}
	require.NoError(t, target.Apply(ctx, event, onlineTargetOK))
	image, deleted, token = target.Image(t, table, -1)
	require.False(t, deleted)
	require.Equal(t, newer.Order, token)
	require.Equal(t, "new", image["name"])
	deletion := event
	deletion.ID = "delete"
	deletion.Operation = online.Delete
	deletion.Order = online.OrderToken{EndLSN: 102}
	deletion.Image = nil
	require.NoError(t, target.Apply(ctx, deletion, onlineTargetOK))
	old.ID = "late-after-delete"
	require.NoError(t, target.Apply(ctx, old, onlineTargetOK))
	require.NoError(t, target.Apply(ctx, deletion, onlineTargetOK))
	image, deleted, token = target.Image(t, table, -1)
	require.True(t, deleted)
	require.Nil(t, image)
	require.Equal(t, deletion.Order, token)
	newer.ID = "reinsert"
	newer.Operation = online.Insert
	newer.Order = online.OrderToken{EndLSN: pq.LSN(1 << 63), Ordinal: 3}
	require.NoError(t, target.Apply(ctx, newer, onlineTargetOK))
	require.NoError(t, target.Apply(ctx, deletion, onlineTargetOK))
	image, deleted, token = target.Image(t, table, -1)
	require.False(t, deleted)
	require.Equal(t, newer.Order, token)
	require.Equal(t, "new", image["name"])
	other := event
	other.ID = "other-table"
	other.Table.Name = "other"
	require.NoError(t, target.Apply(ctx, other, onlineTargetOK))
	_, _, token = target.Image(t, other.Table, -1)
	require.Equal(t, other.Order, token)
	numeric := event
	numeric.ID = "numeric-image"
	numeric.Key = 9223372036854775807
	numeric.Image = map[string]any{"id": int64(9223372036854775807), "minimum": int64(-9223372036854775808), "nullable": nil, "large": strings.Repeat("full-image", 10000)}
	require.NoError(t, target.Apply(ctx, numeric, onlineTargetOK))
	image, deleted, _ = target.Image(t, table, numeric.Key)
	require.False(t, deleted)
	require.Equal(t, json.Number("9223372036854775807"), image["id"])
	require.Equal(t, json.Number("-9223372036854775808"), image["minimum"])
	require.Nil(t, image["nullable"])
	require.Equal(t, numeric.Image["large"], image["large"])
	// Close and restart the target server without recreating its data directory.
	require.NoError(t, target.Conn.Close(ctx))
	require.NoError(t, fixture.Container.Stop(ctx, nil))
	require.NoError(t, fixture.Container.Start(ctx))
	target = newOnlineTarget(t, fixture)
	require.NoError(t, target.Apply(ctx, newer, onlineTargetOK))
	image, deleted, token = target.Image(t, table, -1)
	require.False(t, deleted)
	require.Equal(t, newer.Order, token)
	require.Equal(t, "new", image["name"])
	require.NoError(t, target.Conn.QueryRow(ctx, `SELECT count(*) FROM online_event_ledger`).Scan(&ledger))
	require.Equal(t, 9, ledger)
}

func TestOnlineMetadataAttempts(t *testing.T) {
	ctx := context.Background()
	fixture := newOnlinePGFixture(t)
	admin := fixture.Connect(t)
	// A quoted, non-superuser role demonstrates manual provisioning and grants.
	role := `cdc"restricted`
	q := pgx.Identifier{role}.Sanitize()
	onlineExec(t, admin, "CREATE ROLE "+q+" LOGIN PASSWORD 'postgres'")
	restrictedConfig, err := pgx.ParseConfig(fixture.DSN)
	require.NoError(t, err)
	restrictedConfig.User = role
	restricted, err := pgx.ConnectConfig(ctx, restrictedConfig)
	require.NoError(t, err)
	defer restricted.Close(ctx)
	var systemID, adminSystemID string
	// PG16 images may grant this function to PUBLIC. Remove that grant only
	// inside this fixture to exercise the reader's restricted-role denial path.
	onlineExec(t, admin, `REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC`)
	require.Error(t, restricted.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&systemID))
	onlineExec(t, admin, "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "+q)
	require.NoError(t, restricted.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&systemID))
	require.NoError(t, admin.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&adminSystemID))
	require.Equal(t, adminSystemID, systemID)
	require.Error(t, online.ValidateProvisioned(ctx, restricted))
	sql, err := online.ProvisionSQL(role)
	require.NoError(t, err)
	onlineExec(t, admin, sql)
	onlineExec(t, admin, sql)
	require.NoError(t, online.ValidateProvisioned(ctx, restricted))
	onlineExec(t, admin, "REVOKE UPDATE ON cdc_online.windows FROM "+q)
	require.ErrorContains(t, online.ValidateProvisioned(ctx, restricted), "grant USAGE")
	onlineExec(t, admin, "GRANT UPDATE ON cdc_online.windows TO "+q)
	require.NoError(t, online.ValidateProvisioned(ctx, restricted))
	var payloads int
	require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='cdc_online' AND (data_type IN ('json','jsonb','bytea') OR column_name LIKE '%payload%')`).Scan(&payloads))
	require.Zero(t, payloads)
	onlineExec(t, admin, `CREATE TABLE public.items(id bigint PRIMARY KEY,name text); CREATE PUBLICATION "online" FOR TABLE public.items;`)
	require.ErrorContains(t, online.EnsureWatermarkPublication(ctx, restricted, "online", false), "ALTER PUBLICATION")
	require.Error(t, online.EnsureWatermarkPublication(ctx, restricted, "online", true))
	require.NoError(t, online.EnsureWatermarkPublication(ctx, admin, "online", true))
	require.NoError(t, online.EnsureWatermarkPublication(ctx, restricted, "online", false))
	onlineExec(t, admin, `CREATE PUBLICATION online_columns FOR TABLE cdc_online.watermarks(connector_id,job_id)`)
	require.ErrorContains(t, online.EnsureWatermarkPublication(ctx, admin, "online_columns", false), "complete unfiltered")
	onlineExec(t, admin, `CREATE PUBLICATION online_filtered FOR TABLE cdc_online.watermarks WHERE (generation > 1)`)
	require.ErrorContains(t, online.EnsureWatermarkPublication(ctx, admin, "online_filtered", false), "complete unfiltered")
	onlineExec(t, admin, `DROP PUBLICATION online_columns; DROP PUBLICATION online_filtered;`)
	onlineExec(t, admin, `ALTER PUBLICATION "online" ADD TABLE cdc_online.jobs`)
	require.ErrorContains(t, online.EnsureWatermarkPublication(ctx, admin, "online", true), "forbidden metadata")
	onlineExec(t, admin, `ALTER PUBLICATION "online" DROP TABLE cdc_online.jobs`)
	onlineExec(t, admin, `CREATE PUBLICATION online_broad FOR TABLES IN SCHEMA cdc_online`)
	require.Error(t, online.EnsureWatermarkPublication(ctx, admin, "online_broad", false))
	onlineExec(t, admin, `DROP PUBLICATION online_broad`)
	restrictedURL, err := url.Parse(fixture.DSN)
	require.NoError(t, err)
	restrictedURL.User = url.UserPassword(role, "postgres")
	params := restrictedURL.Query()
	params.Set("synchronous_commit", "off")
	restrictedURL.RawQuery = params.Encode()
	restrictedDSN := restrictedURL.String()
	onlineExec(t, restricted, `SET synchronous_commit=off`)
	onlineExec(t, admin, `CREATE FUNCTION public.require_metadata_sync() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') <> 'on' THEN RAISE EXCEPTION 'metadata commit must be synchronous'; END IF; RETURN NEW; END $$; CREATE TRIGGER metadata_sync BEFORE INSERT OR UPDATE ON cdc_online.jobs FOR EACH ROW EXECUTE FUNCTION public.require_metadata_sync(); CREATE TRIGGER window_sync BEFORE INSERT OR UPDATE ON cdc_online.windows FOR EACH ROW EXECUTE FUNCTION public.require_metadata_sync(); CREATE TRIGGER marker_sync BEFORE INSERT OR UPDATE ON cdc_online.watermarks FOR EACH ROW EXECUTE FUNCTION public.require_metadata_sync()`)
	store, err := online.OpenStore(ctx, restrictedDSN, "slot")
	require.NoError(t, err)
	defer store.Close(ctx)
	var ownerUser string
	require.NoError(t, admin.QueryRow(ctx, `SELECT usename FROM pg_stat_activity WHERE pid=$1`, int32(store.BackendPID())).Scan(&ownerUser))
	require.Equal(t, role, ownerUser)
	_, err = online.OpenStore(ctx, restrictedDSN, "slot")
	require.ErrorIs(t, err, online.ErrOwned)
	job := online.Job{ID: onlineID(t), ConnectorID: onlineID(t), Slot: "slot", SlotBinding: onlineID(t), Source: online.SourceIdentity{SystemID: systemID, Database: "online_poc", Timeline: 1}, Generation: 1, ConfigFingerprint: "cfg", Status: "active", Checkpoint: 100}
	require.NoError(t, store.CreateJob(ctx, job))
	loaded, err := store.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, job, loaded)
	upper := int64(10)
	table := online.Table{Schema: "public", Name: "items"}
	progress := online.TableProgress{JobID: job.ID, Table: table, SchemaFingerprint: "schema", KeyFingerprint: "key", UpperKey: &upper}
	require.NoError(t, store.InitTable(ctx, job.ID, 1, progress))
	attempt := online.Window{ID: onlineID(t), JobID: job.ID, Generation: 1, Table: table, LowID: onlineID(t), HighID: onlineID(t)}
	require.NoError(t, store.RegisterAttempt(ctx, attempt))
	another := attempt
	another.ID = onlineID(t)
	another.LowID = onlineID(t)
	another.HighID = onlineID(t)
	require.Error(t, store.RegisterAttempt(ctx, another))
	require.ErrorIs(t, store.Checkpoint(ctx, job.ID, 1, 101), online.ErrConditional)
	require.ErrorIs(t, store.RecordLOW(ctx, attempt.ID, 1, attempt.LowID, 110), online.ErrConditional)
	marker, err := store.EmitLOW(ctx, restricted, attempt.ID, 1)
	require.NoError(t, err)
	require.Equal(t, online.Low, marker.Phase)
	require.Equal(t, attempt.LowID, marker.ID)
	_, err = store.EmitLOW(ctx, restricted, attempt.ID, 1)
	require.ErrorIs(t, err, online.ErrConditional)
	var phase string
	require.NoError(t, admin.QueryRow(ctx, `SELECT phase FROM cdc_online.watermarks WHERE connector_id=$1::uuid`, job.ConnectorID).Scan(&phase))
	require.Equal(t, "LOW", phase)
	require.NoError(t, store.RecordLOW(ctx, attempt.ID, 1, attempt.LowID, 110))
	loaded, err = store.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, pq.LSN(110), loaded.Checkpoint)
	_, err = store.EmitHeartbeat(ctx, restricted, job.ID, 1)
	require.ErrorIs(t, err, online.ErrConditional)
	_, err = store.EmitHIGH(ctx, restricted, attempt.ID, 1)
	require.NoError(t, err)
	last := int64(-9223372036854775808)
	require.NoError(t, store.RecordHIGH(ctx, attempt.ID, 1, attempt.HighID, 120, &last))
	// A source exception after all writes but before commit proves atomic rollback.
	onlineExec(t, admin, `CREATE FUNCTION public.reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_lsn='0/78'::pg_lsn THEN RAISE EXCEPTION 'checkpoint fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER checkpoint_fault BEFORE UPDATE ON cdc_online.jobs FOR EACH ROW EXECUTE FUNCTION public.reject_checkpoint()`)
	require.ErrorContains(t, store.CompleteAttempt(ctx, attempt.ID, 1, false), "checkpoint fault")
	current, err := store.GetAttempt(ctx, attempt.ID)
	require.NoError(t, err)
	require.Equal(t, "high", current.Status)
	p, err := store.GetTable(ctx, job.ID, table)
	require.NoError(t, err)
	require.Nil(t, p.Cursor)
	loaded, err = store.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, pq.LSN(110), loaded.Checkpoint)
	onlineExec(t, admin, `DROP TRIGGER checkpoint_fault ON cdc_online.jobs`)
	require.NoError(t, store.CompleteAttempt(ctx, attempt.ID, 1, false))
	// A caller with an unknown response resolves state using the original UUID.
	current, err = store.GetAttempt(ctx, attempt.ID)
	require.NoError(t, err)
	require.Equal(t, "completed", current.Status)
	p, err = store.GetTable(ctx, job.ID, table)
	require.NoError(t, err)
	require.Equal(t, &last, p.Cursor)
	loaded, err = store.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, pq.LSN(120), loaded.Checkpoint)
	require.ErrorIs(t, store.CompleteAttempt(ctx, attempt.ID, 1, true), online.ErrConditional)
	attempt2 := another
	attempt2.PreviousCursor = &last
	require.NoError(t, store.RegisterAttempt(ctx, attempt2))
	_, err = store.EmitLOW(ctx, restricted, attempt2.ID, 1)
	require.NoError(t, err)
	require.NoError(t, store.RecordLOW(ctx, attempt2.ID, 1, attempt2.LowID, 130))
	resumed, err := store.ResumeJob(ctx, job)
	require.NoError(t, err)
	require.Equal(t, int64(2), resumed.Generation)
	current, err = store.GetAttempt(ctx, attempt2.ID)
	require.NoError(t, err)
	require.Equal(t, "abandoned", current.Status)
	require.ErrorIs(t, store.CompleteAttempt(ctx, attempt2.ID, 1, true), online.ErrConditional)
	_, err = store.EmitHIGH(ctx, restricted, attempt2.ID, 1)
	require.ErrorIs(t, err, online.ErrConditional)
	changed := job
	changed.SlotBinding = onlineID(t)
	_, err = store.ResumeJob(ctx, changed)
	require.ErrorIs(t, err, online.ErrBinding)
	require.NoError(t, store.Checkpoint(ctx, job.ID, 2, 140))
	require.ErrorIs(t, store.Checkpoint(ctx, job.ID, 2, 139), online.ErrConditional)
	_, err = store.EmitHeartbeat(ctx, restricted, job.ID, 2)
	require.NoError(t, err)
	attempt3 := online.Window{ID: onlineID(t), JobID: job.ID, Generation: 2, Table: table, PreviousCursor: &last, LowID: onlineID(t), HighID: onlineID(t)}
	require.NoError(t, store.RegisterAttempt(ctx, attempt3))
	_, err = store.EmitLOW(ctx, restricted, attempt3.ID, 2)
	require.NoError(t, err)
	require.NoError(t, store.RecordLOW(ctx, attempt3.ID, 2, attempt3.LowID, 150))
	_, err = store.EmitHIGH(ctx, restricted, attempt3.ID, 2)
	require.NoError(t, err)
	require.NoError(t, store.RecordHIGH(ctx, attempt3.ID, 2, attempt3.HighID, 160, nil))
	require.NoError(t, store.CompleteAttempt(ctx, attempt3.ID, 2, true))
	p, err = store.GetTable(ctx, job.ID, table)
	require.NoError(t, err)
	require.True(t, p.Complete)
	require.Equal(t, &last, p.Cursor)
	require.NoError(t, store.MarkSnapshotComplete(ctx, job.ID, 2))
	// Terminate only the isolated owner's backend; no transparent reconnect.
	var killed bool
	require.NoError(t, admin.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int32(store.BackendPID())).Scan(&killed))
	require.True(t, killed)
	select {
	case <-store.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("owner loss was not signaled")
	}
	require.ErrorIs(t, store.Err(), online.ErrOwnerLost)
	_, err = store.LoadJob(ctx)
	require.ErrorIs(t, err, online.ErrOwnerLost)
	require.NoError(t, store.Close(ctx))
	replacement, err := online.OpenStore(ctx, restrictedDSN, "slot")
	require.NoError(t, err)
	defer replacement.Close(ctx)
	loaded, err = replacement.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, pq.LSN(160), loaded.Checkpoint)
	require.Equal(t, "snapshot_complete", loaded.Status)
	require.False(t, strings.Contains(sql, "jsonb"))
}

func TestOnlineMetadataBoundaries(t *testing.T) {
	ctx := context.Background()
	fixture := newOnlinePGFixture(t)
	sql := fixture.Connect(t)
	require.NoError(t, online.Provision(ctx, sql, ""))
	unsafeFixture := newOnlinePGFixtureDurability(t, false)
	unsafeSQL := unsafeFixture.Connect(t)
	require.NoError(t, online.Provision(ctx, unsafeSQL, ""))
	require.ErrorContains(t, online.ValidateProvisioned(ctx, unsafeSQL), "fsync=on")
	_, err := online.OpenStore(ctx, unsafeFixture.DSN, "unsafe")
	require.ErrorContains(t, err, "fsync=on")
	store, err := online.OpenStore(ctx, fixture.DSN, "boundaries")
	require.NoError(t, err)
	defer store.Close(ctx)
	job := online.Job{ID: onlineID(t), ConnectorID: onlineID(t), Slot: "boundaries", SlotBinding: onlineID(t), Source: online.SourceIdentity{SystemID: "123", Database: "online_poc", Timeline: 1}, Generation: 1, ConfigFingerprint: "cfg", Status: "active", Checkpoint: 100}
	require.NoError(t, store.CreateJob(ctx, job))
	upper := int64(0)
	minimum := int64(-9223372036854775808)
	table := online.Table{Schema: `quoted"schema`, Name: "integer table"}
	require.NoError(t, store.InitTable(ctx, job.ID, 1, online.TableProgress{JobID: job.ID, Table: table, SchemaFingerprint: "schema", KeyFingerprint: "key", UpperKey: &upper}))
	empty := online.Table{Schema: "public", Name: "empty"}
	require.NoError(t, store.InitTable(ctx, job.ID, 1, online.TableProgress{JobID: job.ID, Table: empty, SchemaFingerprint: "schema", KeyFingerprint: "key"}))
	tables, err := store.Tables(ctx, job.ID)
	require.NoError(t, err)
	require.Len(t, tables, 2)
	require.ErrorIs(t, store.MarkSnapshotComplete(ctx, job.ID, 1), online.ErrConditional)
	attempt := online.Window{ID: onlineID(t), JobID: job.ID, Generation: 1, Table: table, LowID: onlineID(t), HighID: onlineID(t)}
	bad := attempt
	bad.PreviousCursor = &minimum
	require.ErrorIs(t, store.RegisterAttempt(ctx, bad), online.ErrConditional)
	require.NoError(t, store.RegisterAttempt(ctx, attempt))
	_, err = store.EmitHIGH(ctx, sql, attempt.ID, 1)
	require.ErrorIs(t, err, online.ErrConditional)
	_, err = store.EmitLOW(ctx, sql, attempt.ID, 1)
	require.NoError(t, err)
	require.ErrorIs(t, store.RecordLOW(ctx, attempt.ID, 1, attempt.LowID, 100), online.ErrConditional)
	require.ErrorIs(t, store.RecordLOW(ctx, attempt.ID, 1, attempt.HighID, 110), online.ErrConditional)
	require.ErrorIs(t, store.RecordLOW(ctx, attempt.ID, 2, attempt.LowID, 110), online.ErrConditional)
	require.NoError(t, store.RecordLOW(ctx, attempt.ID, 1, attempt.LowID, 110))
	_, err = store.EmitHIGH(ctx, sql, attempt.ID, 1)
	require.NoError(t, err)
	above := int64(1)
	require.ErrorIs(t, store.RecordHIGH(ctx, attempt.ID, 1, attempt.HighID, 120, &above), online.ErrConditional)
	require.ErrorIs(t, store.RecordHIGH(ctx, attempt.ID, 1, attempt.HighID, 110, &minimum), online.ErrConditional)
	require.NoError(t, store.RecordHIGH(ctx, attempt.ID, 1, attempt.HighID, 120, &minimum))
	require.NoError(t, store.CompleteAttempt(ctx, attempt.ID, 1, false))
	progress, err := store.GetTable(ctx, job.ID, table)
	require.NoError(t, err)
	require.Equal(t, &minimum, progress.Cursor)
	// Empty scans use a real LOW/HIGH completion fence, never a minimum-key sentinel.
	emptyAttempt := online.Window{ID: onlineID(t), JobID: job.ID, Generation: 1, Table: empty, LowID: onlineID(t), HighID: onlineID(t)}
	require.NoError(t, store.RegisterAttempt(ctx, emptyAttempt))
	_, err = store.EmitLOW(ctx, sql, emptyAttempt.ID, 1)
	require.NoError(t, err)
	require.NoError(t, store.RecordLOW(ctx, emptyAttempt.ID, 1, emptyAttempt.LowID, 130))
	_, err = store.EmitHIGH(ctx, sql, emptyAttempt.ID, 1)
	require.NoError(t, err)
	require.ErrorIs(t, store.RecordHIGH(ctx, emptyAttempt.ID, 1, emptyAttempt.HighID, 140, &minimum), online.ErrConditional)
	require.NoError(t, store.RecordHIGH(ctx, emptyAttempt.ID, 1, emptyAttempt.HighID, 140, nil))
	require.ErrorIs(t, store.CompleteAttempt(ctx, emptyAttempt.ID, 1, false), online.ErrConditional)
	require.NoError(t, store.CompleteAttempt(ctx, emptyAttempt.ID, 1, true))
	progress, err = store.GetTable(ctx, job.ID, empty)
	require.NoError(t, err)
	require.True(t, progress.Complete)
	require.Nil(t, progress.Cursor)
	// Diagnostic history is bounded; abandoned attempts never mutate the cursor
	// or LOW checkpoint, and pruned UUIDs are unknown rather than adoptable.
	var oldest string
	for i := 0; i < online.MaxDiagnosticWindows+3; i++ {
		w := online.Window{ID: onlineID(t), JobID: job.ID, Generation: 1, Table: table, PreviousCursor: &minimum, LowID: onlineID(t), HighID: onlineID(t)}
		if i == 0 {
			oldest = w.ID
		}
		require.NoError(t, store.RegisterAttempt(ctx, w))
		require.NoError(t, store.AbandonAttempt(ctx, w.ID, 1))
		require.ErrorIs(t, store.AbandonAttempt(ctx, w.ID, 1), online.ErrConditional)
	}
	var count int
	require.NoError(t, sql.QueryRow(ctx, `SELECT count(*) FROM cdc_online.windows WHERE job_id=$1::uuid`, job.ID).Scan(&count))
	require.Equal(t, online.MaxDiagnosticWindows, count)
	_, err = store.GetAttempt(ctx, oldest)
	require.ErrorIs(t, err, pgx.ErrNoRows)
	loaded, err := store.LoadJob(ctx)
	require.NoError(t, err)
	require.Equal(t, pq.LSN(140), loaded.Checkpoint)
	progress, err = store.GetTable(ctx, job.ID, table)
	require.NoError(t, err)
	require.Equal(t, &minimum, progress.Cursor)
	_, err = store.ResumeJob(ctx, job)
	require.NoError(t, err)
	_, err = store.ResumeJob(ctx, job)
	require.ErrorIs(t, err, online.ErrConditional)
}
