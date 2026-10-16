package online

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5"
)

var (
	ErrBinding     = errors.New("online source/slot/configuration binding mismatch")
	ErrConditional = errors.New("online metadata transition rejected")
	ErrOwnerLost   = errors.New("online metadata owner connection lost")
	ErrOwned       = errors.New("online slot already has an owner")
)

type SourceIdentity = replication.OnlineSourceIdentity

type Job struct {
	ID                string
	ConnectorID       string
	Slot              string
	SlotBinding       string
	Source            SourceIdentity
	Generation        int64
	ConfigFingerprint string
	Status            string
	Checkpoint        pq.LSN
}
type SlotState struct {
	Name         string
	Exists       bool
	Invalid      bool
	ConfirmedLSN pq.LSN
}

// ValidateResume checks supplied slot facts, not slot creation provenance. The
// coordinator must obtain these facts from the primary and reject unknown slots.
func ValidateResume(job Job, slot SlotState, source SourceIdentity, fingerprint string) error {
	if job.ID == "" || job.SlotBinding == "" || job.Checkpoint == 0 || job.Source != source || job.ConfigFingerprint != fingerprint || !slot.Exists || slot.Invalid || slot.Name != job.Slot || slot.ConfirmedLSN > job.Checkpoint {
		return ErrBinding
	}
	return nil
}

type TableProgress struct {
	JobID             string
	Table             Table
	SchemaFingerprint string
	KeyFingerprint    string
	UpperKey          *int64 // nil represents a captured empty table, not an integer sentinel.
	Cursor            *int64
	Complete          bool
}
type Window struct {
	ID             string
	JobID          string
	Generation     int64
	Table          Table
	PreviousCursor *int64
	LastKey        *int64
	LowID          string
	HighID         string
	LowEnd         pq.LSN
	HighEnd        pq.LSN
	LowEmitted     bool
	HighEmitted    bool
	Status         string
}
type MarkerPhase string

const (
	Low       MarkerPhase = "LOW"
	High      MarkerPhase = "HIGH"
	Heartbeat MarkerPhase = "HEARTBEAT"
)

type Marker struct {
	ConnectorID string
	JobID       string
	Generation  int64
	AttemptID   string
	Phase       MarkerPhase
	ID          string
}

// MetadataMigrationV1 is manually executable and idempotent. Only watermarks
// belongs in the business publication; no source row images are stored here.
const MetadataMigrationV1 = `
CREATE SCHEMA IF NOT EXISTS cdc_online;
CREATE TABLE IF NOT EXISTS cdc_online.schema_version (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), version integer NOT NULL);
INSERT INTO cdc_online.schema_version VALUES (true,1) ON CONFLICT DO NOTHING;
DO $$ BEGIN IF (SELECT version FROM cdc_online.schema_version WHERE singleton) IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'unsupported cdc_online schema version'; END IF; END $$;
CREATE TABLE IF NOT EXISTS cdc_online.jobs (
 job_id uuid PRIMARY KEY, connector_id uuid NOT NULL UNIQUE, slot_name text NOT NULL UNIQUE, slot_binding uuid NOT NULL UNIQUE,
 system_id text NOT NULL, database_name text NOT NULL, timeline integer NOT NULL CHECK(timeline>0),
 generation bigint NOT NULL CHECK(generation>0), config_fingerprint text NOT NULL,
 status text NOT NULL CHECK(status IN ('active','snapshot_complete')), source_lsn pg_lsn NOT NULL);
CREATE TABLE IF NOT EXISTS cdc_online.table_progress (
 job_id uuid NOT NULL REFERENCES cdc_online.jobs, table_schema text NOT NULL, table_name text NOT NULL,
 schema_fingerprint text NOT NULL, key_fingerprint text NOT NULL, upper_key bigint, cursor_key bigint,
 complete boolean NOT NULL DEFAULT false, PRIMARY KEY(job_id,table_schema,table_name),
 CHECK(upper_key IS NOT NULL OR cursor_key IS NULL), CHECK(cursor_key IS NULL OR cursor_key<=upper_key));
CREATE TABLE IF NOT EXISTS cdc_online.windows (
 attempt_id uuid PRIMARY KEY, job_id uuid NOT NULL, generation bigint NOT NULL,
 table_schema text NOT NULL, table_name text NOT NULL, previous_cursor bigint, last_key bigint,
 low_id uuid NOT NULL UNIQUE, high_id uuid NOT NULL UNIQUE, low_end pg_lsn, high_end pg_lsn,
 low_emitted boolean NOT NULL DEFAULT false, high_emitted boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 status text NOT NULL CHECK(status IN ('registered','low','high','completed','abandoned')),
 FOREIGN KEY(job_id,table_schema,table_name) REFERENCES cdc_online.table_progress,
 CHECK(low_id<>high_id), CHECK(high_end IS NULL OR (low_end IS NOT NULL AND high_end>low_end)));
CREATE UNIQUE INDEX IF NOT EXISTS online_one_window ON cdc_online.windows(job_id) WHERE status IN ('registered','low','high');
CREATE TABLE IF NOT EXISTS cdc_online.watermarks (
 connector_id uuid PRIMARY KEY REFERENCES cdc_online.jobs(connector_id), job_id uuid NOT NULL REFERENCES cdc_online.jobs,
 generation bigint NOT NULL, attempt_id uuid, phase text NOT NULL CHECK(phase IN ('LOW','HIGH','HEARTBEAT')), marker_id uuid NOT NULL,
 CHECK((phase='HEARTBEAT')=(attempt_id IS NULL)));
ALTER TABLE cdc_online.watermarks REPLICA IDENTITY FULL;
`

func quoteIdentifier(name string) (string, error) {
	if name == "" || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("empty or NUL-containing PostgreSQL identifier")
	}
	return pgx.Identifier{name}.Sanitize(), nil
}
func ProvisionSQL(role string) (string, error) {
	sql := "BEGIN;\nSELECT pg_advisory_xact_lock(hashtextextended('cdc_online/migration',0));\n" + MetadataMigrationV1
	if role != "" {
		q, err := quoteIdentifier(role)
		if err != nil {
			return "", err
		}
		sql += "\nGRANT USAGE ON SCHEMA cdc_online TO " + q + ";\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cdc_online TO " + q + ";\n"
	}
	return sql + "\nCOMMIT;", nil
}
func Provision(ctx context.Context, conn *pgx.Conn, role string) error {
	sql, err := ProvisionSQL(role)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, sql)
	return err
}
func ValidateProvisioned(ctx context.Context, conn *pgx.Conn) error {
	var fsync bool
	if err := conn.QueryRow(ctx, `SELECT current_setting('fsync')::boolean`).Scan(&fsync); err != nil {
		return err
	}
	if !fsync {
		return fmt.Errorf("online source metadata durability requires fsync=on")
	}
	for _, table := range []string{"schema_version", "jobs", "table_progress", "windows", "watermarks"} {
		var permanent bool
		if err := conn.QueryRow(ctx, `SELECT c.relkind='r' AND c.relpersistence='p' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='cdc_online' AND c.relname=$1`, table).Scan(&permanent); err != nil || !permanent {
			return fmt.Errorf("online metadata %s must be an ordinary permanent table: %v", table, err)
		}
	}
	var version int
	if err := conn.QueryRow(ctx, "SELECT version FROM cdc_online.schema_version WHERE singleton").Scan(&version); err != nil {
		return fmt.Errorf("pre-provision cdc_online with ProvisionSQL and grant schema/table privileges: %w", err)
	}
	if version != 1 {
		return fmt.Errorf("unsupported cdc_online schema version %d", version)
	}
	for _, table := range []string{"jobs", "table_progress", "windows", "watermarks"} {
		var allowed bool
		err := conn.QueryRow(ctx, `SELECT has_schema_privilege(current_user,'cdc_online','USAGE') AND has_table_privilege(current_user,$1,'SELECT') AND has_table_privilege(current_user,$1,'INSERT') AND has_table_privilege(current_user,$1,'UPDATE') AND has_table_privilege(current_user,$1,'DELETE')`, "cdc_online."+table).Scan(&allowed)
		if err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("grant USAGE on cdc_online and SELECT, INSERT, UPDATE, DELETE on cdc_online.%s", table)
		}
	}
	return nil
}

// EnsureWatermarkPublication never assumes an existing publication was updated.
// Automatic ALTER still requires ownership; restricted users can apply the
// returned actionable SQL manually. Broad metadata capture fails closed.
func EnsureWatermarkPublication(ctx context.Context, conn *pgx.Conn, name string, automatic bool) error {
	quoted, err := quoteIdentifier(name)
	if err != nil {
		return err
	}
	var all, updates, schema bool
	err = conn.QueryRow(ctx, `SELECT p.puballtables,p.pubupdate,EXISTS(SELECT 1 FROM pg_publication_namespace n JOIN pg_namespace s ON s.oid=n.pnnspid WHERE n.pnpubid=p.oid AND s.nspname='cdc_online') FROM pg_publication p WHERE p.pubname=$1`, name).Scan(&all, &updates, &schema)
	if err != nil {
		return fmt.Errorf("online publication must exist: %w", err)
	}
	if all || schema || !updates {
		return fmt.Errorf("online publication must explicitly publish watermarks updates without broad metadata membership")
	}
	rows, err := conn.Query(ctx, `SELECT tablename FROM pg_publication_tables WHERE pubname=$1 AND schemaname='cdc_online'`, name)
	if err != nil {
		return err
	}
	member := false
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			rows.Close()
			return err
		}
		if table != "watermarks" {
			rows.Close()
			return fmt.Errorf("online publication captures forbidden metadata table %s", table)
		}
		member = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if member {
		var complete bool
		err = conn.QueryRow(ctx, `SELECT c.relreplident='f' AND r.prattrs IS NULL AND r.prqual IS NULL FROM pg_publication_rel r JOIN pg_publication p ON p.oid=r.prpubid JOIN pg_class c ON c.oid=r.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE p.pubname=$1 AND n.nspname='cdc_online' AND c.relname='watermarks'`, name).Scan(&complete)
		if err != nil {
			return err
		}
		if !complete {
			return fmt.Errorf("online watermarks require complete unfiltered publication membership and FULL replica identity")
		}
		return nil
	}
	sql := "ALTER PUBLICATION " + quoted + " ADD TABLE cdc_online.watermarks;"
	if !automatic {
		return fmt.Errorf("online watermark membership missing; publication owner must execute: %s", sql)
	}
	if _, err = conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("publication owner must execute %s: %w", sql, err)
	}
	return EnsureWatermarkPublication(ctx, conn, name, false)
}
