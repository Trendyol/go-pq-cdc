BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('cdc_online/migration',0));

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

COMMIT;