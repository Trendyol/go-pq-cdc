#!/bin/sh
# Runs once at initdb on the primary. synchronous_standby_names goes through
# ALTER SYSTEM rather than the command line so it only applies to the real
# server: on the init-time server the CREATE TABLE would otherwise wait for
# standbys that do not exist yet.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE TABLE orders (id serial PRIMARY KEY, note text NOT NULL, created_at timestamptz DEFAULT now());
ALTER SYSTEM SET synchronous_standby_names = 'ANY 2 (standby1, standby2)';
SQL
