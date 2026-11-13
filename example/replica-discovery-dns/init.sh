#!/bin/sh
# Runs once at initdb on the primary: replication access for the standbys and the table.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE TABLE orders (id serial PRIMARY KEY, note text NOT NULL, created_at timestamptz DEFAULT now());
SQL
