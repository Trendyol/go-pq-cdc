-- High-traffic database used to simulate WAL pressure from another database
-- on the same Postgres instance.
--
-- This database is NOT used by the CDC connector. It only generates WAL.

CREATE DATABASE high_db;

\connect high_db

CREATE TABLE IF NOT EXISTS public.hightraffic (
    id    serial PRIMARY KEY,
    value text NOT NULL
);


