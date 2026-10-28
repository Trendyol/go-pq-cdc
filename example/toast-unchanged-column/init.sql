-- wal_level=logical, the cdc_user/cdc_db setup and the replication limits come from docker-compose.yml.

CREATE TABLE docs (id int PRIMARY KEY, title text, payload text);
-- Disable compression so the value definitely goes out-of-line to TOAST.
ALTER TABLE docs ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO docs VALUES (1, 'a', repeat('x', 5000));
-- REPLICA IDENTITY stays DEFAULT on purpose (do not set FULL here).
