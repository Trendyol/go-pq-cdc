-- Note: test_heartbeat_table is auto-created by the library

CREATE TABLE public.users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    created_on timestamptz
);