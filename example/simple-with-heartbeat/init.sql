CREATE TABLE public.test_heartbeat_table (
    id   bigserial primary key,
    txt  text,
    ts   timestamptz default now()
);

CREATE TABLE public.users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    created_on timestamptz
);