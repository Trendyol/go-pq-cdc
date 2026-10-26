CREATE TABLE public.partitioned_events (
    event_id BIGINT NOT NULL,
    created_date DATE NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (event_id, created_date)
) PARTITION BY RANGE (created_date);

CREATE TABLE public.partitioned_events_2026_01
    PARTITION OF public.partitioned_events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE public.partitioned_events_2026_02
    PARTITION OF public.partitioned_events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE public.partitioned_events_2026_03
    PARTITION OF public.partitioned_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE public.partitioned_events_2026_04
    PARTITION OF public.partitioned_events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE public.partitioned_events_2026_05
    PARTITION OF public.partitioned_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

INSERT INTO public.partitioned_events (event_id, created_date, payload)
SELECT
    i,
    DATE '2026-01-15',
    jsonb_build_object(
        'sequence', i,
        'content', repeat(md5(i::text), 16)
    )
FROM generate_series(1, 100) AS i;

INSERT INTO public.partitioned_events (event_id, created_date, payload)
SELECT
    100 + i,
    DATE '2026-02-15',
    jsonb_build_object('sequence', 100 + i, 'content', repeat(md5(i::text), 16))
FROM generate_series(1, 10) AS i;

-- March intentionally remains empty.

INSERT INTO public.partitioned_events (event_id, created_date, payload)
SELECT
    110 + i,
    DATE '2026-04-15',
    jsonb_build_object('sequence', 110 + i, 'content', repeat(md5(i::text), 16))
FROM generate_series(1, 500) AS i;

INSERT INTO public.partitioned_events (event_id, created_date, payload)
SELECT
    610 + i,
    DATE '2026-05-15',
    jsonb_build_object('sequence', 610 + i, 'content', repeat(md5(i::text), 16))
FROM generate_series(1, 300) AS i;

CREATE TABLE public.regular_events (
    event_id BIGINT PRIMARY KEY,
    payload JSONB NOT NULL
);

INSERT INTO public.regular_events (event_id, payload)
SELECT
    i,
    jsonb_build_object('sequence', i, 'source', 'regular')
FROM generate_series(1, 75) AS i;

CREATE TABLE public.filtered_partitioned_events (
    event_id BIGINT NOT NULL,
    created_date DATE NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (event_id, created_date)
) PARTITION BY RANGE (created_date);

CREATE TABLE public.filtered_partitioned_events_2026_01
    PARTITION OF public.filtered_partitioned_events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE public.filtered_partitioned_events_2026_02
    PARTITION OF public.filtered_partitioned_events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE public.filtered_partitioned_events_2026_03
    PARTITION OF public.filtered_partitioned_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

INSERT INTO public.filtered_partitioned_events (event_id, created_date, status, payload)
SELECT
    i,
    DATE '2026-01-15',
    CASE WHEN i <= 20 THEN 'active' ELSE 'inactive' END,
    jsonb_build_object('sequence', i, 'source', 'filtered_partitioned')
FROM generate_series(1, 30) AS i;

INSERT INTO public.filtered_partitioned_events (event_id, created_date, status, payload)
SELECT
    i,
    DATE '2026-02-15',
    CASE WHEN i <= 35 THEN 'active' ELSE 'inactive' END,
    jsonb_build_object('sequence', i, 'source', 'filtered_partitioned')
FROM generate_series(31, 50) AS i;

INSERT INTO public.filtered_partitioned_events (event_id, created_date, status, payload)
SELECT
    i,
    DATE '2026-03-15',
    'inactive',
    jsonb_build_object('sequence', i, 'source', 'filtered_partitioned')
FROM generate_series(51, 60) AS i;
