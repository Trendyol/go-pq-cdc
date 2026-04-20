CREATE TABLE test_events
(
    id          UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_time  TIMESTAMPTZ NOT NULL,
    event_stuff TEXT        NOT NULL,
    code        text,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);


CREATE TABLE test_events_1 PARTITION OF test_events FOR VALUES FROM ('2026-04-01') TO ('2026-04-02');
CREATE TABLE test_events_2 PARTITION OF test_events FOR VALUES FROM ('2026-04-02') TO ('2026-04-03');
CREATE TABLE test_events_3 PARTITION OF test_events FOR VALUES FROM ('2026-04-03') TO ('2026-04-04');
CREATE TABLE test_events_4 PARTITION OF test_events FOR VALUES FROM ('2026-04-04') TO ('2026-04-05');
CREATE TABLE test_events_5 PARTITION OF test_events FOR VALUES FROM ('2026-04-05') TO ('2026-04-06');
CREATE TABLE test_events_6 PARTITION OF test_events FOR VALUES FROM ('2026-04-06') TO ('2026-04-07');
CREATE TABLE test_events_7 PARTITION OF test_events FOR VALUES FROM ('2026-04-07') TO ('2026-04-08');
CREATE TABLE test_events_8 PARTITION OF test_events FOR VALUES FROM ('2026-04-08') TO ('2026-04-09');
CREATE TABLE test_events_9 PARTITION OF test_events FOR VALUES FROM ('2026-04-09') TO ('2026-04-10');
CREATE TABLE test_events_10 PARTITION OF test_events FOR VALUES FROM ('2026-04-10') TO ('2026-04-11');
