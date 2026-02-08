CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE inbox_event_status AS ENUM (
    'pending',
    'processed',
    'processing'
    'failed'
);

CREATE TABLE inbox_events (
    event_id UUID        PRIMARY KEY NOT NULL,
    seq      BIGINT      GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE CHECK ( seq >= 0 ),

    topic     TEXT   NOT NULL,
    key       TEXT   NOT NULL,
    type      TEXT   NOT NULL,
    version   INT    NOT NULL,
    producer  TEXT   NOT NULL,
    payload   JSONB  NOT NULL,
    partition INT    NOT NULL CHECK ( partition >= 0 ),
    kafka_offset BIGINT NOT NULL CHECK ( kafka_offset >= 0 ),

    reserved_by     TEXT,

    status          inbox_event_status NOT NULL DEFAULT 'pending', -- pending | processed | failed
    attempts        INT NOT NULL DEFAULT 0 CHECK ( attempts >= 0 ),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    last_attempt_at TIMESTAMPTZ,
    last_error      TEXT,

    processed_at    TIMESTAMPTZ,
    produced_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE UNIQUE INDEX inbox_events_topic_partition_offset_uidx
    ON inbox_events (topic, partition, kafka_offset);

CREATE INDEX inbox_events_pending_ready_idx
    ON inbox_events (next_attempt_at, seq)
    WHERE status = 'pending';

CREATE INDEX inbox_events_key_idx
    ON inbox_events (key);

CREATE INDEX inbox_events_type_idx
    ON inbox_events (type);