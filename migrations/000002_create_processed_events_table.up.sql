CREATE TABLE IF NOT EXISTS processed_events (
    -- Dedup key: the same event_id may appear on both the Avro and Proto topics.
    -- Each consumer group processes it independently, so all three columns form the PK.
    event_id        TEXT        NOT NULL,
    consumer_group  TEXT        NOT NULL,
    topic           TEXT        NOT NULL,

    -- Observability columns (not part of the dedup key).
    -- kafka_partition / kafka_offset avoid collision with PostgreSQL reserved words.
    kafka_partition INTEGER     NOT NULL DEFAULT 0,
    kafka_offset    BIGINT      NOT NULL DEFAULT 0,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_group, topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at);
