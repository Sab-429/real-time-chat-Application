CREATE TABLE IF NOT EXISTS message_events (
    event_id        TEXT        PRIMARY KEY,      -- idempotency: Kafka redelivery inserts nothing
    message_id      TEXT        NOT NULL,
    conversation_id TEXT        NOT NULL,
    sender_id       TEXT        NOT NULL,
    seq             BIGINT      NOT NULL,
    recipient_count INT         NOT NULL,
    body_bytes      INT         NOT NULL,         -- size only; message text is never copied here
    occurred_at     TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_message_events_occurred ON message_events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_message_events_conv ON message_events (conversation_id, occurred_at);
