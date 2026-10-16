-- Used by events.PostgresDedupe for consumers without their own idempotency table.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS idx_processed_events_at ON processed_events (processed_at);
