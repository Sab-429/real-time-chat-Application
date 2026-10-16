CREATE TABLE IF NOT EXISTS outbox_events (
    id           BIGSERIAL PRIMARY KEY,          -- publish order == id order
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,           -- Kafka partition key (conversation_id)
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON outbox_events (published_at) WHERE published_at IS NOT NULL;
