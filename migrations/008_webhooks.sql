-- Reconcile with your existing 003_webhook_payload.sql: if these tables already
-- exist, convert this into ALTER statements instead.
CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id    TEXT        NOT NULL,          -- use UUID + FK if users.id is UUID
    url         TEXT        NOT NULL,
    secret      TEXT        NOT NULL,          -- needed to sign; encrypt at rest (KMS/pgcrypto)
    event_types TEXT[]      NOT NULL DEFAULT ARRAY['chat.message.created'],
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_owner_active
    ON webhook_endpoints (owner_id) WHERE active;

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id               BIGSERIAL PRIMARY KEY,
    endpoint_id      UUID        NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    event_id         TEXT        NOT NULL,
    status           TEXT        NOT NULL CHECK (status IN ('delivered','retrying','failed')),
    attempts         INT         NOT NULL DEFAULT 0,
    last_status_code INT,
    last_error       TEXT,
    next_retry_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (endpoint_id, event_id)             -- idempotency key
);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_endpoint_updated
    ON webhook_deliveries (endpoint_id, updated_at DESC);
