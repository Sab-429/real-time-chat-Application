-- Renumber to fit your sequence (replaces the planned 006/007 from the earlier plan).
CREATE TABLE IF NOT EXISTS device_tokens (
    id           BIGSERIAL PRIMARY KEY,
    user_id      TEXT        NOT NULL,         -- use UUID + FK if users.id is UUID
    platform     TEXT        NOT NULL CHECK (platform IN ('android','ios','web')),
    token        TEXT        NOT NULL UNIQUE,  -- a physical device belongs to one account at a time
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_device_tokens_user ON device_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_device_tokens_last_seen ON device_tokens (last_seen_at);

-- Per-(event,user) idempotency: Kafka redelivery never re-pushes to a notified user.
CREATE TABLE IF NOT EXISTS notification_log (
    event_id TEXT        NOT NULL,
    user_id  TEXT        NOT NULL,
    sent_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_notification_log_sent_at ON notification_log (sent_at);

-- Per-conversation mute (skip push when true).
ALTER TABLE conversation_members ADD COLUMN IF NOT EXISTS muted BOOLEAN NOT NULL DEFAULT FALSE;