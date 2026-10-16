CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY,
    sender_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages (sender_id, recipient_id, created_at);

CREATE TABLE IF NOT EXISTS webhook_subscription (
    id UUID PRIMARY KEY,
    owner_id TEXT NOT NULL,
    target_url TEXT NOT NULL,
    secret  TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'chat.message.created',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    subscription_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, subscription_id)
);