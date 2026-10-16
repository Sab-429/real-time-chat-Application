-- IMPORTANT: if 001_init.sql / 002_users.sql already created `conversations`,
-- `conversation_members` or `messages`, CREATE TABLE IF NOT EXISTS below would
-- silently keep the OLD shape. In that case convert this file into ALTER TABLE
-- statements that add: conversations(last_seq, last_message_at, direct_key, title, type),
-- conversation_members(last_read_seq, muted, role), messages(seq, client_msg_id)
-- and the named constraints below (the code relies on the constraint names).

CREATE TABLE IF NOT EXISTS conversations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT        NOT NULL CHECK (type IN ('direct','group')),
    title           TEXT,
    direct_key      TEXT UNIQUE,                 -- "userA:userB" sorted; NULL for groups
    last_seq        BIGINT      NOT NULL DEFAULT 0,
    last_message_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conversation_members (
    conversation_id UUID        NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id         TEXT        NOT NULL,        -- use UUID + FK to users if you prefer
    role            TEXT        NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
    last_read_seq   BIGINT      NOT NULL DEFAULT 0,
    muted           BOOLEAN     NOT NULL DEFAULT FALSE,
    joined_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_members_user ON conversation_members (user_id);

CREATE TABLE IF NOT EXISTS messages (
    id              UUID        PRIMARY KEY,
    conversation_id UUID        NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    seq             BIGINT      NOT NULL,
    sender_id       TEXT        NOT NULL,
    client_msg_id   TEXT        NOT NULL,
    body            TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- also serves history/replay in both directions (btree scans backwards)
    CONSTRAINT messages_conv_seq_uq          UNIQUE (conversation_id, seq),
    -- idempotency key; the repository matches on this exact name
    CONSTRAINT messages_sender_client_msg_uq UNIQUE (sender_id, client_msg_id)
);
