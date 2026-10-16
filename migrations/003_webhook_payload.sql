-- The webhook worker needs the exact bytes it has to sign and post. Without
-- this column a delivery would have to re-derive the event from the messages
-- table, which loses the envelope (event id, type, timestamp) and makes
-- redelivery impossible once the source event has moved on.
ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS payload BYTEA;

-- Deliveries created before this migration carry no payload; they are marked
-- failed rather than being posted as an empty body.
UPDATE webhook_deliveries
   SET payload = NULL,
       status = 'failed',
       last_error = 'missing payload, created before webhook_deliveries.payload existed'
 WHERE payload IS NULL;

ALTER TABLE webhook_deliveries
    ALTER COLUMN payload SET DEFAULT '\x'::bytea;

-- Recovery marker: a row whose next_attempt_at is in the past is claimable,
-- which is how deliveries left 'inflight' by a crashed worker are picked up.
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_due
    ON webhook_deliveries (next_attempt_at)
    WHERE status IN ('pending', 'inflight');
