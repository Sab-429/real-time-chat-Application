# real-time-chat

A production-oriented real-time chat backend in Go.

- **Live delivery** over WebSocket, routed between gateway instances with Redis Pub/Sub.
- **PostgreSQL** is the source of truth. Every message gets a gap-free per-conversation sequence number.
- **Kafka** carries everything asynchronous (webhooks, push notifications, analytics) through a transactional **outbox**, so Kafka problems can never slow down or lose a chat message.

```
                 ┌───────────── hot path (tens of ms) ─────────────┐
 Client ⇄ WSS ⇄  Gateway ──► Chat service ──► Postgres (message + outbox, ONE tx)
                    ▲                │
                    │   Redis Pub/Sub│ (after commit, best effort)
                    └────────────────┘

                 ┌───────────── cold path (reliable) ──────────────┐
 Postgres outbox ──► Relay ──► Kafka ─┬─► worker-notification ──► FCM (offline users only)
                                      ├─► worker-webhook ───────► customer endpoints (retry tiers + DLQ)
                                      └─► worker-analytics ─────► analytics store
```

## Why it is built this way

| Decision | Reason |
|---|---|
| Kafka is **not** on the live path | Consumer-group rebalances and lag would show up as chat latency, and WebSocket connections are stateful (a message must reach the one gateway holding the socket). |
| Redis Pub/Sub for live fan-out | Low latency, and one subscription **per gateway** (not per user) keeps Redis load flat. |
| Outbox + single leader relay | Solves the dual-write problem (saved-but-never-published). One publisher at a time preserves per-conversation order in Kafka. |
| Per-conversation `seq` | Clients detect gaps and call `sync`; this is what makes Pub/Sub's "no delivery guarantee" safe. |
| `client_msg_id` idempotency | Client retries never create duplicates (`UNIQUE(sender_id, client_msg_id)`). |
| Separate consumer groups per worker | A slow analytics job can lag for hours without affecting anything else. |

## Services

| Binary | Role | Scale on |
|---|---|---|
| `cmd/api` | REST: conversations, history, sync, read pointers, webhook + device-token management | request rate |
| `cmd/gateway` | WebSocket server, presence, graceful drain | open connections |
| `cmd/relay` | Outbox → Kafka (leader-elected; run 2+ replicas) | 1 active |
| `cmd/worker-notification` | Push to **offline** recipients (FCM), token pruning | consumer lag |
| `cmd/worker-webhook` | Signed webhook delivery, retry tiers 1m/5m/30m, circuit breaker, DLQ | consumer lag |
| `cmd/worker-analytics` | Batched message metrics (never message text) | consumer lag |
| `cmd/migrate` | SQL migrations + Kafka topic creation (one-shot job) | n/a |
| `cmd/devtoken`, `cmd/smoke` | Dev-only: mint a JWT, run an end-to-end check | n/a |

## Quick start (Docker)

Prerequisites: Docker Desktop, Go 1.22+.

```powershell
Copy-Item .env.example .env          # Linux/macOS: cp .env.example .env
docker compose --env-file .env -f deployments/docker-compose.yml up --build -d
go run ./cmd/smoke -pipeline
```

`smoke` creates two users, opens two WebSockets, sends messages and verifies: live delivery, acks and
sequence numbers, idempotent retry, history, sync, unread counts, authorization, and (with `-pipeline`)
that the outbox drained to Kafka and the analytics worker consumed the events.

First run on an existing database that already has migrations 001–003 applied by an older runner:

```powershell
go run ./cmd/migrate -baseline 3
```

## Local development without Docker for the Go services

```powershell
docker compose --env-file .env -f deployments/docker-compose.yml up -d postgres redis kafka
go run ./cmd/migrate
# one terminal each:
go run ./cmd/api
go run ./cmd/gateway
go run ./cmd/relay
go run ./cmd/worker-notification      # logs pushes instead of sending in dev
go run ./cmd/worker-webhook
go run ./cmd/worker-analytics
```

Manual testing:

```powershell
$token = go run ./cmd/devtoken -user alice -name Alice
curl.exe -H "Authorization: Bearer $token" http://localhost:8080/v1/conversations
# WebSocket: ws://localhost:8081/ws?token=<jwt>   (e.g. with: npx wscat -c "ws://localhost:8081/ws?token=$token")
```

## WebSocket protocol

Every message is one JSON frame: `{"type": "...", "id": "<request id>", "data": {...}}`.

| Direction | Type | Data |
|---|---|---|
| client → server | `message.send` | `{conversation_id, client_msg_id, body}` |
| server → client | `message.ack` (echoes `id`) | `{message_id, conversation_id, seq, created_at, duplicate}` |
| server → client | `message.new` | the message: `{id, conversation_id, seq, sender_id, body, created_at}` |
| client → server | `sync.request` | `{conversations: [{conversation_id, last_seq}]}` |
| server → client | `sync.response` | `{results: [{conversation_id, messages, has_more}]}` |
| server → client | `error` | `{code, message}` (`invalid_input`, `forbidden`, `rate_limited`, ...) |

Client rules that make the guarantees hold:

1. Generate a UUID `client_msg_id` per message and **reuse it on retry**.
2. Track the highest `seq` per conversation. If `message.new` arrives with `seq > last + 1`, or after any reconnect, send `sync.request`.
3. Reconnect with jittered exponential backoff (the server sends close code 1012 during deploys).
4. Register the push token with `PUT /v1/devices` on every app launch; `DELETE /v1/devices` on logout.

## REST API

| Method & path | Purpose |
|---|---|
| `POST /v1/conversations` | `{type: "direct"\|"group", title?, member_ids}` (direct chats are idempotent per pair) |
| `GET /v1/conversations` | list with `unread` counts |
| `GET /v1/conversations/{id}/messages?before_seq=&limit=` | history, newest first |
| `POST /v1/conversations/{id}/messages` | HTTP fallback send (`201` new, `200` idempotent retry) |
| `POST /v1/conversations/{id}/read` | `{seq}` advance read pointer |
| `POST /v1/sync` | same as the WebSocket `sync.request` |
| `POST/GET/DELETE /v1/webhooks`, `GET /v1/webhooks/{id}/deliveries` | webhook endpoints (secret shown once) |
| `PUT/DELETE /v1/devices` | push tokens |
| `GET /healthz`, `GET /readyz` | liveness / readiness |

Auth: `Authorization: Bearer <HS256 JWT>` with `sub` = user id, `exp`, optional `name`.

Webhook deliveries carry `X-Webhook-Id` (event id, **dedupe on it**), `X-Webhook-Timestamp`,
`X-Webhook-Attempt` and `X-Webhook-Signature: v1=<hex HMAC-SHA256 of "<timestamp>.<body>">`.

## Configuration

All via environment variables (an optional `.env` is loaded in development). See `.env.example`.

| Variable | Default (dev) | Notes |
|---|---|---|
| `ENV` | `dev` | `dev` or `prod`. Prod has **no** defaults for infra addresses and rejects unsafe settings. |
| `DATABASE_URL` | local postgres | |
| `REDIS_ADDRS` / `REDIS_PASSWORD` / `REDIS_TLS` | `localhost:6379` | several addresses = Redis Cluster |
| `KAFKA_BROKERS` / `KAFKA_REPLICATION` | `localhost:9092` / 1 | use 3 in prod (also sets `min.insync.replicas=2`) |
| `JWT_SECRET` | dev value | ≥ 32 chars |
| `ALLOWED_ORIGINS` | `http://localhost:3000` | `*` rejected in prod |
| `RATE_SEND_PER_SEC` / `RATE_SEND_BURST` | 5 / 20 | per-user, REST + WebSocket |
| `RATE_API_PER_SEC` / `RATE_API_BURST` | 30 / 60 | per-user, whole REST API |
| `FCM_CREDENTIALS_FILE` | empty | empty in dev = log pushes; empty in prod = Application Default Credentials |
| `PUSH_INCLUDE_PREVIEW` | `false` | message text is not sent to Google/Apple unless enabled |
| `WEBHOOK_ALLOW_PRIVATE` | `false` | dev only; the SSRF guard blocks private/loopback/link-local targets |
| `HEALTH_ADDR` | off in dev, `:9090` in prod | workers and relay health port |

## Guarantees and failure modes

| Failure | What happens |
|---|---|
| Redis down | Sends still succeed (live publish is best effort); clients resync by `seq`. Presence lookup fails open (extra pushes rather than missed ones). Rate limiter fails open. New WebSocket connections are refused until presence works. |
| Kafka down | Chat is unaffected. Events accumulate in `outbox_events`; the relay drains them on recovery. Watch the backlog log line. |
| A webhook endpoint is down | Retries at 1m/5m/30m, then DLQ. Its circuit breaker stops it consuming worker time. Other endpoints are unaffected. |
| FCM outage | 3 in-process retries per device, then `notification.dlq`; the partition never stalls. |
| Gateway crash | Its presence entries expire within the TTL (60s); clients reconnect and `sync`. |
| Duplicate Kafka delivery | Every consumer dedupes (`webhook_deliveries`, `notification_log`, `message_events.event_id`). |
| Two retries race | The `UNIQUE(sender_id, client_msg_id)` loser rolls back and returns the winner's message. |

Delivery semantics: **at-least-once** end to end with idempotent consumers; **exactly-once is not claimed**.

## Project layout

```
cmd/            one main.go per binary (api, gateway, relay, worker-*, migrate, devtoken, smoke)
deployments/    docker-compose.yml
internal/
  analytics/    event projection + sinks
  auth/         JWT verification
  cache/        Redis client + ALL key/channel names (keys.go)
  chat/         domain: send (tx + outbox), history, sync, conversations, REST + WebSocket handlers
  config/       env loading and validation
  database/     pgx pool
  events/       Kafka producer/consumer, topics, shared event schema, dedupe
  httpx/        middleware (recover, access log, CORS)
  notifications/push to offline users
  observability/logging, health endpoints, graceful HTTP
  outbox/       relay + repository (backlog, prune)
  ratelimit/    Redis token bucket + middleware
  realtime/     hub, WebSocket server, presence, Redis publisher/subscriber
  webhooks/     endpoints, signing, dispatcher, retry tiers, circuit breaker
migrations/     ordered SQL (applied by cmd/migrate, tracked in schema_migrations)
```

## Testing

```powershell
go test -race ./...
```

Unit tests cover the transaction/idempotency/ordering logic (fake store), the hub, retry and
breaker behaviour, signatures, rate limiting, dedupe, and a **contract test** that fails the build if
the event the chat service writes stops matching what the consumers read. `cmd/smoke` is the
end-to-end check against a running stack.

## Production checklist

- [ ] `ENV=prod`, real `JWT_SECRET`, `KAFKA_REPLICATION=3`, TLS to Postgres/Redis/Kafka.
- [ ] Encrypt `webhook_endpoints.secret` at rest (KMS / pgcrypto).
- [ ] 2+ replicas of every service; gateway behind an L4/L7 balancer with WebSocket support and idle timeout > 60s.
- [ ] Alerts: outbox backlog age, consumer lag per group, DLQ topic depth, gateway connection count, 5xx rate.
- [ ] Replace the Postgres analytics sink with ClickHouse/BigQuery (implement `analytics.Sink`).
- [ ] Add Prometheus metrics and OpenTelemetry tracing (only logs and health endpoints exist today).
- [ ] Partition `messages` by time or hash once it grows large; consider Cassandra/Scylla only at very high write volume.
- [ ] Load-test the gateway (connections per instance) and one hot conversation (the per-conversation row lock is the ceiling).

## Known limitations

- Typing indicators and read receipts over WebSocket are not implemented (read pointers are REST-only).
- Group membership changes (add/remove members, roles) are not implemented.
- `kafka-go` has no idempotent producer; duplicates are possible and handled by consumers.
- The outbox relay publishes sequentially (hundreds to ~1k events/s); use batching or Debezium beyond that.
- The gateway talks to Postgres directly (modular monolith). Split behind an RPC client if you need to.
