# syntax=docker/dockerfile:1

########################################################################
# builder - compile both binaries once, shared by the api/worker images
########################################################################
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache module downloads separately from source edits.
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

# Static, reproducible binaries. pgx is pure Go, so CGO stays off.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

########################################################################
# api - serves REST, WebSocket and SSE; runs migrations on start
########################################################################
FROM alpine:3.20 AS api
# ca-certificates keeps outbound TLS (e.g. a TLS DATABASE_URL) working.
RUN apk add --no-cache ca-certificates \
 && adduser -D -H -u 10001 app
USER app
COPY --from=builder /out/api /usr/local/bin/api
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]

########################################################################
# worker - consumes the bus and dispatches signed webhooks
########################################################################
FROM alpine:3.20 AS worker
# The worker POSTs to arbitrary subscription targets, so the CA trust store is
# required for https webhooks.
RUN apk add --no-cache ca-certificates \
 && adduser -D -H -u 10001 app
USER app
COPY --from=builder /out/worker /usr/local/bin/worker
ENTRYPOINT ["/usr/local/bin/worker"]
