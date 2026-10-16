package webhooks

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("webhooks: not found")
	ErrCircuitOpen    = errors.New("webhooks: circuit open")
	ErrBlockedAddress = errors.New("webhooks: destination address not allowed")
)

const EventMessageCreated = "chat.message.created"

// Delivery statuses stored in webhook_deliveries.
const (
	StatusDelivered = "delivered"
	StatusRetrying  = "retrying"
	StatusFailed    = "failed" // exhausted retries or permanent error -> DLQ
)

// Endpoint is a customer-registered webhook destination.
type Endpoint struct {
	ID         string    `json:"id"`
	OwnerID    string    `json:"owner_id"`
	URL        string    `json:"url"`
	Secret     string    `json:"-"` // never serialized; shown once on creation
	EventTypes []string  `json:"event_types"`
	Active     bool      `json:"active"`
	CreatedAt  time.Time `json:"created_at"`
}

// Envelope is the internal Kafka event (topic chat.message.created).
// Map this from your events.MessageCreated in internal/events/schema.go.
type Envelope struct {
	EventID      string          `json:"event_id"`
	Type         string          `json:"type"`
	Version      int             `json:"version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RecipientIDs []string        `json:"recipient_ids"` // internal only, never sent to customers
	Data         json.RawMessage `json:"data"`
}

// OutboundPayload is exactly what the customer's server receives.
type OutboundPayload struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

func (e Envelope) Outbound() OutboundPayload {
	return OutboundPayload{ID: e.EventID, Type: e.Type, Version: e.Version, OccurredAt: e.OccurredAt, Data: e.Data}
}

// Task is a single-endpoint delivery job carried on retry and DLQ topics.
type Task struct {
	EndpointID string    `json:"endpoint_id"`
	Attempt    int       `json:"attempt"` // attempts already made
	NotBefore  time.Time `json:"not_before"`
	Envelope   Envelope  `json:"envelope"`
	LastError  string    `json:"last_error,omitempty"`
}

type DeliveryRecord struct {
	EndpointID  string
	EventID     string
	Status      string
	Attempts    int
	StatusCode  int
	LastError   string
	NextRetryAt *time.Time
}

type DeliverySummary struct {
	EventID    string    `json:"event_id"`
	Status     string    `json:"status"`
	Attempts   int       `json:"attempts"`
	StatusCode int       `json:"last_status_code,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}