package webhooks

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Publisher is satisfied by a thin wrapper over your events.Producer.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

type Store interface {
	ListActiveForOwners(ctx context.Context, ownerIDs []string, eventType string) ([]Endpoint, error)
	GetActive(ctx context.Context, id string) (Endpoint, error)
	IsDelivered(ctx context.Context, endpointID, eventID string) (bool, error)
	RecordDelivery(ctx context.Context, d DeliveryRecord) error
}

type ProcessorConfig struct {
	Store       Store
	Dispatcher  *Dispatcher
	Breakers    *Breakers
	Publisher   Publisher
	Policy      RetryPolicy
	Concurrency int // parallel endpoint deliveries per event
	Logger      *slog.Logger
}

type Processor struct {
	store       Store
	dispatcher  *Dispatcher
	breakers    *Breakers
	pub         Publisher
	policy      RetryPolicy
	concurrency int
	log         *slog.Logger
}

func NewProcessor(c ProcessorConfig) *Processor {
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if len(c.Policy.Tiers) == 0 {
		c.Policy = DefaultRetryPolicy()
	}
	if c.Breakers == nil {
		c.Breakers = NewBreakers(BreakerConfig{})
	}
	return &Processor{
		store: c.Store, dispatcher: c.Dispatcher, breakers: c.Breakers, pub: c.Publisher,
		policy: c.Policy, concurrency: c.Concurrency, log: c.Logger,
	}
}

// HandleEvent processes a message from TopicEvents (first attempt).
// Return value contract for the Kafka consumer loop:
//   nil   -> commit the offset (delivered, scheduled for retry, or dead-lettered)
//   error -> do NOT commit; infrastructure failed (DB/Kafka), message will be redelivered
func (p *Processor) HandleEvent(ctx context.Context, payload []byte) error {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil || env.EventID == "" {
		return p.deadLetterRaw(ctx, payload, "malformed event")
	}
	endpoints, err := p.store.ListActiveForOwners(ctx, env.RecipientIDs, env.Type)
	if err != nil {
		return err
	}
	return p.fanOut(ctx, endpoints, func(ep Endpoint) error {
		return p.attempt(ctx, ep, env, 1)
	})
}

// HandleRetry processes a message from a retry tier topic.
func (p *Processor) HandleRetry(ctx context.Context, payload []byte) error {
	var t Task
	if err := json.Unmarshal(payload, &t); err != nil || t.EndpointID == "" {
		return p.deadLetterRaw(ctx, payload, "malformed task")
	}
	if err := WaitUntil(ctx, t.NotBefore); err != nil {
		return err
	}
	ep, err := p.store.GetActive(ctx, t.EndpointID)
	if errors.Is(err, ErrNotFound) {
		return nil // endpoint deleted or disabled meanwhile: drop
	}
	if err != nil {
		return err
	}
	return p.attempt(ctx, ep, t.Envelope, t.Attempt+1)
}

func (p *Processor) attempt(ctx context.Context, ep Endpoint, env Envelope, attempt int) error {
	done, err := p.store.IsDelivered(ctx, ep.ID, env.EventID)
	if err != nil {
		return err
	}
	if done {
		return nil // idempotent: already delivered (Kafka redelivery)
	}

	var res Result
	if !p.breakers.Allow(ep.ID) {
		res = Result{Err: ErrCircuitOpen, Retryable: true}
	} else {
		res = p.dispatcher.Deliver(ctx, ep, env, attempt)
		if res.Success() || !res.Retryable {
			p.breakers.Success(ep.ID) // endpoint answered, so it is alive
		} else {
			p.breakers.Failure(ep.ID)
		}
	}

	rec := DeliveryRecord{EndpointID: ep.ID, EventID: env.EventID, Attempts: attempt, StatusCode: res.StatusCode}

	if res.Success() {
		rec.Status = StatusDelivered
		return p.store.RecordDelivery(ctx, rec)
	}
	if ctx.Err() != nil {
		return ctx.Err() // shutting down: leave offset uncommitted instead of burning an attempt
	}

	rec.LastError = res.Err.Error()
	task := Task{EndpointID: ep.ID, Attempt: attempt, Envelope: env, LastError: rec.LastError}

	if res.Retryable {
		if tier, notBefore, ok := p.policy.Next(attempt, time.Now()); ok {
			task.NotBefore = notBefore
			if err := p.publishTask(ctx, tier.Topic, task); err != nil {
				return err
			}
			rec.Status, rec.NextRetryAt = StatusRetrying, &notBefore
			return p.store.RecordDelivery(ctx, rec)
		}
	}

	// Permanent failure or retries exhausted.
	if err := p.publishTask(ctx, TopicDLQ, task); err != nil {
		return err
	}
	p.log.Warn("webhook dead-lettered",
		"endpoint_id", ep.ID, "event_id", env.EventID, "attempts", attempt, "error", rec.LastError)
	rec.Status = StatusFailed
	return p.store.RecordDelivery(ctx, rec)
}

func (p *Processor) publishTask(ctx context.Context, topic string, t Task) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	// Key by endpoint so one endpoint's retries stay ordered within a partition.
	return p.pub.Publish(ctx, topic, t.EndpointID, b)
}

func (p *Processor) deadLetterRaw(ctx context.Context, payload []byte, reason string) error {
	p.log.Error("poison message sent to DLQ", "reason", reason)
	return p.pub.Publish(ctx, TopicDLQ, "poison", payload)
}

func (p *Processor) fanOut(ctx context.Context, eps []Endpoint, fn func(Endpoint) error) error {
	sem := make(chan struct{}, p.concurrency)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
loop:
	for _, ep := range eps {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			break loop
		}
		wg.Add(1)
		go func(ep Endpoint) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ep); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(ep)
	}
	wg.Wait()
	return errors.Join(errs...)
}