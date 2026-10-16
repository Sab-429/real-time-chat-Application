package webhooks

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	TopicEvents = "chat.message.created"
	TopicDLQ    = "webhook.dlq"
)

type Tier struct {
	Topic string
	Delay time.Duration
}

// RetryPolicy: attempt 1 is the initial delivery (from TopicEvents);
// each failure moves the task to the next tier topic; after the last tier -> DLQ.
// Each tier topic has a uniform delay, so its head is always the oldest due task
// and the retry consumer can simply sleep until NotBefore.
type RetryPolicy struct {
	Tiers          []Tier
	JitterFraction float64 // 0.1 = add up to +10% to avoid thundering herds
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Tiers: []Tier{
			{Topic: "webhook.retry.1m", Delay: 1 * time.Minute},
			{Topic: "webhook.retry.5m", Delay: 5 * time.Minute},
			{Topic: "webhook.retry.30m", Delay: 30 * time.Minute},
		},
		JitterFraction: 0.1,
	}
}

// Next returns the tier for a task that has made attemptsMade attempts.
func (p RetryPolicy) Next(attemptsMade int, now time.Time) (Tier, time.Time, bool) {
	idx := attemptsMade - 1
	if idx < 0 || idx >= len(p.Tiers) {
		return Tier{}, time.Time{}, false
	}
	t := p.Tiers[idx]
	var jitter time.Duration
	if p.JitterFraction > 0 {
		jitter = time.Duration(rand.Float64() * p.JitterFraction * float64(t.Delay))
	}
	return t, now.Add(t.Delay + jitter), true
}

func (p RetryPolicy) Topics() []string {
	out := make([]string, len(p.Tiers))
	for i, t := range p.Tiers {
		out[i] = t.Topic
	}
	return out
}

// WaitUntil blocks until t or ctx is cancelled.
func WaitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}