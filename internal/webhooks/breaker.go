package webhooks

import (
	"sync"
	"time"
)

type BreakerConfig struct {
	FailureThreshold int           // consecutive failures before opening
	OpenFor          time.Duration // cool-down before a single probe is allowed
}

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

type breaker struct {
	state    breakerState
	failures int
	since    time.Time // when it opened / when the probe started
}

// Breakers is a per-endpoint circuit breaker registry. A dead customer
// endpoint stops consuming worker time after FailureThreshold failures.
// State is per worker process, which is fine: each replica protects itself.
type Breakers struct {
	mu  sync.Mutex
	cfg BreakerConfig
	m   map[string]*breaker
	now func() time.Time
}

func NewBreakers(cfg BreakerConfig) *Breakers {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenFor <= 0 {
		cfg.OpenFor = 30 * time.Second
	}
	return &Breakers{cfg: cfg, m: make(map[string]*breaker), now: time.Now}
}

func (b *Breakers) get(key string) *breaker {
	br, ok := b.m[key]
	if !ok {
		br = &breaker{}
		b.m[key] = br
	}
	return br
}

// Allow reports whether a request to key may proceed.
func (b *Breakers) Allow(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(key)
	switch br.state {
	case stateClosed:
		return true
	case stateOpen:
		if b.now().Sub(br.since) >= b.cfg.OpenFor {
			br.state, br.since = stateHalfOpen, b.now() // one probe
			return true
		}
		return false
	default: // half-open: probe in flight; allow another only if it never reported back
		if b.now().Sub(br.since) >= b.cfg.OpenFor {
			br.since = b.now()
			return true
		}
		return false
	}
}

func (b *Breakers) Success(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(key)
	br.state, br.failures = stateClosed, 0
}

func (b *Breakers) Failure(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	br := b.get(key)
	if br.state == stateHalfOpen {
		br.state, br.since = stateOpen, b.now()
		return
	}
	br.failures++
	if br.failures >= b.cfg.FailureThreshold {
		br.state, br.since = stateOpen, b.now()
	}
}