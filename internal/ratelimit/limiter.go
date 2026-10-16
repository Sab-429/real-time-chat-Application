package ratelimit

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sabya/real-time-chat/internal/cache"
)

type Config struct {
	Rate  float64 // tokens added per second (sustained rate)
	Burst int     // bucket size (short bursts allowed up to this)
}

func (c Config) withDefaults() Config {
	if c.Rate <= 0 {
		c.Rate = 10
	}
	if c.Burst < 1 {
		c.Burst = 20
	}
	return c
}

type Result struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration // when denied: time until one token is available
}

type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

// ---------------------------------------------------------------- Redis (shared across instances)

// Atomic token bucket. Uses Redis TIME so app-server clock skew cannot matter.
const bucketScript = `
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t     = redis.call('TIME')
local now   = t[1] * 1000 + math.floor(t[2] / 1000)

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) * 2)
return {allowed, math.floor(tokens), retry}
`

type RedisLimiter struct {
	rdb    redis.Scripter
	cfg    Config
	script *redis.Script
}

func NewRedis(rdb redis.Scripter, cfg Config) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, cfg: cfg.withDefaults(), script: redis.NewScript(bucketScript)}
}

func (l *RedisLimiter) Allow(ctx context.Context, key string) (Result, error) {
	out, err := l.script.Run(ctx, l.rdb, []string{cache.RateLimitKey(key)}, l.cfg.Rate, l.cfg.Burst).Int64Slice()
	if err != nil {
		return Result{}, err
	}
	return Result{
		Allowed:    out[0] == 1,
		Remaining:  int(out[1]),
		RetryAfter: time.Duration(out[2]) * time.Millisecond,
	}, nil
}

// ---------------------------------------------------------------- memory (tests, single-process dev)

type bucket struct {
	tokens float64
	last   time.Time
}

type MemoryLimiter struct {
	mu      sync.Mutex
	cfg     Config
	buckets map[string]*bucket
	now     func() time.Time
}

func NewMemory(cfg Config) *MemoryLimiter {
	return &MemoryLimiter{cfg: cfg.withDefaults(), buckets: map[string]*bucket{}, now: time.Now}
}

func (m *MemoryLimiter) Allow(_ context.Context, key string) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	b, ok := m.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(m.cfg.Burst), last: now}
		m.buckets[key] = b
		if len(m.buckets) > 10_000 {
			m.sweep(now)
		}
	}
	b.tokens = math.Min(float64(m.cfg.Burst), b.tokens+now.Sub(b.last).Seconds()*m.cfg.Rate)
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return Result{Allowed: true, Remaining: int(b.tokens)}, nil
	}
	retry := time.Duration((1 - b.tokens) / m.cfg.Rate * float64(time.Second))
	return Result{Allowed: false, RetryAfter: retry}, nil
}

// sweep drops buckets that would be completely refilled anyway.
func (m *MemoryLimiter) sweep(now time.Time) {
	idle := time.Duration(float64(m.cfg.Burst) / m.cfg.Rate * 2 * float64(time.Second))
	for k, b := range m.buckets {
		if now.Sub(b.last) > idle {
			delete(m.buckets, k)
		}
	}
}

// ---------------------------------------------------------------- HTTP middleware

// KeyFunc returns the bucket key for a request (e.g. "api:"+userID).
// ok=false skips limiting (e.g. unauthenticated: let the handler answer 401).
type KeyFunc func(r *http.Request) (key string, ok bool)

// Middleware answers 429 + Retry-After when the caller's bucket is empty.
// It FAILS OPEN when the limiter backend errors: an outage of Redis must not
// take the API down with it.
func Middleware(l Limiter, key KeyFunc, log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k, ok := key(r)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			res, err := l.Allow(r.Context(), k)
			if err != nil {
				log.Warn("rate limiter unavailable; failing open", "error", err)
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			if !res.Allowed {
				secs := int(math.Ceil(res.RetryAfter.Seconds()))
				if secs < 1 {
					secs = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(secs))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
