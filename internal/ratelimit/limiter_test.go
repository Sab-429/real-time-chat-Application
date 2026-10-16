package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTest(cfg Config) (*MemoryLimiter, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	m := NewMemory(cfg)
	m.now = c.now
	return m, c
}

func TestBurstThenDeny(t *testing.T) {
	m, _ := newTest(Config{Rate: 1, Burst: 3})
	for i := 0; i < 3; i++ {
		if r, _ := m.Allow(context.Background(), "u1"); !r.Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	r, _ := m.Allow(context.Background(), "u1")
	if r.Allowed || r.RetryAfter <= 0 {
		t.Fatalf("4th request must be denied with a RetryAfter, got %+v", r)
	}
}

func TestRefillsOverTime(t *testing.T) {
	m, c := newTest(Config{Rate: 2, Burst: 2}) // 2 tokens/sec
	for i := 0; i < 2; i++ {
		m.Allow(context.Background(), "u1")
	}
	if r, _ := m.Allow(context.Background(), "u1"); r.Allowed {
		t.Fatal("bucket should be empty")
	}
	c.t = c.t.Add(500 * time.Millisecond) // one token back
	if r, _ := m.Allow(context.Background(), "u1"); !r.Allowed {
		t.Fatal("a token should have refilled after 500ms")
	}
	if r, _ := m.Allow(context.Background(), "u1"); r.Allowed {
		t.Fatal("only one token should have refilled")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	m, _ := newTest(Config{Rate: 1, Burst: 1})
	m.Allow(context.Background(), "u1")
	if r, _ := m.Allow(context.Background(), "u1"); r.Allowed {
		t.Fatal("u1 should be limited")
	}
	if r, _ := m.Allow(context.Background(), "u2"); !r.Allowed {
		t.Fatal("u2 must not be affected by u1")
	}
}

func TestBurstIsCapped(t *testing.T) {
	m, c := newTest(Config{Rate: 10, Burst: 2})
	m.Allow(context.Background(), "u1")
	c.t = c.t.Add(time.Hour) // a long idle period must not bank unlimited tokens
	allowed := 0
	for i := 0; i < 5; i++ {
		if r, _ := m.Allow(context.Background(), "u1"); r.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("allowed=%d, want burst of 2", allowed)
	}
}

func TestMiddlewareReturns429WithRetryAfter(t *testing.T) {
	m, _ := newTest(Config{Rate: 0.5, Burst: 1})
	h := Middleware(m, func(*http.Request) (string, bool) { return "k", true }, nil)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("first request: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second request: code=%d retry-after=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestMiddlewareSkipsWhenNoKey(t *testing.T) {
	m, _ := newTest(Config{Rate: 1, Burst: 1})
	called := 0
	h := Middleware(m, func(*http.Request) (string, bool) { return "", false }, nil)(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	if called != 5 {
		t.Fatalf("called=%d", called)
	}
}

type brokenLimiter struct{}

func (brokenLimiter) Allow(context.Context, string) (Result, error) {
	return Result{}, errors.New("redis down")
}

func TestMiddlewareFailsOpen(t *testing.T) {
	called := false
	h := Middleware(brokenLimiter{}, func(*http.Request) (string, bool) { return "k", true }, nil)(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("must fail open when the limiter backend is down")
	}
}
