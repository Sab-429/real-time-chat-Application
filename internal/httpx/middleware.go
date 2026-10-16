package httpx

import (
	"log/slog"
	"net/http"
	"slices"
	"time"
)

type Middleware func(http.Handler) http.Handler

// Chain wraps h so that the FIRST middleware is the outermost.
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// Recoverer is Recover with an explicit logger: the panic barrier used by
// cmd/api and cmd/gateway. It shares one implementation with Recover, so both
// handle http.ErrAbortHandler and broken-pipe panics identically.
func Recoverer(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return recoverWith(log, next)
	}
}

// accessRecorder is the lightweight wrapper AccessLog uses: it remembers only
// the status code. (httpx.go has its own statusRecorder, which also counts
// bytes for RequestLogger.)
type accessRecorder struct {
	http.ResponseWriter
	status int
}

func (s *accessRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *accessRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func AccessLog(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &accessRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
				return
			}
			log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rec.status,
				"duration_ms", time.Since(start).Milliseconds())
		})
	}
}

// CORS allows the listed origins (exact match; "*" only for dev).
func CORS(allowed []string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && (slices.Contains(allowed, "*") || slices.Contains(allowed, origin)) {
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", origin)
				h.Add("Vary", "Origin")
				h.Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
				h.Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
				h.Set("Access-Control-Max-Age", "600")
				if r.Method == http.MethodOptions {
					w.WriteHeader(http.StatusNoContent)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}