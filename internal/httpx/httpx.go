// Package httpx contains the small helpers shared by every HTTP endpoint of
// the API: JSON responses, request body decoding, access logging, panic
// containment and - most importantly - classification of the "the peer is
// gone" errors (broken pipe, connection reset, closed socket, ...) that are
// expected whenever a browser, a mobile client or a proxy drops a long lived
// SSE or WebSocket stream.
//
// Treating those errors as ordinary client churn instead of failures is what
// keeps a real time chat server quiet and healthy: an EPIPE on a write to a
// disconnected subscriber is not an error of the server, it is just the
// network telling us that somebody closed the tab.
package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

// MaxBodyBytes is the maximum size of a JSON request body accepted by Decode.
const MaxBodyBytes = 1 << 20 // 1 MiB

type errorEnvelope struct {
	Error string `json:"error"`
}

// JSON writes v to w as a JSON document using the given status code.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// A failure here means the peer disappeared while the response was being
	// written. The status line is already on the wire, there is nobody left to
	// report the failure to, so it is deliberately ignored. Detecting a dead
	// peer is the job of the handlers that own long lived streams.
	_ = json.NewEncoder(w).Encode(v)
}

// Error writes a JSON error envelope: {"error": "message"}.
func Error(w http.ResponseWriter, status int, message string) {
	JSON(w, status, errorEnvelope{Error: message})
}

// Decode reads a single JSON object from the request body into dst. The body
// is capped at MaxBodyBytes and unknown fields are rejected so that typos in
// client payloads fail loudly instead of being silently ignored.
func Decode(w http.ResponseWriter, r *http.Request, dst any) error {
	body := http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("request body exceeds %d bytes", MaxBodyBytes)
		}
		if errors.Is(err, io.EOF) {
			return errors.New("request body is empty")
		}
		return fmt.Errorf("invalid JSON body: %v", err)
	}

	// Exactly one JSON value is accepted.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

// disconnectedMarkers covers the error strings produced by the OS and by the
// stdlib http stack for the "other side is gone" family of failures. Some of
// them are wrapped several layers deep (net.OpError -> os.SyscallError ->
// syscall.Errno) and do not always compare equal through errors.Is, so a
// textual fallback is kept as well.
var disconnectedMarkers = []string{
	"broken pipe",
	"connection reset by peer",
	"use of closed network connection",
	"software caused connection abort",    // ECONNABORTED: peer reset mid transfer
	"forcibly closed by the remote host",  // Windows wording for the same reset
	"no such device or address",           // ENXIO, seen on some WSL/macOS sockets
	"transport endpoint is not connected", // ENOTCONN
	"i/o timeout",                         // stringified form of os.ErrDeadlineExceeded
	"unexpected EOF",
	"http: request body too large", // MaxBytesReader cuts the connection too
}

// IsDisconnected reports whether err is the ordinary "the peer is gone"
// family of errors. These are expected for SSE, WebSocket and any other long
// lived stream: they must be logged as client churn, never as server faults.
//
// Both shapes the OS hands us are covered:
//   - "the connection died": EPIPE, ECONNRESET, ECONNABORTED, ENOTCONN,
//     ETIMEDOUT, closed sockets, EOFs;
//   - "the peer stopped consuming": a write deadline exceeded
//     (os.ErrDeadlineExceeded, "i/o timeout") means the client's TCP window
//     stayed full until we gave up. Also churn, not an incident.
//
// This is the function that turns a scary "write: broken pipe" stack trace
// into a one line "subscriber disconnected" debug message.
//
// Scope: pass it errors that come from writing to the response socket (or
// from a panic raised there). The textual fallback matches any "i/o timeout",
// which is right for a stalled client but would be wrong for a database dial
// failure - those must keep surfacing at error level.
func IsDisconnected(err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED),
		errors.Is(err, syscall.ETIMEDOUT),
		errors.Is(err, syscall.ENOTCONN),
		errors.Is(err, os.ErrDeadlineExceeded),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, io.ErrClosedPipe),
		errors.Is(err, http.ErrHijacked),
		errors.Is(err, http.ErrAbortHandler):
		return true
	}

	msg := err.Error()
	for _, marker := range disconnectedMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// LogWriteFailure reports a failed write on a stream (SSE, WebSocket, plain
// response) at the appropriate level: client churn stays at debug level so
// that a browser refreshing the page does not look like an incident.
func LogWriteFailure(op, stream string, err error) {
	if err == nil {
		return
	}
	if IsDisconnected(err) {
		slog.Debug("stream write aborted, client is gone",
			"op", op, "stream", stream, "reason", err.Error())
		return
	}
	slog.Error("stream write failed", "op", op, "stream", stream, "error", err)
}

// statusRecorder remembers the status code and byte count of a response so
// that the access log can report them.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying ResponseWriter,
// which is what keeps Flush/SetWriteDeadline/Hijack working through the log
// wrapper (http.ResponseController walks the Unwrap chain).
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// RequestLogger emits one structured line per completed request. Streaming
// routes (SSE, WebSocket) should stay outside of this middleware: their
// handlers only return when the stream ends, which would log a single entry
// minutes after the request started.
func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		level := slog.LevelInfo
		if rec.status >= 500 || (rec.status >= 400 && rec.status != http.StatusNotFound) {
			level = slog.LevelWarn
		}
		slog.Log(r.Context(), level, "http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration", time.Since(start).Round(time.Millisecond).String(),
			"remote", r.RemoteAddr,
		)
	})
}

// Recover keeps a panic inside a handler from tearing down the whole server
// (and from dumping a full goroutine stack into the logs every time a client
// drops a connection mid-flight). It logs through the process wide slog
// default; use Recoverer when the panic logs must land in another logger.
func Recover(next http.Handler) http.Handler {
	return recoverWith(slog.Default(), next)
}

// recoverWith is the single panic barrier behind Recover and Recoverer. It
// classifies the panic before anything is written:
//
//   - http.ErrAbortHandler is re-raised, so net/http can abort the response
//     without logging it (its documented contract);
//   - a disconnect panic (broken pipe, reset, write timeout on a committed
//     response) is client churn: one debug line, no stack, no 500;
//   - anything else is a real fault: logged with its stack and answered with
//     a JSON 500, but only if the response has not started yet.
//
// Keeping this classification in ONE place matters: the previous split
// implementation made the weaker copy the one that got wired into cmd/api.
func recoverWith(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			// net/http uses ErrAbortHandler to abort a response without
			// logging; re-panic so the stdlib keeps that contract.
			if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(rec)
			}
			// A peer that vanished mid write must not look like an incident.
			if err, ok := rec.(error); ok && IsDisconnected(err) {
				log.Debug("handler aborted by a disconnected peer",
					"path", r.URL.Path,
					"method", r.Method,
					"reason", err.Error(),
				)
				return
			}
			log.Error("panic recovered in handler",
				"path", r.URL.Path,
				"method", r.Method,
				"panic", fmt.Sprintf("%v", rec),
				"stack", string(debug.Stack()),
			)
			httpxPanicResponse(w)
		}()
		next.ServeHTTP(w, r)
	})
}

// httpxPanicResponse reports a panic to the client when the response has not
// been started yet. If headers were already flushed (a half written SSE
// stream, a hijacked WebSocket) there is nothing sensible to send and the
// connection is simply left for the stdlib to close.
func httpxPanicResponse(w http.ResponseWriter) {
	defer func() {
		_ = recover() // WriteHeader after Write panics; the response is already gone
	}()
	Error(w, http.StatusInternalServerError, "internal server error")
}
