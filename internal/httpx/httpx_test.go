package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestIsDisconnected(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"raw EPIPE", syscall.EPIPE, true},
		{"raw ECONNRESET", syscall.ECONNRESET, true},
		{"raw ECONNABORTED", syscall.ECONNABORTED, true},
		{"write deadline exceeded", os.ErrDeadlineExceeded, true},
		{"deadline text", errors.New("write tcp 1.2.3.4:80->5.6.7.8:1234: i/o timeout"), true},
		{"aborted text", errors.New("write tcp: software caused connection abort"), true},
		{"windows reset text", errors.New("read tcp: An existing connection was forcibly closed by the remote host"), true},
		{"raw ENOTCONN", syscall.ENOTCONN, true},
		{"net.ErrClosed", net.ErrClosed, true},
		{"context cancelled", context.Canceled, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"io.EOF", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"closed pipe", io.ErrClosedPipe, true},
		{"hijacked", http.ErrHijacked, true},
		{"wrapped op error", &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}, true},
		{"os error text", errors.New("write tcp4 1.2.3.4:80->5.6.7.8:1234: broken pipe"), true},
		{"reset text", errors.New("read tcp: connection reset by peer"), true},
		{"closed conn text", errors.New("write: use of closed network connection"), true},
		{"plain failure", errors.New("boom"), false},
		{"database failure", errors.New(`relation "messages" does not exist`), false},
		{"kafka failure", errors.New("kafka: [3] UnknownTopicOrPartition"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsDisconnected(tc.err); got != tc.want {
				t.Fatalf("IsDisconnected(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestDecodeAcceptsSingleObject(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"a":"b"}`))

	var dst struct {
		A string `json:"a"`
	}
	if err := Decode(rec, r, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.A != "b" {
		t.Fatalf("decoded %+v", dst)
	}
}

func TestDecodeRejectsBadBodies(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"unknown field", `{"a":"b","nope":1}`},
		{"not json", `hello`},
		{"trailing garbage", `{"a":"b"} {"a":"c"}`},
		{"array instead of object", `[{"a":"b"}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(tc.body))
			var dst struct {
				A string `json:"a"`
			}
			if err := Decode(rec, r, &dst); err == nil {
				t.Fatalf("body %q was accepted", tc.body)
			}
		})
	}
}

func TestDecodeCapsBodySize(t *testing.T) {
	rec := httptest.NewRecorder()
	huge := `{"payload":"` + strings.Repeat("x", MaxBodyBytes+1024) + `"}`
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(huge))

	var dst struct {
		Payload string `json:"payload"`
	}
	err := Decode(rec, r, &dst)
	if err == nil {
		t.Fatal("oversized body was accepted")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJSONWritesEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	JSON(rec, http.StatusTeapot, map[string]string{"error": "short and stout"})

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q", ct)
	}
	var out map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if out["error"] != "short and stout" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestRequestLoggerRecordsStatus(t *testing.T) {
	var gotStatus int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := RequestLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		gotStatus = rec.status
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))
	if gotStatus != http.StatusNoContent {
		t.Fatalf("recorded status = %d", gotStatus)
	}
}

// quiet keeps panic tests from spraying stacks into the test output.
func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRecovererFaultBecomesJSON500(t *testing.T) {
	rec := httptest.NewRecorder()
	Recoverer(quiet())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var env struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if env.Error != "internal server error" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

// A peer that disappears while its response is being written must not be
// answered (the socket is gone) and must not be reported as a fault.
func TestRecovererDoesNotAnswerGonePeer(t *testing.T) {
	rec := httptest.NewRecorder()
	Recoverer(quiet())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(&net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE})
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/gone", nil))

	if rec.Body.Len() != 0 {
		t.Fatalf("wrote %q to a peer that is already gone", rec.Body.String())
	}
}

// http.ErrAbortHandler is how net/http asks to abort quietly; swallowing it
// would turn an ordinary disconnect into a logged 500.
func TestRecovererReRaisesAbortHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("ErrAbortHandler was swallowed, net/http can no longer abort silently")
		}
	}()
	Recoverer(quiet())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/abort", nil))
}
