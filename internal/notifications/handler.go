package notifications

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
)

type DeviceStore interface {
	UpsertDevice(ctx context.Context, userID, platform, token string) error
	RemoveDevice(ctx context.Context, userID, token string) error
}

type HandlerConfig struct {
	Store  DeviceStore
	UserID func(r *http.Request) (string, bool) // plug in your internal/auth middleware
}

type Handler struct{ HandlerConfig }

func NewHandler(c HandlerConfig) *Handler { return &Handler{c} }

// Register mounts the device-token routes (Go 1.22+ mux patterns).
//   PUT    /v1/devices  {"platform":"android|ios|web","token":"..."}  call on every app launch (refreshes last_seen_at)
//   DELETE /v1/devices  {"token":"..."}                               call on logout
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /v1/devices", h.upsert)
	mux.HandleFunc("DELETE /v1/devices", h.remove)
}

type deviceRequest struct {
	Platform string `json:"platform"`
	Token    string `json:"token"`
}

func (h *Handler) upsert(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req deviceRequest
	if !decode(w, r, &req) {
		return
	}
	if !slices.Contains([]string{PlatformAndroid, PlatformIOS, PlatformWeb}, req.Platform) {
		writeErr(w, http.StatusBadRequest, "invalid platform")
		return
	}
	if req.Token == "" || len(req.Token) > 4096 {
		writeErr(w, http.StatusBadRequest, "invalid token")
		return
	}
	if err := h.Store.UpsertDevice(r.Context(), uid, req.Platform, req.Token); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req deviceRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Token == "" {
		writeErr(w, http.StatusBadRequest, "invalid token")
		return
	}
	if err := h.Store.RemoveDevice(r.Context(), uid, req.Token); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return false
	}
	return true
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}