package chat

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
)

type HandlerConfig struct {
	Service  *Service
	Identity func(r *http.Request) (Identity, bool) // plug in your internal/auth middleware
	Logger   *slog.Logger
}

type Handler struct{ HandlerConfig }

func NewHandler(c HandlerConfig) *Handler {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return &Handler{c}
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Register mounts the REST API (Go 1.22+ mux patterns). Live sending happens
// over WebSocket; POST .../messages is the HTTP fallback (and what bots use).
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/conversations", h.createConversation)
	mux.HandleFunc("GET /v1/conversations", h.listConversations)
	mux.HandleFunc("GET /v1/conversations/{id}/messages", h.history)
	mux.HandleFunc("POST /v1/conversations/{id}/messages", h.send)
	mux.HandleFunc("POST /v1/conversations/{id}/read", h.markRead)
	mux.HandleFunc("POST /v1/sync", h.sync)
}

func (h *Handler) createConversation(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	var req struct {
		Type      string   `json:"type"`
		Title     string   `json:"title"`
		MemberIDs []string `json:"member_ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	c, err := h.Service.CreateConversation(r.Context(), CreateConversationInput{
		CreatorID: id.ID, Type: req.Type, Title: req.Title, MemberIDs: req.MemberIDs,
	})
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (h *Handler) listConversations(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	out, err := h.Service.ListConversations(r.Context(), id.ID, intParam(r, "limit", 50))
	if err != nil {
		h.fail(w, err)
		return
	}
	if out == nil {
		out = []ConversationSummary{}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	convID, ok := pathID(w, r)
	if !ok {
		return
	}
	msgs, err := h.Service.History(r.Context(), id.ID, convID, int64(intParam(r, "before_seq", 0)), intParam(r, "limit", 50))
	if err != nil {
		h.fail(w, err)
		return
	}
	if msgs == nil {
		msgs = []Message{}
	}
	writeJSON(w, http.StatusOK, msgs) // newest first; page with before_seq = last item's seq
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	convID, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		ClientMsgID string `json:"client_msg_id"`
		Body        string `json:"body"`
	}
	if !decode(w, r, &req) {
		return
	}
	res, err := h.Service.Send(r.Context(), SendInput{
		ConversationID: convID, SenderID: id.ID, SenderName: id.Name,
		ClientMsgID: req.ClientMsgID, Body: req.Body,
	})
	if err != nil {
		h.fail(w, err)
		return
	}
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK // idempotent retry
	}
	writeJSON(w, status, res.Message)
}

func (h *Handler) markRead(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	convID, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		Seq int64 `json:"seq"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := h.Service.MarkRead(r.Context(), id.ID, convID, req.Seq); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) sync(w http.ResponseWriter, r *http.Request) {
	id, ok := h.auth(w, r)
	if !ok {
		return
	}
	var req struct {
		Conversations []SyncRequest `json:"conversations"`
	}
	if !decode(w, r, &req) {
		return
	}
	out, err := h.Service.Sync(r.Context(), id.ID, req.Conversations)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": out})
}

// ------------------------------------------------------------------ helpers

func (h *Handler) auth(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	id, ok := h.Identity(r)
	if !ok || id.ID == "" {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return Identity{}, false
	}
	return id, true
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidInput):
		writeErr(w, http.StatusBadRequest, invalidMessage(err))
	case errors.Is(err, ErrNotMember):
		writeErr(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	default:
		h.Logger.Error("request failed", "error", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return "", false
	}
	return id, true
}

func intParam(r *http.Request, name string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil && v >= 0 {
		return v
	}
	return def
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}