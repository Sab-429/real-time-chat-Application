package webhooks

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
)

type EndpointStore interface {
	Create(ctx context.Context, ep Endpoint) (Endpoint, error)
	ListByOwner(ctx context.Context, ownerID string) ([]Endpoint, error)
	CountByOwner(ctx context.Context, ownerID string) (int, error)
	Delete(ctx context.Context, id, ownerID string) (bool, error)
	ListDeliveries(ctx context.Context, endpointID, ownerID string, limit int) ([]DeliverySummary, error)
}

type HandlerConfig struct {
	Store         EndpointStore
	UserID        func(r *http.Request) (string, bool) // plug in your internal/auth middleware
	AllowInsecure bool                                 // permit http:// (dev only)
	MaxPerUser    int
}

type Handler struct{ HandlerConfig }

func NewHandler(c HandlerConfig) *Handler {
	if c.MaxPerUser <= 0 {
		c.MaxPerUser = 10
	}
	return &Handler{c}
}

// Register mounts routes. Wrap the mux with your auth middleware.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/webhooks", h.create)
	mux.HandleFunc("GET /v1/webhooks", h.list)
	mux.HandleFunc("DELETE /v1/webhooks/{id}", h.delete)
	mux.HandleFunc("GET /v1/webhooks/{id}/deliveries", h.deliveries)
}

type createRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
}

type createResponse struct {
	Endpoint
	Secret string `json:"secret"` // returned exactly once
}

var (
	supportedEvents = []string{EventMessageCreated}
	uuidRe          = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req createRequest
	if !decode(w, r, &req) {
		return
	}
	if err := ValidateEndpointURL(req.URL, h.AllowInsecure); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.EventTypes) == 0 {
		req.EventTypes = []string{EventMessageCreated}
	}
	for _, t := range req.EventTypes {
		if !slices.Contains(supportedEvents, t) {
			writeErr(w, http.StatusBadRequest, "unsupported event type: "+t)
			return
		}
	}
	n, err := h.Store.CountByOwner(r.Context(), uid)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if n >= h.MaxPerUser {
		writeErr(w, http.StatusConflict, "webhook endpoint limit reached")
		return
	}
	secret, err := GenerateSecret()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	ep, err := h.Store.Create(r.Context(), Endpoint{OwnerID: uid, URL: req.URL, Secret: secret, EventTypes: req.EventTypes})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, createResponse{Endpoint: ep, Secret: secret})
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	eps, err := h.Store.ListByOwner(r.Context(), uid)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if eps == nil {
		eps = []Endpoint{}
	}
	writeJSON(w, http.StatusOK, eps)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	deleted, err := h.Store.Delete(r.Context(), id, uid)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !deleted {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) deliveries(w http.ResponseWriter, r *http.Request) {
	uid, ok := h.UserID(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 200 {
		limit = v
	}
	out, err := h.Store.ListDeliveries(r.Context(), id, uid, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	if out == nil {
		out = []DeliverySummary{}
	}
	writeJSON(w, http.StatusOK, out)
}

// ValidateEndpointURL rejects bad schemes, credentials in the URL and literal
// private IPs early. The dispatcher's dial guard still enforces this at connect
// time (covers hostnames resolving to private IPs).
func ValidateEndpointURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("invalid url")
	}
	if len(raw) > 2048 {
		return errors.New("url too long")
	}
	if u.User != nil {
		return errors.New("credentials in url not allowed")
	}
	if u.Scheme != "https" && !(allowInsecure && u.Scheme == "http") {
		return errors.New("url must use https")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !allowInsecure && isBlockedIP(ip) {
		return errors.New("destination address not allowed")
	}
	return nil
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

// Swap these two for your internal/httpx helpers if you prefer.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}