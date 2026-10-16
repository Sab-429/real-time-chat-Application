package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sabya/real-time-chat/internal/httpx"
)

// User is the public representation of an account. It never carries the
// password hash.
type User struct {
	ID          string    `json:"id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
}

// ErrUsernameTaken is returned when the requested username already exists.
var ErrUsernameTaken = errors.New("username is already taken")

// ErrUnknownUser is returned when no account matches the given id/username.
var ErrUnknownUser = errors.New("user not found")

var usernameRe = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)

// Store persists accounts.
type Store struct {
	DB *pgxpool.Pool
}

// NewStore builds a Store backed by the given pool.
func NewStore(db *pgxpool.Pool) *Store { return &Store{DB: db} }

// Create inserts a new account.
func (s *Store) Create(ctx context.Context, username, displayName, passwordHash string) (User, error) {
	user := User{
		ID:          uuid.NewString(),
		Username:    username,
		DisplayName: displayName,
		CreatedAt:   time.Now().UTC(),
	}
	err := s.DB.QueryRow(ctx,
		`INSERT INTO users (id, username, display_name, password_hash, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING created_at`,
		user.ID, user.Username, displayName, passwordHash, user.CreatedAt,
	).Scan(&user.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return User{}, ErrUsernameTaken
		}
		return User{}, err
	}
	return user, nil
}

// Credentials looks up the id, username and password hash of an account.
func (s *Store) Credentials(ctx context.Context, username string) (User, string, error) {
	var (
		user User
		hash string
	)
	err := s.DB.QueryRow(ctx,
		`SELECT id, username, display_name, password_hash, created_at
		   FROM users WHERE username = $1`,
		username,
	).Scan(&user.ID, &user.Username, &user.DisplayName, &hash, &user.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, "", ErrUnknownUser
	}
	if err != nil {
		return User{}, "", err
	}
	return user, hash, nil
}

// ByID fetches one account.
func (s *Store) ByID(ctx context.Context, id string) (User, error) {
	return s.single(ctx, `SELECT id, username, display_name, created_at FROM users WHERE id = $1`, id)
}

// ByUsername fetches one account by its username.
func (s *Store) ByUsername(ctx context.Context, username string) (User, error) {
	return s.single(ctx, `SELECT id, username, display_name, created_at FROM users WHERE username = $1`, username)
}

// Directory lists accounts for the contact picker, optionally filtered by a
// username/display name substring. The caller's own account is excluded.
func (s *Store) Directory(ctx context.Context, query, excludeID string, limit int) ([]User, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	q := strings.TrimSpace(query)

	rows, err := s.DB.Query(ctx,
		`SELECT id, username, display_name, created_at
		   FROM users
		  WHERE id <> $1
		    AND ($2 = '' OR username ILIKE '%' || $2 || '%' OR display_name ILIKE '%' || $2 || '%')
		  ORDER BY username
		  LIMIT $3`,
		excludeID, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]User, 0)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.DisplayName, &u.CreatedAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) single(ctx context.Context, query, id string) (User, error) {
	var u User
	err := s.DB.QueryRow(ctx, query, id).Scan(&u.ID, &u.Username, &u.DisplayName, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrUnknownUser
	}
	if err != nil {
		return User{}, err
	}
	return u, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Handler exposes the authentication and directory endpoints.
type Handler struct {
	store  *Store
	tokens *Manager
}

// NewHandler wires the auth endpoints.
func NewHandler(store *Store, tokens *Manager) *Handler {
	return &Handler{store: store, tokens: tokens}
}

// RegisterPublic adds the endpoints that must work without a token.
func (h *Handler) RegisterPublic(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/register", h.register)
	mux.HandleFunc("POST /api/v1/auth/login", h.login)
}

// RegisterProtected adds the endpoints behind auth.Authenticate.
func (h *Handler) RegisterProtected(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/auth/me", h.me)
	mux.HandleFunc("GET /api/v1/users", h.directory)
}

// Register adds every auth route to mux, protected and public alike. It is
// the convenience entry point for tests and for deployments that put their
// own middleware in front of the whole tree.
func (h *Handler) Register(mux *http.ServeMux) {
	h.RegisterPublic(mux)
	h.RegisterProtected(mux)
}

type registerRequest struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Password    string `json:"password"`
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := httpx.Decode(w, r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	username := strings.ToLower(strings.TrimSpace(req.Username))
	if !usernameRe.MatchString(username) {
		httpx.Error(w, http.StatusUnprocessableEntity,
			"username must be 3-32 characters of a-z, 0-9 or _")
		return
	}

	hash, err := HashPassword(req.Password)
	if err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	displayName := strings.TrimSpace(req.DisplayName)
	if displayName == "" {
		displayName = username
	}

	user, err := h.store.Create(r.Context(), username, displayName, hash)
	if err != nil {
		switch {
		case errors.Is(err, ErrUsernameTaken):
			httpx.Error(w, http.StatusConflict, ErrUsernameTaken.Error())
		default:
			slog.Error("register failed", "error", err)
			httpx.Error(w, http.StatusInternalServerError, "could not create the account")
		}
		return
	}

	token, err := h.tokens.Issue(user.ID, user.Username)
	if err != nil {
		slog.Error("issue token failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "could not issue a token")
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]any{"user": user, "token": token, "token_ttl": h.tokens.TTL().String()})
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := httpx.Decode(w, r, &req); err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	username := strings.ToLower(strings.TrimSpace(req.Username))
	// Always run a bcrypt comparison, even for unknown usernames, so that the
	// response time does not tell an attacker whether the account exists.
	user, hash, err := h.store.Credentials(r.Context(), username)
	if err != nil {
		if errors.Is(err, ErrUnknownUser) {
			_, _ = HashPassword(req.Password)
			httpx.Error(w, http.StatusUnauthorized, ErrInvalidCredentials.Error())
			return
		}
		slog.Error("login lookup failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "could not verify the credentials")
		return
	}
	if !CheckPassword(hash, req.Password) {
		httpx.Error(w, http.StatusUnauthorized, ErrInvalidCredentials.Error())
		return
	}

	token, err := h.tokens.Issue(user.ID, user.Username)
	if err != nil {
		slog.Error("issue token failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "could not issue a token")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"user": user, "token": token, "token_ttl": h.tokens.TTL().String()})
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	userID, ok := UserID(r.Context())
	if !ok {
		httpx.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	user, err := h.store.ByID(r.Context(), userID)
	if err != nil {
		if errors.Is(err, ErrUnknownUser) {
			httpx.Error(w, http.StatusNotFound, ErrUnknownUser.Error())
			return
		}
		slog.Error("load user failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "could not load the account")
		return
	}
	httpx.JSON(w, http.StatusOK, user)
}

func (h *Handler) directory(w http.ResponseWriter, r *http.Request) {
	userID, ok := UserID(r.Context())
	if !ok {
		httpx.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	users, err := h.store.Directory(r.Context(), r.URL.Query().Get("q"), userID, 50)
	if err != nil {
		slog.Error("directory query failed", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "could not list users")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"users": users})
}
