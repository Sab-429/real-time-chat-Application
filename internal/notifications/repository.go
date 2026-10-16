package notifications

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct{ db *pgxpool.Pool }

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

var (
	_ Store       = (*Repository)(nil)
	_ DeviceStore = (*Repository)(nil)
	_ Pruner      = (*Repository)(nil)
)

// NotifiableUsers returns the given members who have NOT muted the conversation.
// (Cast to text so this works whether conversation_members.user_id is UUID or TEXT.)
func (r *Repository) NotifiableUsers(ctx context.Context, conversationID string, userIDs []string) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT user_id::text FROM conversation_members
		 WHERE conversation_id = $1 AND user_id::text = ANY($2) AND NOT muted`,
		conversationID, userIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (r *Repository) DevicesForUsers(ctx context.Context, userIDs []string) ([]Device, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	rows, err := r.db.Query(ctx,
		`SELECT user_id, platform, token FROM device_tokens WHERE user_id = ANY($1)`, userIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.UserID, &d.Platform, &d.Token); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (r *Repository) DeleteDevice(ctx context.Context, token string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM device_tokens WHERE token = $1`, token)
	return err
}

func (r *Repository) WasSent(ctx context.Context, eventID, userID string) (bool, error) {
	var ok bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM notification_log WHERE event_id=$1 AND user_id=$2)`,
		eventID, userID).Scan(&ok)
	return ok, err
}

func (r *Repository) MarkSent(ctx context.Context, eventID, userID string) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO notification_log (event_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		eventID, userID)
	return err
}

// ---- device registration (used by handler.go)

// UpsertDevice also reassigns a token to the current user when the same
// physical device logs into a different account.
func (r *Repository) UpsertDevice(ctx context.Context, userID, platform, token string) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO device_tokens (user_id, platform, token) VALUES ($1,$2,$3)
		 ON CONFLICT (token) DO UPDATE
		   SET user_id=EXCLUDED.user_id, platform=EXCLUDED.platform, last_seen_at=now()`,
		userID, platform, token)
	return err
}

func (r *Repository) RemoveDevice(ctx context.Context, userID, token string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM device_tokens WHERE token=$1 AND user_id=$2`, token, userID)
	return err
}

// ---- housekeeping

func (r *Repository) PruneStaleDevices(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM device_tokens WHERE last_seen_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	return tag.RowsAffected(), err
}

func (r *Repository) PruneLog(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM notification_log WHERE sent_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	return tag.RowsAffected(), err
}