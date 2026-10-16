package chat

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct{ db *pgxpool.Pool }

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

var _ Store = (*Repository)(nil)

const msgCols = `id::text, conversation_id::text, seq, sender_id, client_msg_id, body, created_at`

type scanner interface{ Scan(dest ...any) error }

func scanMessage(s scanner) (Message, error) {
	var m Message
	err := s.Scan(&m.ID, &m.ConversationID, &m.Seq, &m.SenderID, &m.ClientMsgID, &m.Body, &m.CreatedAt)
	return m, err
}

func collectMessages(rows pgx.Rows) ([]Message, error) {
	defer rows.Close()
	var out []Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ------------------------------------------------------------------ transactions

func (r *Repository) InTx(ctx context.Context, fn func(Tx) error) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit
	if err := fn(&pgTx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type pgTx struct{ tx pgx.Tx }

// NextSeq increments the per-conversation counter. The UPDATE row-locks the
// conversation until commit, which serialises senders and gives gap-free,
// strictly increasing seq numbers. (A single extremely hot conversation is the
// throughput ceiling; that is the price of strict ordering.)
func (t *pgTx) NextSeq(ctx context.Context, conversationID string) (int64, error) {
	var seq int64
	err := t.tx.QueryRow(ctx,
		`UPDATE conversations SET last_seq = last_seq + 1, last_message_at = now()
		 WHERE id = $1 RETURNING last_seq`, conversationID).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return seq, err
}

// InsertMessage stores the message and advances the sender's own read pointer
// (you never have an unread message you wrote yourself).
func (t *pgTx) InsertMessage(ctx context.Context, m Message) (Message, error) {
	err := t.tx.QueryRow(ctx,
		`INSERT INTO messages (id, conversation_id, seq, sender_id, client_msg_id, body)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING created_at`,
		m.ID, m.ConversationID, m.Seq, m.SenderID, m.ClientMsgID, m.Body).Scan(&m.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "messages_sender_client_msg_uq" {
			return Message{}, ErrDuplicate
		}
		return Message{}, err
	}
	if _, err := t.tx.Exec(ctx,
		`UPDATE conversation_members SET last_read_seq = $3 WHERE conversation_id = $1 AND user_id = $2`,
		m.ConversationID, m.SenderID, m.Seq); err != nil {
		return Message{}, err
	}
	return m, nil
}

func (t *pgTx) InsertOutbox(ctx context.Context, topic, key string, payload []byte) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO outbox_events (topic, key, payload) VALUES ($1,$2,$3::jsonb)`,
		topic, key, string(payload))
	return err
}

// ------------------------------------------------------------------ reads

func (r *Repository) Members(ctx context.Context, conversationID string) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT user_id FROM conversation_members WHERE conversation_id = $1`, conversationID)
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

func (r *Repository) IsMember(ctx context.Context, conversationID, userID string) (bool, error) {
	var ok bool
	err := r.db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = $1 AND user_id = $2)`,
		conversationID, userID).Scan(&ok)
	return ok, err
}

func (r *Repository) MemberOf(ctx context.Context, userID string, conversationIDs []string) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT conversation_id::text FROM conversation_members
		 WHERE user_id = $1 AND conversation_id::text = ANY($2)`, userID, conversationIDs)
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

func (r *Repository) FindByClientMsgID(ctx context.Context, senderID, clientMsgID string) (*Message, error) {
	m, err := scanMessage(r.db.QueryRow(ctx,
		`SELECT `+msgCols+` FROM messages WHERE sender_id = $1 AND client_msg_id = $2`, senderID, clientMsgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (r *Repository) MessagesAfter(ctx context.Context, conversationID string, afterSeq int64, limit int) ([]Message, error) {
	rows, err := r.db.Query(ctx,
		`SELECT `+msgCols+` FROM messages WHERE conversation_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3`,
		conversationID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	return collectMessages(rows)
}

func (r *Repository) MessagesBefore(ctx context.Context, conversationID string, beforeSeq int64, limit int) ([]Message, error) {
	rows, err := r.db.Query(ctx,
		`SELECT `+msgCols+` FROM messages WHERE conversation_id = $1 AND seq < $2 ORDER BY seq DESC LIMIT $3`,
		conversationID, beforeSeq, limit)
	if err != nil {
		return nil, err
	}
	return collectMessages(rows)
}

func (r *Repository) ListConversations(ctx context.Context, userID string, limit int) ([]ConversationSummary, error) {
	rows, err := r.db.Query(ctx,
		`SELECT c.id::text, c.type, COALESCE(c.title,''), c.last_seq, c.created_at, m.last_read_seq, m.muted
		 FROM conversation_members m
		 JOIN conversations c ON c.id = m.conversation_id
		 WHERE m.user_id = $1
		 ORDER BY COALESCE(c.last_message_at, c.created_at) DESC
		 LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConversationSummary
	for rows.Next() {
		var c ConversationSummary
		if err := rows.Scan(&c.ID, &c.Type, &c.Title, &c.LastSeq, &c.CreatedAt, &c.LastReadSeq, &c.Muted); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// MarkRead never rewinds and never exceeds the conversation's last_seq.
func (r *Repository) MarkRead(ctx context.Context, conversationID, userID string, seq int64) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE conversation_members m
		    SET last_read_seq = GREATEST(m.last_read_seq, LEAST($3::bigint, c.last_seq))
		   FROM conversations c
		  WHERE c.id = m.conversation_id AND m.conversation_id = $1 AND m.user_id = $2`,
		conversationID, userID, seq)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotMember
	}
	return nil
}

// ------------------------------------------------------------------ conversations

func (r *Repository) CreateConversation(ctx context.Context, nc NewConversation) (*Conversation, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var directKeyArg any // nil -> NULL for groups
	if nc.DirectKey != "" {
		directKeyArg = nc.DirectKey
	}

	var c Conversation
	err = tx.QueryRow(ctx,
		`INSERT INTO conversations (type, title, direct_key)
		 VALUES ($1, NULLIF($2,''), $3)
		 ON CONFLICT (direct_key) DO NOTHING
		 RETURNING id::text, type, COALESCE(title,''), last_seq, created_at`,
		nc.Type, nc.Title, directKeyArg).Scan(&c.ID, &c.Type, &c.Title, &c.LastSeq, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) && nc.DirectKey != "" {
		_ = tx.Rollback(ctx)
		return r.getByDirectKey(ctx, nc.DirectKey) // the pair already has a conversation
	}
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO conversation_members (conversation_id, user_id, role)
		 SELECT $1::uuid, u, CASE WHEN u = $3 THEN 'admin' ELSE 'member' END
		 FROM unnest($2::text[]) AS u`,
		c.ID, nc.MemberIDs, nc.CreatorID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *Repository) getByDirectKey(ctx context.Context, key string) (*Conversation, error) {
	var c Conversation
	err := r.db.QueryRow(ctx,
		`SELECT id::text, type, COALESCE(title,''), last_seq, created_at FROM conversations WHERE direct_key = $1`,
		key).Scan(&c.ID, &c.Type, &c.Title, &c.LastSeq, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}