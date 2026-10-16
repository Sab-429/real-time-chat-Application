// Package migrations applies the SQL schema at startup.
//
// There is no migration framework on purpose: the files are numbered, each one
// is applied exactly once inside a transaction, and a Postgres advisory lock
// serialises concurrent API/worker replicas so the same file is never applied
// twice. Re-running the process is always safe.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

// lockID is the advisory lock key guarding schema changes. Any constant will
// do as long as it belongs to this application.
const lockID int64 = 727272

// validName keeps the filename interpolation in the generated script safe.
var validName = regexp.MustCompile(`^[0-9A-Za-z_.-]+$`)

// Apply runs every not-yet-applied *.sql file in lexicographic order.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrations: acquire connection: %w", err)
	}
	defer conn.Release()

	// One replica at a time; released automatically if the process dies.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("migrations: take advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, lockID); err != nil {
			slog.Warn("migrations: advisory unlock failed", "error", err)
		}
	}()

	if _, err := conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			name TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("migrations: create bookkeeping table: %w", err)
	}

	names, err := list()
	if err != nil {
		return err
	}

	for _, name := range names {
		var applied bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = $1)`, name,
		).Scan(&applied); err != nil {
			return fmt.Errorf("migrations: check %s: %w", name, err)
		}
		if applied {
			continue
		}
		if err := applyOne(ctx, conn, name); err != nil {
			return err
		}
		slog.Info("migration applied", "name", name)
	}
	return nil
}

func list() ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("migrations: read embedded files: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		if !validName.MatchString(e.Name()) {
			return nil, fmt.Errorf("migrations: unsafe file name %q", e.Name())
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// applyOne runs a single file. pgx uses the simple query protocol when a
// statement has no parameters, which is what allows a multi statement script
// to travel as one unit; wrapping it in BEGIN/COMMIT makes it atomic and lets
// the bookkeeping row commit with the schema change.
func applyOne(ctx context.Context, conn *pgxpool.Conn, name string) error {
	body, err := files.ReadFile(name)
	if err != nil {
		return fmt.Errorf("migrations: read %s: %w", name, err)
	}
	script := fmt.Sprintf("BEGIN;\n%s\n;\nINSERT INTO schema_migrations(name) VALUES ('%s');\nCOMMIT;\n",
		body, name)

	if _, err := conn.Exec(ctx, script); err != nil {
		// The script aborted mid-way; make sure the connection is usable
		// before reporting the failure.
		_, _ = conn.Exec(ctx, `ROLLBACK`)
		return fmt.Errorf("migrations: apply %s: %w", name, err)
	}
	return nil
}
