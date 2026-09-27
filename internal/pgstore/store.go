// Package pgstore opens the control-plane database and creates its schema.
package pgstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const schema = `
CREATE TABLE IF NOT EXISTS harness_ideas (
  id text PRIMARY KEY,
  title text NOT NULL,
  share_url text NOT NULL,
  transcript text NOT NULL,
  status text NOT NULL,
  story_id text NOT NULL DEFAULT ''
);
`

// Store is a Postgres connection for the control plane.
type Store struct {
	db *sql.DB
}

// Open migrates the schema and checks the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// SaveIdea inserts or updates one idea row.
func (s *Store) SaveIdea(ctx context.Context, id, title, shareURL, transcript, status, storyID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_ideas (id, title, share_url, transcript, status, story_id)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, transcript = EXCLUDED.transcript, status = EXCLUDED.status, story_id = EXCLUDED.story_id
`, id, title, shareURL, transcript, status, storyID)
	if err != nil {
		return fmt.Errorf("save idea: %w", err)
	}
	return nil
}

// Close closes the pool.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
