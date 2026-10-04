// Package pgstore opens the control-plane database and creates its schema.
package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/viovy/iazio-harness-api/internal/control"
)

const schema = `
CREATE TABLE IF NOT EXISTS harness_ideas (
  id text PRIMARY KEY,
  title text NOT NULL,
  share_url text NOT NULL,
  transcript text NOT NULL,
  status text NOT NULL,
  story_id text NOT NULL DEFAULT '',
  previous_status text NOT NULL DEFAULT ''
);

ALTER TABLE harness_ideas ADD COLUMN IF NOT EXISTS previous_status text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS harness_prompts (
  id text PRIMARY KEY,
  title text NOT NULL,
  body text NOT NULL,
  engine text NOT NULL DEFAULT 'agent',
  story_id text NOT NULL DEFAULT '',
  source_idea_id text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'READY',
  revision integer NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS harness_repos (
  host_id text NOT NULL,
  worktree_path text NOT NULL,
  docs_hub_path text NOT NULL DEFAULT '',
  clone_url text NOT NULL DEFAULT '',
  default_branch text NOT NULL DEFAULT 'main',
  queue text NOT NULL DEFAULT 'OPEN',
  lock text NOT NULL DEFAULT 'IDLE',
  reason text NOT NULL DEFAULT '',
  healing_attempts integer NOT NULL DEFAULT 0,
  porcelain text NOT NULL DEFAULT '',
  discard_pending boolean NOT NULL DEFAULT false,
  PRIMARY KEY (host_id, worktree_path)
);

CREATE TABLE IF NOT EXISTS harness_hosts (
  id text PRIMARY KEY,
  name text NOT NULL,
  kind text NOT NULL,
  presence text NOT NULL DEFAULT 'ONLINE',
  last_seen timestamp with time zone,
  fetch_failed boolean NOT NULL DEFAULT false,
  tools jsonb NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS harness_history (
  job_id text PRIMARY KEY,
  host_id text NOT NULL,
  worktree_path text NOT NULL,
  schedule_id text NOT NULL,
  prompt_title text NOT NULL,
  engine text NOT NULL DEFAULT 'agent',
  status text NOT NULL DEFAULT 'RUNNING',
  clean boolean NOT NULL DEFAULT false,
  ase_complete boolean NOT NULL DEFAULT false,
  conversation_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_harness_history_repo ON harness_history (host_id, worktree_path, created_at ASC);
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
func (s *Store) SaveIdea(ctx context.Context, idea control.Idea) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_ideas (id, title, share_url, transcript, status, story_id, previous_status)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO UPDATE SET
  title = EXCLUDED.title,
  share_url = EXCLUDED.share_url,
  transcript = EXCLUDED.transcript,
  status = EXCLUDED.status,
  story_id = EXCLUDED.story_id,
  previous_status = EXCLUDED.previous_status
`, idea.ID, idea.Title, idea.ShareURL, idea.Transcript, idea.Status, idea.StoryID, idea.PreviousStatus)
	if err != nil {
		return fmt.Errorf("save idea: %w", err)
	}
	return nil
}

// SaveIdeaArgs maintains backward compatibility with legacy argument signature.
func (s *Store) SaveIdeaArgs(ctx context.Context, id, title, shareURL, transcript, status, storyID string) error {
	return s.SaveIdea(ctx, control.Idea{
		ID:         id,
		Title:      title,
		ShareURL:   shareURL,
		Transcript: transcript,
		Status:     status,
		StoryID:    storyID,
	})
}

// LoadIdeas loads all ideas from the database.
func (s *Store) LoadIdeas(ctx context.Context) ([]control.Idea, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, title, share_url, transcript, status, story_id, previous_status
FROM harness_ideas ORDER BY id
`)
	if err != nil {
		return nil, fmt.Errorf("load ideas: %w", err)
	}
	defer rows.Close()

	var out []control.Idea
	for rows.Next() {
		var it control.Idea
		if err := rows.Scan(&it.ID, &it.Title, &it.ShareURL, &it.Transcript, &it.Status, &it.StoryID, &it.PreviousStatus); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// SavePrompt inserts or updates a prompt row.
func (s *Store) SavePrompt(ctx context.Context, p control.Prompt) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_prompts (id, title, body, engine, story_id, source_idea_id, status, revision)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (id) DO UPDATE SET
  title = EXCLUDED.title,
  body = EXCLUDED.body,
  engine = EXCLUDED.engine,
  story_id = EXCLUDED.story_id,
  source_idea_id = EXCLUDED.source_idea_id,
  status = EXCLUDED.status,
  revision = EXCLUDED.revision
`, p.ID, p.Title, p.Body, p.Engine, p.StoryID, p.SourceIdeaID, p.Status, p.Revision)
	if err != nil {
		return fmt.Errorf("save prompt: %w", err)
	}
	return nil
}

// DeletePrompt removes a prompt row.
func (s *Store) DeletePrompt(ctx context.Context, id string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM harness_prompts WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete prompt: %w", err)
	}
	return nil
}

// LoadPrompts loads all prompts from the database.
func (s *Store) LoadPrompts(ctx context.Context) ([]control.Prompt, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, title, body, engine, story_id, source_idea_id, status, revision
FROM harness_prompts ORDER BY id
`)
	if err != nil {
		return nil, fmt.Errorf("load prompts: %w", err)
	}
	defer rows.Close()

	var out []control.Prompt
	for rows.Next() {
		var p control.Prompt
		if err := rows.Scan(&p.ID, &p.Title, &p.Body, &p.Engine, &p.StoryID, &p.SourceIdeaID, &p.Status, &p.Revision); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SaveRepo inserts or updates a repository row.
func (s *Store) SaveRepo(ctx context.Context, r control.Repo) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_repos (host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason, healing_attempts, porcelain, discard_pending)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (host_id, worktree_path) DO UPDATE SET
  docs_hub_path = EXCLUDED.docs_hub_path,
  clone_url = EXCLUDED.clone_url,
  default_branch = EXCLUDED.default_branch,
  queue = EXCLUDED.queue,
  lock = EXCLUDED.lock,
  reason = EXCLUDED.reason,
  healing_attempts = EXCLUDED.healing_attempts,
  porcelain = EXCLUDED.porcelain,
  discard_pending = EXCLUDED.discard_pending
`, r.HostID, r.WorktreePath, r.DocsHubPath, r.CloneURL, r.DefaultBranch, r.Queue, r.Lock, r.Reason, r.HealingAttempts, r.Porcelain, r.DiscardPending)
	if err != nil {
		return fmt.Errorf("save repo: %w", err)
	}
	return nil
}

// DeleteRepo removes a repository row.
func (s *Store) DeleteRepo(ctx context.Context, hostID, path string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM harness_repos WHERE host_id = $1 AND worktree_path = $2`, hostID, path)
	if err != nil {
		return fmt.Errorf("delete repo: %w", err)
	}
	return nil
}

// LoadRepos loads all repositories from the database.
func (s *Store) LoadRepos(ctx context.Context) ([]control.Repo, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason, healing_attempts, porcelain, discard_pending
FROM harness_repos ORDER BY host_id, worktree_path
`)
	if err != nil {
		return nil, fmt.Errorf("load repos: %w", err)
	}
	defer rows.Close()

	var out []control.Repo
	for rows.Next() {
		var r control.Repo
		if err := rows.Scan(&r.HostID, &r.WorktreePath, &r.DocsHubPath, &r.CloneURL, &r.DefaultBranch, &r.Queue, &r.Lock, &r.Reason, &r.HealingAttempts, &r.Porcelain, &r.DiscardPending); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveHost inserts or updates a host row.
func (s *Store) SaveHost(ctx context.Context, h control.Host) error {
	if s == nil || s.db == nil {
		return nil
	}
	toolsBytes, err := json.Marshal(h.Tools)
	if err != nil {
		toolsBytes = []byte("[]")
	}
	var lastSeen *time.Time
	if !h.LastSeen.IsZero() {
		lastSeen = &h.LastSeen
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO harness_hosts (id, name, kind, presence, last_seen, fetch_failed, tools)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  kind = EXCLUDED.kind,
  presence = EXCLUDED.presence,
  last_seen = EXCLUDED.last_seen,
  fetch_failed = EXCLUDED.fetch_failed,
  tools = EXCLUDED.tools
`, h.ID, h.Name, h.Kind, h.Presence, lastSeen, h.FetchFailed, string(toolsBytes))
	if err != nil {
		return fmt.Errorf("save host: %w", err)
	}
	return nil
}

// LoadHosts loads all hosts from the database.
func (s *Store) LoadHosts(ctx context.Context) ([]control.Host, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, kind, presence, last_seen, fetch_failed, tools
FROM harness_hosts ORDER BY id
`)
	if err != nil {
		return nil, fmt.Errorf("load hosts: %w", err)
	}
	defer rows.Close()

	var out []control.Host
	for rows.Next() {
		var h control.Host
		var lastSeen *time.Time
		var toolsRaw []byte
		if err := rows.Scan(&h.ID, &h.Name, &h.Kind, &h.Presence, &lastSeen, &h.FetchFailed, &toolsRaw); err != nil {
			return nil, err
		}
		if lastSeen != nil {
			h.LastSeen = *lastSeen
		}
		if len(toolsRaw) > 0 {
			_ = json.Unmarshal(toolsRaw, &h.Tools)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// SaveHistory inserts or updates one history row.
func (s *Store) SaveHistory(ctx context.Context, hostID, worktreePath string, row control.HistoryRow) error {
	if s == nil || s.db == nil {
		return nil
	}
	convBytes, err := json.Marshal(row.ConversationIDs)
	if err != nil {
		convBytes = []byte("[]")
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO harness_history (job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean, ase_complete, conversation_ids)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (job_id) DO UPDATE SET
  host_id = EXCLUDED.host_id,
  worktree_path = EXCLUDED.worktree_path,
  schedule_id = EXCLUDED.schedule_id,
  prompt_title = EXCLUDED.prompt_title,
  engine = EXCLUDED.engine,
  status = EXCLUDED.status,
  clean = EXCLUDED.clean,
  ase_complete = EXCLUDED.ase_complete,
  conversation_ids = EXCLUDED.conversation_ids
`, row.JobID, hostID, worktreePath, row.ScheduleID, row.PromptTitle, row.Engine, row.Status, row.Clean, row.ASEComplete, string(convBytes))
	if err != nil {
		return fmt.Errorf("save history: %w", err)
	}
	return nil
}

// LoadHistory loads all history rows grouped by repoKey (host_id + "\x00" + worktree_path).
func (s *Store) LoadHistory(ctx context.Context) (map[string][]control.HistoryRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean, ase_complete, conversation_ids
FROM harness_history ORDER BY created_at ASC
`)
	if err != nil {
		return nil, fmt.Errorf("load history: %w", err)
	}
	defer rows.Close()

	out := make(map[string][]control.HistoryRow)
	for rows.Next() {
		var r control.HistoryRow
		var hostID, worktreePath string
		var convRaw []byte
		if err := rows.Scan(&r.JobID, &hostID, &worktreePath, &r.ScheduleID, &r.PromptTitle, &r.Engine, &r.Status, &r.Clean, &r.ASEComplete, &convRaw); err != nil {
			return nil, err
		}
		if len(convRaw) > 0 {
			_ = json.Unmarshal(convRaw, &r.ConversationIDs)
		}
		if r.ConversationIDs == nil {
			r.ConversationIDs = []string{}
		}
		key := hostID + "\x00" + worktreePath
		out[key] = append(out[key], r)
	}
	return out, rows.Err()
}

// Close closes the pool.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

