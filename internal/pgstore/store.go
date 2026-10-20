// Package pgstore opens the control-plane database and creates its schema.
package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/viovy/iazio-harness-api/internal/codebase"
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
  repo_alias text NOT NULL DEFAULT '',
  PRIMARY KEY (host_id, worktree_path)
);

ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS repo_alias text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS harness_repos_host_alias_idx ON harness_repos (host_id, lower(repo_alias)) WHERE repo_alias <> '';

CREATE TABLE IF NOT EXISTS harness_hosts (
  id text PRIMARY KEY,
  name text NOT NULL,
  kind text NOT NULL,
  presence text NOT NULL DEFAULT 'ONLINE',
  last_seen timestamp with time zone,
  fetch_failed boolean NOT NULL DEFAULT false,
  tools jsonb NOT NULL DEFAULT '[]'::jsonb,
  distribution_profile text NOT NULL DEFAULT 'generic'
);

ALTER TABLE harness_hosts ADD COLUMN IF NOT EXISTS distribution_profile text NOT NULL DEFAULT 'generic';

CREATE TABLE IF NOT EXISTS fleet_distribution_profiles (
  name text PRIMARY KEY,
  description text NOT NULL DEFAULT '',
  is_default boolean NOT NULL DEFAULT false,
  install_roots jsonb NOT NULL DEFAULT '{}'::jsonb,
  endpoints jsonb NOT NULL DEFAULT '{}'::jsonb,
  tools jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE fleet_distribution_profiles ADD COLUMN IF NOT EXISTS endpoints jsonb NOT NULL DEFAULT '{}'::jsonb;

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

ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS reason text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS error_message text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS diagnostic_code text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS model text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS remediation text NOT NULL DEFAULT '';

DELETE FROM harness_history h1
WHERE h1.status = 'DECLINED'
  AND EXISTS (
    SELECT 1 FROM harness_history h2
    WHERE h2.schedule_id = h1.schedule_id
      AND h2.status = 'DECLINED'
      AND h2.created_at > h1.created_at
  );

CREATE TABLE IF NOT EXISTS harness_git_credentials (
  id text PRIMARY KEY,
  name text NOT NULL,
  repository_pattern text NOT NULL,
  token text NOT NULL,
  type text NOT NULL DEFAULT 'pat',
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS codebase_repositories (
  name text PRIMARY KEY,
  path text NOT NULL,
  default_branch text NOT NULL DEFAULT 'main',
  git_tree_sha text NOT NULL DEFAULT '',
  description text NOT NULL DEFAULT '',
  host_id text NOT NULL DEFAULT '',
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE codebase_repositories ADD COLUMN IF NOT EXISTS host_id text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS codebase_chunks (
  id text PRIMARY KEY,
  repo_name text NOT NULL,
  file_path text NOT NULL,
  start_line integer NOT NULL,
  end_line integer NOT NULL,
  symbol text NOT NULL DEFAULT '',
  content text NOT NULL,
  file_blob_sha text NOT NULL,
  git_tree_sha text NOT NULL,
  content_sha256 text NOT NULL DEFAULT '',
  superseded boolean NOT NULL DEFAULT false,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE codebase_chunks ADD COLUMN IF NOT EXISTS content_sha256 text NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_codebase_chunks_repo_tree ON codebase_chunks (repo_name, git_tree_sha, superseded);
CREATE INDEX IF NOT EXISTS idx_codebase_chunks_path ON codebase_chunks (repo_name, file_path);
CREATE INDEX IF NOT EXISTS idx_codebase_chunks_content_sha ON codebase_chunks (content_sha256) WHERE content_sha256 <> '';

CREATE OR REPLACE VIEW fleet_repos AS
SELECT name, path, default_branch, git_tree_sha AS indexed_tree_sha, git_tree_sha, description, host_id, updated_at
FROM codebase_repositories;
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

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer conn.Close()

	// Acquire an advisory lock during schema migration to serialize DDL execution
	// across concurrent processes/tests and eliminate pg_type catalog race conditions.
	_, _ = conn.ExecContext(ctx, "SELECT pg_advisory_lock(847291039)")
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(847291039)")
	}()

	var execErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, execErr = conn.ExecContext(ctx, schema)
		if execErr == nil {
			break
		}
		if strings.Contains(execErr.Error(), "pg_type_typname_nsp_index") || strings.Contains(execErr.Error(), "23505") {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		break
	}
	if execErr != nil {
		_ = db.Close()
		return nil, execErr
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
	alias := r.EffectiveAlias()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_repos (host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason, healing_attempts, porcelain, discard_pending, repo_alias)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (host_id, worktree_path) DO UPDATE SET
  docs_hub_path = EXCLUDED.docs_hub_path,
  clone_url = EXCLUDED.clone_url,
  default_branch = EXCLUDED.default_branch,
  queue = EXCLUDED.queue,
  lock = EXCLUDED.lock,
  reason = EXCLUDED.reason,
  healing_attempts = EXCLUDED.healing_attempts,
  porcelain = EXCLUDED.porcelain,
  discard_pending = EXCLUDED.discard_pending,
  repo_alias = EXCLUDED.repo_alias
`, r.HostID, r.WorktreePath, r.DocsHubPath, r.CloneURL, r.DefaultBranch, r.Queue, r.Lock, r.Reason, r.HealingAttempts, r.Porcelain, r.DiscardPending, alias)
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
SELECT host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason, healing_attempts, porcelain, discard_pending, repo_alias
FROM harness_repos ORDER BY host_id, worktree_path
`)
	if err != nil {
		return nil, fmt.Errorf("load repos: %w", err)
	}
	defer rows.Close()

	var out []control.Repo
	for rows.Next() {
		var r control.Repo
		if err := rows.Scan(&r.HostID, &r.WorktreePath, &r.DocsHubPath, &r.CloneURL, &r.DefaultBranch, &r.Queue, &r.Lock, &r.Reason, &r.HealingAttempts, &r.Porcelain, &r.DiscardPending, &r.RepoAlias); err != nil {
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
	profile := h.DistributionProfile
	if profile == "" {
		profile = "generic"
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO harness_hosts (id, name, kind, presence, last_seen, fetch_failed, tools, distribution_profile)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  kind = EXCLUDED.kind,
  presence = EXCLUDED.presence,
  last_seen = EXCLUDED.last_seen,
  fetch_failed = EXCLUDED.fetch_failed,
  tools = EXCLUDED.tools,
  distribution_profile = EXCLUDED.distribution_profile
`, h.ID, h.Name, h.Kind, h.Presence, lastSeen, h.FetchFailed, string(toolsBytes), profile)
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
SELECT id, name, kind, presence, last_seen, fetch_failed, tools, distribution_profile
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
		if err := rows.Scan(&h.ID, &h.Name, &h.Kind, &h.Presence, &lastSeen, &h.FetchFailed, &toolsRaw, &h.DistributionProfile); err != nil {
			return nil, err
		}
		if h.DistributionProfile == "" {
			h.DistributionProfile = "generic"
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

// SaveProfile inserts or updates a distribution profile row.
func (s *Store) SaveProfile(ctx context.Context, p control.DistributionProfile) error {
	if s == nil || s.db == nil {
		return nil
	}
	rootsBytes, err := json.Marshal(p.InstallRoots)
	if err != nil {
		rootsBytes = []byte("{}")
	}
	endpointsBytes, err := json.Marshal(p.Endpoints)
	if err != nil || p.Endpoints == nil {
		endpointsBytes = []byte("{}")
	}
	toolsBytes, err := json.Marshal(p.Tools)
	if err != nil {
		toolsBytes = []byte("[]")
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fleet_distribution_profiles (name, description, is_default, install_roots, endpoints, tools, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (name) DO UPDATE SET
  description = EXCLUDED.description,
  is_default = EXCLUDED.is_default,
  install_roots = EXCLUDED.install_roots,
  endpoints = EXCLUDED.endpoints,
  tools = EXCLUDED.tools,
  updated_at = EXCLUDED.updated_at
`, p.Name, p.Description, p.IsDefault, string(rootsBytes), string(endpointsBytes), string(toolsBytes), p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save profile: %w", err)
	}
	return nil
}

// DeleteProfile removes a distribution profile row.
func (s *Store) DeleteProfile(ctx context.Context, name string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM fleet_distribution_profiles WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete profile: %w", err)
	}
	return nil
}

// LoadProfiles loads all distribution profiles from the database.
func (s *Store) LoadProfiles(ctx context.Context) ([]control.DistributionProfile, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT name, description, is_default, install_roots, endpoints, tools, created_at, updated_at
FROM fleet_distribution_profiles ORDER BY name
`)
	if err != nil {
		return nil, fmt.Errorf("load profiles: %w", err)
	}
	defer rows.Close()

	var out []control.DistributionProfile
	for rows.Next() {
		var p control.DistributionProfile
		var rootsRaw, endpointsRaw, toolsRaw []byte
		if err := rows.Scan(&p.Name, &p.Description, &p.IsDefault, &rootsRaw, &endpointsRaw, &toolsRaw, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		if len(rootsRaw) > 0 {
			_ = json.Unmarshal(rootsRaw, &p.InstallRoots)
		}
		if len(endpointsRaw) > 0 {
			_ = json.Unmarshal(endpointsRaw, &p.Endpoints)
		}
		if len(toolsRaw) > 0 {
			_ = json.Unmarshal(toolsRaw, &p.Tools)
		}
		out = append(out, p)
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
INSERT INTO harness_history (job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean, ase_complete, conversation_ids, reason, error_message, diagnostic_code, model, remediation)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (job_id) DO UPDATE SET
  host_id = EXCLUDED.host_id,
  worktree_path = EXCLUDED.worktree_path,
  schedule_id = EXCLUDED.schedule_id,
  prompt_title = EXCLUDED.prompt_title,
  engine = EXCLUDED.engine,
  status = EXCLUDED.status,
  clean = EXCLUDED.clean,
  ase_complete = EXCLUDED.ase_complete,
  conversation_ids = EXCLUDED.conversation_ids,
  reason = EXCLUDED.reason,
  error_message = EXCLUDED.error_message,
  diagnostic_code = EXCLUDED.diagnostic_code,
  model = EXCLUDED.model,
  remediation = EXCLUDED.remediation
`, row.JobID, hostID, worktreePath, row.ScheduleID, row.PromptTitle, row.Engine, row.Status, row.Clean, row.ASEComplete, string(convBytes), row.Reason, row.ErrorMessage, row.DiagnosticCode, row.Model, row.Remediation)
	if err != nil {
		return fmt.Errorf("save history: %w", err)
	}
	return nil
}

// DeleteHistory deletes one history row by job_id.
func (s *Store) DeleteHistory(ctx context.Context, jobID string) error {
	if s == nil || s.db == nil || jobID == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM harness_history WHERE job_id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("delete history: %w", err)
	}
	return nil
}

// ClearRepoHistory deletes all history rows for a host and worktree.
func (s *Store) ClearRepoHistory(ctx context.Context, hostID, worktreePath string) error {
	if s == nil || s.db == nil || hostID == "" || worktreePath == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM harness_history WHERE host_id = $1 AND worktree_path = $2`, hostID, worktreePath)
	if err != nil {
		return fmt.Errorf("clear repo history: %w", err)
	}
	return nil
}

// LoadHistory loads all history rows grouped by repoKey (host_id + "\x00" + worktree_path).
func (s *Store) LoadHistory(ctx context.Context) (map[string][]control.HistoryRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean, ase_complete, conversation_ids, reason, error_message, diagnostic_code, model, remediation, created_at
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
		if err := rows.Scan(&r.JobID, &hostID, &worktreePath, &r.ScheduleID, &r.PromptTitle, &r.Engine, &r.Status, &r.Clean, &r.ASEComplete, &convRaw, &r.Reason, &r.ErrorMessage, &r.DiagnosticCode, &r.Model, &r.Remediation, &r.CreatedAt); err != nil {
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

// SaveGitCredential persists a git credential.
func (s *Store) SaveGitCredential(ctx context.Context, cred control.GitCredential) error {
	if s == nil || s.db == nil {
		return nil
	}
	t := cred.Type
	if t == "" {
		t = "pat"
	}
	ca := cred.CreatedAt
	if ca.IsZero() {
		ca = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_git_credentials (id, name, repository_pattern, token, type, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  repository_pattern = EXCLUDED.repository_pattern,
  token = EXCLUDED.token,
  type = EXCLUDED.type;
`, cred.ID, cred.Name, cred.Pattern, cred.Token, t, ca)
	return err
}

// DeleteGitCredential removes a git credential by ID.
func (s *Store) DeleteGitCredential(ctx context.Context, id string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM harness_git_credentials WHERE id = $1;`, id)
	return err
}

// LoadGitCredentials returns all registered git credentials.
func (s *Store) LoadGitCredentials(ctx context.Context) ([]control.GitCredential, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, repository_pattern, token, type, created_at
FROM harness_git_credentials
ORDER BY created_at ASC;
`)
	if err != nil {
		return nil, fmt.Errorf("load git credentials: %w", err)
	}
	defer rows.Close()

	var out []control.GitCredential
	for rows.Next() {
		var c control.GitCredential
		if err := rows.Scan(&c.ID, &c.Name, &c.Pattern, &c.Token, &c.Type, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LoadCodebaseRepos loads all registered fleet repositories from the database.
func (s *Store) LoadCodebaseRepos(ctx context.Context) ([]codebase.Repository, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT name, path, default_branch, git_tree_sha, description, host_id, updated_at
FROM codebase_repositories
ORDER BY name ASC;
`)
	if err != nil {
		return nil, fmt.Errorf("load codebase repos: %w", err)
	}
	defer rows.Close()

	var out []codebase.Repository
	for rows.Next() {
		var r codebase.Repository
		if err := rows.Scan(&r.Name, &r.Path, &r.DefaultBranch, &r.GitTreeSHA, &r.Description, &r.HostID, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveCodebaseRepo upserts a fleet repository in the database.
func (s *Store) SaveCodebaseRepo(ctx context.Context, repo codebase.Repository) error {
	if s == nil || s.db == nil {
		return nil
	}
	ua := repo.UpdatedAt
	if ua.IsZero() {
		ua = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO codebase_repositories (name, path, default_branch, git_tree_sha, description, host_id, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (name) DO UPDATE SET
  path = EXCLUDED.path,
  default_branch = EXCLUDED.default_branch,
  git_tree_sha = EXCLUDED.git_tree_sha,
  description = EXCLUDED.description,
  host_id = EXCLUDED.host_id,
  updated_at = EXCLUDED.updated_at;
`, repo.Name, repo.Path, repo.DefaultBranch, repo.GitTreeSHA, repo.Description, repo.HostID, ua)
	return err
}

// SaveCodebaseChunks upserts code chunks in the database.
func (s *Store) SaveCodebaseChunks(ctx context.Context, chunks []codebase.CodeChunk) error {
	if s == nil || s.db == nil || len(chunks) == 0 {
		return nil
	}
	for _, c := range chunks {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO codebase_chunks (id, repo_name, file_path, start_line, end_line, symbol, content, file_blob_sha, git_tree_sha, content_sha256, superseded)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (id) DO UPDATE SET
  content = EXCLUDED.content,
  file_blob_sha = EXCLUDED.file_blob_sha,
  git_tree_sha = EXCLUDED.git_tree_sha,
  content_sha256 = EXCLUDED.content_sha256,
  superseded = EXCLUDED.superseded;
`, c.ID, c.RepoName, c.FilePath, c.StartLine, c.EndLine, c.Symbol, c.Content, c.FileBlobSHA, c.GitTreeSHA, c.ContentSHA256, c.Superseded)
		if err != nil {
			return fmt.Errorf("save chunk %s: %w", c.ID, err)
		}
	}
	return nil
}

// LoadCodebaseChunks loads active code chunks for a given repository.
func (s *Store) LoadCodebaseChunks(ctx context.Context, repoName string) ([]codebase.CodeChunk, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, repo_name, file_path, start_line, end_line, symbol, content, file_blob_sha, git_tree_sha, content_sha256, superseded
FROM codebase_chunks
WHERE repo_name = $1 AND superseded = false
ORDER BY file_path ASC, start_line ASC;
`, repoName)
	if err != nil {
		return nil, fmt.Errorf("load codebase chunks: %w", err)
	}
	defer rows.Close()

	var out []codebase.CodeChunk
	for rows.Next() {
		var c codebase.CodeChunk
		if err := rows.Scan(&c.ID, &c.RepoName, &c.FilePath, &c.StartLine, &c.EndLine, &c.Symbol, &c.Content, &c.FileBlobSHA, &c.GitTreeSHA, &c.ContentSHA256, &c.Superseded); err != nil {
			return nil, err
		}
		out = append(out, c)
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
