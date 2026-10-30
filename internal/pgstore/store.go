// Package pgstore opens the control-plane database and creates its schema.
package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
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
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS healing_reason_code text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS healing_reason_message text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS healing_started_at timestamp with time zone;
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS healing_attempt_count integer NOT NULL DEFAULT 0;
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS max_healing_attempts integer NOT NULL DEFAULT 0;
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS last_healing_error text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS active_healer_run_id text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS metadata_yml_raw text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS metadata_yml_sha text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS metadata_yml_mod_time text NOT NULL DEFAULT '';
ALTER TABLE harness_repos ADD COLUMN IF NOT EXISTS git_version text NOT NULL DEFAULT '';
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
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS trace_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS parent_job_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS root_job_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS initial_conversation_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS active_conversation_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS resume_mode text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS resume_fallback_reason text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS interrupted_ase_state jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS conversation_lineage jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS execution_attempt integer NOT NULL DEFAULT 1;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS execution_chain jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS same_conversation_confirmed boolean NOT NULL DEFAULT false;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS attach_confirmation text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS lease_token text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS lease_ack_deadline_at timestamp with time zone;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS failure_category text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS failure_stage text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS failure_reason_summary text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS retriable boolean NOT NULL DEFAULT false;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS attempt_history jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS zipkin_trace_id text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS zipkin_trace_url text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS traceparent text NOT NULL DEFAULT '';
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS infra_requeue_count integer NOT NULL DEFAULT 0;

UPDATE harness_history
SET failure_category = CASE
  WHEN (reason ILIKE '%claim%' OR error_message ILIKE '%claim%' OR diagnostic_code ILIKE '%claim%' OR reason ILIKE '%startup_heartbeat%') THEN 'DISPATCH_CLAIM_TIMEOUT'
  WHEN (reason ILIKE '%429%' OR error_message ILIKE '%429%' OR reason ILIKE '%quota%' OR error_message ILIKE '%quota%' OR reason ILIKE '%auth%' OR error_message ILIKE '%auth%') THEN 'TOOL_QUOTA_OR_AUTH'
  WHEN (reason ILIKE '%worktree%' OR error_message ILIKE '%worktree%' OR reason ILIKE '%submodule%' OR error_message ILIKE '%submodule%' OR reason ILIKE '%lock%') THEN 'WORKSPACE_PREP_ERROR'
  WHEN (reason ILIKE '%1h0m0s%' OR error_message ILIKE '%1h0m0s%' OR reason ILIKE '%background task%' OR error_message ILIKE '%background task%' OR status = 'LEASE_EXPIRED') THEN 'BACKGROUND_DRAIN_TIMEOUT'
  WHEN (reason ILIKE '%circuit%' OR error_message ILIKE '%circuit%') THEN 'CIRCUIT_BREAKER_TRIPPED'
  WHEN (reason ILIKE '%gate%' OR error_message ILIKE '%gate%' OR diagnostic_code ILIKE '%gate%') THEN 'ASE_GATE_INCOMPLETE'
  ELSE 'TOOL_CRASH'
END,
failure_stage = CASE
  WHEN (reason ILIKE '%claim%' OR error_message ILIKE '%claim%' OR diagnostic_code ILIKE '%claim%') THEN 'QUEUE_DISPATCH'
  WHEN (reason ILIKE '%worktree%' OR error_message ILIKE '%worktree%' OR reason ILIKE '%submodule%' OR error_message ILIKE '%submodule%' OR reason ILIKE '%lock%') THEN 'WORKSPACE_PREP'
  WHEN (reason ILIKE '%1h0m0s%' OR error_message ILIKE '%1h0m0s%' OR reason ILIKE '%background task%' OR error_message ILIKE '%background task%' OR status = 'LEASE_EXPIRED') THEN 'BACKGROUND_DRAIN'
  ELSE 'TOOL_EXECUTION'
END,
failure_reason_summary = CASE
  WHEN error_message <> '' THEN error_message
  WHEN reason <> '' THEN reason
  ELSE 'Historical failure backfill'
END,
retriable = CASE
  WHEN (reason ILIKE '%gate%' OR error_message ILIKE '%gate%' OR diagnostic_code ILIKE '%gate%') THEN false
  ELSE true
END,
zipkin_trace_id = CASE
  WHEN zipkin_trace_id = '' AND trace_id <> '' THEN trace_id
  ELSE zipkin_trace_id
END,
zipkin_trace_url = CASE
  WHEN zipkin_trace_url = '' AND trace_id <> '' THEN '/zipkin/traces/' || trace_id
  ELSE zipkin_trace_url
END
WHERE status IN ('FAILED', 'DECLINED', 'ABANDONED', 'LEASE_EXPIRED')
  AND (failure_category = '' OR failure_category = 'NONE');

UPDATE harness_history
SET initial_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399',
    active_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399'
WHERE job_id = 'job-1311' AND (initial_conversation_id = '' OR initial_conversation_id IS NULL);

UPDATE harness_history
SET parent_job_id = 'job-1311',
    root_job_id = 'job-1311',
    initial_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399',
    execution_attempt = 2,
    execution_chain = '["job-1311", "job-1313"]'::jsonb,
    conversation_lineage = '[{"conversation_id": "aad902c8-bda6-44c7-8a68-c468a1562399", "role": "root_initial", "reason": "reconciled_from_root_job"}, {"conversation_id": "1d446877-5bf9-44eb-9db5-e1c007a9fe31", "role": "legacy_unpinned_secondary", "reason": "split_brain_unpinned_phase_1"}, {"conversation_id": "14de9a1f-5679-4f11-a08f-f32bc7e47663", "role": "legacy_unpinned_secondary", "reason": "split_brain_unpinned_phase_2"}]'::jsonb
WHERE job_id = 'job-1313' AND (parent_job_id = '' OR parent_job_id IS NULL OR initial_conversation_id != 'aad902c8-bda6-44c7-8a68-c468a1562399');

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

CREATE TABLE IF NOT EXISTS harness_stories (
  id text PRIMARY KEY,
  repo text NOT NULL,
  submodule text NOT NULL DEFAULT '',
  title text NOT NULL,
  priority text NOT NULL DEFAULT 'P1',
  status text NOT NULL DEFAULT 'pending',
  sync_state text NOT NULL DEFAULT 'PENDING_WORKSPACE_MATERIALIZATION',
  body text NOT NULL DEFAULT '',
  problem text NOT NULL DEFAULT '',
  acceptance_criteria jsonb NOT NULL DEFAULT '[]'::jsonb,
  target_files jsonb NOT NULL DEFAULT '[]'::jsonb,
  depends_on jsonb NOT NULL DEFAULT '[]'::jsonb,
  verification_commands jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending';
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS sync_state text NOT NULL DEFAULT 'PENDING_WORKSPACE_MATERIALIZATION';
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS problem text NOT NULL DEFAULT '';
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS acceptance_criteria jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS target_files jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS depends_on jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_stories ADD COLUMN IF NOT EXISTS verification_commands jsonb NOT NULL DEFAULT '[]'::jsonb;

-- Durable global dispatch queue jobs with fencing tokens and lease tracking.
CREATE TABLE IF NOT EXISTS harness_dispatch_jobs (
  id text PRIMARY KEY,
  type text NOT NULL,
  repo text NOT NULL DEFAULT '',
  target_host text NOT NULL DEFAULT '',
  claimed_host text NOT NULL DEFAULT '',
  host_id text NOT NULL DEFAULT '',
  target_agent text NOT NULL DEFAULT '',
  story_id text NOT NULL DEFAULT '',
  normalized_prompt text NOT NULL DEFAULT '',
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending',
  result jsonb,
  error text NOT NULL DEFAULT '',
  trace_id text NOT NULL DEFAULT '',
  fencing_token bigint NOT NULL DEFAULT 0,
  lease_version bigint NOT NULL DEFAULT 0,
  lease_owner text NOT NULL DEFAULT '',
  lease_token text NOT NULL DEFAULT '',
  attempts integer NOT NULL DEFAULT 0,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  claimed_at timestamp with time zone,
  heartbeat_at timestamp with time zone,
  lease_expires_at timestamp with time zone,
  completed_at timestamp with time zone,
  story_title text NOT NULL DEFAULT '',
  story_path text NOT NULL DEFAULT '',
  story_allocation_status text NOT NULL DEFAULT '',
  failure_category text NOT NULL DEFAULT '',
  failure_stage text NOT NULL DEFAULT '',
  failure_reason_summary text NOT NULL DEFAULT '',
  retriable boolean NOT NULL DEFAULT false,
  attempt_history jsonb NOT NULL DEFAULT '[]'::jsonb,
  infra_requeue_count integer NOT NULL DEFAULT 0,
  parent_id text NOT NULL DEFAULT '',
  final_turn_count integer NOT NULL DEFAULT 0,
  final_transcript_sha256 text NOT NULL DEFAULT '',
  allocation_source text NOT NULL DEFAULT '',
  priority_rank_selected integer NOT NULL DEFAULT 0,
  effective_action text NOT NULL DEFAULT '',
  source_conversation_title text NOT NULL DEFAULT '',
  agent_affinity text NOT NULL DEFAULT '',
  agent_pid integer NOT NULL DEFAULT 0,
  active_conversation_id text NOT NULL DEFAULT '',
  lease_ack_deadline_at timestamp with time zone,
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS story_title text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS story_path text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS story_allocation_status text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS failure_category text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS failure_stage text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS failure_reason_summary text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS retriable boolean NOT NULL DEFAULT false;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS attempt_history jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS infra_requeue_count integer NOT NULL DEFAULT 0;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS parent_id text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS final_turn_count integer NOT NULL DEFAULT 0;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS final_transcript_sha256 text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS allocation_source text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS priority_rank_selected integer NOT NULL DEFAULT 0;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS effective_action text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS source_conversation_title text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS agent_affinity text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS agent_pid integer NOT NULL DEFAULT 0;
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS active_conversation_id text NOT NULL DEFAULT '';
ALTER TABLE harness_dispatch_jobs ADD COLUMN IF NOT EXISTS lease_ack_deadline_at timestamp with time zone;

CREATE INDEX IF NOT EXISTS harness_dispatch_jobs_status_idx ON harness_dispatch_jobs (status);
CREATE INDEX IF NOT EXISTS harness_dispatch_jobs_host_idx ON harness_dispatch_jobs (claimed_host, status);

CREATE TABLE IF NOT EXISTS harness_settings (
  key text PRIMARY KEY,
  value jsonb NOT NULL,
  updated_at timestamp with time zone NOT NULL DEFAULT now()
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
	var healingStartedAt *time.Time
	if !r.HealingStartedAt.IsZero() {
		healingStartedAt = &r.HealingStartedAt
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_repos (
  host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason,
  healing_attempts, porcelain, discard_pending, repo_alias,
  healing_reason_code, healing_reason_message, healing_started_at, healing_attempt_count,
  max_healing_attempts, last_healing_error, active_healer_run_id,
  metadata_yml_raw, metadata_yml_sha, metadata_yml_mod_time, git_version
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
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
  repo_alias = EXCLUDED.repo_alias,
  healing_reason_code = EXCLUDED.healing_reason_code,
  healing_reason_message = EXCLUDED.healing_reason_message,
  healing_started_at = EXCLUDED.healing_started_at,
  healing_attempt_count = EXCLUDED.healing_attempt_count,
  max_healing_attempts = EXCLUDED.max_healing_attempts,
  last_healing_error = EXCLUDED.last_healing_error,
  active_healer_run_id = EXCLUDED.active_healer_run_id,
  metadata_yml_raw = EXCLUDED.metadata_yml_raw,
  metadata_yml_sha = EXCLUDED.metadata_yml_sha,
  metadata_yml_mod_time = EXCLUDED.metadata_yml_mod_time,
  git_version = EXCLUDED.git_version
`, r.HostID, r.WorktreePath, r.DocsHubPath, r.CloneURL, r.DefaultBranch, r.Queue, r.Lock, r.Reason,
		r.HealingAttempts, r.Porcelain, r.DiscardPending, alias,
		r.HealingReasonCode, r.HealingReasonMessage, healingStartedAt, r.HealingAttemptCount,
		r.MaxHealingAttempts, r.LastHealingError, r.ActiveHealerRunID,
		r.MetadataYmlRaw, r.MetadataYmlSHA, r.MetadataYmlModTime, r.GitVersion)
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
SELECT
  host_id, worktree_path, docs_hub_path, clone_url, default_branch, queue, lock, reason,
  healing_attempts, porcelain, discard_pending, repo_alias,
  healing_reason_code, healing_reason_message, healing_started_at, healing_attempt_count,
  max_healing_attempts, last_healing_error, active_healer_run_id,
  metadata_yml_raw, metadata_yml_sha, metadata_yml_mod_time, git_version
FROM harness_repos ORDER BY host_id, worktree_path
`)
	if err != nil {
		return nil, fmt.Errorf("load repos: %w", err)
	}
	defer rows.Close()

	var out []control.Repo
	for rows.Next() {
		var r control.Repo
		var healingStartedAt *time.Time
		if err := rows.Scan(
			&r.HostID, &r.WorktreePath, &r.DocsHubPath, &r.CloneURL, &r.DefaultBranch, &r.Queue, &r.Lock, &r.Reason,
			&r.HealingAttempts, &r.Porcelain, &r.DiscardPending, &r.RepoAlias,
			&r.HealingReasonCode, &r.HealingReasonMessage, &healingStartedAt, &r.HealingAttemptCount,
			&r.MaxHealingAttempts, &r.LastHealingError, &r.ActiveHealerRunID,
			&r.MetadataYmlRaw, &r.MetadataYmlSHA, &r.MetadataYmlModTime, &r.GitVersion,
		); err != nil {
			return nil, err
		}
		if healingStartedAt != nil {
			r.HealingStartedAt = *healingStartedAt
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
	interruptedBytes, err := json.Marshal(row.InterruptedASEState)
	if err != nil || len(row.InterruptedASEState) == 0 {
		interruptedBytes = []byte("{}")
	}
	lineageBytes, err := json.Marshal(row.ConversationLineage)
	if err != nil || len(row.ConversationLineage) == 0 {
		lineageBytes = []byte("[]")
	}
	chainBytes, err := json.Marshal(row.ExecutionChain)
	if err != nil || len(row.ExecutionChain) == 0 {
		chainBytes = []byte("[]")
	}
	attemptHistoryBytes, err := json.Marshal(row.AttemptHistory)
	if err != nil || len(row.AttemptHistory) == 0 {
		attemptHistoryBytes = []byte("[]")
	}
	attempt := row.ExecutionAttempt
	if attempt <= 0 {
		attempt = 1
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO harness_history (
  job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean,
  ase_complete, conversation_ids, reason, error_message, diagnostic_code, model,
  remediation, trace_id, parent_job_id, root_job_id, initial_conversation_id,
  active_conversation_id, resume_mode, resume_fallback_reason, interrupted_ase_state,
  conversation_lineage, execution_attempt, execution_chain, same_conversation_confirmed, attach_confirmation,
  lease_token, lease_ack_deadline_at, failure_category, failure_stage,
  failure_reason_summary, retriable, attempt_history, zipkin_trace_id,
  zipkin_trace_url, traceparent, infra_requeue_count
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39)
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
  remediation = EXCLUDED.remediation,
  trace_id = EXCLUDED.trace_id,
  parent_job_id = EXCLUDED.parent_job_id,
  root_job_id = EXCLUDED.root_job_id,
  initial_conversation_id = EXCLUDED.initial_conversation_id,
  active_conversation_id = EXCLUDED.active_conversation_id,
  resume_mode = EXCLUDED.resume_mode,
  resume_fallback_reason = EXCLUDED.resume_fallback_reason,
  interrupted_ase_state = EXCLUDED.interrupted_ase_state,
  conversation_lineage = EXCLUDED.conversation_lineage,
  execution_attempt = EXCLUDED.execution_attempt,
  execution_chain = EXCLUDED.execution_chain,
  same_conversation_confirmed = EXCLUDED.same_conversation_confirmed,
  attach_confirmation = EXCLUDED.attach_confirmation,
  lease_token = EXCLUDED.lease_token,
  lease_ack_deadline_at = EXCLUDED.lease_ack_deadline_at,
  failure_category = EXCLUDED.failure_category,
  failure_stage = EXCLUDED.failure_stage,
  failure_reason_summary = EXCLUDED.failure_reason_summary,
  retriable = EXCLUDED.retriable,
  attempt_history = EXCLUDED.attempt_history,
  zipkin_trace_id = EXCLUDED.zipkin_trace_id,
  zipkin_trace_url = EXCLUDED.zipkin_trace_url,
  traceparent = EXCLUDED.traceparent,
  infra_requeue_count = EXCLUDED.infra_requeue_count
`, row.JobID, hostID, worktreePath, row.ScheduleID, row.PromptTitle, row.Engine, row.Status, row.Clean, row.ASEComplete, string(convBytes), row.Reason, row.ErrorMessage, row.DiagnosticCode, row.Model, row.Remediation, row.TraceID, row.ParentJobID, row.RootJobID, row.InitialConversationID, row.ActiveConversationID, row.ResumeMode, row.ResumeFallbackReason, string(interruptedBytes), string(lineageBytes), attempt, string(chainBytes), row.SameConversationConfirmed, row.AttachConfirmation, row.LeaseToken, row.LeaseAckDeadlineAt, row.FailureCategory, row.FailureStage, row.FailureReasonSummary, row.Retriable, string(attemptHistoryBytes), row.ZipkinTraceID, row.ZipkinTraceURL, row.Traceparent, row.InfraRequeueCount)
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
SELECT
  job_id, host_id, worktree_path, schedule_id, prompt_title, engine, status, clean,
  ase_complete, conversation_ids, reason, error_message, diagnostic_code, model,
  remediation, trace_id, created_at, parent_job_id, root_job_id, initial_conversation_id,
  active_conversation_id, resume_mode, resume_fallback_reason, interrupted_ase_state,
  conversation_lineage, execution_attempt, execution_chain, same_conversation_confirmed, attach_confirmation,
  lease_token, lease_ack_deadline_at, failure_category, failure_stage, failure_reason_summary,
  retriable, attempt_history, zipkin_trace_id, zipkin_trace_url, traceparent, infra_requeue_count
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
		var convRaw, interruptedRaw, lineageRaw, chainRaw []byte
		var attemptHistoryRaw []byte
		var leaseAckDeadline sql.NullTime
		if err := rows.Scan(
			&r.JobID, &hostID, &worktreePath, &r.ScheduleID, &r.PromptTitle, &r.Engine,
			&r.Status, &r.Clean, &r.ASEComplete, &convRaw, &r.Reason, &r.ErrorMessage,
			&r.DiagnosticCode, &r.Model, &r.Remediation, &r.TraceID, &r.CreatedAt,
			&r.ParentJobID, &r.RootJobID, &r.InitialConversationID, &r.ActiveConversationID,
			&r.ResumeMode, &r.ResumeFallbackReason, &interruptedRaw, &lineageRaw,
			&r.ExecutionAttempt, &chainRaw, &r.SameConversationConfirmed, &r.AttachConfirmation,
			&r.LeaseToken, &leaseAckDeadline, &r.FailureCategory, &r.FailureStage, &r.FailureReasonSummary,
			&r.Retriable, &attemptHistoryRaw, &r.ZipkinTraceID, &r.ZipkinTraceURL, &r.Traceparent, &r.InfraRequeueCount,
		); err != nil {
			return nil, err
		}
		if leaseAckDeadline.Valid {
			t := leaseAckDeadline.Time
			r.LeaseAckDeadlineAt = &t
		}
		if len(attemptHistoryRaw) > 0 {
			_ = json.Unmarshal(attemptHistoryRaw, &r.AttemptHistory)
		}
		if r.AttemptHistory == nil {
			r.AttemptHistory = []control.AttemptFailure{}
		}
		if len(convRaw) > 0 {
			_ = json.Unmarshal(convRaw, &r.ConversationIDs)
		}
		if r.ConversationIDs == nil {
			r.ConversationIDs = []string{}
		}
		if len(interruptedRaw) > 0 {
			if errU := json.Unmarshal(interruptedRaw, &r.InterruptedASEState); errU != nil {
				log.Printf("[pgstore] warning: corrupt interrupted_ase_state JSON for job %s: %v", r.JobID, errU)
			}
		}
		if len(lineageRaw) > 0 {
			if errU := json.Unmarshal(lineageRaw, &r.ConversationLineage); errU != nil {
				log.Printf("[pgstore] warning: corrupt conversation_lineage JSON for job %s: %v", r.JobID, errU)
			}
		}
		if r.ConversationLineage == nil {
			r.ConversationLineage = []control.ConversationLineage{}
		}
		if len(chainRaw) > 0 {
			_ = json.Unmarshal(chainRaw, &r.ExecutionChain)
		}
		if r.ExecutionChain == nil {
			r.ExecutionChain = []string{}
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

// SaveStory inserts or updates a story in harness_stories.
func (s *Store) SaveStory(ctx context.Context, st control.Story) error {
	if s == nil || s.db == nil {
		return nil
	}
	acBytes, _ := json.Marshal(st.AcceptanceCriteria)
	tfBytes, _ := json.Marshal(st.TargetFiles)
	dpBytes, _ := json.Marshal(st.DependsOn)
	vcBytes, _ := json.Marshal(st.VerificationCommands)

	_, err := s.db.ExecContext(ctx, `
INSERT INTO harness_stories (
  id, repo, submodule, title, priority, status, sync_state, body, problem,
  acceptance_criteria, target_files, depends_on, verification_commands, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (id) DO UPDATE SET
  repo = EXCLUDED.repo,
  submodule = EXCLUDED.submodule,
  title = EXCLUDED.title,
  priority = EXCLUDED.priority,
  status = EXCLUDED.status,
  sync_state = EXCLUDED.sync_state,
  body = EXCLUDED.body,
  problem = EXCLUDED.problem,
  acceptance_criteria = EXCLUDED.acceptance_criteria,
  target_files = EXCLUDED.target_files,
  depends_on = EXCLUDED.depends_on,
  verification_commands = EXCLUDED.verification_commands,
  updated_at = EXCLUDED.updated_at;
`, st.ID, st.Repo, st.Submodule, st.Title, st.Priority, st.Status, st.SyncState, st.Body, st.Problem,
		string(acBytes), string(tfBytes), string(dpBytes), string(vcBytes), st.CreatedAt, st.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save story: %w", err)
	}
	return nil
}

// LoadStories loads all stories from the database.
func (s *Store) LoadStories(ctx context.Context) ([]control.Story, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, repo, submodule, title, priority, status, sync_state, body, problem,
       acceptance_criteria, target_files, depends_on, verification_commands, created_at, updated_at
FROM harness_stories
ORDER BY created_at DESC;
`)
	if err != nil {
		return nil, fmt.Errorf("load stories: %w", err)
	}
	defer rows.Close()

	var out []control.Story
	for rows.Next() {
		var st control.Story
		var acBytes, tfBytes, dpBytes, vcBytes []byte
		if err := rows.Scan(
			&st.ID, &st.Repo, &st.Submodule, &st.Title, &st.Priority, &st.Status, &st.SyncState, &st.Body, &st.Problem,
			&acBytes, &tfBytes, &dpBytes, &vcBytes, &st.CreatedAt, &st.UpdatedAt,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(acBytes, &st.AcceptanceCriteria)
		_ = json.Unmarshal(tfBytes, &st.TargetFiles)
		_ = json.Unmarshal(dpBytes, &st.DependsOn)
		_ = json.Unmarshal(vcBytes, &st.VerificationCommands)
		out = append(out, st)
	}
	return out, rows.Err()
}

// UpdateStorySyncState updates sync_state for a story.
func (s *Store) UpdateStorySyncState(ctx context.Context, id, syncState string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE harness_stories
SET sync_state = $1, updated_at = now()
WHERE id = $2;
`, syncState, id)
	return err
}

// ReconcileHistoricalLineage runs an administrative lineage reconciliation query over historical jobs.
func (s *Store) ReconcileHistoricalLineage(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE harness_history
SET initial_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399',
    active_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399'
WHERE job_id = 'job-1311' AND (initial_conversation_id = '' OR initial_conversation_id IS NULL);

UPDATE harness_history
SET parent_job_id = 'job-1311',
    root_job_id = 'job-1311',
    initial_conversation_id = 'aad902c8-bda6-44c7-8a68-c468a1562399',
    execution_attempt = 2,
    execution_chain = '["job-1311", "job-1313"]'::jsonb,
    conversation_lineage = '[{"conversation_id": "aad902c8-bda6-44c7-8a68-c468a1562399", "role": "root_initial", "reason": "reconciled_from_root_job"}, {"conversation_id": "1d446877-5bf9-44eb-9db5-e1c007a9fe31", "role": "legacy_unpinned_secondary", "reason": "split_brain_unpinned_phase_1"}, {"conversation_id": "14de9a1f-5679-4f11-a08f-f32bc7e47663", "role": "legacy_unpinned_secondary", "reason": "split_brain_unpinned_phase_2"}]'::jsonb
WHERE job_id = 'job-1313' AND (parent_job_id = '' OR parent_job_id IS NULL OR initial_conversation_id != 'aad902c8-bda6-44c7-8a68-c468a1562399');
`)
	return err
}

// SaveQueueJobUpsertSQL is the canonical upsert query for harness_dispatch_jobs.
// It updates all mutable job fields on conflict so dynamic story bindings, results,
// target allocations, payloads, harvest metrics, active conversations, and failure taxonomy survive API restarts.
const SaveQueueJobUpsertSQL = `
INSERT INTO harness_dispatch_jobs (
  id, type, repo, target_host, claimed_host, host_id, target_agent, story_id,
  normalized_prompt, payload, status, result, error, trace_id, fencing_token, lease_version,
  lease_owner, lease_token, attempts, created_at, claimed_at, heartbeat_at, lease_expires_at, completed_at,
  story_title, story_path, story_allocation_status, failure_category, failure_stage,
  failure_reason_summary, retriable, attempt_history, infra_requeue_count,
  parent_id, final_turn_count, final_transcript_sha256, allocation_source,
  priority_rank_selected, effective_action, source_conversation_title,
  agent_affinity, agent_pid, active_conversation_id, lease_ack_deadline_at, updated_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8,
  $9, $10, $11, $12, $13, $14, $15, $16,
  $17, $18, $19, $20, $21, $22, $23, $24,
  $25, $26, $27, $28, $29,
  $30, $31, $32, $33,
  $34, $35, $36, $37,
  $38, $39, $40,
  $41, $42, $43, $44, now()
)
ON CONFLICT (id) DO UPDATE SET
  type = EXCLUDED.type,
  repo = EXCLUDED.repo,
  target_host = EXCLUDED.target_host,
  claimed_host = EXCLUDED.claimed_host,
  host_id = EXCLUDED.host_id,
  target_agent = EXCLUDED.target_agent,
  story_id = EXCLUDED.story_id,
  normalized_prompt = EXCLUDED.normalized_prompt,
  payload = EXCLUDED.payload,
  status = EXCLUDED.status,
  result = COALESCE(EXCLUDED.result, harness_dispatch_jobs.result),
  error = EXCLUDED.error,
  trace_id = EXCLUDED.trace_id,
  fencing_token = EXCLUDED.fencing_token,
  lease_version = EXCLUDED.lease_version,
  lease_owner = EXCLUDED.lease_owner,
  lease_token = EXCLUDED.lease_token,
  attempts = EXCLUDED.attempts,
  claimed_at = EXCLUDED.claimed_at,
  heartbeat_at = EXCLUDED.heartbeat_at,
  lease_expires_at = EXCLUDED.lease_expires_at,
  completed_at = EXCLUDED.completed_at,
  story_title = EXCLUDED.story_title,
  story_path = EXCLUDED.story_path,
  story_allocation_status = EXCLUDED.story_allocation_status,
  failure_category = EXCLUDED.failure_category,
  failure_stage = EXCLUDED.failure_stage,
  failure_reason_summary = EXCLUDED.failure_reason_summary,
  retriable = EXCLUDED.retriable,
  attempt_history = EXCLUDED.attempt_history,
  infra_requeue_count = EXCLUDED.infra_requeue_count,
  parent_id = EXCLUDED.parent_id,
  final_turn_count = EXCLUDED.final_turn_count,
  final_transcript_sha256 = EXCLUDED.final_transcript_sha256,
  allocation_source = EXCLUDED.allocation_source,
  priority_rank_selected = EXCLUDED.priority_rank_selected,
  effective_action = EXCLUDED.effective_action,
  source_conversation_title = EXCLUDED.source_conversation_title,
  agent_affinity = EXCLUDED.agent_affinity,
  agent_pid = EXCLUDED.agent_pid,
  active_conversation_id = EXCLUDED.active_conversation_id,
  lease_ack_deadline_at = EXCLUDED.lease_ack_deadline_at,
  updated_at = now();
`

// SaveQueueJob persists or upserts a dispatch queue job into harness_dispatch_jobs.
func (s *Store) SaveQueueJob(ctx context.Context, job *dispatch.Job) error {
	if s == nil || s.db == nil || job == nil {
		return nil
	}
	payloadBytes := job.Payload
	if len(payloadBytes) == 0 {
		payloadBytes = []byte("{}")
	}
	var resultBytes []byte = job.Result
	if len(resultBytes) == 0 {
		resultBytes = nil
	}
	attemptHistBytes, err := json.Marshal(job.AttemptHistory)
	if err != nil || len(attemptHistBytes) == 0 || string(attemptHistBytes) == "null" {
		attemptHistBytes = []byte("[]")
	}
	affinityJSON := ""
	if job.AgentAffinity != nil {
		if b, mErr := json.Marshal(job.AgentAffinity); mErr == nil && string(b) != "null" {
			affinityJSON = string(b)
		}
	}
	_, err = s.db.ExecContext(ctx, SaveQueueJobUpsertSQL,
		job.ID, job.Type, job.Repo, job.TargetHost, job.ClaimedHost, job.HostID, job.TargetAgent, job.StoryID,
		job.NormalizedPrompt, payloadBytes, job.Status, resultBytes, job.Error, job.TraceID, job.FencingToken, job.LeaseVersion,
		job.LeaseOwner, job.LeaseToken, job.Attempts, job.CreatedAt, job.ClaimedAt, job.HeartbeatAt, job.LeaseExpiresAt, job.CompletedAt,
		job.StoryTitle, job.StoryPath, job.StoryAllocationStatus, job.FailureCategory, job.FailureStage,
		job.FailureReasonSummary, job.Retriable, string(attemptHistBytes), job.InfraRequeueCount,
		job.ParentID, job.FinalTurnCount, job.FinalTranscriptSHA256, job.AllocationSource,
		job.PriorityRankSelected, job.EffectiveAction, job.SourceConversationTitle,
		affinityJSON, job.AgentPID, job.ActiveConversationID, job.LeaseAckDeadlineAt,
	)
	return err
}

// UpdateQueueJobLease updates the lease owner, version, and heartbeat timestamps with optimistic concurrency.
// It fails if a newer lease version has already taken over, preventing split-brain lease overwrites.
func (s *Store) UpdateQueueJobLease(ctx context.Context, jobID string, leaseOwner string, leaseVersion int64, heartbeatAt, expiresAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE harness_dispatch_jobs
SET lease_owner = $2,
    lease_version = $3,
    heartbeat_at = $4,
    lease_expires_at = $5,
    updated_at = now()
WHERE id = $1 AND (lease_version IS NULL OR lease_version <= $3);
`, jobID, leaseOwner, leaseVersion, heartbeatAt, expiresAt)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("lease update conflict for job %s: lease_version %d was fenced or job not found", jobID, leaseVersion)
	}
	return nil
}

// CompleteQueueJob marks a queue job as completed or failed with result payload.
func (s *Store) CompleteQueueJob(ctx context.Context, jobID string, status string, result json.RawMessage, errStr string, completedAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	var resBytes []byte = result
	if len(resBytes) == 0 {
		resBytes = nil
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE harness_dispatch_jobs
SET status = $2,
    result = $3,
    error = $4,
    completed_at = $5,
    updated_at = now()
WHERE id = $1 AND status != 'completed';
`, jobID, status, resBytes, errStr, completedAt)
	if err != nil {
		return err
	}
	_ = res
	return nil
}

// LoadQueueJobs loads active, pending, and recent jobs from harness_dispatch_jobs.
func (s *Store) LoadQueueJobs(ctx context.Context) ([]*dispatch.Job, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, type, repo, target_host, claimed_host, host_id, target_agent, story_id,
       normalized_prompt, payload, status, result, error, trace_id, fencing_token,
       lease_version, lease_owner, lease_token, attempts, created_at, claimed_at,
       heartbeat_at, lease_expires_at, completed_at,
       story_title, story_path, story_allocation_status, failure_category, failure_stage,
       failure_reason_summary, retriable, attempt_history, infra_requeue_count,
       parent_id, final_turn_count, final_transcript_sha256, allocation_source,
       priority_rank_selected, effective_action, source_conversation_title,
       agent_affinity, agent_pid, active_conversation_id, lease_ack_deadline_at
FROM harness_dispatch_jobs
ORDER BY created_at ASC;
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*dispatch.Job
	for rows.Next() {
		var j dispatch.Job
		var payloadBytes, resultBytes, attemptHistBytes []byte
		var affinityJSON string
		var claimedAt, heartbeatAt, leaseExpiresAt, completedAt, leaseAckDeadlineAt *time.Time
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Repo, &j.TargetHost, &j.ClaimedHost, &j.HostID, &j.TargetAgent, &j.StoryID,
			&j.NormalizedPrompt, &payloadBytes, &j.Status, &resultBytes, &j.Error, &j.TraceID, &j.FencingToken,
			&j.LeaseVersion, &j.LeaseOwner, &j.LeaseToken, &j.Attempts, &j.CreatedAt, &claimedAt,
			&heartbeatAt, &leaseExpiresAt, &completedAt,
			&j.StoryTitle, &j.StoryPath, &j.StoryAllocationStatus, &j.FailureCategory, &j.FailureStage,
			&j.FailureReasonSummary, &j.Retriable, &attemptHistBytes, &j.InfraRequeueCount,
			&j.ParentID, &j.FinalTurnCount, &j.FinalTranscriptSHA256, &j.AllocationSource,
			&j.PriorityRankSelected, &j.EffectiveAction, &j.SourceConversationTitle,
			&affinityJSON, &j.AgentPID, &j.ActiveConversationID, &leaseAckDeadlineAt,
		); err != nil {
			return nil, err
		}
		j.Payload = payloadBytes
		j.Result = resultBytes
		j.ClaimedAt = claimedAt
		j.HeartbeatAt = heartbeatAt
		j.LeaseExpiresAt = leaseExpiresAt
		j.CompletedAt = completedAt
		j.LeaseAckDeadlineAt = leaseAckDeadlineAt
		if strings.TrimSpace(affinityJSON) != "" {
			var aff dispatch.AgentAffinity
			if json.Unmarshal([]byte(affinityJSON), &aff) == nil {
				j.AgentAffinity = &aff
			}
		}
		if len(attemptHistBytes) > 0 {
			_ = json.Unmarshal(attemptHistBytes, &j.AttemptHistory)
		}
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

// SaveToolPriority persists the global tool priority list into harness_settings.
func (s *Store) SaveToolPriority(ctx context.Context, tools []string) error {
	if s == nil || s.db == nil {
		return nil
	}
	raw, err := json.Marshal(tools)
	if err != nil {
		return fmt.Errorf("marshal tool priority: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO harness_settings (key, value, updated_at)
VALUES ('tool_priority', $1::jsonb, now())
ON CONFLICT (key) DO UPDATE SET
  value = EXCLUDED.value,
  updated_at = now()
`, string(raw))
	if err != nil {
		return fmt.Errorf("save tool priority: %w", err)
	}
	return nil
}

// LoadToolPriority loads the persisted global tool priority list from harness_settings.
func (s *Store) LoadToolPriority(ctx context.Context) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM harness_settings WHERE key = 'tool_priority'`).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load tool priority: %w", err)
	}
	var tools []string
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("unmarshal tool priority: %w", err)
	}
	return tools, nil
}

