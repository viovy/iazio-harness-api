-- Dedicated database for the harness control plane.
-- Do not point this schema at the transcript database.

CREATE TABLE IF NOT EXISTS harness_ideas (
  id text PRIMARY KEY,
  title text NOT NULL,
  share_url text NOT NULL,
  transcript text NOT NULL,
  status text NOT NULL,
  story_id text NOT NULL DEFAULT '',
  previous_status text NOT NULL DEFAULT ''
);

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

CREATE UNIQUE INDEX IF NOT EXISTS harness_repos_host_alias_idx ON harness_repos (host_id, lower(repo_alias)) WHERE repo_alias <> '';

CREATE TABLE IF NOT EXISTS harness_hosts (
  id text PRIMARY KEY,
  name text NOT NULL,
  kind text NOT NULL,
  presence text NOT NULL DEFAULT 'ONLINE',
  last_seen timestamp with time zone,
  fetch_failed boolean NOT NULL DEFAULT false,
  tools jsonb NOT NULL DEFAULT '[]'::jsonb
);

-- Persists execution history across container restarts for iazio-web inspection.
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
ALTER TABLE harness_history ADD COLUMN IF NOT EXISTS trace_id text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS harness_git_credentials (
  id text PRIMARY KEY,
  name text NOT NULL,
  repository_pattern text NOT NULL,
  token text NOT NULL,
  type text NOT NULL DEFAULT 'pat',
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

-- Fleet repository registry and hybrid codebase RAG chunk store.
CREATE TABLE IF NOT EXISTS codebase_repositories (
  name text PRIMARY KEY,
  path text NOT NULL,
  default_branch text NOT NULL DEFAULT 'main',
  git_tree_sha text NOT NULL DEFAULT '',
  description text NOT NULL DEFAULT '',
  host_id text NOT NULL DEFAULT '',
  updated_at timestamp with time zone NOT NULL DEFAULT now()
);

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

