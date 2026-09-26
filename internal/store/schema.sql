-- Dedicated database for the harness control plane.
-- Do not point this schema at the transcript database.

CREATE TABLE IF NOT EXISTS ideas (
  id text PRIMARY KEY,
  share_url text NOT NULL,
  transcript text NOT NULL,
  status text NOT NULL,
  previous_status text,
  story_id text
);

CREATE TABLE IF NOT EXISTS prompts (
  id text PRIMARY KEY,
  title text NOT NULL,
  body text NOT NULL,
  engine text NOT NULL,
  status text NOT NULL,
  revision integer NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
  id text PRIMARY KEY,
  prompt_id text,
  revision integer NOT NULL,
  iterations_remaining integer NOT NULL,
  max_execution_duration_seconds integer NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id text PRIMARY KEY,
  schedule_id text,
  status text NOT NULL,
  lease_holder text
);
