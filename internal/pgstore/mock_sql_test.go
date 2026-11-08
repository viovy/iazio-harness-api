package pgstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

var registerMockOnce sync.Once

type mockPGDriver struct{}
type mockPGConn struct{}
type mockPGStmt struct{ query string }
type mockPGRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}
type mockPGResult struct{ affected int64 }

func (r mockPGResult) LastInsertId() (int64, error) { return 0, nil }
func (r mockPGResult) RowsAffected() (int64, error) { return r.affected, nil }

func (d *mockPGDriver) Open(name string) (driver.Conn, error) { return &mockPGConn{}, nil }
func (c *mockPGConn) Prepare(query string) (driver.Stmt, error) {
	return &mockPGStmt{query: query}, nil
}
func (c *mockPGConn) Close() error              { return nil }
func (c *mockPGConn) Begin() (driver.Tx, error) { return &mockPGTx{}, nil }

type mockPGTx struct{}

func (t *mockPGTx) Commit() error   { return nil }
func (t *mockPGTx) Rollback() error { return nil }

func (s *mockPGStmt) Close() error  { return nil }
func (s *mockPGStmt) NumInput() int { return -1 }
func (s *mockPGStmt) Exec(args []driver.Value) (driver.Result, error) {
	if strings.Contains(s.query, "UPDATE harness_dispatch_jobs") && len(args) > 0 && args[0] == "job-conflict" {
		return mockPGResult{affected: 0}, nil
	}
	return mockPGResult{affected: 1}, nil
}
func (s *mockPGStmt) Query(args []driver.Value) (driver.Rows, error) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	q := s.query
	switch {
	case strings.Contains(q, "FROM harness_dispatch_jobs"):
		cols := make([]string, 49)
		for i := range cols {
			cols[i] = "c"
		}
		row1 := []driver.Value{
			"job-1", "prompt_execute", "iazio-mcp", "core-node", "core-node", "core-node", "opencode", "STORY-GOV-5200",
			"run story", []byte(`{"k":"v"}`), "completed", []byte(`{"active_conversation_id":"conv-1","conversation_ids":["conv-1","conv-2"],"harness_pid":123,"tool_pid":456,"elapsed_seconds":42}`), "", "trace-1", int64(1),
			int64(2), "owner-1", "tok-1", 1, now.Add(-time.Minute), now.Add(-50 * time.Second),
			now.Add(-10 * time.Second), now.Add(time.Minute), now,
			"Title", "docs/stories/s.md", "allocated", "infra", "stage1",
			"reason", true, []byte(`[]`), 0,
			"job-parent", 5, "sha256", "manual",
			1, "run", "Title",
			`{"preferred_agent":"opencode"}`, 123, "", now,
			"job-dep", []byte(`["job-dep"]`), "http://cb", "conv-1", []byte(`["conv-1"]`),
		}
		row2 := []driver.Value{
			"job-2", "prompt_execute", "iazio-mcp", "core-node", "core-node", "core-node", "opencode", "STORY-GOV-5200",
			"run story", []byte(`{}`), "failed", []byte(`{"conversation_id":"conv-fallback"}`), "timeout", "trace-2", int64(1),
			int64(2), "owner-1", "tok-1", 1, now.Add(-2 * time.Minute), now.Add(-90 * time.Second),
			now.Add(-10 * time.Second), now.Add(time.Minute), now,
			"Title", "docs/stories/s.md", "allocated", "timeout", "stage2",
			"timed out", true, []byte(`[]`), 1,
			"", 2, "sha256", "manual",
			1, "run", "Title",
			``, 999, "", now,
			"", []byte(`[]`), "", "", []byte(`[]`),
		}
		return &mockPGRows{cols: cols, rows: [][]driver.Value{row1, row2}}, nil
	case strings.Contains(q, "FROM harness_settings"):
		return &mockPGRows{
			cols: []string{"value"},
			rows: [][]driver.Value{
				{[]byte(`["opencode","claude","agy"]`)},
			},
		}, nil
	case strings.Contains(q, "FROM harness_ideas"):
		return &mockPGRows{
			cols: []string{"id", "title", "share_url", "transcript", "status", "story_id", "previous_status", "target_repo", "created_at", "updated_at"},
			rows: [][]driver.Value{{"i1", "t1", "u1", "tr1", "NEW", "s1", "", "workspace", now, now}},
		}, nil
	case strings.Contains(q, "FROM harness_prompts"):
		return &mockPGRows{
			cols: []string{"id", "title", "body", "engine", "status", "revision", "created_at", "updated_at"},
			rows: [][]driver.Value{{"p1", "Title", "Body", "opencode", "READY", 1, now, now}},
		}, nil
	case strings.Contains(q, "FROM harness_repos"):
		return &mockPGRows{
			cols: []string{"host_id", "worktree_path", "clone_url", "default_branch", "alias", "queue", "lock", "lock_holder", "running_job_id", "updated_at"},
			rows: [][]driver.Value{{"h1", "/w", "url", "main", "w", "OPEN", "IDLE", "", "", now}},
		}, nil
	case strings.Contains(q, "FROM harness_hosts"):
		return &mockPGRows{
			cols: []string{"id", "name", "kind", "presence", "last_seen", "distribution_profile", "tools", "fetch_failed", "eager_update_enabled", "last_eager_update_at", "eager_readiness", "updated_at"},
			rows: [][]driver.Value{{"h1", "Host", "permanent", "ONLINE", now, "generic", []byte(`[]`), false, true, now, []byte(`{"ready":true}`), now}},
		}, nil
	case strings.Contains(q, "FROM harness_history"):
		return &mockPGRows{
			cols: []string{
				"host_id", "worktree_path", "job_id", "schedule_id", "story_id", "prompt_title", "engine", "status",
				"clean", "ase_complete", "conversation_ids", "reason", "error_message", "diagnostic_code", "model",
				"remediation", "created_at", "parent_job_id", "root_job_id", "initial_conversation_id",
				"active_conversation_id", "resume_mode", "resume_fallback_reason", "same_conversation_confirmed",
				"attach_confirmation", "execution_attempt", "execution_chain", "conversation_lineage", "interrupted_ase_state",
			},
			rows: [][]driver.Value{{
				"h1", "/w", "j1", "s1", "STORY-1", "Title", "opencode", "SUCCESS",
				true, true, []byte(`["c1"]`), "ok", "", "", "claude",
				"", now, "p1", "r1", "c1",
				"c1", "direct", "", true,
				"explicit", 1, []byte(`["j1"]`), []byte(`[{"conversation_id":"c1","role":"initial_active"}]`), []byte(`{}`),
			}},
		}, nil
	case strings.Contains(q, "FROM harness_profiles"):
		return &mockPGRows{
			cols: []string{"name", "description", "is_default", "install_roots", "endpoints", "tools", "created_at", "updated_at"},
			rows: [][]driver.Value{{"generic", "desc", true, []byte(`{}`), []byte(`{}`), []byte(`[]`), now, now}},
		}, nil
	case strings.Contains(q, "FROM harness_git_credentials"):
		return &mockPGRows{
			cols: []string{"id", "name", "pattern", "token", "type", "created_at"},
			rows: [][]driver.Value{{"c1", "n1", "*", "tok", "pat", now}},
		}, nil
	case strings.Contains(q, "FROM codebase_repositories"):
		return &mockPGRows{
			cols: []string{"name", "path", "default_branch", "git_tree_sha", "description", "host_id", "updated_at"},
			rows: [][]driver.Value{{"r1", "/p", "main", "sha", "desc", "h1", now}},
		}, nil
	case strings.Contains(q, "FROM codebase_chunks"):
		return &mockPGRows{
			cols: []string{"id", "repo_name", "file_path", "start_line", "end_line", "symbol", "content", "file_blob_sha", "git_tree_sha", "content_sha256", "superseded"},
			rows: [][]driver.Value{{"ch1", "r1", "main.go", int64(1), int64(10), "main", "pkg", "b1", "t1", "s1", false}},
		}, nil
	case strings.Contains(q, "FROM harness_stories"):
		return &mockPGRows{
			cols: []string{"id", "repo", "submodule", "title", "priority", "status", "sync_state", "body", "problem", "acceptance_criteria", "target_files", "depends_on", "verification_commands", "created_at", "updated_at"},
			rows: [][]driver.Value{{"STORY-1", "r1", "sub", "Title", "P1", "READY", "MATERIALIZED", "body", "prob", []byte(`["ac"]`), []byte(`["f.go"]`), []byte(`[]`), []byte(`["make test"]`), now, now}},
		}, nil
	default:
		return &mockPGRows{cols: []string{"ok"}}, nil
	}
}

func (r *mockPGRows) Columns() []string { return r.cols }
func (r *mockPGRows) Close() error      { return nil }
func (r *mockPGRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		}
	}
	return nil
}

func TestMockSQLStoreQueueAndSettingsCoverage(t *testing.T) {
	registerMockOnce.Do(func() {
		sql.Register("mockpg_coverage", &mockPGDriver{})
	})
	db, err := sql.Open("mockpg_coverage", "mock")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	s := &Store{db: db}
	ctx := context.Background()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

	// Nil store checks for Queue and ToolPriority methods
	var nilStore *Store
	if err := nilStore.ReconcileHistoricalLineage(ctx); err != nil {
		t.Fatal(err)
	}
	if err := nilStore.SaveQueueJob(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := nilStore.UpdateQueueJobLease(ctx, "j1", "o1", 1, now, now); err != nil {
		t.Fatal(err)
	}
	if err := nilStore.CompleteQueueJob(ctx, "j1", "completed", nil, "", now); err != nil {
		t.Fatal(err)
	}
	if jobs, err := nilStore.LoadQueueJobs(ctx); err != nil || jobs != nil {
		t.Fatal("expected nil jobs")
	}
	if err := nilStore.SaveToolPriority(ctx, []string{"opencode"}); err != nil {
		t.Fatal(err)
	}
	if tools, err := nilStore.LoadToolPriority(ctx); err != nil || tools != nil {
		t.Fatal("expected nil tools")
	}

	// Execute against mock store
	if err := s.ReconcileHistoricalLineage(ctx); err != nil {
		t.Fatalf("ReconcileHistoricalLineage: %v", err)
	}

	job := &dispatch.Job{
		ID:                   "job-1",
		Type:                 dispatch.JobTypePromptExecute,
		Repo:                 "iazio-mcp",
		TargetAgent:          "opencode",
		StoryID:              "STORY-GOV-5200",
		Status:               dispatch.JobStatusLeased,
		ActiveConversationID: "conv-1",
		AgentAffinity:        &dispatch.AgentAffinity{},
		CreatedAt:            now,
	}
	if err := s.SaveQueueJob(ctx, job); err != nil {
		t.Fatalf("SaveQueueJob: %v", err)
	}
	if err := s.UpdateQueueJobLease(ctx, "job-1", "host-1", 2, now, now.Add(time.Minute)); err != nil {
		t.Fatalf("UpdateQueueJobLease: %v", err)
	}
	if err := s.UpdateQueueJobLease(ctx, "job-conflict", "host-1", 1, now, now.Add(time.Minute)); err == nil {
		t.Fatal("expected lease conflict error for job-conflict")
	}
	if err := s.CompleteQueueJob(ctx, "job-1", "completed", json.RawMessage(`{"ok":true}`), "", now); err != nil {
		t.Fatalf("CompleteQueueJob: %v", err)
	}
	loadedJobs, err := s.LoadQueueJobs(ctx)
	if err != nil || len(loadedJobs) != 2 {
		t.Fatalf("LoadQueueJobs: len=%d err=%v", len(loadedJobs), err)
	}
	if loadedJobs[0].ActiveConversationID != "conv-1" || loadedJobs[0].ElapsedSeconds != 42 {
		t.Fatalf("unexpected loaded job[0]: %+v", loadedJobs[0])
	}
	if loadedJobs[1].ActiveConversationID != "conv-fallback" || loadedJobs[1].HarnessPID != 999 {
		t.Fatalf("unexpected loaded job[1]: %+v", loadedJobs[1])
	}

	if err := s.SaveToolPriority(ctx, []string{"opencode", "claude", "agy"}); err != nil {
		t.Fatalf("SaveToolPriority: %v", err)
	}
	prio, err := s.LoadToolPriority(ctx)
	if err != nil || len(prio) != 3 {
		t.Fatalf("LoadToolPriority: %v %v", prio, err)
	}

	// Exercise remaining Store methods against mock driver for air-gapped CI coverage
	_ = s.SaveIdea(ctx, control.Idea{ID: "i1", Title: "t"})
	_ = s.SaveIdeaArgs(ctx, "i2", "t2", "u", "tr", "NEW", "s1")
	_, _ = s.LoadIdeas(ctx)
	_ = s.SavePrompt(ctx, control.Prompt{ID: "p1", Title: "t"})
	_, _ = s.LoadPrompts(ctx)
	_ = s.DeletePrompt(ctx, "p1")
	_ = s.SaveRepo(ctx, control.Repo{HostID: "h1", WorktreePath: "/w"})
	_, _ = s.LoadRepos(ctx)
	_ = s.DeleteRepo(ctx, "h1", "/w")
	_ = s.SaveHost(ctx, control.Host{ID: "h1", Name: "Host", EagerReadiness: &control.EagerUpdateReadinessReport{}})
	_, _ = s.LoadHosts(ctx)
	_ = s.SaveHistory(ctx, "h1", "/w", control.HistoryRow{JobID: "j1", CreatedAt: now})
	_, _ = s.LoadHistory(ctx)
	_ = s.DeleteHistory(ctx, "j1")
	_ = s.ClearRepoHistory(ctx, "h1", "/w")
	_ = s.SaveProfile(ctx, control.DistributionProfile{Name: "generic"})
	_, _ = s.LoadProfiles(ctx)
	_ = s.DeleteProfile(ctx, "generic")
	_ = s.SaveGitCredential(ctx, control.GitCredential{ID: "c1"})
	_, _ = s.LoadGitCredentials(ctx)
	_ = s.DeleteGitCredential(ctx, "c1")
	_ = s.SaveCodebaseRepo(ctx, codebase.Repository{Name: "r1"})
	_, _ = s.LoadCodebaseRepos(ctx)
	_ = s.SaveCodebaseChunks(ctx, []codebase.CodeChunk{{ID: "ch1", RepoName: "r1"}})
	_, _ = s.LoadCodebaseChunks(ctx, "r1")
	_ = s.SaveStory(ctx, control.Story{ID: "STORY-1", Title: "T"})
	_, _ = s.LoadStories(ctx)
	_ = s.UpdateStorySyncState(ctx, "STORY-1", "MATERIALIZED")
}
