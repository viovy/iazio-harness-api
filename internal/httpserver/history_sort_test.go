package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestListHistory_ChronologicalSorting(t *testing.T) {
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  dispatch.NewInMemoryQueue(),
	}

	_ = s.Engine.UpsertRepo(control.Repo{HostID: "host-01", WorktreePath: "/work/app-repo"})

	// 1. Add older engine history items (e.g. 2 hours ago, 1 hour ago)
	now := time.Now().UTC()
	t1 := now.Add(-2 * time.Hour)
	t2 := now.Add(-1 * time.Hour)
	s.Engine.SetHistory("host-01:/work/app-repo", []control.HistoryRow{
		{JobID: "hist-oldest", CreatedAt: t1, Status: "COMPLETED", Engine: "autopilot"},
		{JobID: "hist-middle", CreatedAt: t2, Status: "COMPLETED", Engine: "autopilot"},
	})

	// 2. Add recent dispatch queue jobs (e.g. 5 minutes ago)
	t3 := now.Add(-5 * time.Minute)
	jobPayload, _ := json.Marshal(map[string]any{
		"prompt":       "Execute story STORY-TEST",
		"prompt_title": "Test Story Execution",
	})
	qJob := &dispatch.Job{
		ID:          "job-newest-dispatch",
		Type:        "prompt.execute",
		Status:      dispatch.JobStatusCompleted,
		Repo:        "/work/app-repo",
		TargetHost:  "host-01",
		TargetAgent: "autopilot",
		Payload:     jobPayload,
		CreatedAt:   t3,
	}
	_, _ = s.Queue.Enqueue(context.Background(), qJob)

	// Query /v1/history
	req := httptest.NewRequest(http.MethodGet, "/v1/history?limit=10", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var resp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.History) != 3 {
		t.Fatalf("expected 3 history items, got %d", len(resp.History))
	}

	// First item must be the newest dispatch job
	if resp.History[0]["job_id"] != "job-newest-dispatch" {
		t.Errorf("expected first item to be job-newest-dispatch, got %v", resp.History[0]["job_id"])
	}
	if resp.History[1]["job_id"] != "hist-middle" {
		t.Errorf("expected second item to be hist-middle, got %v", resp.History[1]["job_id"])
	}
	if resp.History[2]["job_id"] != "hist-oldest" {
		t.Errorf("expected third item to be hist-oldest, got %v", resp.History[2]["job_id"])
	}

	// Test limit enforcement
	reqLimit := httptest.NewRequest(http.MethodGet, "/v1/history?limit=2", nil)
	rrLimit := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrLimit, reqLimit)

	var respLimit struct {
		History []map[string]any `json:"history"`
	}
	_ = json.NewDecoder(rrLimit.Body).Decode(&respLimit)
	if len(respLimit.History) != 2 {
		t.Errorf("expected 2 history items with limit=2, got %d", len(respLimit.History))
	}
	if respLimit.History[0]["job_id"] != "job-newest-dispatch" {
		t.Errorf("expected newest item in limited result, got %v", respLimit.History[0]["job_id"])
	}
}

func TestListPrompts_AlphabeticalSortingHTTP(t *testing.T) {
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  dispatch.NewInMemoryQueue(),
	}
	s.Engine.PutPrompt(control.Prompt{ID: "p-zeta", Title: "zeta prompt", Body: "run", Engine: "agy", Status: "READY"})
	s.Engine.PutPrompt(control.Prompt{ID: "p-empty", Title: "   ", Body: "run", Engine: "agy", Status: "READY"})
	s.Engine.PutPrompt(control.Prompt{ID: "p-alpha", Title: "Alpha Prompt", Body: "run", Engine: "agy", Status: "READY"})
	s.Engine.PutPrompt(control.Prompt{ID: "p-beta", Title: "beta prompt", Body: "run", Engine: "agy", Status: "READY"})

	req := httptest.NewRequest(http.MethodGet, "/v1/prompts", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	var resp struct {
		Prompts []map[string]any `json:"prompts"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode /v1/prompts: %v", err)
	}
	wantIDs := []string{"p-alpha", "p-beta", "p-zeta", "p-empty"}
	if len(resp.Prompts) != len(wantIDs) {
		t.Fatalf("expected %d prompts, got %d", len(wantIDs), len(resp.Prompts))
	}
	for i, want := range wantIDs {
		if got := resp.Prompts[i]["id"]; got != want {
			t.Errorf("position %d: expected id %q, got %v", i, want, got)
		}
	}
}

func TestRepoDetailAndHistory_IncludesDispatchJob3ffa5d0be1649f0e(t *testing.T) {
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  dispatch.NewInMemoryQueue(),
	}

	_ = s.Engine.UpsertRepo(control.Repo{HostID: "debian", WorktreePath: "/home/romeo/work/meta-repo-02"})

	now := time.Date(2026, 10, 31, 17, 49, 15, 0, time.UTC)
	claimedAt := now.Add(-6434 * time.Second)
	qJob := &dispatch.Job{
		ID:                   "job_3ffa5d0be1649f0e",
		Type:                 dispatch.JobTypePromptExecute,
		Status:               dispatch.JobStatusCompleted,
		Repo:                 "/home/romeo/work/meta-repo-02",
		TargetHost:           "debian",
		ClaimedHost:          "debian",
		HostID:               "debian",
		TargetAgent:          "agy",
		StoryID:              "STORY-GOV-3850",
		ActiveConversationID: "ba042127-c63f-4b1e-a42e-95b307e61bbc",
		ConversationIDs:      []string{"ba042127-c63f-4b1e-a42e-95b307e61bbc"},
		AgentPID:             2779723,
		HarnessPID:           2779723,
		ToolPID:              2781140,
		ElapsedSeconds:       6434,
		TraceID:              "7e97788c6f8c8bcea43de8bd9cc29e0b",
		Payload:              json.RawMessage(`{"prompt":"/autonomous-story-execution","target_agent":"agy"}`),
		CreatedAt:            claimedAt,
		ClaimedAt:            &claimedAt,
		CompletedAt:          &now,
	}
	_, _ = s.Queue.Enqueue(context.Background(), qJob)

	// 1. Check /v1/history?host=debian&path=/home/romeo/work/meta-repo-02
	reqHist := httptest.NewRequest(http.MethodGet, "/v1/history?host=debian&path=/home/romeo/work/meta-repo-02", nil)
	rrHist := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrHist, reqHist)
	if rrHist.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/history, got %d", rrHist.Code)
	}
	var histResp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.NewDecoder(rrHist.Body).Decode(&histResp); err != nil {
		t.Fatalf("decode /v1/history: %v", err)
	}
	if len(histResp.History) != 1 || histResp.History[0]["job_id"] != "job_3ffa5d0be1649f0e" {
		t.Fatalf("expected job_3ffa5d0be1649f0e in /v1/history for debian/meta-repo-02, got %+v", histResp.History)
	}
	if histResp.History[0]["story_id"] != "STORY-GOV-3850" {
		t.Errorf("expected story_id=STORY-GOV-3850, got %v", histResp.History[0]["story_id"])
	}

	// 2. Check /v1/hosts/debian/repos?path=/home/romeo/work/meta-repo-02 (used by /agents/debian/meta-repo-02/history)
	reqRepo := httptest.NewRequest(http.MethodGet, "/v1/hosts/debian/repos?path=/home/romeo/work/meta-repo-02", nil)
	rrRepo := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrRepo, reqRepo)
	if rrRepo.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/hosts/debian/repos, got %d", rrRepo.Code)
	}
	var repoResp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.NewDecoder(rrRepo.Body).Decode(&repoResp); err != nil {
		t.Fatalf("decode /v1/hosts/debian/repos: %v", err)
	}
	if len(repoResp.History) != 1 || repoResp.History[0]["job_id"] != "job_3ffa5d0be1649f0e" {
		t.Fatalf("expected job_3ffa5d0be1649f0e in repo detail history, got %+v", repoResp.History)
	}

	// 3. Check /v1/jobs/job_3ffa5d0be1649f0e detail and fallback chunk telemetry
	reqJob := httptest.NewRequest(http.MethodGet, "/v1/jobs/job_3ffa5d0be1649f0e", nil)
	rrJob := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrJob, reqJob)
	if rrJob.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/jobs/job_3ffa5d0be1649f0e, got %d", rrJob.Code)
	}
	var jobResp map[string]any
	if err := json.NewDecoder(rrJob.Body).Decode(&jobResp); err != nil {
		t.Fatalf("decode /v1/jobs/job_3ffa5d0be1649f0e: %v", err)
	}
	if jobResp["active_conversation_id"] != "ba042127-c63f-4b1e-a42e-95b307e61bbc" {
		t.Errorf("expected active_conversation_id=ba042127-c63f-4b1e-a42e-95b307e61bbc, got %v", jobResp["active_conversation_id"])
	}
}
