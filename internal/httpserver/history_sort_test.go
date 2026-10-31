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
