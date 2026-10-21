package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestJobLifecycleHandler_GetJob(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        "iazio-mcp",
		TargetAgent: "autopilot",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.GetJob)

	// Existing job
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var detail JobDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if detail.ID != job.ID || detail.Repo != "iazio-mcp" || detail.Status != dispatch.JobStatusPending {
		t.Fatalf("unexpected detail: %+v", detail)
	}

	// Missing job -> 404
	req404 := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
	rr404 := httptest.NewRecorder()
	mux.ServeHTTP(rr404, req404)

	if rr404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", rr404.Code)
	}
}

func TestJobLifecycleHandler_ListQueue(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	_, _ = q.Enqueue(context.Background(), &dispatch.Job{
		Type: dispatch.JobTypeStoryCreate,
		Repo: "repo-1",
	})
	_, _ = q.Enqueue(context.Background(), &dispatch.Job{
		Type: dispatch.JobTypePromptExecute,
		Repo: "repo-2",
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/queue", h.ListQueue)

	// All jobs
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queue", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var listResp ListQueueResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if listResp.Total != 2 {
		t.Fatalf("expected 2 jobs, got %d", listResp.Total)
	}

	// Filter by repo
	reqFilter := httptest.NewRequest(http.MethodGet, "/api/v1/queue?repo=repo-1", nil)
	rrFilter := httptest.NewRecorder()
	mux.ServeHTTP(rrFilter, reqFilter)

	var listFiltered ListQueueResponse
	_ = json.Unmarshal(rrFilter.Body.Bytes(), &listFiltered)
	if listFiltered.Total != 1 || listFiltered.Items[0].Repo != "repo-1" {
		t.Fatalf("unexpected filtered response: %+v", listFiltered)
	}
}

func TestJobLifecycleHandler_CancelJob(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	job, _ := q.Enqueue(context.Background(), &dispatch.Job{
		Type: dispatch.JobTypeStoryCreate,
		Repo: "repo-1",
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", h.CancelJob)

	body := bytes.NewReader([]byte(`{"reason":"user abort"}`))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/cancel", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var canceled JobDetailResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &canceled)
	if canceled.Status != dispatch.JobStatusCanceled || canceled.Error != "user abort" {
		t.Fatalf("unexpected canceled job: %+v", canceled)
	}
}

func TestJobLifecycleHandler_RetryJob(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	job, _ := q.Enqueue(context.Background(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        "repo-retry",
		TargetAgent: "autopilot",
	})
	_ = q.Complete(context.Background(), job.ID, nil, "some failure")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", h.RetryJob)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/retry", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rr.Code, rr.Body.String())
	}

	var retried JobDetailResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &retried)
	if retried.ID == job.ID || retried.Status != dispatch.JobStatusPending || retried.Repo != "repo-retry" {
		t.Fatalf("unexpected retried job: %+v", retried)
	}
}

type mockStoryValidator struct {
	availableStories map[string]bool
}

func (m *mockStoryValidator) StoryExists(storyID, submodule string) bool {
	return m.availableStories[storyID]
}

func TestJobLifecycleHandler_ExecuteStory(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)
	v := &mockStoryValidator{
		availableStories: map[string]bool{
			"STORY-GOV-9999": true,
		},
	}
	h.SetStoryValidator(v)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/stories/{id}/execute", h.ExecuteStory)

	// 1. Non-existent story -> 404 Not Found with descriptive error message
	bodyNonExistent := bytes.NewReader([]byte(`{"submodule":".","target_agent":"autopilot"}`))
	reqNonExistent := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-TEST-0006/execute", bodyNonExistent)
	rrNonExistent := httptest.NewRecorder()
	mux.ServeHTTP(rrNonExistent, reqNonExistent)

	if rrNonExistent.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for nonexistent story, got %d: %s", rrNonExistent.Code, rrNonExistent.Body.String())
	}
	if !strings.Contains(rrNonExistent.Body.String(), "cannot execute nonexistent story") {
		t.Fatalf("expected guidance to create story in error, got: %s", rrNonExistent.Body.String())
	}

	// 2. Existing story with TraceID -> 201 Created and properly queued
	body := bytes.NewReader([]byte(`{"submodule":".","target_agent":"autopilot","trace_id":"trace-zipkin-test-123"}`))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-GOV-9999/execute", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rr.Code, rr.Body.String())
	}

	var execResp ExecuteStoryResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &execResp)
	if execResp.StoryID != "STORY-GOV-9999" || execResp.Submodule != "." || execResp.JobID == "" || execResp.Status != dispatch.JobStatusPending {
		t.Fatalf("unexpected execute response: %+v", execResp)
	}

	// Verify enqueued job has TraceID
	job, found := q.Get(execResp.JobID)
	if !found || job == nil {
		t.Fatalf("job %s not found in queue", execResp.JobID)
	}
	if job.TraceID != "trace-zipkin-test-123" {
		t.Errorf("expected TraceID trace-zipkin-test-123, got %s", job.TraceID)
	}
}

func TestJobLifecycleHandler_Attribution(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        "iazio-mcp",
		TargetAgent: "autopilot",
		TraceID:     "trace-abc-123",
		SearchID:    "srch-xyz-789",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Complete the job successfully
	if err := q.Complete(context.Background(), job.ID, json.RawMessage(`{"status":"ok"}`), ""); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/{id}/attribution", h.GetJobAttribution)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID+"/attribution", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var attr dispatch.JobAttributionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &attr); err != nil {
		t.Fatal(err)
	}

	if attr.JobID != job.ID || attr.TraceID != "trace-abc-123" || attr.SearchID != "srch-xyz-789" {
		t.Fatalf("unexpected attribution: %+v", attr)
	}
	if !attr.RelevanceEmitted {
		t.Fatal("expected relevance_emitted = true on successful completion")
	}
	if len(attr.PromotedMemories) == 0 || attr.PromotedMemories[0] != "mem-srch-xyz-789" {
		t.Fatalf("expected promoted memories, got %+v", attr.PromotedMemories)
	}
}

