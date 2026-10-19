package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestAgentJobsHandler_ClaimAndComplete(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	handler := NewAgentJobsHandler(q)

	// Enqueue a job first
	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:       dispatch.JobTypeStoryCreate,
		Repo:       "iazio-mcp",
		TargetHost: "host-alpha",
		Payload:    json.RawMessage(`{"title":"hello"}`),
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 1. Claim job
	claimBody, _ := json.Marshal(ClaimJobRequest{
		HostID:   "host-alpha",
		JobTypes: []string{dispatch.JobTypeStoryCreate},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	w := httptest.NewRecorder()
	handler.ClaimHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK claiming, got %d: %s", w.Code, w.Body.String())
	}

	var claimed dispatch.Job
	if err := json.NewDecoder(w.Body).Decode(&claimed); err != nil {
		t.Fatalf("failed to decode claimed job: %v", err)
	}
	if claimed.ID != job.ID {
		t.Errorf("expected claimed ID %s, got %s", job.ID, claimed.ID)
	}

	// 2. Complete job
	completeBody, _ := json.Marshal(CompleteJobRequest{
		Result: json.RawMessage(`{"story_id":"STORY-001"}`),
	})
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/complete", bytes.NewReader(completeBody))
	w2 := httptest.NewRecorder()
	handler.CompleteHandler(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK completing, got %d: %s", w2.Code, w2.Body.String())
	}

	// Verify job is now completed in queue
	updated, ok := q.Get(job.ID)
	if !ok || updated.Status != dispatch.JobStatusCompleted {
		t.Fatalf("expected completed status in queue, got %+v", updated)
	}
}

func TestAgentJobsHandler_ClaimEmpty(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	handler := NewAgentJobsHandler(q)

	claimBody, _ := json.Marshal(ClaimJobRequest{
		HostID: "host-alpha",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	w := httptest.NewRecorder()
	handler.ClaimHandler(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content when queue empty, got %d", w.Code)
	}
}
