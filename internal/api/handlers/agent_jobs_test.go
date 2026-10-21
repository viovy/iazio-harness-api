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

	// 2. Complete job with valid fencing token
	completeBody, _ := json.Marshal(CompleteJobRequest{
		FencingToken: claimed.FencingToken,
		Result:       json.RawMessage(`{"story_id":"STORY-001"}`),
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

func TestAgentJobsHandler_Errors(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	handler := NewAgentJobsHandler(q)

	// Claim MethodNotAllowed
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/jobs/claim", nil)
	w := httptest.NewRecorder()
	handler.ClaimHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// Claim with nil queue
	nilHandler := NewAgentJobsHandler(nil)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", nil)
	w = httptest.NewRecorder()
	nilHandler.ClaimHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}

	// Complete MethodNotAllowed
	req = httptest.NewRequest(http.MethodGet, "/api/v1/agent/jobs/job1/complete", nil)
	w = httptest.NewRecorder()
	handler.CompleteHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// Complete bad path prefix/suffix
	req = httptest.NewRequest(http.MethodPost, "/wrong/path", bytes.NewReader([]byte("{}")))
	w = httptest.NewRecorder()
	handler.CompleteHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// Complete empty job ID
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs//complete", bytes.NewReader([]byte("{}")))
	w = httptest.NewRecorder()
	handler.CompleteHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// Complete malformed json body
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/job1/complete", bytes.NewReader([]byte("invalid-json")))
	w = httptest.NewRecorder()
	handler.CompleteHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// Complete nil queue
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/job1/complete", bytes.NewReader([]byte("{}")))
	w = httptest.NewRecorder()
	nilHandler.CompleteHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}

	// Complete job not found (404)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/job-nonexistent/complete", bytes.NewReader([]byte("{}")))
	w = httptest.NewRecorder()
	handler.CompleteHandler(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestAgentJobsHandler_HeartbeatAndFencing(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	handler := NewAgentJobsHandler(q)

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:       dispatch.JobTypeStoryCreate,
		Repo:       "iazio-mcp",
		TargetHost: "host-alpha",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// 1. Claim job
	claimBody, _ := json.Marshal(ClaimJobRequest{HostID: "host-alpha"})
	reqClaim := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	wClaim := httptest.NewRecorder()
	handler.ClaimHandler(wClaim, reqClaim)
	if wClaim.Code != http.StatusOK {
		t.Fatalf("claim failed: %d", wClaim.Code)
	}

	var claimed dispatch.Job
	_ = json.NewDecoder(wClaim.Body).Decode(&claimed)
	if claimed.FencingToken != 1 {
		t.Fatalf("expected fencing token 1, got %d", claimed.FencingToken)
	}

	// 2. Valid heartbeat
	hbBody, _ := json.Marshal(HeartbeatJobRequest{FencingToken: claimed.FencingToken})
	reqHb := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/heartbeat", bytes.NewReader(hbBody))
	wHb := httptest.NewRecorder()
	handler.HeartbeatHandler(wHb, reqHb)
	if wHb.Code != http.StatusOK {
		t.Fatalf("expected 200 OK heartbeat, got %d: %s", wHb.Code, wHb.Body.String())
	}

	// 3. Stale token heartbeat -> 409 Conflict
	hbStaleBody, _ := json.Marshal(HeartbeatJobRequest{FencingToken: 999})
	reqStaleHb := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/heartbeat", bytes.NewReader(hbStaleBody))
	wStaleHb := httptest.NewRecorder()
	handler.HeartbeatHandler(wStaleHb, reqStaleHb)
	if wStaleHb.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale heartbeat token, got %d", wStaleHb.Code)
	}

	// 4. Stale complete -> 409 Conflict
	compStaleBody, _ := json.Marshal(CompleteJobRequest{
		FencingToken: 999,
		Result:       json.RawMessage(`{"failed":true}`),
	})
	reqStaleComp := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/complete", bytes.NewReader(compStaleBody))
	wStaleComp := httptest.NewRecorder()
	handler.CompleteHandler(wStaleComp, reqStaleComp)
	if wStaleComp.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale complete token, got %d", wStaleComp.Code)
	}

	// 5. Valid complete -> 200 OK
	compValidBody, _ := json.Marshal(CompleteJobRequest{
		FencingToken: claimed.FencingToken,
		Result:       json.RawMessage(`{"story_id":"STORY-OK"}`),
	})
	reqValidComp := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/complete", bytes.NewReader(compValidBody))
	wValidComp := httptest.NewRecorder()
	handler.CompleteHandler(wValidComp, reqValidComp)
	if wValidComp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for valid complete, got %d: %s", wValidComp.Code, wValidComp.Body.String())
	}
}
