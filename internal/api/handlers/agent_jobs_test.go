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

func TestAgentJobsHandler_HeartbeatRehydratesInFlightJobOnAPIRestart(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	handler := NewAgentJobsHandler(q)

	hbBody, _ := json.Marshal(HeartbeatJobRequest{
		FencingToken:         7,
		StoryID:              "STORY-GOV-5145",
		WorktreePath:         "/home/romeo/work/meta-repo",
		AgentPID:             4242,
		ToolPID:              4243,
		ActiveConversationID: "29c18bab-aa8a-4d78-8426-4808d832582a",
		ElapsedSeconds:       36000,
	})
	reqHb := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/job_multiday_in_flight/heartbeat", bytes.NewReader(hbBody))
	wHb := httptest.NewRecorder()
	handler.HeartbeatHandler(wHb, reqHb)
	if wHb.Code != http.StatusOK {
		t.Fatalf("expected 200 OK rehydrating in-flight job on heartbeat, got %d: %s", wHb.Code, wHb.Body.String())
	}
	rehydrated, ok := q.Get("job_multiday_in_flight")
	if !ok || rehydrated == nil {
		t.Fatalf("expected job_multiday_in_flight to be rehydrated in queue")
	}
	if rehydrated.Status != dispatch.JobStatusRunning {
		t.Fatalf("expected rehydrated job status=running, got %s", rehydrated.Status)
	}
	if rehydrated.InitialConversationID != "29c18bab-aa8a-4d78-8426-4808d832582a" || rehydrated.ResumeConversationID != "29c18bab-aa8a-4d78-8426-4808d832582a" {
		t.Fatalf("expected pinned conversation ID on rehydrated job, got initial=%q resume=%q", rehydrated.InitialConversationID, rehydrated.ResumeConversationID)
	}
}

func TestStory0190_RealTimeAndCompletedConversationIDAndURL(t *testing.T) {
	t.Setenv("IAZIO_WEB_BASE_URL", "")
	t.Setenv("PUBLIC_WEB_URL", "")

	q := dispatch.NewInMemoryQueue()
	agentHandler := NewAgentJobsHandler(q)
	lifecycleHandler := NewJobLifecycleHandler(q)

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:       dispatch.JobTypePromptExecute,
		Repo:       "iazio-mcp",
		TargetHost: "tian",
		Payload:    json.RawMessage(`{"title":"Execute story"}`),
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Claim job
	claimBody, _ := json.Marshal(ClaimJobRequest{HostID: "tian"})
	reqClaim := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	wClaim := httptest.NewRecorder()
	agentHandler.ClaimHandler(wClaim, reqClaim)
	if wClaim.Code != http.StatusOK {
		t.Fatalf("claim failed: %d", wClaim.Code)
	}
	var claimed dispatch.Job
	_ = json.NewDecoder(wClaim.Body).Decode(&claimed)

	// Send real-time heartbeat with conversation_id while job is RUNNING
	hbBody, _ := json.Marshal(HeartbeatJobRequest{
		FencingToken:   claimed.FencingToken,
		StoryID:        "STORY-IAZIO-0190",
		ConversationID: "aff9bb70-6c84-4121-ba1e-68745328700a",
	})
	reqHb := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+job.ID+"/heartbeat", bytes.NewReader(hbBody))
	wHb := httptest.NewRecorder()
	agentHandler.HeartbeatHandler(wHb, reqHb)
	if wHb.Code != http.StatusOK {
		t.Fatalf("heartbeat failed: %d: %s", wHb.Code, wHb.Body.String())
	}

	// Query GetJob while job is still RUNNING
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID, nil)
	reqGet.SetPathValue("id", job.ID)
	wGet := httptest.NewRecorder()
	lifecycleHandler.GetJob(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("GetJob failed: %d", wGet.Code)
	}
	var detail JobDetailResponse
	if err := json.NewDecoder(wGet.Body).Decode(&detail); err != nil {
		t.Fatalf("decode GetJob: %v", err)
	}
	if detail.Status != dispatch.JobStatusRunning {
		t.Fatalf("expected status=running, got %s", detail.Status)
	}
	if detail.ConversationID != "aff9bb70-6c84-4121-ba1e-68745328700a" || detail.ActiveConversationID != "aff9bb70-6c84-4121-ba1e-68745328700a" {
		t.Fatalf("expected real-time conversation_id aff9bb70-6c84-4121-ba1e-68745328700a, got conv=%q active=%q", detail.ConversationID, detail.ActiveConversationID)
	}
	if detail.ConversationURL != "https://tian.go.ro/iazio-web/conversation/aff9bb70-6c84-4121-ba1e-68745328700a" {
		t.Fatalf("expected conversation_url https://tian.go.ro/iazio-web/conversation/aff9bb70-6c84-4121-ba1e-68745328700a, got %q", detail.ConversationURL)
	}
	if detail.LiveURL != "https://tian.go.ro/iazio-web/agents/tian/iazio-mcp/live?job="+job.ID {
		t.Fatalf("expected live_url to use https://tian.go.ro/iazio-web, got %q", detail.LiveURL)
	}

	// Also verify a completed job whose conversation_id is only in Result JSON
	job2, _ := q.Enqueue(context.Background(), &dispatch.Job{
		Type:       dispatch.JobTypePromptExecute,
		Repo:       "iazio-web",
		TargetHost: "tian",
	})
	claimed2, _ := q.Claim(context.Background(), "tian", nil)
	_ = q.CompleteWithFencing(context.Background(), job2.ID, claimed2.FencingToken, json.RawMessage(`{"conversation_id":"bbbb2222-3333-4444-5555-666677778888"}`), "")

	reqQueue := httptest.NewRequest(http.MethodGet, "/api/v1/queue", nil)
	wQueue := httptest.NewRecorder()
	lifecycleHandler.ListQueue(wQueue, reqQueue)
	if wQueue.Code != http.StatusOK {
		t.Fatalf("ListQueue failed: %d", wQueue.Code)
	}
	var listResp ListQueueResponse
	_ = json.NewDecoder(wQueue.Body).Decode(&listResp)
	foundCompleted := false
	for _, item := range listResp.Items {
		if item.ID == job2.ID {
			foundCompleted = true
			if item.ConversationID != "bbbb2222-3333-4444-5555-666677778888" {
				t.Fatalf("expected extracted conversation_id on completed job, got %q", item.ConversationID)
			}
			if item.ConversationURL != "https://tian.go.ro/iazio-web/conversation/bbbb2222-3333-4444-5555-666677778888" {
				t.Fatalf("expected computed conversation_url on completed job, got %q", item.ConversationURL)
			}
		}
	}
	if !foundCompleted {
		t.Fatalf("completed job %s not found in ListQueue response", job2.ID)
	}
}
