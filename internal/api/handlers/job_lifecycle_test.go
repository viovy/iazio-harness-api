package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
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

	// 1b. Non-existent story STORY-ROOT-12288 -> 404 Not Found with descriptive error message
	bodyRoot12288 := bytes.NewReader([]byte(`{"submodule":".","story_id":"STORY-ROOT-12288"}`))
	reqRoot12288 := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-ROOT-12288/execute", bodyRoot12288)
	rrRoot12288 := httptest.NewRecorder()
	mux.ServeHTTP(rrRoot12288, reqRoot12288)
	if rrRoot12288.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for STORY-ROOT-12288, got %d: %s", rrRoot12288.Code, rrRoot12288.Body.String())
	}
	if !strings.Contains(rrRoot12288.Body.String(), "STORY-ROOT-12288") {
		t.Fatalf("expected error body to mention STORY-ROOT-12288, got: %s", rrRoot12288.Body.String())
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

type mockHistoryFinder struct {
	row   control.HistoryRow
	host  string
	path  string
	found bool
}

func (m *mockHistoryFinder) FindHistoryRow(jobID string) (control.HistoryRow, string, string, bool) {
	if m.found && m.row.JobID == jobID {
		return m.row, m.host, m.path, true
	}
	return control.HistoryRow{}, "", "", false
}

func TestJobLifecycleHandler_GetJob_HistoryFallback(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)
	h.SetScheduler(nil)

	now := time.Now().UTC()
	finder := &mockHistoryFinder{
		row: control.HistoryRow{
			JobID:        "job-hist-123",
			Engine:       "autopilot",
			Status:       "completed",
			ErrorMessage: "",
			CreatedAt:    now,
		},
		host:  "debian",
		path:  "/work/app-repo",
		found: true,
	}
	h.SetHistoryFinder(finder)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.GetJob)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-hist-123", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var detail JobDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if detail.ID != "job-hist-123" || detail.TargetHost != "debian" || detail.Repo != "/work/app-repo" {
		t.Fatalf("unexpected detail: %+v", detail)
	}
}

func TestJobLifecycleHandler_ExecuteStory_HarnessAllocated(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/stories/execute", h.ExecuteStory)
	mux.HandleFunc("PATCH /api/v1/jobs/{id}/story-binding", h.BindStory)

	// Minimal Gemini Web payload without story_id or agent
	body := bytes.NewReader([]byte(`{"request":"Refactor auth middleware to zero trust","source_conversation_title":"Fleet Codebase Architecture Review"}`))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories/execute", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rr.Code, rr.Body.String())
	}

	var execResp ExecuteStoryResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &execResp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if execResp.StoryID != "" {
		t.Errorf("expected empty story_id, got %q", execResp.StoryID)
	}
	if execResp.StoryAllocationStatus != "pending_harness_creation" {
		t.Errorf("expected pending_harness_creation, got %q", execResp.StoryAllocationStatus)
	}
	if execResp.EffectiveAction != "/autonomous-story-execution" {
		t.Errorf("expected default action /autonomous-story-execution, got %q", execResp.EffectiveAction)
	}
	if !strings.HasPrefix(execResp.NormalizedPrompt, "/autonomous-story-execution Refactor auth middleware") {
		t.Errorf("expected normalized prompt with prefix, got %q", execResp.NormalizedPrompt)
	}

	// Now simulate harness binding story ID
	bindBody := bytes.NewReader([]byte(`{"story_id":"STORY-GOV-3688","story_title":"Gemini Web MCP Frictionless ASE","story_path":"docs/stories/2026-10-25-gov-3688.story.md"}`))
	bindReq := httptest.NewRequest(http.MethodPatch, "/api/v1/jobs/"+execResp.JobID+"/story-binding", bindBody)
	bindRr := httptest.NewRecorder()
	mux.ServeHTTP(bindRr, bindReq)

	if bindRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for bind, got %d: %s", bindRr.Code, bindRr.Body.String())
	}

	var detail JobDetailResponse
	if err := json.Unmarshal(bindRr.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to unmarshal bind response: %v", err)
	}
	if detail.StoryID != "STORY-GOV-3688" {
		t.Errorf("expected story_id STORY-GOV-3688, got %q", detail.StoryID)
	}
	if detail.StoryAllocationStatus != "allocated" {
		t.Errorf("expected story_allocation_status allocated, got %q", detail.StoryAllocationStatus)
	}
	if detail.StoryTitle != "Gemini Web MCP Frictionless ASE" {
		t.Errorf("expected story_title, got %q", detail.StoryTitle)
	}

	// Claim the job to give it a non-zero fencing token
	claimed, claimErr := q.Claim(context.Background(), "test-host", []string{dispatch.JobTypePromptExecute})
	if claimErr != nil || claimed == nil {
		t.Fatalf("failed to claim job: %v", claimErr)
	}

	// Stale fencing token in BindStory is rejected with 409 Conflict
	staleBindBody := bytes.NewReader([]byte(fmt.Sprintf(`{"fencing_token":%d,"story_id":"STORY-OVERWRITE"}`, claimed.FencingToken+999)))
	staleBindReq := httptest.NewRequest(http.MethodPatch, "/api/v1/jobs/"+claimed.ID+"/story-binding", staleBindBody)
	staleBindRr := httptest.NewRecorder()
	mux.ServeHTTP(staleBindRr, staleBindReq)
	if staleBindRr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for stale fencing token, got %d: %s", staleBindRr.Code, staleBindRr.Body.String())
	}

	// Valid fencing token in BindStory succeeds with 200 OK
	validBindBody := bytes.NewReader([]byte(fmt.Sprintf(`{"fencing_token":%d,"story_id":"STORY-VALID-BOUND"}`, claimed.FencingToken)))
	validBindReq := httptest.NewRequest(http.MethodPatch, "/api/v1/jobs/"+claimed.ID+"/story-binding", validBindBody)
	validBindRr := httptest.NewRecorder()
	mux.ServeHTTP(validBindRr, validBindReq)
	if validBindRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for valid fencing token, got %d: %s", validBindRr.Code, validBindRr.Body.String())
	}

	// Zero/omitted fencing token on claimed job is rejected with 409 Conflict (no bypass)
	bypassBindBody := bytes.NewReader([]byte(`{"fencing_token":0,"story_id":"STORY-BYPASS"}`))
	bypassBindReq := httptest.NewRequest(http.MethodPatch, "/api/v1/jobs/"+claimed.ID+"/story-binding", bypassBindBody)
	bypassBindRr := httptest.NewRecorder()
	mux.ServeHTTP(bypassBindRr, bypassBindReq)
	if bypassBindRr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for omitted/zero fencing token on claimed job, got %d: %s", bypassBindRr.Code, bypassBindRr.Body.String())
	}
}

func TestJobLifecycleHandler_AckLease_Heartbeat_Failover(t *testing.T) {
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
	mux.HandleFunc("POST /api/v1/jobs/{id}/ack-lease", h.AckLease)
	mux.HandleFunc("POST /api/v1/jobs/{id}/heartbeat", h.Heartbeat)
	mux.HandleFunc("POST /api/v1/jobs/{id}/failover", h.Failover)
	mux.HandleFunc("POST /api/v1/global-queue/jobs/retry-retriable", h.RetryRetriable)

	// Claim the job into leased status
	claimed, err := q.Claim(context.Background(), "agent-1", []string{dispatch.JobTypePromptExecute})
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// 1. AckLease
	ackBody := bytes.NewReader([]byte(fmt.Sprintf(`{"lease_token":%q,"agent_pid":1234,"active_conversation_id":"conv-1"}`, claimed.LeaseToken)))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/ack-lease", ackBody)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for AckLease, got %d: %s", rr.Code, rr.Body.String())
	}

	// 2. Heartbeat
	hbBody := bytes.NewReader([]byte(fmt.Sprintf(`{"lease_token":%q,"fencing_token":%d}`, claimed.LeaseToken, claimed.FencingToken)))
	hbReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/heartbeat", hbBody)
	hbRr := httptest.NewRecorder()
	mux.ServeHTTP(hbRr, hbReq)

	if hbRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for Heartbeat, got %d: %s", hbRr.Code, hbRr.Body.String())
	}

	// 3. Failover
	failBody := bytes.NewReader([]byte(`{"host":"pc1","tool":"agy","workspace":"/tmp/ws","failure_category":"rate_limit_exceeded","exit_code":429,"reason":"quota exhausted"}`))
	failReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/failover", failBody)
	failRr := httptest.NewRecorder()
	mux.ServeHTTP(failRr, failReq)

	if failRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for Failover, got %d: %s", failRr.Code, failRr.Body.String())
	}

	// 4. RetryRetriable
	retryBody := bytes.NewReader([]byte(`{}`))
	retryReq := httptest.NewRequest(http.MethodPost, "/api/v1/global-queue/jobs/retry-retriable", retryBody)
	retryRr := httptest.NewRecorder()
	mux.ServeHTTP(retryRr, retryReq)

	if retryRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for RetryRetriable, got %d: %s", retryRr.Code, retryRr.Body.String())
	}
}

func TestJobLifecycleHandler_ForceDispatch_Cancel_Retry(t *testing.T) {
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
	mux.HandleFunc("POST /api/v1/jobs/{id}/force-dispatch", h.ForceDispatchJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", h.CancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", h.RetryJob)

	// ForceDispatch
	fdReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/force-dispatch", nil)
	fdRr := httptest.NewRecorder()
	mux.ServeHTTP(fdRr, fdReq)
	if fdRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for ForceDispatch, got %d: %s", fdRr.Code, fdRr.Body.String())
	}

	// Cancel
	cancelBody := bytes.NewReader([]byte(`{"reason":"user test"}`))
	cReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/cancel", cancelBody)
	cRr := httptest.NewRecorder()
	mux.ServeHTTP(cRr, cReq)
	if cRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for Cancel, got %d: %s", cRr.Code, cRr.Body.String())
	}

	// Retry
	rReq := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+job.ID+"/retry", nil)
	rRr := httptest.NewRecorder()
	mux.ServeHTTP(rRr, rReq)
	if rRr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for Retry, got %d: %s", rRr.Code, rRr.Body.String())
	}
}

func TestJobLifecycleHandler_GetJob_ClaimedHostAndFailureDetails(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        "iazio-mcp",
		TargetHost:  "",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	claimed, err := q.Claim(context.Background(), "mac-mini", []string{dispatch.JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim: %v", err)
	}
	claimed.FailureCategory = "git_conflict"
	claimed.FailureStage = "git_push"
	claimed.FailureReasonSummary = "non-fast-forward"
	claimed.Retriable = true
	if err := q.Complete(context.Background(), claimed.ID, nil, "git push error"); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.GetJob)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.ID, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var detail JobDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if detail.ClaimedHost != "mac-mini" || detail.TargetHost != "mac-mini" {
		t.Fatalf("expected ClaimedHost and fallback TargetHost to be mac-mini, got ClaimedHost=%q TargetHost=%q", detail.ClaimedHost, detail.TargetHost)
	}
	if !strings.Contains(detail.LiveURL, "/agents/mac-mini/iazio-mcp/live?job="+job.ID) {
		t.Fatalf("expected LiveURL to use claimed host mac-mini, got %q", detail.LiveURL)
	}
	if detail.FailureCategory != "git_conflict" || detail.FailureStage != "git_push" || !detail.Retriable {
		t.Fatalf("expected failure classification fields in JobDetailResponse, got %+v", detail)
	}
}

func TestStoriesHandler_AsyncCreatePopulatesJobMetadata(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	sh := NewStoriesHandler(store, q, nil)

	body := bytes.NewReader([]byte(`{
		"submodule": "iazio-mcp",
		"title": "Harden Story Metadata on Enqueue",
		"priority": "P0",
		"problem": "Missing StoryTitle and StoryAllocationStatus on enqueued job",
		"acceptance_criteria": ["StoryTitle populated", "StoryAllocationStatus populated"],
		"target_files": ["internal/api/handlers/stories.go"]
	}`))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", body)
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusAccepted {
		t.Fatalf("expected 201 or 202, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if resp.HarnessJobID == "" {
		t.Fatal("expected non-empty HarnessJobID")
	}

	enqJob, ok := q.Get(resp.HarnessJobID)
	if !ok || enqJob == nil {
		t.Fatalf("expected job %s in queue", resp.HarnessJobID)
	}
	if enqJob.StoryTitle != "Harden Story Metadata on Enqueue" {
		t.Fatalf("expected StoryTitle 'Harden Story Metadata on Enqueue', got %q", enqJob.StoryTitle)
	}
	if enqJob.StoryAllocationStatus != "pending_harness_creation" {
		t.Fatalf("expected StoryAllocationStatus 'pending_harness_creation', got %q", enqJob.StoryAllocationStatus)
	}
	if enqJob.EffectiveAction != "story.create_and_execute" {
		t.Fatalf("expected EffectiveAction 'story.create_and_execute', got %q", enqJob.EffectiveAction)
	}
}

func TestV4_StoryLifecycleTransitionsAndListQueueTotalAndFinalHarvest(t *testing.T) {
	ctx := context.Background()
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	tmpDir := t.TempDir()

	_ = engine.SaveStory(control.Story{
		ID:        "STORY-GOV-3806",
		Title:     "Enterprise V4 Remediation",
		Submodule: "iazio-harness-api",
		Priority:  "P0",
		Status:    "READY",
		SyncState: "PENDING_WORKSPACE_MATERIALIZATION",
	})

	jl := NewJobLifecycleHandler(q)
	jl.SetEngine(engine)
	jl.SetRepoRoot(tmpDir)

	aj := NewAgentJobsHandler(q)
	aj.SetEngine(engine)
	aj.SetRepoRoot(tmpDir)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/stories/{id}/execute", jl.ExecuteStory)
	mux.HandleFunc("GET /api/v1/queue", jl.ListQueue)
	mux.HandleFunc("GET /api/v1/jobs/{id}", jl.GetJob)
	mux.HandleFunc("POST /api/v1/agent/jobs/claim", aj.ClaimHandler)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/heartbeat", aj.HeartbeatHandler)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/complete", aj.CompleteHandler)

	// 1. ExecuteStory resolves StoryTitle, StoryPath, Submodule from Engine and transitions story to IN_PROGRESS
	execReq := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-GOV-3806/execute", bytes.NewBufferString(`{"target_agent":"agy"}`))
	execRr := httptest.NewRecorder()
	mux.ServeHTTP(execRr, execReq)
	if execRr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created from ExecuteStory, got %d: %s", execRr.Code, execRr.Body.String())
	}
	var execResp ExecuteStoryResponse
	_ = json.Unmarshal(execRr.Body.Bytes(), &execResp)
	if execResp.Submodule != "iazio-harness-api" {
		t.Fatalf("expected ExecuteStory to resolve Submodule=iazio-harness-api from story, got %q", execResp.Submodule)
	}
	enqJob, _ := q.Get(execResp.JobID)
	if enqJob.StoryTitle != "Enterprise V4 Remediation" || enqJob.StoryPath == "" {
		t.Fatalf("expected StoryTitle and StoryPath populated on job, got title=%q path=%q", enqJob.StoryTitle, enqJob.StoryPath)
	}
	stAfterExec, _ := engine.GetStory("STORY-GOV-3806")
	if stAfterExec.Status != "IN_PROGRESS" {
		t.Fatalf("expected story status IN_PROGRESS after ExecuteStory, got %q", stAfterExec.Status)
	}

	// 2. Claim job via AgentJobsHandler -> status leased
	claimReq := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewBufferString(`{"host_id":"host-1"}`))
	claimRr := httptest.NewRecorder()
	mux.ServeHTTP(claimRr, claimReq)
	if claimRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from ClaimHandler, got %d", claimRr.Code)
	}
	var claimedJob dispatch.Job
	_ = json.Unmarshal(claimRr.Body.Bytes(), &claimedJob)

	// Enqueue two more jobs to test ListQueue pre-limit Total
	_, _ = q.Enqueue(ctx, &dispatch.Job{Type: dispatch.JobTypePromptExecute, Repo: "iazio-harness-api"})
	_, _ = q.Enqueue(ctx, &dispatch.Job{Type: dispatch.JobTypePromptExecute, Repo: "iazio-harness-api"})
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/queue?status=queued&limit=1", nil)
	listRr := httptest.NewRecorder()
	mux.ServeHTTP(listRr, listReq)
	var listResp ListQueueResponse
	_ = json.Unmarshal(listRr.Body.Bytes(), &listResp)
	if len(listResp.Items) != 1 || listResp.Total != 2 {
		t.Fatalf("expected ListQueue limit=1 to return 1 item with Total=2, got items=%d total=%d", len(listResp.Items), listResp.Total)
	}

	// 3. Complete job via AgentJobsHandler with FinalTurnCount and FinalTranscriptSHA256 -> story transitions to DONE + MATERIALIZED
	compBody, _ := json.Marshal(CompleteJobRequest{
		FencingToken:          claimedJob.FencingToken,
		Result:                json.RawMessage(`{"status":"ok"}`),
		FinalTurnCount:        9,
		FinalTranscriptSHA256: "sha256-v4-verified",
	})
	compReq := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/"+claimedJob.ID+"/complete", bytes.NewReader(compBody))
	compRr := httptest.NewRecorder()
	mux.ServeHTTP(compRr, compReq)
	if compRr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from CompleteHandler, got %d: %s", compRr.Code, compRr.Body.String())
	}

	stAfterComp, _ := engine.GetStory("STORY-GOV-3806")
	if stAfterComp.Status != "DONE" || stAfterComp.SyncState != "MATERIALIZED" {
		t.Fatalf("expected story status DONE and SyncState MATERIALIZED after CompleteHandler, got status=%q sync=%q",
			stAfterComp.Status, stAfterComp.SyncState)
	}

	// 4. GetJob returns FinalTurnCount and FinalTranscriptSHA256
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+claimedJob.ID, nil)
	getRr := httptest.NewRecorder()
	mux.ServeHTTP(getRr, getReq)
	var jobDetail JobDetailResponse
	_ = json.Unmarshal(getRr.Body.Bytes(), &jobDetail)
	if jobDetail.FinalTurnCount != 9 || jobDetail.FinalTranscriptSHA256 != "sha256-v4-verified" {
		t.Fatalf("expected FinalTurnCount=9 and FinalTranscriptSHA256=sha256-v4-verified in JobDetailResponse, got turns=%d sha=%q",
			jobDetail.FinalTurnCount, jobDetail.FinalTranscriptSHA256)
	}
}

func TestExecuteStory_ThreeModes_And_ResolveBacklogStoriesRoot(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	tmpDir := t.TempDir()

	_ = engine.SaveStory(control.Story{
		ID:        "STORY-GOV-3900",
		Title:     "Three Mode Story Dispatch",
		Submodule: "iazio-web",
		Priority:  "P1",
		Status:    "READY",
	})

	jl := NewJobLifecycleHandler(q)
	jl.SetEngine(engine)
	jl.SetRepoRoot(tmpDir)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/stories/execute", jl.ExecuteStory)
	mux.HandleFunc("POST /api/v1/stories/{id}/execute", jl.ExecuteStory)

	// 1. Mode 1: story.create_and_execute ignores any passed story_id and concatenates /autonomous-story-execution + prompt
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/stories/execute", bytes.NewBufferString(`{
		"mode": "story.create_and_execute",
		"story_id": "STORY-FAKE-999",
		"backlog": "iazio",
		"submodule": "iazio-web",
		"prompt": "Sort prompts alphabetically"
	}`))
	rr1 := httptest.NewRecorder()
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for story.create_and_execute, got %d: %s", rr1.Code, rr1.Body.String())
	}
	var resp1 ExecuteStoryResponse
	_ = json.Unmarshal(rr1.Body.Bytes(), &resp1)
	if resp1.StoryID != "" {
		t.Fatalf("expected empty StoryID for story.create_and_execute, got %q", resp1.StoryID)
	}
	if resp1.Mode != "story.create_and_execute" || resp1.EffectiveAction != "/autonomous-story-execution" {
		t.Fatalf("unexpected mode/action: mode=%q action=%q", resp1.Mode, resp1.EffectiveAction)
	}
	if !strings.HasPrefix(resp1.NormalizedPrompt, "/autonomous-story-execution") || !strings.Contains(resp1.NormalizedPrompt, "Sort prompts alphabetically") {
		t.Fatalf("unexpected NormalizedPrompt: %q", resp1.NormalizedPrompt)
	}

	// 2. Mode 2: story.execute without story_id returns 422
	req2Err := httptest.NewRequest(http.MethodPost, "/api/v1/stories/execute", bytes.NewBufferString(`{
		"mode": "story.execute",
		"prompt": "Missing story id"
	}`))
	rr2Err := httptest.NewRecorder()
	mux.ServeHTTP(rr2Err, req2Err)
	if rr2Err.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for story.execute without story_id, got %d", rr2Err.Code)
	}

	// Mode 2 with story_id concatenates /autonomous-story-execution + story_id + extra prompt
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-GOV-3900/execute", bytes.NewBufferString(`{
		"mode": "story.execute",
		"prompt": "run TDD"
	}`))
	rr2 := httptest.NewRecorder()
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for story.execute, got %d: %s", rr2.Code, rr2.Body.String())
	}
	var resp2 ExecuteStoryResponse
	_ = json.Unmarshal(rr2.Body.Bytes(), &resp2)
	if resp2.NormalizedPrompt != "/autonomous-story-execution STORY-GOV-3900 run TDD" {
		t.Fatalf("expected '/autonomous-story-execution STORY-GOV-3900 run TDD', got %q", resp2.NormalizedPrompt)
	}

	// 3. Mode 3: story.create uses /story-create and clears story_id
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/stories/execute", bytes.NewBufferString(`{
		"mode": "story.create",
		"backlog": "autopilot",
		"submodule": "autopilot-plugins",
		"prompt": "Create story for backlog family hub"
	}`))
	rr3 := httptest.NewRecorder()
	mux.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for story.create, got %d: %s", rr3.Code, rr3.Body.String())
	}
	var resp3 ExecuteStoryResponse
	_ = json.Unmarshal(rr3.Body.Bytes(), &resp3)
	if resp3.StoryID != "" || resp3.EffectiveAction != "/story-create" {
		t.Fatalf("expected empty StoryID and /story-create for story.create, got id=%q action=%q", resp3.StoryID, resp3.EffectiveAction)
	}

	// 4. Verify resolveBacklogStoriesRoot across family hubs
	autopilotHub := filepath.Join(tmpDir, "autopilot-umbrella", "docs", "stories")
	if err := os.MkdirAll(autopilotHub, 0o755); err != nil {
		t.Fatalf("mkdir autopilotHub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(autopilotHub, "index.yml"), []byte("stories: []\n"), 0o600); err != nil {
		t.Fatalf("write index.yml: %v", err)
	}
	if got := resolveBacklogStoriesRoot(tmpDir, "autopilot", ""); got != filepath.Join(tmpDir, "autopilot-umbrella") {
		t.Fatalf("expected autopilot-umbrella root, got %q", got)
	}
	for _, b := range []string{"", "root", "iazio", "vigo", "eye-habit", "fin-hub", "custom-sub"} {
		_ = resolveBacklogStoriesRoot(tmpDir, b, "")
	}
}

