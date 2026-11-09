package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestJobLifecycleHandler_RunComparisonEndpoints(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	h := NewJobLifecycleHandler(q)

	ctx := context.Background()
	baseJob, err := q.Enqueue(ctx, &dispatch.Job{
		Type:        dispatch.JobTypeStoryExecute,
		Repo:        "iazio-harness-api",
		StoryID:     "STORY-105",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue base job: %v", err)
	}
	_, _ = q.Claim(ctx, "mac-mini", []string{"agy"})
	_ = q.Complete(ctx, baseJob.ID, json.RawMessage(`{"test_failure_count":2,"failed_tests":["TestAlpha","TestBeta"],"span_latencies":[{"span_name":"run_tests","duration_ms":400,"has_error":true,"error_tag":"exit_code=1"}]}`), "unit tests failed")

	time.Sleep(10 * time.Millisecond)
	targetJob, err := q.Enqueue(ctx, &dispatch.Job{
		Type:        dispatch.JobTypeStoryExecute,
		Repo:        "iazio-harness-api",
		StoryID:     "STORY-105",
		TargetAgent: "agy",
		ParentID:    baseJob.ID,
	})
	if err != nil {
		t.Fatalf("enqueue target job: %v", err)
	}
	_, _ = q.Claim(ctx, "mac-mini", []string{"agy"})
	_ = q.Complete(ctx, targetJob.ID, json.RawMessage(`{"test_failure_count":0,"test_pass_count":12,"span_latencies":[{"span_name":"run_tests","duration_ms":210,"has_error":false}]}`), "")

	// 1. Verify GET /api/v1/jobs/{id} surfaces run_comparison and phase4_review_context
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+targetJob.ID, nil)
	reqGet.SetPathValue("id", targetJob.ID)
	recGet := httptest.NewRecorder()
	h.GetJob(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GetJob, got %d: %s", recGet.Code, recGet.Body.String())
	}
	var detail JobDetailResponse
	if err := json.Unmarshal(recGet.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal JobDetailResponse: %v", err)
	}
	if detail.RunComparison == nil {
		t.Fatal("expected JobDetailResponse.RunComparison to be populated")
	}
	if detail.RunComparison.Verdict != dispatch.VerdictRemediationVerified {
		t.Fatalf("expected verdict=%s, got %s", dispatch.VerdictRemediationVerified, detail.RunComparison.Verdict)
	}
	if len(detail.RunComparison.Buckets.ResolvedErrors) == 0 {
		t.Fatal("expected RESOLVED_ERRORS bucket to be non-empty")
	}
	if !strings.Contains(detail.Phase4ReviewContext, "RESOLVED_ERRORS") {
		t.Fatalf("expected Phase4ReviewContext to include RESOLVED_ERRORS, got: %s", detail.Phase4ReviewContext)
	}

	// 2. Verify GET /api/v1/jobs/{id}/comparison
	reqComp := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+targetJob.ID+"/comparison", nil)
	reqComp.SetPathValue("id", targetJob.ID)
	recComp := httptest.NewRecorder()
	h.GetJobComparison(recComp, reqComp)
	if recComp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GetJobComparison, got %d: %s", recComp.Code, recComp.Body.String())
	}
	var compDiff dispatch.RunComparisonDiff
	if err := json.Unmarshal(recComp.Body.Bytes(), &compDiff); err != nil {
		t.Fatalf("unmarshal RunComparisonDiff: %v", err)
	}
	if compDiff.TestFailureDelta.BaselineFailures != 2 || compDiff.TestFailureDelta.CurrentFailures != 0 {
		t.Fatalf("expected 2->0 test failures, got %+v", compDiff.TestFailureDelta)
	}

	// 2b. Verify GET /api/v1/jobs/{id}/comparison with standalone job (pre-job baseline fallback) and error statuses
	standaloneJob, _ := q.Enqueue(ctx, &dispatch.Job{
		Type:        dispatch.JobTypeStoryExecute,
		Repo:        "iazio-harness-api",
		StoryID:     "STORY-999-STANDALONE",
		TargetAgent: "agy",
	})
	reqStandalone := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+standaloneJob.ID+"/comparison", nil)
	reqStandalone.SetPathValue("id", standaloneJob.ID)
	recStandalone := httptest.NewRecorder()
	h.GetJobComparison(recStandalone, reqStandalone)
	if recStandalone.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for standalone job comparison, got %d", recStandalone.Code)
	}

	// Story-matched baseline resolution (same story without ParentID)
	storyFollowupJob, _ := q.Enqueue(ctx, &dispatch.Job{
		Type:        dispatch.JobTypeStoryExecute,
		Repo:        "iazio-harness-api",
		StoryID:     "STORY-105",
		TargetAgent: "agy",
	})
	reqStoryComp := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+storyFollowupJob.ID+"/comparison", nil)
	reqStoryComp.SetPathValue("id", storyFollowupJob.ID)
	recStoryComp := httptest.NewRecorder()
	h.GetJobComparison(recStoryComp, reqStoryComp)
	if recStoryComp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for story-matched comparison, got %d", recStoryComp.Code)
	}

	// Error cases for GetJobComparison
	recWrongMethod := httptest.NewRecorder()
	h.GetJobComparison(recWrongMethod, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/x/comparison", nil))
	if recWrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", recWrongMethod.Code)
	}
	recEmptyID := httptest.NewRecorder()
	h.GetJobComparison(recEmptyID, httptest.NewRequest(http.MethodGet, "/api/v1/jobs//comparison", nil))
	if recEmptyID.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recEmptyID.Code)
	}
	reqNotFound := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent/comparison", nil)
	reqNotFound.SetPathValue("id", "nonexistent")
	recNotFound := httptest.NewRecorder()
	h.GetJobComparison(recNotFound, reqNotFound)
	if recNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recNotFound.Code)
	}

	// 3. Verify POST /api/v1/jobs/compare with synthetic snapshots and job IDs
	postBody, _ := json.Marshal(CompareRunsRequest{
		Baseline: &dispatch.RunTelemetrySnapshot{
			RunID:            "base-snap",
			TestFailureCount: 1,
			ErrorClusters: []dispatch.ErrorClusterItem{
				{Signature: "timeout connecting to redis", Count: 2},
			},
		},
		Current: &dispatch.RunTelemetrySnapshot{
			RunID:            "curr-snap",
			TestFailureCount: 0,
		},
	})
	reqPost := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", bytes.NewReader(postBody))
	recPost := httptest.NewRecorder()
	h.CompareRuns(recPost, reqPost)
	if recPost.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from CompareRuns, got %d: %s", recPost.Code, recPost.Body.String())
	}
	var postDiff dispatch.RunComparisonDiff
	if err := json.Unmarshal(recPost.Body.Bytes(), &postDiff); err != nil {
		t.Fatalf("unmarshal postDiff: %v", err)
	}
	if !postDiff.RemediationVerified || len(postDiff.ResolvedErrors) != 2 {
		t.Fatalf("expected 2 resolved errors and RemediationVerified=true, got %+v", postDiff)
	}

	// CompareRuns by job IDs
	byIDBody, _ := json.Marshal(CompareRunsRequest{
		BaselineJobID: baseJob.ID,
		CurrentJobID:  targetJob.ID,
	})
	recByID := httptest.NewRecorder()
	h.CompareRuns(recByID, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", bytes.NewReader(byIDBody)))
	if recByID.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for CompareRuns by job IDs, got %d", recByID.Code)
	}

	// CompareRuns error cases (405, invalid JSON, missing baseline, missing current, 404 job IDs)
	recPost405 := httptest.NewRecorder()
	h.CompareRuns(recPost405, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/compare", nil))
	if recPost405.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", recPost405.Code)
	}
	recBadJSON := httptest.NewRecorder()
	h.CompareRuns(recBadJSON, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", strings.NewReader("{bad")))
	if recBadJSON.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on bad JSON, got %d", recBadJSON.Code)
	}
	recMissingBase := httptest.NewRecorder()
	h.CompareRuns(recMissingBase, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", strings.NewReader("{}")))
	if recMissingBase.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on missing baseline, got %d", recMissingBase.Code)
	}
	recMissingBaseID := httptest.NewRecorder()
	h.CompareRuns(recMissingBaseID, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", strings.NewReader(`{"baseline_job_id":"missing"}`)))
	if recMissingBaseID.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on missing baseline job, got %d", recMissingBaseID.Code)
	}
	recMissingCurr := httptest.NewRecorder()
	h.CompareRuns(recMissingCurr, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", strings.NewReader(`{"baseline_job_id":"`+baseJob.ID+`"}`)))
	if recMissingCurr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on missing current, got %d", recMissingCurr.Code)
	}
	recMissingCurrID := httptest.NewRecorder()
	h.CompareRuns(recMissingCurrID, httptest.NewRequest(http.MethodPost, "/api/v1/jobs/compare", strings.NewReader(`{"baseline_job_id":"`+baseJob.ID+`","current_job_id":"missing"}`)))
	if recMissingCurrID.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on missing current job, got %d", recMissingCurrID.Code)
	}
}
