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

	// 3. Verify POST /api/v1/jobs/compare with synthetic snapshots
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
}
