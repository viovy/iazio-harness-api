package dispatch

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRunComparisonService_CompareRuns_AllFourBuckets(t *testing.T) {
	svc := NewRunComparisonService()

	baseline := RunTelemetrySnapshot{
		RunID:            "job_100#attempt-1",
		JobID:            "job_100",
		StoryID:          "STORY-101",
		AttemptNumber:    1,
		Status:           JobStatusFailed,
		DurationMs:       4200,
		TurnCount:        18,
		TokenCount:       24000,
		TestFailureCount: 2,
		FailedTests:      []string{"TestAuthHandshake", "TestFlakyTimeout"},
		ErrorClusters: []ErrorClusterItem{
			{
				Signature:     "oauth2: token expired on host alpha",
				Level:         "ERROR",
				Service:       "iazio-mcp",
				Category:      "error_cluster",
				Count:         4,
				SampleMessage: "oauth2: token expired on host alpha",
			},
			{
				Signature:     "connection pool exhausted",
				Level:         "ERROR",
				Service:       "iazio-harness-api",
				Category:      "error_cluster",
				Count:         2,
				SampleMessage: "connection pool exhausted",
			},
		},
		SpanLatencies: []SpanLatencyItem{
			{
				SpanName:    "POST /api/v1/jobs",
				ServiceName: "iazio-harness-api",
				DurationMs:  120,
				HasError:    false,
			},
			{
				SpanName:    "GET /api/v1/stories",
				ServiceName: "iazio-harness-api",
				DurationMs:  80,
				HasError:    true,
				ErrorTag:    "http.status_code=502",
			},
		},
	}

	target := RunTelemetrySnapshot{
		RunID:            "job_100#attempt-2",
		JobID:            "job_100",
		StoryID:          "STORY-101",
		AttemptNumber:    2,
		Status:           JobStatusCompleted,
		DurationMs:       3100,
		TurnCount:        12,
		TokenCount:       15000,
		TestFailureCount: 2,
		FailedTests:      []string{"TestFlakyTimeout", "TestNewRegressionCase"},
		ErrorClusters: []ErrorClusterItem{
			{
				Signature:     "connection pool exhausted",
				Level:         "ERROR",
				Service:       "iazio-harness-api",
				Category:      "error_cluster",
				Count:         1,
				SampleMessage: "connection pool exhausted",
			},
			{
				Signature:     "nil pointer dereference in cache handler",
				Level:         "ERROR",
				Service:       "iazio-mcp",
				Category:      "error_cluster",
				Count:         3,
				SampleMessage: "nil pointer dereference in cache handler",
			},
		},
		SpanLatencies: []SpanLatencyItem{
			{
				SpanName:    "POST /api/v1/jobs",
				ServiceName: "iazio-harness-api",
				DurationMs:  260, // +140ms (+116.67%) -> regressed
				HasError:    false,
			},
			{
				SpanName:    "GET /api/v1/stories",
				ServiceName: "iazio-harness-api",
				DurationMs:  45, // resolved error tag & faster
				HasError:    false,
			},
		},
	}

	diff := svc.CompareRuns(baseline, target)
	if diff == nil {
		t.Fatal("expected non-nil RunComparisonDiff")
	}

	if diff.ComparisonMode != "attempt_to_attempt" {
		t.Fatalf("expected comparison_mode=attempt_to_attempt, got %q", diff.ComparisonMode)
	}
	if diff.Verdict != VerdictRegressionIntroduced {
		t.Fatalf("expected verdict=%s when new regressions exist, got %q", VerdictRegressionIntroduced, diff.Verdict)
	}
	if diff.RemediationVerified {
		t.Fatal("expected remediation_verified=false when new regressions exist")
	}

	// Verify Bucket 1: RESOLVED_ERRORS (oauth2 error + span error tag + TestAuthHandshake)
	if len(diff.ResolvedErrors) != 3 {
		t.Fatalf("expected 3 RESOLVED_ERRORS, got %d: %+v", len(diff.ResolvedErrors), diff.ResolvedErrors)
	}
	if len(diff.Buckets.ResolvedErrors) != 3 {
		t.Fatalf("expected buckets.RESOLVED_ERRORS length 3, got %d", len(diff.Buckets.ResolvedErrors))
	}

	// Verify Bucket 2: NEW_REGRESSIONS (nil pointer error + TestNewRegressionCase)
	if len(diff.NewRegressions) != 2 {
		t.Fatalf("expected 2 NEW_REGRESSIONS, got %d: %+v", len(diff.NewRegressions), diff.NewRegressions)
	}

	// Verify Bucket 3: PERSISTENT_ERRORS (connection pool exhausted + TestFlakyTimeout)
	if len(diff.PersistentErrors) != 2 {
		t.Fatalf("expected 2 PERSISTENT_ERRORS, got %d: %+v", len(diff.PersistentErrors), diff.PersistentErrors)
	}

	// Verify Bucket 4: LATENCY_DELTAS (2 spans + job.total_duration)
	if len(diff.LatencyDeltas) != 3 {
		t.Fatalf("expected 3 LATENCY_DELTAS, got %d: %+v", len(diff.LatencyDeltas), diff.LatencyDeltas)
	}
	foundRegressedSpan := false
	for _, ld := range diff.LatencyDeltas {
		if ld.SpanName == "POST /api/v1/jobs" {
			foundRegressedSpan = ld.Regressed
			if ld.DeltaMs != 140 {
				t.Fatalf("expected POST /api/v1/jobs DeltaMs=140, got %v", ld.DeltaMs)
			}
		}
	}
	if !foundRegressedSpan {
		t.Fatal("expected POST /api/v1/jobs span to be flagged as regressed")
	}

	// Verify Phase 4 review context contains all 4 canonical bucket labels
	for _, token := range []string{
		BucketResolvedErrors,
		BucketNewRegressions,
		BucketPersistentErrors,
		BucketLatencyDeltas,
		"RunComparisonService",
	} {
		if !strings.Contains(diff.Phase4ReviewContext, token) {
			t.Errorf("expected Phase4ReviewContext to contain %q, got:\n%s", token, diff.Phase4ReviewContext)
		}
	}
}

func TestRunComparisonService_RemediationVerifiedVerdict(t *testing.T) {
	svc := NewRunComparisonService()
	base := RunTelemetrySnapshot{
		RunID:            "job_a",
		StoryID:          "STORY-102",
		TestFailureCount: 3,
		FailedTests:      []string{"TestA", "TestB", "TestC"},
		ErrorClusters: []ErrorClusterItem{
			{Signature: "database deadlock detected", Count: 2},
		},
		SpanLatencies: []SpanLatencyItem{
			{SpanName: "query_db", DurationMs: 250, HasError: true, ErrorTag: "deadlock"},
		},
	}
	curr := RunTelemetrySnapshot{
		RunID:            "job_b",
		StoryID:          "STORY-102",
		TestFailureCount: 0,
		SpanLatencies: []SpanLatencyItem{
			{SpanName: "query_db", DurationMs: 95, HasError: false},
		},
	}

	diff := svc.CompareRuns(base, curr)
	if diff.Verdict != VerdictRemediationVerified || !diff.RemediationVerified {
		t.Fatalf("expected REMEDIATION_VERIFIED (true), got verdict=%s verified=%t", diff.Verdict, diff.RemediationVerified)
	}
	if len(diff.NewRegressions) != 0 {
		t.Fatalf("expected 0 regressions, got %d", len(diff.NewRegressions))
	}
	if len(diff.ResolvedErrors) != 5 {
		t.Fatalf("expected 5 resolved errors (3 tests + 1 cluster + 1 span error), got %d", len(diff.ResolvedErrors))
	}
	if diff.TestFailureDelta.Delta != -3 {
		t.Fatalf("expected test failure delta -3, got %d", diff.TestFailureDelta.Delta)
	}
}

func TestCompareJobWithBaseline_AttemptHistoryAndResultJSON(t *testing.T) {
	t0 := time.Now().Add(-5 * time.Minute).UTC()
	t1 := t0.Add(45 * time.Second)
	t2 := t1.Add(30 * time.Second)

	resBytes, _ := json.Marshal(map[string]any{
		"test_failure_count": 0,
		"test_pass_count":    14,
		"token_count":        9500,
		"turn_count":         7,
		"span_latencies": []SpanLatencyItem{
			{SpanName: "VerifyStory", ServiceName: "harness", DurationMs: 110},
		},
	})

	job := &Job{
		ID:             "job_retry_1",
		StoryID:        "STORY-103",
		Status:         JobStatusCompleted,
		Attempts:       2,
		FinalTurnCount: 7,
		ElapsedSeconds: 30,
		ClaimedAt:      &t1,
		CompletedAt:    &t2,
		Result:         resBytes,
		AttemptHistory: []AttemptFailure{
			{
				AttemptNumber:   1,
				StartedAt:       t0.Format(time.RFC3339),
				EndedAt:         t1.Format(time.RFC3339),
				FailureCategory: "TEST_FAILURE",
				Reason:          "--- FAIL: TestPipelineGate (0.12s)\nexit status 1",
				StderrTail:      "--- FAIL: TestPipelineGate (0.12s)\nexit status 1",
			},
		},
	}

	diff := CompareJobWithBaseline(job, nil, nil, nil)
	if diff == nil {
		t.Fatal("expected CompareJobWithBaseline to produce diff from AttemptHistory")
	}
	if !diff.RemediationVerified || diff.Verdict != VerdictRemediationVerified {
		t.Fatalf("expected remediation verified on clean retry, got verdict=%s", diff.Verdict)
	}
	if diff.TestFailureDelta.BaselineFailures != 1 || diff.TestFailureDelta.CurrentFailures != 0 {
		t.Fatalf("unexpected test failure delta: %+v", diff.TestFailureDelta)
	}
}
