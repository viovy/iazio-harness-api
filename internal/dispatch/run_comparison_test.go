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
				DurationMs:  260,
				HasError:    false,
			},
			{
				SpanName:    "GET /api/v1/stories",
				ServiceName: "iazio-harness-api",
				DurationMs:  45,
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

	if len(diff.ResolvedErrors) != 3 {
		t.Fatalf("expected 3 RESOLVED_ERRORS, got %d: %+v", len(diff.ResolvedErrors), diff.ResolvedErrors)
	}
	if len(diff.Buckets.ResolvedErrors) != 3 {
		t.Fatalf("expected buckets.RESOLVED_ERRORS length 3, got %d", len(diff.Buckets.ResolvedErrors))
	}
	if len(diff.NewRegressions) != 2 {
		t.Fatalf("expected 2 NEW_REGRESSIONS, got %d: %+v", len(diff.NewRegressions), diff.NewRegressions)
	}
	if len(diff.PersistentErrors) != 2 {
		t.Fatalf("expected 2 PERSISTENT_ERRORS, got %d: %+v", len(diff.PersistentErrors), diff.PersistentErrors)
	}
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

func TestCompareJobWithBaseline_SupersededParentAttemptHistoryFallback(t *testing.T) {
	// STORY-IAZIO-0202 N3: superseded parent Result is clean child success, but AttemptHistory
	// carries pre-supersede Result JSON in StderrTail — CompareJobWithBaseline must still
	// surface RESOLVED_ERRORS via snapshotLooksClean fallback.
	failJSON, _ := json.Marshal(map[string]any{
		"test_failure_count": 2,
		"failed_tests":       []string{"TestAlpha", "TestBeta"},
		"span_latencies": []SpanLatencyItem{
			{SpanName: "run_tests", DurationMs: 400, HasError: true, ErrorTag: "exit_code=1"},
		},
	})
	okJSON, _ := json.Marshal(map[string]any{
		"test_failure_count": 0,
		"test_pass_count":    12,
		"status":             "completed",
	})
	baseline := &Job{
		ID:      "job_parent_superseded_0202",
		StoryID: "STORY-IAZIO-0202",
		Status:  JobStatusCompleted,
		Result:  okJSON,
		AttemptHistory: []AttemptFailure{
			{
				AttemptNumber:   1,
				FailureCategory: "TEST_FAILURE",
				Reason:          "pre_supersede_failure",
				StderrTail:      string(failJSON),
				OutcomeSummary:  "unit tests failed",
				TriggerReason:   "superseded_by_completed_replacement_child",
			},
		},
	}
	target := &Job{
		ID:       "job_child_ok_0202",
		ParentID: baseline.ID,
		StoryID:  "STORY-IAZIO-0202",
		Status:   JobStatusCompleted,
		Result:   okJSON,
	}
	diff := CompareJobWithBaseline(target, baseline, nil, nil)
	if diff == nil {
		t.Fatal("expected CompareJobWithBaseline diff")
	}
	if diff.Verdict != VerdictRemediationVerified {
		t.Fatalf("expected REMEDIATION_VERIFIED, got %s", diff.Verdict)
	}
	if diff.TestFailureDelta.BaselineFailures != 2 || diff.TestFailureDelta.CurrentFailures != 0 {
		t.Fatalf("unexpected test failure delta: %+v", diff.TestFailureDelta)
	}
	if len(diff.Buckets.ResolvedErrors) == 0 {
		t.Fatal("expected RESOLVED_ERRORS from StderrTail JSON parse")
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
		"duration_ms":        30000,
		"commit_sha":         "abc12345",
		"warning_signatures": []string{"WARN: slow query"},
		"error_clusters":     []ErrorClusterItem{},
		"span_latencies": []SpanLatencyItem{
			{SpanName: "VerifyStory", ServiceName: "harness", DurationMs: 110},
		},
	})

	job := &Job{
		ID:             "job_retry_1",
		StoryID:        "STORY-103",
		Status:         JobStatusCompleted,
		Attempts:       2,
		FinalTurnCount: 0,
		ElapsedSeconds: 0,
		ClaimedAt:      &t1,
		CompletedAt:    &t2,
		Result:         resBytes,
		AttemptHistory: []AttemptFailure{
			{
				AttemptNumber:   0,
				StartedAt:       t0.Format(time.RFC3339),
				EndedAt:         t1.Format(time.RFC3339),
				FailureCategory: "TEST_FAILURE",
				Reason:          "--- FAIL: TestPipelineGate (0.12s)\nexit status 1",
				StderrTail:      "--- FAIL: TestPipelineGate (0.12s)\nexit status 1",
			},
		},
	}

	diff := CompareJobWithBaseline(job, nil, []string{"INFO starting", "WARN: minor warning", "ERROR: transient err", "ERROR: transient err"}, nil)
	if diff == nil {
		t.Fatal("expected CompareJobWithBaseline to produce diff from AttemptHistory")
	}
	if diff.TestFailureDelta.BaselineFailures != 1 || diff.TestFailureDelta.CurrentFailures != 0 {
		t.Fatalf("unexpected test failure delta: %+v", diff.TestFailureDelta)
	}
}

func TestRunComparisonService_EdgeCasesAndAggregateCounts(t *testing.T) {
	var nilSvc *RunComparisonService
	// 1. Clean run with zero errors & pre_post_window mode
	cleanDiff := nilSvc.CompareRuns(
		RunTelemetrySnapshot{JobID: "j1", StoryID: "S1", WindowLabel: "pre"},
		RunTelemetrySnapshot{JobID: "j2", WindowLabel: "post"},
	)
	if cleanDiff.Verdict != VerdictCleanRun || cleanDiff.ComparisonMode != "pre_post_window" {
		t.Fatalf("unexpected cleanDiff: %+v", cleanDiff)
	}
	if FormatPhase4ReviewContext(nil) != "" {
		t.Fatal("expected empty string for nil diff")
	}

	// 2. Aggregate test counts without FailedTests list: 0->2, 2->4, 2->2, 2->0
	svc := &RunComparisonService{LatencyRegressionThresholdPct: 0, LatencyRegressionMinDeltaMs: 0}
	dNewFail := svc.CompareRuns(RunTelemetrySnapshot{TestFailureCount: 0}, RunTelemetrySnapshot{TestFailureCount: 2})
	if dNewFail.Verdict != VerdictRegressionIntroduced || len(dNewFail.NewRegressions) != 1 {
		t.Fatalf("expected new test failure regression, got %+v", dNewFail)
	}
	dIncFail := svc.CompareRuns(RunTelemetrySnapshot{TestFailureCount: 2}, RunTelemetrySnapshot{TestFailureCount: 4})
	if dIncFail.Verdict != VerdictRegressionIntroduced {
		t.Fatalf("expected increased test failure regression, got %+v", dIncFail)
	}
	dPersist := svc.CompareRuns(RunTelemetrySnapshot{TestFailureCount: 2}, RunTelemetrySnapshot{TestFailureCount: 2})
	if dPersist.Verdict != VerdictPersistentFailure || len(dPersist.PersistentErrors) != 1 {
		t.Fatalf("expected persistent failure, got %+v", dPersist)
	}
	dRes := svc.CompareRuns(RunTelemetrySnapshot{TestFailureCount: 2}, RunTelemetrySnapshot{TestFailureCount: 0})
	if dRes.Verdict != VerdictRemediationVerified || len(dRes.ResolvedErrors) != 1 {
		t.Fatalf("expected resolved test failure count, got %+v", dRes)
	}

	// 3. Duplicate cluster aggregation, warning level auto-category, status 500 span error, duplicate spans, raw error fallback
	base := RunTelemetrySnapshot{
		RawError:          "unhandled panic in worker",
		WarningSignatures: []string{"deprecated flag"},
		SpanLatencies: []SpanLatencyItem{
			{SpanName: "op", ServiceName: "svc", DurationMs: 0, P95DurationMs: 0},
			{SpanName: "op", ServiceName: "svc", DurationMs: 10, P95DurationMs: 20, HasError: true, ErrorTag: "err1"},
			{SpanName: "", DurationMs: 5},
		},
	}
	curr := RunTelemetrySnapshot{
		ErrorClusters: []ErrorClusterItem{
			{Signature: "warn item", Level: "WARN", Count: 0},
			{Signature: "dup err", Service: "svc", Count: 1},
			{Signature: "dup err", Service: "svc", Count: 2, SampleMessage: "sample"},
			{Signature: "   "},
		},
		SpanLatencies: []SpanLatencyItem{
			{SpanName: "op", ServiceName: "svc", DurationMs: 50, StatusCode: "503"},
			{SpanName: "new_err_span", ServiceName: "svc", DurationMs: 10, HasError: true},
		},
	}
	dComplex := svc.CompareRuns(base, curr)
	if len(dComplex.ResolvedErrors) == 0 || len(dComplex.NewRegressions) == 0 {
		t.Fatalf("expected resolved and regressed items, got %+v", dComplex)
	}

	// 4. NormalizeErrorSignature long string & empty
	if NormalizeErrorSignature("   ") != "" {
		t.Fatal("expected empty normalized signature")
	}
	longSig := NormalizeErrorSignature(strings.Repeat("abcd1234ef567890 98765 ", 20) + "\nsecond line")
	if len(longSig) > 180 {
		t.Fatalf("expected truncated signature <= 180 chars, got %d", len(longSig))
	}

	// 5. ExtractJobSnapshot & CompareJobWithBaseline edge cases
	if CompareJobWithBaseline(nil, nil, nil, nil) != nil {
		t.Fatal("expected nil for nil target job")
	}
	emptySnap := ExtractJobSnapshot(nil, nil)
	if emptySnap.RunID != "" {
		t.Fatalf("expected empty snapshot for nil job, got %+v", emptySnap)
	}
	tStart := time.Now().Add(-10 * time.Second).UTC()
	tEnd := time.Now().UTC()
	j1 := &Job{
		ID:          "j1",
		CreatedAt:   tStart,
		CompletedAt: &tEnd,
		Error:       "--- FAIL: TestX (0.01s)\n--- FAIL: TestX (0.01s)\nfatal crash",
		Result:      []byte(`{invalid-json`),
	}
	j2 := &Job{
		ID:          "j2",
		CreatedAt:   tStart,
		CompletedAt: &tEnd,
	}
	if CompareJobWithBaseline(j2, nil, nil, nil) != nil {
		t.Fatal("expected nil when no baselineJob and no AttemptHistory")
	}
	pairDiff := CompareJobWithBaseline(j2, j1, nil, nil)
	if pairDiff == nil || !pairDiff.RemediationVerified {
		t.Fatalf("expected pairDiff to verify remediation, got %+v", pairDiff)
	}
}
