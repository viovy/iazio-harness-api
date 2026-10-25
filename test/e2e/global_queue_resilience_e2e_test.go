package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/httpserver"
)

func TestE2E_GlobalQueue_TwoPhaseLeaseAndAck(t *testing.T) {
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	eng := control.NewEngine(clock)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Submit a job into global queue
	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-test-lease-1",
		Repo:        "/work/meta-repo",
		Type:        "ase",
		Payload:     []byte(`{"title":"Test 2-Phase Lease"}`),
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 2. Claim job (Phase 1)
	claimed, err := queue.Claim(context.Background(), "host-1", []string{"ase"})
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	if claimed.Status != dispatch.JobStatusLeased {
		t.Fatalf("expected job status %s, got %s", dispatch.JobStatusLeased, claimed.Status)
	}
	if claimed.LeaseToken == "" {
		t.Fatal("expected non-empty lease token")
	}
	if claimed.LeaseAckDeadlineAt == nil {
		t.Fatal("expected non-empty lease ack deadline")
	}

	// 3. Ack lease (Phase 2)
	ackPayload, _ := json.Marshal(map[string]any{
		"lease_token":            claimed.LeaseToken,
		"agent_pid":              12345,
		"active_conversation_id": "conv-test-lease-1",
	})
	resp, err := http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/ack-lease", "application/json", bytes.NewReader(ackPayload))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to ack lease: %v, status: %v", err, resp.StatusCode)
	}

	// Verify job is now RUNNING
	runningJob, ok := queue.Get(job.ID)
	if !ok {
		t.Fatalf("failed to get job: %v", job.ID)
	}
	if runningJob.Status != dispatch.JobStatusRunning {
		t.Fatalf("expected running status, got: %s", runningJob.Status)
	}
	if runningJob.AgentPID != 12345 {
		t.Fatalf("expected agent pid 12345, got %d", runningJob.AgentPID)
	}
}

func TestE2E_GlobalQueue_UnacknowledgedLeaseRequeues(t *testing.T) {
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	_ = clock
	queue := dispatch.NewInMemoryQueue()

	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-test-timeout-1",
		Repo:        "/work/meta-repo",
		Type:        "ase",
		Payload:     []byte(`{"title":"Test Timeout Lease"}`),
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	claimed, err := queue.Claim(context.Background(), "host-1", []string{"ase"})
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	if claimed.Status != dispatch.JobStatusLeased {
		t.Fatalf("expected leased status, got: %s", claimed.Status)
	}

	// Advance time past 20s lease ack deadline and trigger reaper
	pastDeadline := time.Now().UTC().Add(25 * time.Second)
	queue.ReconcileGlobalQueueSlots(pastDeadline, 20*time.Second)

	requeued, ok := queue.Get(job.ID)
	if !ok {
		t.Fatalf("failed to get job: %v", job.ID)
	}
	if requeued.Status != dispatch.JobStatusPending {
		t.Fatalf("expected requeued status, got: %s", requeued.Status)
	}
	if requeued.InfraRequeueCount != 1 {
		t.Fatalf("expected infra requeue count 1, got %d", requeued.InfraRequeueCount)
	}
}

func TestE2E_GlobalQueue_ToolFailoverAndCircuitBreaker(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-failover-1",
		Repo:        "/work/meta-repo",
		Type:        "ase",
		Payload:     []byte(`{"title":"Test Tool Failover"}`),
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	_, err = queue.Claim(context.Background(), "host-1", []string{"ase"})
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Post failover due to 429 quota exhaustion
	failoverPayload, _ := json.Marshal(map[string]any{
		"host":             "host-1",
		"tool":             "agy",
		"workspace":        "/work/meta-repo",
		"exit_code":        1,
		"stderr_tail":      "HTTP 429: ResourceExhausted: rate limit exceeded",
		"failure_category": control.FailureCategoryToolQuotaOrAuth,
		"failure_stage":    control.FailureStageToolExecution,
		"reason":           "PREFLIGHT_OR_EARLY_TOOL_EXIT",
	})
	resp, err := http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/failover", "application/json", bytes.NewReader(failoverPayload))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failover failed: %v, status: %v", err, resp.StatusCode)
	}

	// Verify job was requeued with failover details
	requeued, ok := queue.Get(job.ID)
	if !ok {
		t.Fatalf("failed to get requeued job: %v", job.ID)
	}
	if requeued.Status != dispatch.JobStatusPending {
		t.Fatalf("expected pending status, got: %s", requeued.Status)
	}
	if len(requeued.AttemptHistory) != 1 {
		t.Fatalf("expected 1 attempt failure recorded, got %d", len(requeued.AttemptHistory))
	}
	if requeued.AttemptHistory[0].Tool != "agy" {
		t.Fatalf("expected failed tool agy, got %s", requeued.AttemptHistory[0].Tool)
	}
}

func TestE2E_GlobalQueue_HistoricalTaxonomyReconciliation(t *testing.T) {
	eng := control.NewEngine(nil)

	// Seed historical records across various failure archetypes
	eng.SetHistory("host-1\x00/work/repo", []control.HistoryRow{
		{
			JobID:        "job-hist-1",
			Status:       "FAILED",
			Reason:       "startup_heartbeat timeout on agent dispatch",
			ErrorMessage: "dispatch claim unacknowledged",
		},
		{
			JobID:        "job-hist-2",
			Status:       "FAILED",
			Reason:       "HTTP 429 Too Many Requests (ResourceExhausted)",
			ErrorMessage: "quota exhausted for agy",
		},
		{
			JobID:        "job-hist-3",
			Status:       "FAILED",
			Reason:       "workspace_prep conflict: dirty submodule",
			ErrorMessage: "error: Your local changes to the following files would be overwritten",
		},
		{
			JobID:        "job-hist-4",
			Status:       "LEASE_EXPIRED",
			Reason:       "root agent idle; waiting up to 1h0m0s for background tasks",
			ErrorMessage: "lease expired waiting for background tasks",
		},
	})

	reconciled := eng.ReconcileHistoricalFailureTaxonomy()
	if reconciled != 4 {
		t.Fatalf("expected 4 reconciled records, got %d", reconciled)
	}

	// Verify classifications
	job1, ok := eng.GetJob("job-hist-1")
	if !ok || job1.FailureCategory != control.FailureCategoryDispatchClaimTimeout {
		t.Fatalf("expected DISPATCH_CLAIM_TIMEOUT, got %v", job1.FailureCategory)
	}
	if !job1.Retriable {
		t.Fatal("expected job-hist-1 to be retriable")
	}

	job2, ok := eng.GetJob("job-hist-2")
	if !ok || job2.FailureCategory != control.FailureCategoryToolQuotaOrAuth {
		t.Fatalf("expected TOOL_QUOTA_OR_AUTH, got %v", job2.FailureCategory)
	}

	job3, ok := eng.GetJob("job-hist-3")
	if !ok || job3.FailureCategory != control.FailureCategoryWorkspacePrepError {
		t.Fatalf("expected WORKSPACE_PREP_ERROR, got %v", job3.FailureCategory)
	}

	job4, ok := eng.GetJob("job-hist-4")
	if !ok || job4.FailureCategory != control.FailureCategoryBackgroundDrainTimeout {
		t.Fatalf("expected BACKGROUND_DRAIN_TIMEOUT, got %v", job4.FailureCategory)
	}
}

func TestE2E_GlobalQueue_RetryRetriable(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Seed retriable and non-retriable jobs
	_, _ = queue.Enqueue(context.Background(), &dispatch.Job{
		ID:              "job-retriable-1",
		Repo:            "/work/meta-repo",
		Type:            "ase",
		Status:          dispatch.JobStatusFailed,
		FailureCategory: control.FailureCategoryToolQuotaOrAuth,
		Retriable:       true,
	})
	_, _ = queue.Enqueue(context.Background(), &dispatch.Job{
		ID:              "job-non-retriable-2",
		Repo:            "/work/meta-repo",
		Type:            "ase",
		Status:          dispatch.JobStatusFailed,
		FailureCategory: control.FailureCategoryASEGateIncomplete,
		Retriable:       false,
	})

	resp, err := http.Post(ts.URL+"/api/v1/global-queue/jobs/retry-retriable", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to retry retriable: %v, status: %v", err, resp.StatusCode)
	}

	var res struct {
		RetriedCount int `json:"retried_count"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&res)
	if res.RetriedCount != 1 {
		t.Fatalf("expected 1 retried job, got %d", res.RetriedCount)
	}

	j1, _ := queue.Get("job-retriable-1")
	if j1.Status != dispatch.JobStatusRequeued {
		t.Fatalf("expected job-retriable-1 to be marked requeued, got %s", j1.Status)
	}
	j2, _ := queue.Get("job-non-retriable-2")
	if j2.Status != dispatch.JobStatusFailed {
		t.Fatalf("expected job-non-retriable-2 to remain failed, got %s", j2.Status)
	}
}
