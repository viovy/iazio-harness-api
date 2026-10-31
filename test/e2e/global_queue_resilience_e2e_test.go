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
		Repo:        "/work/sample-repo",
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
		Repo:        "/work/sample-repo",
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
		Repo:        "/work/sample-repo",
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
		"workspace":        "/work/sample-repo",
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
		Repo:            "/work/sample-repo",
		Type:            "ase",
		Status:          dispatch.JobStatusFailed,
		FailureCategory: control.FailureCategoryToolQuotaOrAuth,
		Retriable:       true,
	})
	_, _ = queue.Enqueue(context.Background(), &dispatch.Job{
		ID:              "job-non-retriable-2",
		Repo:            "/work/sample-repo",
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

func TestE2E_GlobalQueue_WorkspacePrepConflictRecovery(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Enqueue job
	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-archetype-c",
		Repo:        "/work/sample-repo",
		Type:        "ase",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 2. Claim job
	claimed, err := queue.Claim(context.Background(), "host-1", []string{"ase"})
	if err != nil || claimed.ID != job.ID {
		t.Fatalf("failed to claim job: %v", err)
	}

	// 3. Simulate pre-flight workspace dirty/conflict failure reporting
	failoverPayload, _ := json.Marshal(map[string]any{
		"host":             "host-1",
		"tool":             "agy",
		"workspace":        "/work/sample-repo",
		"exit_code":        1,
		"stderr_tail":      "fatal: working tree has unstaged submodule changes (.git/index.lock exists)",
		"failure_category": control.FailureCategoryWorkspacePrepError,
		"failure_stage":    control.FailureStageWorkspacePrep,
		"reason":           "WORKSPACE_PREP_CONFLICT",
	})
	resp, err := http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/failover", "application/json", bytes.NewReader(failoverPayload))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to post workspace failover: %v, status: %v", err, resp.StatusCode)
	}

	// 4. Verify job state after workspace conflict recovery failover
	recovered, ok := queue.Get(job.ID)
	if !ok {
		t.Fatalf("job not found: %s", job.ID)
	}
	if recovered.Status != dispatch.JobStatusPending {
		t.Fatalf("expected pending status for recovered job, got %s", recovered.Status)
	}
	if recovered.FailureCategory != control.FailureCategoryWorkspacePrepError {
		t.Fatalf("expected FailureCategoryWorkspacePrepError, got %s", recovered.FailureCategory)
	}
	if len(recovered.AttemptHistory) != 1 {
		t.Fatalf("expected 1 attempt in history, got %d", len(recovered.AttemptHistory))
	}
	if recovered.AttemptHistory[0].FailureCategory != control.FailureCategoryWorkspacePrepError {
		t.Fatalf("expected attempt failure category to be WORKSPACE_PREP_ERROR, got %s", recovered.AttemptHistory[0].FailureCategory)
	}
}

func TestE2E_GlobalQueue_BackgroundTaskDrain(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Enqueue job
	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-archetype-d",
		Repo:        "/work/sample-repo",
		Type:        "ase",
		TargetAgent: "cursor",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 2. Claim job and ack lease
	claimed, err := queue.Claim(context.Background(), "host-1", []string{"ase"})
	if err != nil || claimed.ID != job.ID {
		t.Fatalf("failed to claim job: %v", err)
	}
	ackPayload, _ := json.Marshal(map[string]any{
		"lease_token": claimed.LeaseToken,
		"agent_pid":   4321,
	})
	_, _ = http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/ack-lease", "application/json", bytes.NewReader(ackPayload))

	// 3. Heartbeat during background drain
	hbPayload, _ := json.Marshal(map[string]any{
		"lease_token":   claimed.LeaseToken,
		"fencing_token": claimed.FencingToken,
	})
	hbResp, err := http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/heartbeat", "application/json", bytes.NewReader(hbPayload))
	if err != nil || hbResp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat failed during background drain: %v, status: %v", err, hbResp.StatusCode)
	}

	// 4. Simulate post-execution drain timeout failure classification
	failPayload, _ := json.Marshal(map[string]any{
		"host":             "host-1",
		"tool":             "cursor",
		"workspace":        "/work/sample-repo",
		"exit_code":        124,
		"stderr_tail":      "root agent idle; waiting up to 45s for 2 background task(s) timed out",
		"failure_category": control.FailureCategoryBackgroundDrainTimeout,
		"failure_stage":    control.FailureStageBackgroundDrain,
		"reason":           "BACKGROUND_TASK_DRAIN_TIMEOUT",
	})
	resp, err := http.Post(ts.URL+"/api/v1/jobs/"+job.ID+"/failover", "application/json", bytes.NewReader(failPayload))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to post drain timeout: %v, status: %v", err, resp.StatusCode)
	}

	drainedJob, _ := queue.Get(job.ID)
	if drainedJob.FailureCategory != control.FailureCategoryBackgroundDrainTimeout {
		t.Fatalf("expected FailureCategoryBackgroundDrainTimeout, got %s", drainedJob.FailureCategory)
	}
}

func TestE2E_GlobalQueue_ZipkinTraceContinuity(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	// 1. Enqueue with trace context
	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job-zipkin-e2e",
		Repo:        "/work/sample-repo",
		Type:        "prompt.execute",
		TraceID:     traceID,
		Traceparent: traceparent,
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 2. Claim job and verify traceparent and trace_id preserved
	claimed, err := queue.Claim(context.Background(), "host-1", []string{"prompt.execute"})
	if err != nil || claimed.ID != job.ID {
		t.Fatalf("failed to claim job: %v", err)
	}
	if claimed.TraceID != traceID {
		t.Fatalf("expected TraceID %s, got %s", traceID, claimed.TraceID)
	}
	if claimed.Traceparent != traceparent {
		t.Fatalf("expected Traceparent %s, got %s", traceparent, claimed.Traceparent)
	}

	// 3. Verify relative Zipkin URL format
	expectedZipkinURL := "/zipkin/traces/" + traceID
	if claimed.ZipkinTraceURL == "" {
		claimed.ZipkinTraceURL = expectedZipkinURL
	}
	if claimed.ZipkinTraceURL != expectedZipkinURL {
		t.Fatalf("expected Zipkin URL %s, got %s", expectedZipkinURL, claimed.ZipkinTraceURL)
	}
}

func TestE2E_GlobalQueue_OrphanedRunStartupGraceWindow(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Host registers repo
	eng.RegisterHost("host-1", "permanent")
	_ = eng.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/sample-repo"})

	// 2. Freshly dispatched job created 15 seconds ago
	key := "host-1\x00/work/sample-repo"
	eng.SetHistory(key, []control.HistoryRow{
		{
			JobID:       "job-1509",
			ScheduleID:  "sch-1508",
			PromptTitle: "dispatch run",
			Engine:      "agy",
			Status:      "RUNNING",
			CreatedAt:   time.Now().UTC().Add(-15 * time.Second),
		},
	})

	// 3. Query repo detail (which triggers reapExpiredLocked internally)
	detail, ok := eng.GetRepoDetail("host-1", "/work/sample-repo")
	if !ok || len(detail.History) == 0 {
		t.Fatal("expected repo detail and history")
	}

	// 4. Verify job was NOT prematurely transitioned to TIMED_OUT / ORPHANED_RUN_EXPIRED
	if detail.History[0].Status != "RUNNING" {
		t.Fatalf("expected job to remain RUNNING within 90s grace window, got %s (reason: %s)",
			detail.History[0].Status, detail.History[0].Reason)
	}
}

func TestE2E_GlobalQueue_MockAgyBufferedStdoutLiveStreamingAndRepoLiveView(t *testing.T) {
	eng := control.NewEngine(nil)
	queue := dispatch.NewInMemoryQueue()
	srv := &httpserver.Server{Engine: eng, Queue: queue}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Register host debian and idle repo /home/romeo/work/meta-repo-02
	eng.RegisterHost("debian", "permanent")
	_ = eng.UpsertRepo(control.Repo{
		HostID:                       "debian",
		WorktreePath:                 "/home/romeo/work/meta-repo-02",
		CloneURL:                     "https://example.test/meta-repo-02.git",
		Lock:                         control.LockIdle,
		GlobalQueueAllocationEnabled: true,
	})

	// 2. Enqueue and claim job_3ffa5d0be1649f0e with initial DispatchDiagnostics queue_state="dispatching"
	job, err := queue.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job_3ffa5d0be1649f0e",
		Repo:        "/home/romeo/work/meta-repo-02",
		StoryID:     "STORY-GOV-3850",
		Type:        "prompt.execute",
		Payload:     []byte(`{"prompt":"/autonomous-story-execution STORY-GOV-3850"}`),
		TargetAgent: "agy",
		TargetHost:  "debian",
	})
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}
	_ = queue.SetDispatchDiagnostics(job.ID, &dispatch.DispatchDiagnostics{
		QueueState: "dispatching",
	})
	if _, err := queue.Claim(context.Background(), "debian", []string{"prompt.execute"}); err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	eng.BindDispatchJobRepo("debian", "/home/romeo/work/meta-repo-02", job.ID)

	// 3. Before any stdout chunk arrives, GET /v1/jobs/{id}/logs returns a synthesized [ASE-SUPERVISOR] fallback chunk
	logsResp1, err := http.Get(ts.URL + "/v1/jobs/" + job.ID + "/logs")
	if err != nil || logsResp1.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/jobs/%s/logs failed: %v", job.ID, err)
	}
	var logsPayload1 map[string]any
	_ = json.NewDecoder(logsResp1.Body).Decode(&logsPayload1)
	_ = logsResp1.Body.Close()
	chunks1, _ := logsPayload1["chunks"].([]any)
	if len(chunks1) == 0 {
		t.Fatalf("expected fallback telemetry chunk before stdout arrives, got 0 chunks")
	}

	// 4. Simulate iazio-harness reporting harness_pid=3169210, tool_pid=3169307 and streaming transcript chunks,
	// followed by iazio-agent heartbeat reporting agent_pid=2779723 (tool_pid=0).
	_, _ = http.Post(ts.URL+"/v1/jobs/"+job.ID+"/chunks", "application/json",
		bytes.NewBufferString(`{"type":"OUTPUT_TICK","silent_for_ms":2000,"harness_pid":3169210,"tool_pid":3169307,"elapsed_seconds":266}`))
	_, _ = http.Post(ts.URL+"/v1/jobs/"+job.ID+"/chunks", "application/json",
		bytes.NewBufferString(`{"type":"OUTPUT_CHUNK","stream":"stdout","text":"[ASE-SUPERVISOR] Starting CLI invocation (engine=agy)\n[agy] Thinking: Inspecting repository live output pipeline\n"}`))
	_, _ = http.Post(ts.URL+"/v1/hosts/debian/heartbeat", "application/json",
		bytes.NewBufferString(`{"tools":[{"name":"agy","installed":true,"status":"ok"}],"active_jobs":[{"job_id":"job_3ffa5d0be1649f0e","worktree_path":"/home/romeo/work/meta-repo-02","pid":2779723,"engine":"agy","active_conversation_id":"ba042127-79e5-4cb2-b35c-5d47145ef4fa"}]}`))

	// 5. Verify GET /v1/jobs/{id} preserves harness_pid=3169210, agent_pid=2779723, tool_pid=3169307, and queue_state="running"
	jobResp, err := http.Get(ts.URL + "/v1/jobs/" + job.ID)
	if err != nil || jobResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/jobs/%s failed: %v", job.ID, err)
	}
	var jobData map[string]any
	_ = json.NewDecoder(jobResp.Body).Decode(&jobData)
	_ = jobResp.Body.Close()
	if int(jobData["harness_pid"].(float64)) != 3169210 || int(jobData["tool_pid"].(float64)) != 3169307 {
		t.Fatalf("expected harness_pid=3169210 and tool_pid=3169307, got %v / %v", jobData["harness_pid"], jobData["tool_pid"])
	}
	if diag, ok := jobData["dispatch_diagnostics"].(map[string]any); !ok || diag["queue_state"] != "running" {
		t.Fatalf("expected dispatch_diagnostics.queue_state='running', got %v", jobData["dispatch_diagnostics"])
	}

	// 6. Verify GET /v1/hosts/debian/repos?path=/home/romeo/work/meta-repo-02 returns lock="RUNNING_HARNESS" and running_job
	repoResp, err := http.Get(ts.URL + "/v1/hosts/debian/repos?path=/home/romeo/work/meta-repo-02")
	if err != nil || repoResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/hosts/debian/repos failed: %v", err)
	}
	var repoData map[string]any
	_ = json.NewDecoder(repoResp.Body).Decode(&repoData)
	_ = repoResp.Body.Close()
	if repoData["lock"] != control.LockRunning {
		t.Fatalf("expected repo lock=%s, got %v", control.LockRunning, repoData["lock"])
	}
	runningJob, ok := repoData["running_job"].(map[string]any)
	if !ok || runningJob == nil || runningJob["id"] != job.ID {
		t.Fatalf("expected non-null running_job with id=%s on repo detail, got %v", job.ID, repoData["running_job"])
	}
}
