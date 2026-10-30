package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQueue_EnqueueAndGet(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	job, err := q.Enqueue(ctx, &Job{
		Type:    JobTypeStoryCreate,
		Repo:    "iazio-mcp",
		Payload: json.RawMessage(`{"title":"test"}`),
	})
	if err != nil {
		t.Fatalf("unexpected error enqueuing: %v", err)
	}
	if job.ID == "" {
		t.Fatal("expected generated job ID, got empty")
	}
	if job.Status != JobStatusPending {
		t.Fatalf("expected status %s, got %s", JobStatusPending, job.Status)
	}

	retrieved, ok := q.Get(job.ID)
	if !ok || retrieved == nil {
		t.Fatalf("expected job %s to be found", job.ID)
	}
	if retrieved.Repo != "iazio-mcp" {
		t.Fatalf("expected repo iazio-mcp, got %s", retrieved.Repo)
	}
}

func TestQueue_ClaimFilters(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	_, err := q.Enqueue(ctx, &Job{
		Type:       JobTypePromptExecute,
		Repo:       "repo-1",
		TargetHost: "host-a",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	_, err = q.Enqueue(ctx, &Job{
		Type:       JobTypeStoryCreate,
		Repo:       "repo-2",
		TargetHost: "host-b",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Claim with unmatched host should return nil
	claimed, err := q.Claim(ctx, "host-c", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nil claim, got %v", claimed)
	}

	// Claim with matching host and type
	claimed, err = q.Claim(ctx, "host-b", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed job, got nil")
	}
	if claimed.Type != JobTypeStoryCreate || claimed.TargetHost != "host-b" {
		t.Fatalf("unexpected claimed job attributes: %+v", claimed)
	}
	if claimed.Status != JobStatusClaimed && claimed.Status != JobStatusLeased {
		t.Fatalf("expected status claimed or leased, got %s", claimed.Status)
	}
}

func TestQueue_AwaitAndComplete(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "iazio-mcp",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	resultCh := make(chan *Job)
	errCh := make(chan error)

	go func() {
		completedJob, err := q.Await(ctx, job.ID, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- completedJob
	}()

	// Simulate agent processing
	time.Sleep(50 * time.Millisecond)
	err = q.Complete(ctx, job.ID, json.RawMessage(`{"story_id":"STORY-001"}`), "")
	if err != nil {
		t.Fatalf("complete error: %v", err)
	}

	select {
	case res := <-resultCh:
		if res.Status != JobStatusCompleted {
			t.Fatalf("expected completed status, got %s", res.Status)
		}
		if string(res.Result) != `{"story_id":"STORY-001"}` {
			t.Fatalf("unexpected result: %s", string(res.Result))
		}
	case err := <-errCh:
		t.Fatalf("unexpected await error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("test timed out awaiting completion")
	}
}

func TestQueue_AwaitTimeout(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "iazio-mcp",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	_, err = q.Await(ctx, job.ID, 50*time.Millisecond)
	if err != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestQueue_ConcurrentOperations(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	const count = 50
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			_, _ = q.Enqueue(ctx, &Job{
				Type: JobTypeStoryCreate,
				Repo: "concurrent-repo",
			})
		}()
	}
	wg.Wait()

	// Claim all
	claimedCount := 0
	for i := 0; i < count; i++ {
		claimed, err := q.Claim(ctx, "", nil)
		if err != nil {
			t.Fatalf("claim error: %v", err)
		}
		if claimed != nil {
			claimedCount++
			_ = q.Complete(ctx, claimed.ID, json.RawMessage(`{}`), "")
		}
	}
	if claimedCount != count {
		t.Fatalf("expected to claim %d jobs, claimed %d", count, claimedCount)
	}
}

func TestQueue_List_Cancel_Retry(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	job1, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-alpha",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	job2, err := q.Enqueue(ctx, &Job{
		Type: JobTypePromptExecute,
		Repo: "repo-beta",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Test List
	listAll, err := q.List(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	if len(listAll) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(listAll))
	}

	listFiltered, err := q.List(ctx, JobFilter{Repo: "repo-alpha"})
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	if len(listFiltered) != 1 || listFiltered[0].ID != job1.ID {
		t.Fatalf("expected job1 in filtered list, got %+v", listFiltered)
	}

	// Test Cancel
	canceled, err := q.Cancel(ctx, job1.ID, "user request")
	if err != nil {
		t.Fatalf("cancel error: %v", err)
	}
	if canceled.Status != JobStatusCanceled || canceled.Error != "user request" {
		t.Fatalf("expected canceled status and reason, got %+v", canceled)
	}

	// Canceling again should return ErrJobAlreadyCompleted
	_, err = q.Cancel(ctx, job1.ID, "again")
	if err != ErrJobAlreadyCompleted {
		t.Fatalf("expected ErrJobAlreadyCompleted, got %v", err)
	}

	// Test Retry
	retried, err := q.Retry(ctx, job1.ID)
	if err != nil {
		t.Fatalf("retry error: %v", err)
	}
	if retried.ID == job1.ID {
		t.Fatalf("expected new job ID for retried job, got %s", retried.ID)
	}
	if retried.Status != JobStatusPending {
		t.Fatalf("expected pending status, got %s", retried.Status)
	}
	if retried.Repo != "repo-alpha" || retried.Type != JobTypeStoryCreate {
		t.Fatalf("retried job properties mismatch: %+v", retried)
	}

	// Verify job2 still pending
	gotJob2, ok := q.Get(job2.ID)
	if !ok || gotJob2.Status != JobStatusPending {
		t.Fatalf("expected job2 to be pending, got %+v", gotJob2)
	}
}

func TestQueue_CompleteAttributionDispatch(t *testing.T) {
	var mu sync.Mutex
	relevanceCalls := 0
	promoteCalls := 0
	var receivedRelevance map[string]any
	var receivedPromote map[string]any
	var relevanceTraceparent string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/v1/relevance" {
			relevanceCalls++
			relevanceTraceparent = r.Header.Get("traceparent")
			_ = json.Unmarshal(body, &receivedRelevance)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/v1/memories/promote" {
			promoteCalls++
			_ = json.Unmarshal(body, &receivedPromote)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	q := NewInMemoryQueue()
	q.SetAttributionConfig(srv.URL, srv.Client())

	ctx := context.Background()
	job, err := q.Enqueue(ctx, &Job{
		Type:     JobTypePromptExecute,
		Repo:     "test-repo",
		SearchID: "srch-test-12345",
		TraceID:  "trace-test-67890",
		Payload:  json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	err = q.Complete(ctx, job.ID, json.RawMessage(`{"status":"ok"}`), "")
	if err != nil {
		t.Fatalf("failed to complete: %v", err)
	}

	// Wait briefly for background goroutine dispatch
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if relevanceCalls != 1 {
		t.Errorf("expected 1 relevance call, got %d", relevanceCalls)
	}
	if promoteCalls != 1 {
		t.Errorf("expected 1 promote call, got %d", promoteCalls)
	}

	if receivedRelevance == nil {
		t.Fatal("expected receivedRelevance to be non-nil")
	}
	if searchID, ok := receivedRelevance["search_id"].(string); !ok || searchID != "srch-test-12345" {
		t.Errorf("expected search_id 'srch-test-12345', got %v", receivedRelevance["search_id"])
	}
	if outcome, ok := receivedRelevance["outcome"].(string); !ok || outcome != "positive" {
		t.Errorf("expected outcome 'positive' (string), got %v (type %T)", receivedRelevance["outcome"], receivedRelevance["outcome"])
	}
	if traceID, ok := receivedRelevance["trace_id"].(string); !ok || traceID != "trace-test-67890" {
		t.Errorf("expected trace_id 'trace-test-67890', got %v", receivedRelevance["trace_id"])
	}
	if relevanceTraceparent == "" {
		t.Error("expected traceparent header to be transmitted")
	}

	if receivedPromote == nil {
		t.Fatal("expected receivedPromote to be non-nil")
	}
	if tier, ok := receivedPromote["tier"].(string); !ok || tier != "canonical" {
		t.Errorf("expected tier 'canonical', got %v", receivedPromote["tier"])
	}
	if _, ok := receivedPromote["target"]; ok {
		t.Errorf("schema violation: field 'target' should not exist in promotion request")
	}
}

func TestQueue_ClaimLeaseExpiry(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	currTime := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return currTime })
	q.SetTimeouts(10*time.Minute, 60*time.Minute)

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-expire",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	claimed, err := q.Claim(ctx, "host-1", []string{JobTypeStoryCreate})
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claimed.Status != JobStatusClaimed && claimed.Status != JobStatusLeased {
		t.Fatalf("expected claimed, got %s", claimed.Status)
	}

	// Ack lease to transition to running
	if _, err := q.AckLease(ctx, claimed.ID, claimed.LeaseToken, 1234, ""); err != nil {
		t.Fatalf("ack lease failed: %v", err)
	}

	// Advance time past claimTimeout (11 minutes)
	currTime = currTime.Add(11 * time.Minute)

	// Lazy reap via Get
	reaped, ok := q.Get(job.ID)
	if !ok {
		t.Fatal("expected job to be found")
	}
	if reaped.Status != JobStatusFailed {
		t.Fatalf("expected status failed after lease expiry, got %s", reaped.Status)
	}
	if reaped.Error == "" || reaped.CompletedAt == nil {
		t.Fatalf("expected error and CompletedAt set, got err=%q, completedAt=%v", reaped.Error, reaped.CompletedAt)
	}
}

func TestQueue_MaxExecutionTimeout(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	currTime := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return currTime })
	q.SetTimeouts(30*time.Minute, 60*time.Minute)

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypePromptExecute,
		Repo: "repo-max-exec",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Advance time past max execution timeout (65 minutes) without claim
	currTime = currTime.Add(65 * time.Minute)

	list, err := q.List(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 job, got %d", len(list))
	}
	if list[0].ID != job.ID {
		t.Fatalf("expected job %s, got %s", job.ID, list[0].ID)
	}
	if list[0].Status != JobStatusFailed {
		t.Fatalf("expected failed status due to max execution timeout, got %s", list[0].Status)
	}
}

func TestQueue_StoryJobDoesNotTimeoutOnMaxExecution(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	currTime := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return currTime })
	q.SetTimeouts(30*time.Minute, 60*time.Minute)

	job, err := q.Enqueue(ctx, &Job{
		Type:    JobTypePromptExecute,
		Repo:    "repo-story-exec",
		StoryID: "STORY-GOV-3693",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Advance time past max execution timeout (75 minutes)
	currTime = currTime.Add(75 * time.Minute)

	list, err := q.List(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("list error: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 job, got %d", len(list))
	}
	if list[0].ID != job.ID {
		t.Fatalf("expected job %s, got %s", job.ID, list[0].ID)
	}
	if list[0].Status == JobStatusFailed {
		t.Fatalf("expected story job NOT to fail on execution timeout, got status: %s", list[0].Status)
	}
}

func TestQueue_StartReaperBackground(t *testing.T) {
	q := NewInMemoryQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.SetTimeouts(20*time.Millisecond, 50*time.Millisecond)
	q.StartReaper(ctx, 10*time.Millisecond)

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-bg",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	_, err = q.Claim(ctx, "host-1", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	j, ok := q.Get(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}
	if j.Status != JobStatusFailed {
		t.Fatalf("expected background reaper to mark job failed, got %s", j.Status)
	}
}

func TestQueue_AwaitUnblocksOnExpiration(t *testing.T) {
	q := NewInMemoryQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.SetTimeouts(25*time.Millisecond, 100*time.Millisecond)
	q.StartReaper(ctx, 10*time.Millisecond)

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-await",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	_, err = q.Claim(ctx, "host-1", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}

	completedJob, err := q.Await(ctx, job.ID, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("await returned error: %v", err)
	}
	if completedJob == nil || completedJob.Status != JobStatusFailed {
		t.Fatalf("expected await to return failed job on expiry, got %+v", completedJob)
	}
}

func TestQueue_FencedToken_Validation(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-fencing",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// First worker claims job
	claimed1, err := q.Claim(ctx, "host-1", []string{JobTypeStoryCreate})
	if err != nil || claimed1 == nil {
		t.Fatalf("worker 1 claim error: %v", err)
	}
	if claimed1.FencingToken != 1 {
		t.Fatalf("expected fencing token 1, got %d", claimed1.FencingToken)
	}

	// Valid heartbeat
	hb, err := q.Heartbeat(ctx, job.ID, claimed1.FencingToken)
	if err != nil || hb == nil {
		t.Fatalf("heartbeat with valid token failed: %v", err)
	}

	// Stale / mismatched heartbeat rejected
	_, err = q.Heartbeat(ctx, job.ID, 999)
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced on invalid token heartbeat, got %v", err)
	}

	// Stale complete attempt from superseded token rejected
	err = q.CompleteWithFencing(ctx, job.ID, 999, []byte(`{"ok":true}`), "")
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced on stale complete attempt, got %v", err)
	}

	// Valid complete attempt succeeds
	err = q.CompleteWithFencing(ctx, job.ID, claimed1.FencingToken, []byte(`{"ok":true}`), "")
	if err != nil {
		t.Fatalf("valid complete failed: %v", err)
	}

	finalJob, ok := q.Get(job.ID)
	if !ok || finalJob.Status != JobStatusCompleted {
		t.Fatalf("expected completed job, got %+v", finalJob)
	}
}

func TestQueue_StalledRunnerReap_RequeuesPending(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	currTime := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return currTime })

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-stall",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Worker 1 claims: attempt 1
	claimed1, err := q.Claim(ctx, "host-1", []string{JobTypeStoryCreate})
	if err != nil || claimed1 == nil {
		t.Fatalf("claim 1 error: %v", err)
	}
	if claimed1.Attempts != 1 || claimed1.FencingToken != 1 {
		t.Fatalf("unexpected claim 1 state: attempts=%d, token=%d", claimed1.Attempts, claimed1.FencingToken)
	}
	token1 := claimed1.FencingToken

	// Worker 1 stalls: advance time past heartbeat timeout (2 minutes)
	currTime = currTime.Add(3 * time.Minute)

	// Sweep reaps expired heartbeat: should revert to pending since attempts (1) < DefaultMaxAttempts (3)
	reaped := q.ReapExpired(currTime)
	if len(reaped) != 1 || reaped[0] != job.ID {
		t.Fatalf("expected reaped job %s, got %v", job.ID, reaped)
	}

	j, ok := q.Get(job.ID)
	if !ok || j.Status != JobStatusPending {
		t.Fatalf("expected job to revert to pending for retry, got status=%s", j.Status)
	}

	// Worker 2 claims the recovered job: attempt 2, monotonic fencing token 2
	claimed2, err := q.Claim(ctx, "host-2", []string{JobTypeStoryCreate})
	if err != nil || claimed2 == nil {
		t.Fatalf("claim 2 error: %v", err)
	}
	if claimed2.Attempts != 2 || claimed2.FencingToken != 2 {
		t.Fatalf("unexpected claim 2 state: attempts=%d, token=%d", claimed2.Attempts, claimed2.FencingToken)
	}
	token2 := claimed2.FencingToken

	// Delayed zombie write from Worker 1 (token 1) must be deterministically rejected!
	err = q.CompleteWithFencing(ctx, job.ID, token1, []byte(`{"mutated":true}`), "")
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for delayed zombie write, got %v", err)
	}

	// Empty or zero-token write from caller must also be rejected when job has an active fencing token
	err = q.CompleteWithFencing(ctx, job.ID, 0, []byte(`{"bypass":true}`), "")
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for zero-token bypass write, got %v", err)
	}

	// Zero-token heartbeat must also be rejected
	_, err = q.Heartbeat(ctx, job.ID, 0)
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for zero-token heartbeat, got %v", err)
	}

	// Worker 2 completes successfully with token 2
	err = q.CompleteWithFencing(ctx, job.ID, token2, []byte(`{"success":true}`), "")
	if err != nil {
		t.Fatalf("worker 2 complete failed: %v", err)
	}
}

func TestQueue_AgentAffinity(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()

	_, err := q.Enqueue(ctx, &Job{
		Type: JobTypeStoryCreate,
		Repo: "repo-affinity",
		AgentAffinity: &AgentAffinity{
			HostID: "host-dedicated",
		},
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Other host cannot claim
	claimedOther, err := q.Claim(ctx, "host-other", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim other error: %v", err)
	}
	if claimedOther != nil {
		t.Fatalf("expected nil for mismatched host, got %+v", claimedOther)
	}

	// Dedicated host claims successfully
	claimedDedicated, err := q.Claim(ctx, "host-dedicated", []string{JobTypeStoryCreate})
	if err != nil {
		t.Fatalf("claim dedicated error: %v", err)
	}
	if claimedDedicated == nil {
		t.Fatal("expected dedicated host to claim job")
	}
}

func TestQueue_ReconcileGlobalQueueSlots_And_ForceDispatch(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.SetNowFunc(func() time.Time { return now })

	_, err := q.Enqueue(ctx, &Job{
		ID:   "job_18aefb5b4b68bb72",
		Type: JobTypePromptExecute,
		Repo: "workspace-repo",
	})
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	// Claim job by pc1-wsl
	claimed, err := q.Claim(ctx, "pc1-wsl", []string{JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("claim error: %v", err)
	}
	if (claimed.Status != JobStatusClaimed && claimed.Status != JobStatusLeased) || claimed.ClaimedHost != "pc1-wsl" {
		t.Fatalf("expected claimed by pc1-wsl, got status=%s host=%s", claimed.Status, claimed.ClaimedHost)
	}

	// Advance clock by 50s (exceeding 45s stale grace)
	now = now.Add(50 * time.Second)
	reclaimed := q.ReconcileGlobalQueueSlots(now, 45*time.Second)
	if len(reclaimed) != 1 || reclaimed[0] != "job_18aefb5b4b68bb72" {
		t.Fatalf("expected job_18aefb5b4b68bb72 reclaimed, got %v", reclaimed)
	}

	// Verify job is back in pending with diagnostics
	jAfter, ok := q.Get("job_18aefb5b4b68bb72")
	if !ok || jAfter.Status != JobStatusPending {
		t.Fatalf("expected job reverted to pending, got %s", jAfter.Status)
	}
	if jAfter.DispatchDiagnostics == nil || jAfter.DispatchDiagnostics.QueueState != "queued" {
		t.Fatalf("expected queue_state queued, got %+v", jAfter.DispatchDiagnostics)
	}

	// Re-claim on pc1 native
	claimed2, err := q.Claim(ctx, "pc1", []string{JobTypePromptExecute})
	if err != nil || claimed2 == nil {
		t.Fatalf("second claim failed: %v", err)
	}

	// Test ForceDispatch to break claim and re-enqueue immediately
	forced, err := q.ForceDispatch(ctx, "job_18aefb5b4b68bb72")
	if err != nil {
		t.Fatalf("force dispatch failed: %v", err)
	}
	if forced.Status != JobStatusPending || forced.ClaimedHost != "" {
		t.Fatalf("expected reset to pending without claimed host, got %+v", forced)
	}
	if forced.DispatchDiagnostics.QueueState != "dispatching" {
		t.Fatalf("expected queue_state dispatching, got %s", forced.DispatchDiagnostics.QueueState)
	}
}

func TestInMemoryQueue_Failover_RetryRetriable_BindStory_Attribution(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypePromptExecute,
		Repo: "iazio-mcp",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// 1. Failover
	attempt := AttemptFailure{
		Host:            "host1",
		Tool:            "agy",
		Workspace:       "/tmp/w",
		ExitCode:        1,
		StderrTail:      "crash",
		FailureCategory: "preflight_tool_exit",
	}
	foJob, err := q.Failover(ctx, job.ID, attempt)
	if err != nil {
		t.Fatalf("failover failed: %v", err)
	}
	if len(foJob.AttemptHistory) != 1 || foJob.Status != JobStatusPending || !foJob.Retriable {
		t.Fatalf("unexpected failover job state: %+v", foJob)
	}

	// 2. RetryRetriable
	foJob.Status = JobStatusFailed
	retried, err := q.RetryRetriable(ctx)
	if err != nil {
		t.Fatalf("retry retriable failed: %v", err)
	}
	if len(retried) != 1 {
		t.Fatalf("expected 1 retried job, got %d", len(retried))
	}

	// 3. BindStory
	bound, err := q.BindStory(ctx, job.ID, "STORY-GOV-9999", "Test Title", "docs/test.story.md")
	if err != nil {
		t.Fatalf("bind story failed: %v", err)
	}
	if bound.StoryID != "STORY-GOV-9999" || bound.StoryAllocationStatus != "allocated" {
		t.Fatalf("unexpected bound job state: %+v", bound)
	}

	// 4. SetDispatchDiagnostics & GetAttribution
	diag := &DispatchDiagnostics{
		QueueState: "queued",
	}
	if err := q.SetDispatchDiagnostics(job.ID, diag); err != nil {
		t.Fatalf("set dispatch diagnostics failed: %v", err)
	}
	attr, err := q.GetAttribution(ctx, job.ID)
	if err != nil {
		t.Fatalf("get attribution failed: %v", err)
	}
	if attr.JobID != job.ID {
		t.Fatalf("expected jobID %s, got %s", job.ID, attr.JobID)
	}
}

func TestQueue_Failover_CapsRetriesOnIdenticalConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()

	job, err := q.Enqueue(ctx, &Job{
		ID:          "job-1466",
		Type:        JobTypePromptExecute,
		Repo:        "/work/sample-repo",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Attempt 1 failover
	att1 := AttemptFailure{
		Host:            "mac-mini",
		Tool:            "agy",
		FailureCategory: "TOOL_CRASH",
		Reason:          "NON_ZERO_EXIT",
		ExitCode:        1,
		StderrTail:      "child exited with 1",
	}
	job1, err := q.Failover(ctx, job.ID, att1)
	if err != nil {
		t.Fatalf("attempt 1 failover failed: %v", err)
	}
	if job1.Status != JobStatusPending || !job1.Retriable {
		t.Fatalf("expected pending retriable after attempt 1, got status=%s retriable=%v", job1.Status, job1.Retriable)
	}
	if len(job1.AttemptHistory) != 1 {
		t.Fatalf("expected 1 attempt history, got %d", len(job1.AttemptHistory))
	}

	// Attempt 2 failover with IDENTICAL error: must be capped!
	att2 := AttemptFailure{
		Host:            "mac-mini",
		Tool:            "agy",
		FailureCategory: "TOOL_CRASH",
		Reason:          "NON_ZERO_EXIT",
		ExitCode:        1,
		StderrTail:      "child exited with 1",
	}
	job2, err := q.Failover(ctx, job.ID, att2)
	if err != nil {
		t.Fatalf("attempt 2 failover failed: %v", err)
	}
	if job2.Status != JobStatusFailed {
		t.Errorf("expected job to be FAILED after consecutive identical failures, got %s", job2.Status)
	}
	if job2.Retriable {
		t.Errorf("expected job to NOT be retriable after consecutive identical failures")
	}
	if !strings.Contains(job2.Error, "RETRY_CAPPED") {
		t.Errorf("expected RETRY_CAPPED error message, got %s", job2.Error)
	}
}

func TestQueue_ReapExpired_CapsRetriesOnConsecutiveLeaseExpired(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()
	now := time.Now()
	q.SetNowFunc(func() time.Time { return now })

	job, err := q.Enqueue(ctx, &Job{
		ID:          "job-1468",
		Type:        JobTypePromptExecute,
		Repo:        "/work/sample-repo",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Claim job for Attempt 1
	claimed, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}
	if _, err := q.AckLease(ctx, claimed.ID, claimed.LeaseToken, 1001, ""); err != nil {
		t.Fatalf("ack lease 1 failed: %v", err)
	}

	// Expire lease 1
	now = now.Add(DefaultHeartbeatTimeout + 1*time.Second)
	reaped := q.ReapExpired(now)
	if len(reaped) == 0 {
		t.Fatalf("expected job to be reaped on first lease expiry")
	}

	// Check job after first lease expiry: reverted to pending for safe recovery
	j1, _ := q.Get(job.ID)
	if j1.Status != JobStatusPending {
		t.Fatalf("expected pending after first lease expiry, got %s", j1.Status)
	}

	// Claim job for Attempt 2
	claimed2, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimed2 == nil {
		t.Fatalf("second claim failed: %v", err)
	}
	if _, err := q.AckLease(ctx, claimed2.ID, claimed2.LeaseToken, 1002, ""); err != nil {
		t.Fatalf("ack lease 2 failed: %v", err)
	}

	// Expire lease 2: consecutive LEASE_EXPIRED must cap retries!
	now = now.Add(DefaultHeartbeatTimeout + 1*time.Second)
	_ = q.ReapExpired(now)

	j2, _ := q.Get(job.ID)
	if j2.Status != JobStatusFailed {
		t.Errorf("expected job to be FAILED after consecutive lease expirations, got %s", j2.Status)
	}
	if j2.Retriable {
		t.Errorf("expected job to NOT be retriable after consecutive lease expirations")
	}
	if !strings.Contains(j2.Error, "LEASE_EXPIRED: consecutive attempts expired lease") {
		t.Errorf("expected consecutive LEASE_EXPIRED error, got %s", j2.Error)
	}
}

func TestInMemoryQueue_ExtendLeasesForHost(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.SetNowFunc(func() time.Time { return now })

	job, err := q.Enqueue(ctx, &Job{
		ID:          "job-extend-1",
		Type:        JobTypePromptExecute,
		TargetHost:  "mac-mini",
		Repo:        "/Users/romeo/work/meta-repo",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}
	if _, err := q.AckLease(ctx, claimed.ID, claimed.LeaseToken, 1001, ""); err != nil {
		t.Fatalf("ack lease failed: %v", err)
	}

	// 60s passes (original deadline was now+90s)
	now = now.Add(60 * time.Second)

	// Host sends periodic heartbeat with active jobs
	extended, err := q.ExtendLeasesForHost(ctx, "mac-mini", []string{job.ID})
	if err != nil {
		t.Fatalf("ExtendLeasesForHost failed: %v", err)
	}
	if len(extended) != 1 || extended[0] != job.ID {
		t.Fatalf("expected extended [%s], got %v", job.ID, extended)
	}

	// Another 45s passes (total 105s from initial claim).
	// Without ExtendLeasesForHost, the 90s deadline would have expired.
	now = now.Add(45 * time.Second)
	reaped := q.ReapExpired(now)
	if len(reaped) > 0 {
		t.Fatalf("job was unexpectedly reaped: %v", reaped)
	}

	current, ok := q.Get(job.ID)
	if !ok {
		t.Fatalf("job not found")
	}
	if current.Status != JobStatusRunning {
		t.Fatalf("expected job to remain RUNNING, got %s", current.Status)
	}
}

func TestExtendLeasesForHost_AutoReconciliationAndStartupGrace(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return now })
	q.startupTime = now
	q.startupGrace = 5 * time.Minute

	job, err := q.Enqueue(ctx, &Job{
		Type:             JobTypePromptExecute,
		Repo:             "leaf-01",
		NormalizedPrompt: "update tools",
		CreatedAt:        now,
		Status:           JobStatusFailed,
		Error:            "LEASE_EXPIRED: execution lease expired without completion",
		HostID:           "docker1",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Host sends heartbeat with active job - should auto-reconcile and extend
	extended, err := q.ExtendLeasesForHost(ctx, "docker1", []string{job.ID})
	if err != nil {
		t.Fatalf("ExtendLeasesForHost failed: %v", err)
	}
	if len(extended) != 1 || extended[0] != job.ID {
		t.Fatalf("expected extended [%s], got %v", job.ID, extended)
	}

	cur, ok := q.Get(job.ID)
	if !ok || cur.Status != JobStatusRunning {
		t.Fatalf("expected job to be reconciled to RUNNING, got %+v", cur)
	}
	if cur.Error != "" {
		t.Fatalf("expected error to be cleared on auto-reconciliation, got %q", cur.Error)
	}

	// Also verify re-hydration of job reported by host that is missing from queue
	missingID := "job-external-123"
	extMissing, err := q.ExtendLeasesForHost(ctx, "docker1", []string{missingID})
	if err != nil {
		t.Fatalf("ExtendLeasesForHost missing failed: %v", err)
	}
	if len(extMissing) == 0 {
		t.Fatalf("expected missing job to be rehydrated and extended")
	}
	mJob, ok := q.Get(missingID)
	if !ok || mJob.Status != JobStatusRunning {
		t.Fatalf("expected missing job to be rehydrated as RUNNING, got %+v", mJob)
	}
}

func TestCompleteWithFencing_AcceptsCompletionOnReconcilableFailure(t *testing.T) {
	q := NewInMemoryQueue()
	ctx := context.Background()
	now := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	q.SetNowFunc(func() time.Time { return now })

	job, err := q.Enqueue(ctx, &Job{
		Type:             JobTypePromptExecute,
		Repo:             "leaf-01",
		NormalizedPrompt: "update tools",
		CreatedAt:        now,
		Status:           JobStatusFailed,
		Error:            "EXECUTION_TIMEOUT: job exceeded max execution duration",
		FencingToken:     12,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Agent finishes and reports completion with fencing token
	res := json.RawMessage(`{"status":"success","exit_code":0}`)
	err = q.CompleteWithFencing(ctx, job.ID, 12, res, "")
	if err != nil {
		t.Fatalf("CompleteWithFencing should accept completion for reconcilable failure, got: %v", err)
	}

	completed, ok := q.Get(job.ID)
	if !ok || completed.Status != JobStatusCompleted {
		t.Fatalf("expected job to be COMPLETED, got %+v", completed)
	}
	if completed.Error != "" {
		t.Fatalf("expected error to be cleared, got %q", completed.Error)
	}
}

type mockQueueStore struct {
	mu              sync.Mutex
	saved           map[string]*Job
	leases          map[string]int64
	completed       map[string]string
	failUpdateLease bool
}

func newMockQueueStore() *mockQueueStore {
	return &mockQueueStore{
		saved:     make(map[string]*Job),
		leases:    make(map[string]int64),
		completed: make(map[string]string),
	}
}

func (m *mockQueueStore) SaveQueueJob(ctx context.Context, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *job
	m.saved[job.ID] = &cp
	return nil
}

func (m *mockQueueStore) LoadQueueJobs(ctx context.Context) ([]*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Job
	for _, j := range m.saved {
		cp := *j
		out = append(out, &cp)
	}
	return out, nil
}

func (m *mockQueueStore) UpdateQueueJobLease(ctx context.Context, jobID string, leaseOwner string, leaseVersion int64, heartbeatAt, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failUpdateLease {
		return errors.New("simulated store failure")
	}
	m.leases[jobID] = leaseVersion
	if j, ok := m.saved[jobID]; ok {
		j.LeaseOwner = leaseOwner
		j.LeaseVersion = leaseVersion
		j.HeartbeatAt = &heartbeatAt
		j.LeaseExpiresAt = &expiresAt
	}
	return nil
}

func (m *mockQueueStore) CompleteQueueJob(ctx context.Context, jobID string, status string, result json.RawMessage, errStr string, completedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[jobID] = status
	if j, ok := m.saved[jobID]; ok {
		j.Status = status
		j.Result = result
		j.Error = errStr
		j.CompletedAt = &completedAt
	}
	return nil
}

func TestQueue_DurableQueueStore_RestartRehydrationAndFencing(t *testing.T) {
	ctx := context.Background()
	store := newMockQueueStore()

	q1 := NewInMemoryQueue()
	if err := q1.SetStore(store); err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	job, err := q1.Enqueue(ctx, &Job{
		Type:             JobTypePromptExecute,
		Repo:             "leaf-durable",
		NormalizedPrompt: "verify durable queue",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Verify job was persisted to store on enqueue
	store.mu.Lock()
	if _, ok := store.saved[job.ID]; !ok {
		t.Fatalf("expected job to be persisted in store on enqueue")
	}
	store.mu.Unlock()

	// Claim job on q1
	claimed, err := q1.Claim(ctx, "host-primary", []string{JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}
	if claimed.LeaseOwner != "host-primary" || claimed.LeaseVersion != claimed.FencingToken {
		t.Fatalf("expected lease_owner and lease_version to be set on claim, got owner=%s version=%d",
			claimed.LeaseOwner, claimed.LeaseVersion)
	}

	// Simulate complete API crash / restart by initializing a brand new queue instance
	q2 := NewInMemoryQueue()
	if err := q2.SetStore(store); err != nil {
		t.Fatalf("re-hydrating SetStore failed: %v", err)
	}

	// Verify q2 re-hydrated the claimed job from store with fencing token and status
	restored, ok := q2.Get(job.ID)
	if !ok || restored == nil {
		t.Fatalf("expected job to be re-hydrated from database in new queue instance")
	}
	if restored.Status != JobStatusLeased {
		t.Fatalf("expected re-hydrated status %s, got %s", JobStatusLeased, restored.Status)
	}
	if restored.FencingToken != claimed.FencingToken {
		t.Fatalf("expected fencing token %d, got %d", claimed.FencingToken, restored.FencingToken)
	}

	// Complete on q2
	res := json.RawMessage(`{"success":true}`)
	if err := q2.CompleteWithFencing(ctx, job.ID, restored.FencingToken, res, ""); err != nil {
		t.Fatalf("CompleteWithFencing on q2 failed: %v", err)
	}

	// Verify store updated with completed status
	store.mu.Lock()
	if store.completed[job.ID] != JobStatusCompleted {
		t.Fatalf("expected store to record completed status, got %s", store.completed[job.ID])
	}
	store.mu.Unlock()
}

func TestExtendLeasesForHost_FailsClosedOnStoreError(t *testing.T) {
	ctx := context.Background()
	store := newMockQueueStore()
	store.failUpdateLease = true

	q := NewInMemoryQueue()
	if err := q.SetStore(store); err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypePromptExecute,
		Repo: "leaf-failclosed",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	claimed, err := q.Claim(ctx, "host-1", nil)
	if err != nil || claimed == nil {
		t.Fatalf("claim failed: %v", err)
	}

	originalExpires := *claimed.LeaseExpiresAt

	// Advance clock slightly (within lease window)
	now := claimed.HeartbeatAt.Add(10 * time.Second)
	q.SetNowFunc(func() time.Time { return now })

	extended, err := q.ExtendLeasesForHost(ctx, "host-1", []string{job.ID})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(extended) != 0 {
		t.Fatalf("expected 0 extended jobs when store fails, got %d", len(extended))
	}

	// Verify in-memory expiration was not extended to now + DefaultHeartbeatTimeout
	j, ok := q.Get(job.ID)
	if !ok || j == nil {
		t.Fatalf("expected job %s to exist in queue", job.ID)
	}
	if j.LeaseExpiresAt == nil {
		t.Fatalf("expected non-nil LeaseExpiresAt")
	}
	if !j.LeaseExpiresAt.Equal(originalExpires) {
		t.Fatalf("expected lease expiration %v to remain unchanged on store failure, got %v", originalExpires, *j.LeaseExpiresAt)
	}
}

func TestInMemoryQueue_BindStoryWithFencing_And_HeartbeatWithStory(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()

	if _, err := q.Enqueue(ctx, &Job{
		Type:        JobTypePromptExecute,
		Repo:        "test-repo",
		TargetAgent: "agy",
		StoryID:     "pending-allocation",
	}); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	claimed, err := q.Claim(ctx, "host-worker-1", []string{JobTypePromptExecute})
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}
	if claimed.FencingToken <= 0 {
		t.Fatalf("expected positive fencing token, got %d", claimed.FencingToken)
	}

	// 1. Heartbeat with story updates story atomically
	hbJob, err := q.HeartbeatWithStory(ctx, claimed.ID, claimed.FencingToken, "STORY-GOV-DYNAMIC-1")
	if err != nil {
		t.Fatalf("HeartbeatWithStory failed: %v", err)
	}
	if hbJob.StoryID != "STORY-GOV-DYNAMIC-1" {
		t.Errorf("expected story ID STORY-GOV-DYNAMIC-1, got %q", hbJob.StoryID)
	}

	// 2. Heartbeat with stale fencing token rejected
	_, err = q.HeartbeatWithStory(ctx, claimed.ID, claimed.FencingToken+10, "STORY-STALE")
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for stale heartbeat, got %v", err)
	}

	// 3. BindStory with stale fencing token rejected
	_, err = q.BindStoryWithFencing(ctx, claimed.ID, "STORY-STALE-2", "Title", "path", claimed.FencingToken+10)
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for stale BindStoryWithFencing, got %v", err)
	}

	// 4. BindStory with valid fencing token succeeds
	boundJob, err := q.BindStoryWithFencing(ctx, claimed.ID, "STORY-GOV-FINAL", "Final Title", "docs/stories/final.story.md", claimed.FencingToken)
	if err != nil {
		t.Fatalf("BindStoryWithFencing failed: %v", err)
	}
	if boundJob.StoryID != "STORY-GOV-FINAL" {
		t.Errorf("expected STORY-GOV-FINAL, got %q", boundJob.StoryID)
	}

	// 5. BindStory with zero/omitted fencing token on claimed job is rejected (no bypass)
	_, err = q.BindStoryWithFencing(ctx, claimed.ID, "STORY-BYPASS", "Bypass Title", "docs/stories/bypass.story.md", 0)
	if !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("expected ErrLeaseFenced for zero fencing token on claimed job, got %v", err)
	}
}

func TestReconcileGlobalQueueSlotsReclaimsStaleLeasedAndRunningJobs(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.SetNowFunc(func() time.Time { return now })

	jLeased, err := q.Enqueue(ctx, &Job{
		ID:          "job-leased-stale",
		Type:        JobTypePromptExecute,
		Repo:        "iazio-mcp",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue leased failed: %v", err)
	}
	claimedLeased, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimedLeased == nil {
		t.Fatalf("claim leased failed: %v", err)
	}
	if claimedLeased.Status != JobStatusLeased {
		t.Fatalf("expected status LEASED after Claim, got %s", claimedLeased.Status)
	}

	jRunning, err := q.Enqueue(ctx, &Job{
		ID:          "job-running-stale",
		Type:        JobTypePromptExecute,
		Repo:        "iazio-web",
		TargetAgent: "agy",
	})
	if err != nil {
		t.Fatalf("enqueue running failed: %v", err)
	}
	claimedRunning, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimedRunning == nil {
		t.Fatalf("claim running failed: %v", err)
	}
	if _, err := q.AckLease(ctx, claimedRunning.ID, claimedRunning.LeaseToken, 4321, ""); err != nil {
		t.Fatalf("AckLease failed: %v", err)
	}

	// Advance clock past lease expiry and reconcile global queue slots
	now = now.Add(DefaultHeartbeatTimeout + 5*time.Second)
	reclaimedIDs := q.ReconcileGlobalQueueSlots(now, 45*time.Second)
	if len(reclaimedIDs) != 2 {
		t.Fatalf("expected ReconcileGlobalQueueSlots to reclaim 2 stale jobs (LEASED + RUNNING), got %d (%v)", len(reclaimedIDs), reclaimedIDs)
	}

	afterLeased, _ := q.Get(jLeased.ID)
	if afterLeased.Status != JobStatusPending || afterLeased.ClaimedHost != "" || afterLeased.LeaseToken != "" {
		t.Fatalf("expected reclaimed LEASED job to reset to pending with cleared host/token, got %+v", afterLeased)
	}
	afterRunning, _ := q.Get(jRunning.ID)
	if afterRunning.Status != JobStatusPending || afterRunning.ClaimedHost != "" || afterRunning.LeaseToken != "" {
		t.Fatalf("expected reclaimed RUNNING job to reset to pending with cleared host/token, got %+v", afterRunning)
	}
}

type fakeQueueStore struct {
	mu    sync.Mutex
	saved map[string]*Job
}

func (f *fakeQueueStore) SaveQueueJob(_ context.Context, j *Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saved == nil {
		f.saved = make(map[string]*Job)
	}
	cp := *j
	f.saved[j.ID] = &cp
	return nil
}

func (f *fakeQueueStore) LoadQueueJobs(_ context.Context) ([]*Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*Job
	for _, j := range f.saved {
		cp := *j
		out = append(out, &cp)
	}
	return out, nil
}

func (f *fakeQueueStore) UpdateQueueJobLease(_ context.Context, _ string, _ string, _ int64, _, _ time.Time) error {
	return nil
}

func (f *fakeQueueStore) CompleteQueueJob(_ context.Context, _ string, _ string, _ json.RawMessage, _ string, _ time.Time) error {
	return nil
}

func TestQueue_RetryPreservesMetadataAndMarksRequeued(t *testing.T) {
	ctx := context.Background()
	store := &fakeQueueStore{}
	q := NewInMemoryQueue()
	if err := q.SetStore(store); err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	orig, err := q.Enqueue(ctx, &Job{
		Type:                    JobTypePromptExecute,
		Repo:                    "iazio-mcp",
		TargetHost:              "mac-mini",
		TargetAgent:             "agy",
		StoryID:                 "STORY-GOV-3797",
		StoryTitle:              "Forensic v2 Remediation",
		StoryPath:               "docs/stories/2026-10-30-gov-3797.story.md",
		StoryAllocationStatus:   "allocated",
		AllocationSource:        "explicit",
		PriorityRankSelected:    1,
		SourceConversationTitle: "Audit Conversation",
		NormalizedPrompt:        "/autonomous-story-execution STORY-GOV-3797",
		EffectiveAction:         "/autonomous-story-execution",
		AgentAffinity:           &AgentAffinity{HostID: "mac-mini", AgentName: "agy"},
		Payload:                 json.RawMessage(`{"story_id":"STORY-GOV-3797"}`),
		TraceID:                 "trace-v2-001",
		SearchID:                "search-v2-001",
		Retriable:               true,
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	_ = q.Complete(ctx, orig.ID, nil, "transient network failure")

	retried, err := q.Retry(ctx, orig.ID)
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	if retried.StoryID != "STORY-GOV-3797" || retried.StoryTitle != "Forensic v2 Remediation" || retried.StoryPath != "docs/stories/2026-10-30-gov-3797.story.md" {
		t.Fatalf("expected Story metadata preserved on Retry, got %+v", retried)
	}
	if retried.NormalizedPrompt != "/autonomous-story-execution STORY-GOV-3797" || retried.EffectiveAction != "/autonomous-story-execution" {
		t.Fatalf("expected NormalizedPrompt and EffectiveAction preserved on Retry, got %+v", retried)
	}
	if retried.ParentID != orig.ID || !retried.Retriable || retried.AgentAffinity == nil || retried.AgentAffinity.HostID != "mac-mini" {
		t.Fatalf("expected ParentID=%s, Retriable=true, AgentAffinity preserved, got %+v", orig.ID, retried)
	}

	origAfter, _ := q.Get(orig.ID)
	if origAfter.Status != JobStatusRequeued {
		t.Fatalf("expected original job status %s after Retry, got %s", JobStatusRequeued, origAfter.Status)
	}
}

func TestQueue_SetFinalHarvestPersistsToStore(t *testing.T) {
	ctx := context.Background()
	store := &fakeQueueStore{}
	q := NewInMemoryQueue()
	if err := q.SetStore(store); err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	job, err := q.Enqueue(ctx, &Job{
		Type: JobTypePromptExecute,
		Repo: "iazio-harness-api",
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	_ = q.Complete(ctx, job.ID, json.RawMessage(`{"status":"ok"}`), "")
	q.SetFinalHarvest(job.ID, 14, "sha256-final-transcript-abc123")

	store.mu.Lock()
	persisted := store.saved[job.ID]
	store.mu.Unlock()
	if persisted == nil {
		t.Fatal("expected job to be persisted in QueueStore")
	}
	if persisted.FinalTurnCount != 14 || persisted.FinalTranscriptSHA256 != "sha256-final-transcript-abc123" {
		t.Fatalf("expected FinalTurnCount=14 and FinalTranscriptSHA256 persisted in store, got turnCount=%d sha=%s",
			persisted.FinalTurnCount, persisted.FinalTranscriptSHA256)
	}
}



