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
		Repo:        "/work/meta-repo",
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
		Repo:        "/work/meta-repo",
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

