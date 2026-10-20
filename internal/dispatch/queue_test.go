package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	if claimed.Status != JobStatusClaimed {
		t.Fatalf("expected status claimed, got %s", claimed.Status)
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
