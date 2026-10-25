// Package dispatch provides in-memory and distributed job queue mechanisms
// for dispatching prompt execution and story scaffolding jobs to host agents.
package dispatch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Standard job types.
const (
	JobTypeStoryCreate   = "story.create"
	JobTypePromptExecute = "prompt.execute"
)

// Standard job statuses.
const (
	JobStatusPending   = "pending"
	JobStatusClaimed   = "claimed"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
	JobStatusCanceled  = "canceled"
	JobStatusStalled   = "stalled"
)

const (
	// DefaultClaimTimeout is the duration a claimed job can remain active before lease expiration.
	DefaultClaimTimeout = 30 * time.Minute
	// DefaultHeartbeatTimeout is the maximum duration between runner heartbeats before considering a runner stalled (90s, allowing >=6 retry margins).
	DefaultHeartbeatTimeout = 90 * time.Second
	// DefaultJobTimeout is the maximum total duration of a job before execution timeout (0 = unbounded).
	DefaultJobTimeout = 0
	// DefaultMaxAttempts is the maximum number of times a stalled job may be reclaimed.
	DefaultMaxAttempts = 3
)

// EligibleHost describes an agent/workspace candidate evaluated for job dispatch.
type EligibleHost struct {
	Host      string `json:"host"`
	Workspace string `json:"workspace"`
	Tool      string `json:"tool"`
	Status    string `json:"status,omitempty"`
}

// DispatchDiagnostics contains real-time dispatch eligibility and blockage diagnostics for a queue job.
type DispatchDiagnostics struct {
	QueueState               string            `json:"queue_state"` // queued, dispatching, running, completed, failed
	EligibleHosts            []EligibleHost    `json:"eligible_hosts"`
	LastDispatchEvaluationAt string            `json:"last_dispatch_evaluation_at,omitempty"`
	BlockedReasonsByHost     map[string]string `json:"blocked_reasons_by_host,omitempty"`
}

var (
	// ErrJobNotFound indicates the requested job ID was not found.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobAlreadyCompleted indicates the job has already reached a terminal state.
	ErrJobAlreadyCompleted = errors.New("job already completed")
	// ErrTimeout indicates an await operation timed out before job completion.
	ErrTimeout = errors.New("dispatch await timeout")
	// ErrLeaseFenced indicates a superseded worker attempted to mutate job state.
	ErrLeaseFenced = errors.New("lease fenced: stale fencing token presented")
)

// AgentAffinity defines binding constraints for workers claiming jobs.
type AgentAffinity struct {
	HostID               string   `json:"host_id,omitempty"`
	AgentName            string   `json:"agent_name,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
}

// Job represents an asynchronous task dispatched to an iazio-agent daemon.
type Job struct {
	ID               string          `json:"id"`
	Type             string          `json:"type"`
	Repo             string          `json:"repo"`
	TargetHost       string          `json:"target_host,omitempty"`
	ClaimedHost      string          `json:"claimed_host,omitempty"`
	HostID           string          `json:"host_id,omitempty"`
	TargetAgent             string          `json:"target_agent,omitempty"`
	StoryID                 string          `json:"story_id,omitempty"`
	StoryTitle              string          `json:"story_title,omitempty"`
	StoryPath               string          `json:"story_path,omitempty"`
	StoryAllocationStatus   string          `json:"story_allocation_status,omitempty"`
	AllocationSource        string          `json:"allocation_source,omitempty"`
	PriorityRankSelected    int             `json:"priority_rank_selected,omitempty"`
	SourceConversationTitle string          `json:"source_conversation_title,omitempty"`
	NormalizedPrompt        string          `json:"normalized_prompt,omitempty"`
	EffectiveAction         string          `json:"effective_action,omitempty"`
	AgentAffinity           *AgentAffinity  `json:"agent_affinity,omitempty"`
	Payload          json.RawMessage `json:"payload"`
	Status           string          `json:"status"`
	Result           json.RawMessage `json:"result,omitempty"`
	Error            string          `json:"error,omitempty"`
	TraceID          string          `json:"trace_id,omitempty"`
	ParentID         string          `json:"parent_id,omitempty"`
	SearchID         string          `json:"search_id,omitempty"`
	RelevanceEmitted bool            `json:"relevance_emitted"`
	PromotedMemories []string        `json:"promoted_memories,omitempty"`
	FencingToken     int64           `json:"fencing_token"`
	Attempts         int             `json:"attempts"`
	CreatedAt        time.Time       `json:"created_at"`
	ClaimedAt        *time.Time      `json:"claimed_at,omitempty"`
	HeartbeatAt      *time.Time      `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt   *time.Time      `json:"lease_expires_at,omitempty"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
	DispatchDiagnostics *DispatchDiagnostics `json:"dispatch_diagnostics,omitempty"`

	// doneCh signals when the job has been completed or failed.
	doneCh chan struct{}
}

// JobFilter specifies filtering criteria for listing jobs.
type JobFilter struct {
	Repo   string `json:"repo,omitempty"`
	Status string `json:"status,omitempty"`
	Type   string `json:"type,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// Queue defines the contract for enqueuing, claiming, completing, and awaiting agent jobs.
type Queue interface {
	Enqueue(ctx context.Context, job *Job) (*Job, error)
	Claim(ctx context.Context, hostID string, jobTypes []string) (*Job, error)
	Heartbeat(ctx context.Context, jobID string, fencingToken int64) (*Job, error)
	Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error
	CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error
	Get(jobID string) (*Job, bool)
	List(ctx context.Context, filter JobFilter) ([]*Job, error)
	Cancel(ctx context.Context, jobID string, reason string) (*Job, error)
	Retry(ctx context.Context, jobID string) (*Job, error)
	BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error)
	Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error)
	GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error)
	ReapExpired(now time.Time) []string
	StartReaper(ctx context.Context, interval time.Duration)
	ReconcileGlobalQueueSlots(now time.Time, staleGrace time.Duration) []string
	SetDispatchDiagnostics(jobID string, diag *DispatchDiagnostics) error
	ForceDispatch(ctx context.Context, jobID string) (*Job, error)
}

// JobAttributionResponse models the attribution of a job outcome to retrieval context.
type JobAttributionResponse struct {
	JobID            string     `json:"job_id"`
	TraceID          string     `json:"trace_id,omitempty"`
	SearchID         string     `json:"search_id,omitempty"`
	Status           string     `json:"status"`
	RelevanceEmitted bool       `json:"relevance_emitted"`
	PromotedMemories []string   `json:"promoted_memories"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
}

// InMemoryQueue is a thread-safe in-memory implementation of Queue.
type InMemoryQueue struct {
	mu             sync.Mutex
	jobs           map[string]*Job
	fencingSeq     int64
	attributionURL string
	httpClient     *http.Client
	claimTimeout   time.Duration
	jobTimeout     time.Duration
	nowFunc        func() time.Time
}

// SetAttributionConfig sets the URL and optional client used for relevance attribution dispatches.
func (q *InMemoryQueue) SetAttributionConfig(apiURL string, client *http.Client) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.attributionURL = apiURL
	q.httpClient = client
}

// NewInMemoryQueue constructs a new thread-safe InMemoryQueue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		jobs:         make(map[string]*Job),
		claimTimeout: DefaultClaimTimeout,
		jobTimeout:   DefaultJobTimeout,
		nowFunc:      func() time.Time { return time.Now().UTC() },
	}
}

// SetTimeouts overrides default lease and execution timeouts for this queue.
func (q *InMemoryQueue) SetTimeouts(claimTimeout, jobTimeout time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if claimTimeout > 0 {
		q.claimTimeout = claimTimeout
	}
	if jobTimeout > 0 {
		q.jobTimeout = jobTimeout
	}
}

// SetNowFunc overrides the clock for testing deterministic lease expiration.
func (q *InMemoryQueue) SetNowFunc(fn func() time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if fn != nil {
		q.nowFunc = fn
	}
}

func (q *InMemoryQueue) now() time.Time {
	if q.nowFunc != nil {
		return q.nowFunc()
	}
	return time.Now().UTC()
}

func (q *InMemoryQueue) reapExpiredLocked(now time.Time) []string {
	if now.IsZero() {
		now = q.now()
	}
	var expiredIDs []string
	var doneChannels []chan struct{}

	for _, j := range q.jobs {
		if j.Status != JobStatusPending && j.Status != JobStatusClaimed {
			continue
		}

		// 1. Max execution timeout: from CreatedAt (0 = unbounded, stories immune)
		isStory := j.StoryID != "" || j.Type == "story" || j.Type == "refinement"
		if !isStory && q.jobTimeout > 0 && !j.CreatedAt.IsZero() && now.Sub(j.CreatedAt) > q.jobTimeout {
			j.Status = JobStatusFailed
			j.Error = "EXECUTION_TIMEOUT: job exceeded max execution duration"
			completedAt := now
			j.CompletedAt = &completedAt
			expiredIDs = append(expiredIDs, j.ID)
			if j.doneCh != nil {
				doneChannels = append(doneChannels, j.doneCh)
			}
			continue
		}

		// 2. Claim lease & heartbeat timeout:
		if j.Status == JobStatusClaimed {
			if q.claimTimeout > 0 && j.ClaimedAt != nil && !j.ClaimedAt.IsZero() && now.Sub(*j.ClaimedAt) > q.claimTimeout {
				j.Status = JobStatusFailed
				j.Error = "LEASE_EXPIRED: execution lease expired without completion"
				completedAt := now
				j.CompletedAt = &completedAt
				expiredIDs = append(expiredIDs, j.ID)
				if j.doneCh != nil {
					doneChannels = append(doneChannels, j.doneCh)
				}
				continue
			}

			if j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.IsZero() && now.After(*j.LeaseExpiresAt) {
				if j.Attempts < DefaultMaxAttempts {
					// Safe runner recovery: revert to pending for retry without duplicate execution
					j.Status = JobStatusPending
					j.ClaimedHost = ""
					j.HostID = ""
					j.ClaimedAt = nil
					j.HeartbeatAt = nil
					j.LeaseExpiresAt = nil
					expiredIDs = append(expiredIDs, j.ID)
				} else {
					j.Status = JobStatusFailed
					j.Error = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, max retry attempts exceeded"
					completedAt := now
					j.CompletedAt = &completedAt
					expiredIDs = append(expiredIDs, j.ID)
					if j.doneCh != nil {
						doneChannels = append(doneChannels, j.doneCh)
					}
				}
				continue
			}
		}
	}

	for _, ch := range doneChannels {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	return expiredIDs
}

// ReapExpired runs an expiration sweep across all pending and claimed jobs.
func (q *InMemoryQueue) ReapExpired(now time.Time) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reapExpiredLocked(now)
}

// StartReaper runs background expiration sweeps periodically until context is cancelled.
func (q *InMemoryQueue) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				q.ReapExpired(now.UTC())
			}
		}
	}()
}

// Enqueue adds a new job to the queue. If ID is empty, a random ID is generated.
func (q *InMemoryQueue) Enqueue(ctx context.Context, job *Job) (*Job, error) {
	if job == nil {
		return nil, errors.New("job must not be nil")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if job.ID == "" {
		job.ID = generateJobID()
	}
	if job.Status == "" {
		job.Status = JobStatusPending
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = q.now()
	}
	job.doneCh = make(chan struct{})

	// Store copy
	q.jobs[job.ID] = job
	return job, nil
}

// Claim finds the oldest pending job matching hostID and jobTypes, marking it claimed.
func (q *InMemoryQueue) Claim(ctx context.Context, hostID string, jobTypes []string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapExpiredLocked(q.now())

	typeFilter := make(map[string]bool)
	for _, t := range jobTypes {
		typeFilter[t] = true
	}

	var oldest *Job
	for _, j := range q.jobs {
		if j.Status != JobStatusPending {
			continue
		}
		if len(typeFilter) > 0 && !typeFilter[j.Type] {
			continue
		}
		if j.TargetHost != "" && hostID != "" && j.TargetHost != hostID {
			continue
		}
		if j.AgentAffinity != nil && j.AgentAffinity.HostID != "" && hostID != "" && j.AgentAffinity.HostID != hostID {
			continue
		}
		if oldest == nil || j.CreatedAt.Before(oldest.CreatedAt) {
			oldest = j
		}
	}

	if oldest == nil {
		return nil, nil
	}

	now := q.now()
	q.fencingSeq++
	oldest.FencingToken = q.fencingSeq
	oldest.Attempts++
	oldest.Status = JobStatusClaimed
	if hostID != "" {
		oldest.ClaimedHost = hostID
		oldest.HostID = hostID
	}
	oldest.ClaimedAt = &now
	oldest.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	oldest.LeaseExpiresAt = &expires
	return oldest, nil
}

// Heartbeat extends the lease expiration of an in-flight claimed job, validating the worker's fencing token.
func (q *InMemoryQueue) Heartbeat(ctx context.Context, jobID string, fencingToken int64) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if job.Status != JobStatusClaimed {
		return nil, fmt.Errorf("job %s is not currently claimed (status: %s)", jobID, job.Status)
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		return nil, ErrLeaseFenced
	}

	now := q.now()
	job.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	job.LeaseExpiresAt = &expires
	return job, nil
}

// Complete updates a job with final result or error and signals any awaiting callers.
func (q *InMemoryQueue) Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	fencingToken := int64(0)
	if exists && job != nil {
		fencingToken = job.FencingToken
	}
	q.mu.Unlock()
	return q.CompleteWithFencing(ctx, jobID, fencingToken, result, errStr)
}

// CompleteWithFencing validates the fencing token and updates the job with final result or error.
func (q *InMemoryQueue) CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		q.mu.Unlock()
		return ErrJobAlreadyCompleted
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		q.mu.Unlock()
		return ErrLeaseFenced
	}

	now := q.now()
	job.CompletedAt = &now
	if errStr != "" {
		job.Status = JobStatusFailed
		job.Error = errStr
	} else {
		job.Status = JobStatusCompleted
		job.Result = result
	}

	searchID := job.SearchID
	traceID := job.TraceID
	var promoted []string
	if job.Status == JobStatusCompleted && len(result) > 0 {
		var resMap map[string]any
		if err := json.Unmarshal(result, &resMap); err == nil {
			if mems, ok := resMap["promoted_memories"].([]any); ok {
				for _, m := range mems {
					if str, ok := m.(string); ok && str != "" {
						promoted = append(promoted, str)
					}
				}
			}
		}
	}
	if len(promoted) == 0 && searchID != "" && job.Status == JobStatusCompleted {
		promoted = []string{"mem-" + searchID}
	}
	job.PromotedMemories = promoted
	if searchID != "" && job.Status == JobStatusCompleted {
		job.RelevanceEmitted = true
	}

	done := job.doneCh
	q.mu.Unlock()

	if done != nil {
		close(done)
	}

	if (searchID != "" && job.Status == JobStatusCompleted) || len(promoted) > 0 {
		go q.dispatchAttribution(searchID, traceID, promoted)
	}

	return nil
}

func (q *InMemoryQueue) dispatchAttribution(searchID, traceID string, promoted []string) {
	q.mu.Lock()
	apiURL := q.attributionURL
	client := q.httpClient
	q.mu.Unlock()

	if apiURL == "" {
		apiURL = os.Getenv("IAZIO_API_URL")
	}
	if apiURL == "" {
		apiURL = "http://127.0.0.1:8080"
	}
	apiURL = strings.TrimRight(apiURL, "/")

	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}

	// 1. Emit positive relevance attribution if searchID present
	if searchID != "" {
		payload, err := json.Marshal(map[string]any{
			"search_id": searchID,
			"outcome":   "positive",
			"trace_id":  traceID,
		})
		if err == nil {
			req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/relevance", bytes.NewReader(payload))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if traceID != "" {
					req.Header.Set("traceparent", fmt.Sprintf("00-%s-0000000000000001-01", traceID))
				}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("[dispatchAttribution] failed to post relevance attribution for search_id %s: %v", searchID, err)
				} else {
					defer resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
						log.Printf("[dispatchAttribution] unexpected status code %d for search_id %s: %s", resp.StatusCode, searchID, string(body))
					}
				}
			}
		}
	}

	// 2. Promote verified memories
	for _, memID := range promoted {
		payload, err := json.Marshal(map[string]any{
			"id":     memID,
			"tier":   "canonical",
			"reason": "verified harness execution completion",
		})
		if err == nil {
			req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/memories/promote", bytes.NewReader(payload))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if traceID != "" {
					req.Header.Set("traceparent", fmt.Sprintf("00-%s-0000000000000001-01", traceID))
				}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("[dispatchAttribution] failed to promote memory %s: %v", memID, err)
				} else {
					defer resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
						log.Printf("[dispatchAttribution] unexpected status code %d promoting memory %s: %s", resp.StatusCode, memID, string(body))
					}
				}
			}
		}
	}
}

// Get retrieves a job by ID, lazily reaping any expired jobs.
func (q *InMemoryQueue) Get(jobID string) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reapExpiredLocked(q.now())
	j, ok := q.jobs[jobID]
	return j, ok
}

// List returns jobs matching the filter, sorted by CreatedAt descending, lazily reaping expired jobs.
func (q *InMemoryQueue) List(ctx context.Context, filter JobFilter) ([]*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapExpiredLocked(q.now())

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	var matched []*Job
	for _, j := range q.jobs {
		if filter.Repo != "" && j.Repo != filter.Repo {
			continue
		}
		if filter.Status != "" && filter.Status != "all" && j.Status != filter.Status {
			continue
		}
		if filter.Type != "" && j.Type != filter.Type {
			continue
		}
		matched = append(matched, j)
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

// Cancel terminates a pending or claimed job.
func (q *InMemoryQueue) Cancel(ctx context.Context, jobID string, reason string) (*Job, error) {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		q.mu.Unlock()
		return nil, ErrJobAlreadyCompleted
	}

	now := q.now()
	job.Status = JobStatusCanceled
	if reason == "" {
		reason = "cancelled by user"
	}
	job.Error = reason
	job.CompletedAt = &now
	done := job.doneCh
	q.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	return job, nil
}

// Retry re-enqueues a job by creating a fresh pending job with the same parameters.
func (q *InMemoryQueue) Retry(ctx context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	orig, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}

	newJob := &Job{
		ID:          generateJobID(),
		Type:        orig.Type,
		Repo:        orig.Repo,
		TargetHost:  orig.TargetHost,
		TargetAgent: orig.TargetAgent,
		Payload:     orig.Payload,
		TraceID:     orig.TraceID,
		SearchID:    orig.SearchID,
		Status:      JobStatusPending,
		CreatedAt:   q.now(),
		doneCh:      make(chan struct{}),
	}
	q.jobs[newJob.ID] = newJob
	q.mu.Unlock()

	return newJob, nil
}

// BindStory binds a newly allocated story ID and metadata to a job.
func (q *InMemoryQueue) BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}

	cleanStoryID := strings.TrimSpace(storyID)
	if cleanStoryID == "" {
		return nil, errors.New("story_id is required")
	}

	job.StoryID = cleanStoryID
	if storyTitle != "" {
		job.StoryTitle = storyTitle
	}
	if storyPath != "" {
		job.StoryPath = storyPath
	}
	job.StoryAllocationStatus = "allocated"

	if len(job.Payload) > 0 {
		var payloadMap map[string]any
		if err := json.Unmarshal(job.Payload, &payloadMap); err == nil {
			payloadMap["story_id"] = cleanStoryID
			if storyTitle != "" {
				payloadMap["title"] = fmt.Sprintf("Execute story %s: %s", cleanStoryID, storyTitle)
			}
			if updated, err := json.Marshal(payloadMap); err == nil {
				job.Payload = updated
			}
		}
	}

	return job, nil
}

// Await blocks until the specified job completes, the context is cancelled, or the timeout expires.
func (q *InMemoryQueue) Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error) {
	q.mu.Lock()
	q.reapExpiredLocked(q.now())
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed {
		q.mu.Unlock()
		return job, nil
	}
	done := job.doneCh
	q.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	case <-done:
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.jobs[jobID], nil
	}
}

// SetDispatchDiagnostics updates the dispatch diagnostics on a specific job.
func (q *InMemoryQueue) SetDispatchDiagnostics(jobID string, diag *DispatchDiagnostics) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	job.DispatchDiagnostics = diag
	return nil
}

// ForceDispatch breaks any hung or stale claim on a job, re-queues it, and marks it ready for immediate allocation.
func (q *InMemoryQueue) ForceDispatch(ctx context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		return nil, ErrJobAlreadyCompleted
	}

	now := q.now()
	job.Status = JobStatusPending
	job.ClaimedHost = ""
	job.HostID = ""
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.FencingToken = 0
	if job.DispatchDiagnostics == nil {
		job.DispatchDiagnostics = &DispatchDiagnostics{}
	}
	job.DispatchDiagnostics.QueueState = "dispatching"
	job.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)

	return job, nil
}

// ReconcileGlobalQueueSlots detects and reclaims stale leases (> staleGrace) across all claimed jobs.
func (q *InMemoryQueue) ReconcileGlobalQueueSlots(now time.Time, staleGrace time.Duration) []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if now.IsZero() {
		now = q.now()
	}
	if staleGrace <= 0 {
		staleGrace = 45 * time.Second
	}

	var reclaimedIDs []string
	for _, j := range q.jobs {
		if j.Status == JobStatusClaimed {
			stale := false
			if j.HeartbeatAt != nil && now.Sub(*j.HeartbeatAt) > staleGrace {
				stale = true
			}
			if j.LeaseExpiresAt != nil && now.After(*j.LeaseExpiresAt) {
				stale = true
			}
			if stale {
				if j.Attempts < DefaultMaxAttempts {
					j.Status = JobStatusPending
					j.ClaimedHost = ""
					j.HostID = ""
					j.ClaimedAt = nil
					j.HeartbeatAt = nil
					j.LeaseExpiresAt = nil
					j.FencingToken = 0
					if j.DispatchDiagnostics == nil {
						j.DispatchDiagnostics = &DispatchDiagnostics{}
					}
					j.DispatchDiagnostics.QueueState = "queued"
					j.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)
					if j.DispatchDiagnostics.BlockedReasonsByHost == nil {
						j.DispatchDiagnostics.BlockedReasonsByHost = make(map[string]string)
					}
					j.DispatchDiagnostics.BlockedReasonsByHost["lease"] = fmt.Sprintf("reclaimed from %s after heartbeat timeout > %v", j.ClaimedHost, staleGrace)
					reclaimedIDs = append(reclaimedIDs, j.ID)
				} else {
					j.Status = JobStatusFailed
					j.Error = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, max retry attempts exceeded"
					completedAt := now
					j.CompletedAt = &completedAt
					reclaimedIDs = append(reclaimedIDs, j.ID)
					if j.doneCh != nil {
						select {
						case <-j.doneCh:
						default:
							close(j.doneCh)
						}
					}
				}
			}
		}
	}

	return reclaimedIDs
}

// GetAttribution returns attribution details linking a completed job to its retrieval context.
func (q *InMemoryQueue) GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return JobAttributionResponse{}, ErrJobNotFound
	}
	promoted := job.PromotedMemories
	if promoted == nil {
		promoted = []string{}
	}
	return JobAttributionResponse{
		JobID:            job.ID,
		TraceID:          job.TraceID,
		SearchID:         job.SearchID,
		Status:           job.Status,
		RelevanceEmitted: job.RelevanceEmitted,
		PromotedMemories: promoted,
		CompletedAt:      job.CompletedAt,
	}, nil
}

func generateJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("job_%s", hex.EncodeToString(b))
}
