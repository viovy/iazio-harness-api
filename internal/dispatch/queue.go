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
)

var (
	// ErrJobNotFound indicates the requested job ID was not found.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobAlreadyCompleted indicates the job has already reached a terminal state.
	ErrJobAlreadyCompleted = errors.New("job already completed")
	// ErrTimeout indicates an await operation timed out before job completion.
	ErrTimeout = errors.New("dispatch await timeout")
)

// Job represents an asynchronous task dispatched to an iazio-agent daemon.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Repo        string          `json:"repo"`
	TargetHost  string          `json:"target_host,omitempty"`
	TargetAgent string          `json:"target_agent,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	TraceID     string          `json:"trace_id,omitempty"`
	ParentID    string          `json:"parent_id,omitempty"`
	SearchID    string          `json:"search_id,omitempty"`
	RelevanceEmitted bool       `json:"relevance_emitted"`
	PromotedMemories []string   `json:"promoted_memories,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	ClaimedAt   *time.Time      `json:"claimed_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`

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
	Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error
	Get(jobID string) (*Job, bool)
	List(ctx context.Context, filter JobFilter) ([]*Job, error)
	Cancel(ctx context.Context, jobID string, reason string) (*Job, error)
	Retry(ctx context.Context, jobID string) (*Job, error)
	Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error)
	GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error)
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
	attributionURL string
	httpClient     *http.Client
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
		jobs: make(map[string]*Job),
	}
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
		job.CreatedAt = time.Now().UTC()
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
		if oldest == nil || j.CreatedAt.Before(oldest.CreatedAt) {
			oldest = j
		}
	}

	if oldest == nil {
		return nil, nil
	}

	now := time.Now().UTC()
	oldest.Status = JobStatusClaimed
	oldest.ClaimedAt = &now
	return oldest, nil
}

// Complete updates a job with final result or error and signals any awaiting callers.
func (q *InMemoryQueue) Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed {
		q.mu.Unlock()
		return ErrJobAlreadyCompleted
	}

	now := time.Now().UTC()
	job.CompletedAt = &now
	if errStr != "" {
		job.Status = JobStatusFailed
		job.Error = errStr
	} else {
		job.Status = JobStatusCompleted
		job.Result = result
		// Execution-to-memory attribution: automatically record positive relevance and promote
		if job.SearchID != "" || job.TraceID != "" {
			job.RelevanceEmitted = true
			if len(job.PromotedMemories) == 0 && job.SearchID != "" {
				job.PromotedMemories = []string{"mem-" + job.SearchID}
			}
			go q.dispatchAttribution(job.SearchID, job.TraceID, job.PromotedMemories)
		}
	}

	done := job.doneCh
	q.mu.Unlock()

	if done != nil {
		close(done)
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
		payload, _ := json.Marshal(map[string]any{
			"search_id": searchID,
			"outcome":   1,
			"trace_id":  traceID,
		})
		req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/relevance", bytes.NewReader(payload))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}
	}

	// 2. Promote verified memories
	for _, memID := range promoted {
		payload, _ := json.Marshal(map[string]any{
			"id":     memID,
			"target": "canonical",
		})
		req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/memories/promote", bytes.NewReader(payload))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}
	}
}

// Get retrieves a job by ID.
func (q *InMemoryQueue) Get(jobID string) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[jobID]
	return j, ok
}

// List returns jobs matching the filter, sorted by CreatedAt descending.
func (q *InMemoryQueue) List(ctx context.Context, filter JobFilter) ([]*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

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

	now := time.Now().UTC()
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
		CreatedAt:   time.Now().UTC(),
		doneCh:      make(chan struct{}),
	}
	q.jobs[newJob.ID] = newJob
	q.mu.Unlock()

	return newJob, nil
}

// Await blocks until the specified job completes, the context is cancelled, or the timeout expires.
func (q *InMemoryQueue) Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error) {
	q.mu.Lock()
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
