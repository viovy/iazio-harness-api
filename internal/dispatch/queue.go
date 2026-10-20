// Package dispatch provides in-memory and distributed job queue mechanisms
// for dispatching prompt execution and story scaffolding jobs to host agents.
package dispatch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
}

// InMemoryQueue is a thread-safe in-memory implementation of Queue.
type InMemoryQueue struct {
	mu   sync.Mutex
	jobs map[string]*Job
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
	}

	done := job.doneCh
	q.mu.Unlock()

	if done != nil {
		close(done)
	}
	return nil
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

func generateJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("job_%s", hex.EncodeToString(b))
}
