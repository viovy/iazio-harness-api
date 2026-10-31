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

	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

// Standard job types.
const (
	JobTypeStoryCreate   = "story.create"
	JobTypeStoryExecute  = "story.execute"
	JobTypePromptExecute = "prompt.execute"
)

// Standard job statuses.
const (
	JobStatusPending   = "pending"
	JobStatusLeased    = "leased"
	JobStatusClaimed   = "claimed"
	JobStatusRunning   = "running"
	JobStatusVerifying = "verifying"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
	JobStatusCanceled  = "canceled"
	JobStatusStalled   = "stalled"
	JobStatusRequeued  = "requeued"
)

// Canonical Failure Taxonomy
const (
	FailureCategoryNone                   = "NONE"
	FailureCategoryDispatchClaimTimeout   = "DISPATCH_CLAIM_TIMEOUT"
	FailureCategoryToolQuotaOrAuth        = "TOOL_QUOTA_OR_AUTH"
	FailureCategoryToolCrash              = "TOOL_CRASH"
	FailureCategoryWorkspacePrepError     = "WORKSPACE_PREP_ERROR"
	FailureCategoryBackgroundDrainTimeout = "BACKGROUND_DRAIN_TIMEOUT"
	FailureCategoryASEGateIncomplete      = "ASE_GATE_INCOMPLETE"
	FailureCategoryCircuitBreakerTripped  = "CIRCUIT_BREAKER_TRIPPED"
	FailureCategoryStoryNotFound          = "STORY_NOT_FOUND"

	FailureStageIngress         = "INGRESS"
	FailureStageQueueDispatch   = "QUEUE_DISPATCH"
	FailureStageWorkspacePrep   = "WORKSPACE_PREP"
	FailureStageToolExecution   = "TOOL_EXECUTION"
	FailureStageBackgroundDrain = "BACKGROUND_DRAIN"
	FailureStageCompletionGate  = "COMPLETION_GATE"
	FailureStageStoryValidation = "STORY_VALIDATION"
)

const (
	DefaultLeaseAckDeadline = 20 * time.Second
)

// AttemptFailure models one failed dispatch or execution attempt.
type AttemptFailure struct {
	AttemptNumber   int    `json:"attempt_number"`
	Host            string `json:"host"`
	Workspace       string `json:"workspace"`
	Tool            string `json:"tool"`
	FailureCategory string `json:"failure_category"`
	FailureStage    string `json:"failure_stage,omitempty"`
	Reason          string `json:"reason,omitempty"`
	ExitCode        int    `json:"exit_code,omitempty"`
	StderrTail      string `json:"stderr_tail,omitempty"`
	ZipkinSpanID    string `json:"zipkin_span_id,omitempty"`
	Timestamp       string `json:"timestamp,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
	EndedAt         string `json:"ended_at,omitempty"`
	TriggerReason   string `json:"trigger_reason,omitempty"`
	OutcomeSummary  string `json:"outcome_summary,omitempty"`
	LogSeqOffset    int    `json:"log_seq_offset,omitempty"`
	LogByteOffset   int64  `json:"log_byte_offset,omitempty"`
}

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
	ID                      string               `json:"id"`
	Type                    string               `json:"type"`
	Repo                    string               `json:"repo"`
	TargetHost              string               `json:"target_host,omitempty"`
	ClaimedHost             string               `json:"claimed_host,omitempty"`
	HostID                  string               `json:"host_id,omitempty"`
	TargetAgent             string               `json:"target_agent,omitempty"`
	StoryID                 string               `json:"story_id,omitempty"`
	StoryTitle              string               `json:"story_title,omitempty"`
	StoryPath               string               `json:"story_path,omitempty"`
	StoryAllocationStatus   string               `json:"story_allocation_status,omitempty"`
	AllocationSource        string               `json:"allocation_source,omitempty"`
	PriorityRankSelected    int                  `json:"priority_rank_selected,omitempty"`
	SourceConversationTitle string               `json:"source_conversation_title,omitempty"`
	NormalizedPrompt        string               `json:"normalized_prompt,omitempty"`
	EffectiveAction         string               `json:"effective_action,omitempty"`
	AgentAffinity           *AgentAffinity       `json:"agent_affinity,omitempty"`
	Payload                 json.RawMessage      `json:"payload"`
	Status                  string               `json:"status"`
	Result                  json.RawMessage      `json:"result,omitempty"`
	Error                   string               `json:"error,omitempty"`
	TraceID                 string               `json:"trace_id,omitempty"`
	ParentID                string               `json:"parent_id,omitempty"`
	SearchID                string               `json:"search_id,omitempty"`
	RelevanceEmitted        bool                 `json:"relevance_emitted"`
	PromotedMemories        []string             `json:"promoted_memories,omitempty"`
	FencingToken            int64                `json:"fencing_token"`
	Attempts                int                  `json:"attempts"`
	CreatedAt               time.Time            `json:"created_at"`
	ClaimedAt               *time.Time           `json:"claimed_at,omitempty"`
	AckedAt                 *time.Time           `json:"acked_at,omitempty"`
	HeartbeatAt             *time.Time           `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt          *time.Time           `json:"lease_expires_at,omitempty"`
	CompletedAt             *time.Time           `json:"completed_at,omitempty"`
	DispatchDiagnostics     *DispatchDiagnostics `json:"dispatch_diagnostics,omitempty"`
	LeaseToken              string               `json:"lease_token,omitempty"`
	LeaseAckDeadlineAt      *time.Time           `json:"lease_ack_deadline_at,omitempty"`
	LeaseOwner              string               `json:"lease_owner,omitempty"`
	LeaseVersion            int64                `json:"lease_version,omitempty"`
	FailureCategory         string               `json:"failure_category,omitempty"`
	FailureStage            string               `json:"failure_stage,omitempty"`
	FailureReasonSummary    string               `json:"failure_reason_summary,omitempty"`
	Retriable               bool                 `json:"retriable"`
	FinalTurnCount          int                  `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256   string               `json:"final_transcript_sha256,omitempty"`
	AttemptHistory          []AttemptFailure     `json:"attempt_history,omitempty"`
	ZipkinTraceID           string               `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL          string               `json:"zipkin_trace_url,omitempty"`
	Traceparent             string               `json:"traceparent,omitempty"`
	InfraRequeueCount       int                  `json:"infra_requeue_count,omitempty"`
	AgentPID                int                  `json:"agent_pid,omitempty"`
	HarnessPID              int                  `json:"harness_pid,omitempty"`
	ToolPID                 int                  `json:"tool_pid,omitempty"`
	ElapsedSeconds          int64                `json:"elapsed_seconds,omitempty"`
	ActiveConversationID    string               `json:"active_conversation_id,omitempty"`
	ConversationIDs         []string             `json:"conversation_ids,omitempty"`

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
	HeartbeatWithStory(ctx context.Context, jobID string, fencingToken int64, storyID string) (*Job, error)
	Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error
	CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error
	Get(jobID string) (*Job, bool)
	List(ctx context.Context, filter JobFilter) ([]*Job, error)
	Cancel(ctx context.Context, jobID string, reason string) (*Job, error)
	Retry(ctx context.Context, jobID string) (*Job, error)
	BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error)
	BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*Job, error)
	Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error)
	GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error)
	ReapExpired(now time.Time) []string
	StartReaper(ctx context.Context, interval time.Duration)
	ReconcileGlobalQueueSlots(now time.Time, staleGrace time.Duration) []string
	SetDispatchDiagnostics(jobID string, diag *DispatchDiagnostics) error
	ForceDispatch(ctx context.Context, jobID string) (*Job, error)
	AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) (*Job, error)
	Failover(ctx context.Context, jobID string, req AttemptFailure) (*Job, error)
	RetryRetriable(ctx context.Context) ([]*Job, error)
	SetFinalHarvest(jobID string, turnCount int, sha string)
	ExtendLeasesForHost(ctx context.Context, hostID string, activeJobIDs []string) ([]string, error)
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

// QueueStore defines the database persistence interface for queue jobs and distributed leases.
type QueueStore interface {
	SaveQueueJob(ctx context.Context, job *Job) error
	LoadQueueJobs(ctx context.Context) ([]*Job, error)
	UpdateQueueJobLease(ctx context.Context, jobID string, leaseOwner string, leaseVersion int64, heartbeatAt, expiresAt time.Time) error
	CompleteQueueJob(ctx context.Context, jobID string, status string, result json.RawMessage, errStr string, completedAt time.Time) error
}

// InMemoryQueue is a thread-safe in-memory implementation of Queue with optional database backing.
type InMemoryQueue struct {
	mu             sync.Mutex
	jobs           map[string]*Job
	fencingSeq     int64
	attributionURL string
	httpClient     *http.Client
	claimTimeout   time.Duration
	jobTimeout     time.Duration
	startupTime    time.Time
	startupGrace   time.Duration
	store          QueueStore
	nowFunc        func() time.Time
}

// SetStore binds a durable database backing store to the queue and re-hydrates in-flight jobs.
func (q *InMemoryQueue) SetStore(s QueueStore) error {
	q.mu.Lock()
	q.store = s
	q.mu.Unlock()

	if s != nil {
		persisted, err := s.LoadQueueJobs(context.Background())
		if err != nil {
			return err
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, j := range persisted {
			if j.FencingToken > q.fencingSeq {
				q.fencingSeq = j.FencingToken
			}
			if j.LeaseVersion > q.fencingSeq {
				q.fencingSeq = j.LeaseVersion
			}
			if existing, ok := q.jobs[j.ID]; !ok || existing.CreatedAt.Before(j.CreatedAt) {
				if j.doneCh == nil {
					j.doneCh = make(chan struct{})
					if j.Status == JobStatusCompleted || j.Status == JobStatusFailed || j.Status == JobStatusCanceled {
						close(j.doneCh)
					}
				}
				q.jobs[j.ID] = j
			}
		}
	}
	return nil
}

// SetAttributionConfig sets the URL and optional client used for relevance attribution dispatches.
func (q *InMemoryQueue) SetAttributionConfig(apiURL string, client *http.Client) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.attributionURL = apiURL
	q.httpClient = client
}

// SetStartupGrace sets the post-startup reconciliation grace window.
func (q *InMemoryQueue) SetStartupGrace(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.startupGrace = d
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
		if j.Status != JobStatusPending && j.Status != JobStatusClaimed && j.Status != JobStatusLeased && j.Status != JobStatusRunning {
			continue
		}

		// 1. Max execution timeout: from CreatedAt (0 = unbounded, stories immune)
		isStory := j.StoryID != "" || j.Type == "story" || j.Type == "refinement"
		if !isStory && q.jobTimeout > 0 && !j.CreatedAt.IsZero() && now.Sub(j.CreatedAt) > q.jobTimeout {
			j.Status = JobStatusFailed
			j.Error = "EXECUTION_TIMEOUT: job exceeded max execution duration"
			j.FailureCategory = FailureCategoryBackgroundDrainTimeout
			j.FailureStage = FailureStageBackgroundDrain
			j.FailureReasonSummary = j.Error
			j.Retriable = j.Attempts < DefaultMaxAttempts
			completedAt := now
			j.CompletedAt = &completedAt
			expiredIDs = append(expiredIDs, j.ID)
			if j.doneCh != nil {
				doneChannels = append(doneChannels, j.doneCh)
			}
			continue
		}

		// 1b. Unacknowledged lease expiration (Two-Phase Lease Handshake):
		if j.Status == JobStatusLeased {
			if j.LeaseAckDeadlineAt != nil && !j.LeaseAckDeadlineAt.IsZero() && now.After(*j.LeaseAckDeadlineAt) {
				// Revert to pending without consuming story retry budget when reaped within handshake window
				j.Status = JobStatusPending
				if j.Attempts > 0 && (j.LeaseExpiresAt == nil || !now.After(*j.LeaseExpiresAt)) {
					j.Attempts--
				}
				j.InfraRequeueCount++
				j.LeaseOwner = ""
				j.LeaseToken = ""
				j.LeaseAckDeadlineAt = nil
				j.ClaimedHost = ""
				j.HostID = ""
				j.ClaimedAt = nil
				j.HeartbeatAt = nil
				j.LeaseExpiresAt = nil
				j.DispatchDiagnostics = &DispatchDiagnostics{
					QueueState: "queued",
				}
				expiredIDs = append(expiredIDs, j.ID)
				continue
			}
		}

		// 2. Claim lease & heartbeat timeout:
		if j.Status == JobStatusClaimed || j.Status == JobStatusRunning {
			if q.claimTimeout > 0 && j.ClaimedAt != nil && !j.ClaimedAt.IsZero() && now.Sub(*j.ClaimedAt) > q.claimTimeout {
				j.Status = JobStatusFailed
				j.Error = "LEASE_EXPIRED: execution lease expired without completion"
				j.FailureCategory = FailureCategoryBackgroundDrainTimeout
				j.FailureStage = FailureStageBackgroundDrain
				j.FailureReasonSummary = j.Error
				j.Retriable = j.Attempts < DefaultMaxAttempts
				completedAt := now
				j.CompletedAt = &completedAt
				expiredIDs = append(expiredIDs, j.ID)
				if j.doneCh != nil {
					doneChannels = append(doneChannels, j.doneCh)
				}
				continue
			}

			if j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.IsZero() && now.After(*j.LeaseExpiresAt) {
				attemptNum := j.Attempts
				if attemptNum <= 0 {
					attemptNum = len(j.AttemptHistory) + 1
				}
				attemptRecord := AttemptFailure{
					AttemptNumber:   attemptNum,
					Host:            j.HostID,
					Workspace:       j.Repo,
					Tool:            j.TargetAgent,
					FailureCategory: FailureCategoryBackgroundDrainTimeout,
					FailureStage:    FailureStageBackgroundDrain,
					Reason:          "LEASE_EXPIRED",
					ExitCode:        0,
					StderrTail:      "Job execution lease expired before completion",
					Timestamp:       now.UTC().Format(time.RFC3339),
					TriggerReason:   "lease_expired",
					OutcomeSummary:  "Execution lease expired without heartbeat",
				}
				j.AttemptHistory = append(j.AttemptHistory, attemptRecord)

				consecutiveLeaseExpired := false
				if len(j.AttemptHistory) >= 2 {
					prev := j.AttemptHistory[len(j.AttemptHistory)-2]
					if prev.Reason == "LEASE_EXPIRED" {
						consecutiveLeaseExpired = true
					}
				}

				if j.Attempts < DefaultMaxAttempts && !consecutiveLeaseExpired {
					// Safe runner recovery: revert to pending for retry without duplicate execution
					j.Status = JobStatusPending
					j.ClaimedHost = ""
					j.HostID = ""
					j.LeaseOwner = ""
					j.LeaseToken = ""
					j.ClaimedAt = nil
					j.HeartbeatAt = nil
					j.LeaseExpiresAt = nil
					j.LeaseAckDeadlineAt = nil
					expiredIDs = append(expiredIDs, j.ID)
				} else {
					j.Status = JobStatusFailed
					j.Retriable = false
					if consecutiveLeaseExpired {
						j.Error = "LEASE_EXPIRED: consecutive attempts expired lease without progress"
					} else {
						j.Error = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, max retry attempts exceeded"
					}
					j.FailureCategory = FailureCategoryBackgroundDrainTimeout
					j.FailureStage = FailureStageBackgroundDrain
					j.FailureReasonSummary = j.Error
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

	if q.store != nil && len(expiredIDs) > 0 {
		for _, id := range expiredIDs {
			if j, ok := q.jobs[id]; ok && j != nil {
				_ = q.store.SaveQueueJob(context.Background(), j)
			}
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
// During the initial startup grace window (default 5m), the reaper does not mark jobs as lease_expired,
// allowing re-connecting agents to re-hydrate and extend active leases across API restarts/redeployments.
func (q *InMemoryQueue) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	startedAt := q.now()
	grace := q.startupGrace
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if grace > 0 && now.Sub(startedAt) < grace {
					continue
				}
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
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}

	var spanOpts []telemetry.SpanOption
	spanOpts = append(spanOpts, telemetry.WithTag("job.id", job.ID))
	if job.TraceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(job.TraceID, ""))
	}
	_, span := telemetry.StartSpan(ctx, "global_queue.enqueue", spanOpts...)
	if span != nil {
		span.End()
	}

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
	oldest.Status = JobStatusLeased
	oldest.LeaseToken = fmt.Sprintf("lt-%s-%d", oldest.ID, now.UnixNano())
	ackDeadline := now.Add(DefaultLeaseAckDeadline)
	oldest.LeaseAckDeadlineAt = &ackDeadline
	if oldest.TraceID != "" && oldest.Traceparent == "" {
		oldest.Traceparent = fmt.Sprintf("00-%s-%016x-01", oldest.TraceID, now.UnixNano())
	}
	if hostID != "" {
		oldest.ClaimedHost = hostID
		oldest.HostID = hostID
	}
	oldest.ClaimedAt = &now
	oldest.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	oldest.LeaseExpiresAt = &expires
	oldest.LeaseOwner = hostID
	oldest.LeaseVersion = oldest.FencingToken
	if q.store != nil {
		if err := q.store.SaveQueueJob(ctx, oldest); err != nil {
			log.Printf("[dispatch-queue] ERROR: failed to persist claimed job %s: %v", oldest.ID, err)
			return nil, fmt.Errorf("persist claimed job %s: %w", oldest.ID, err)
		}
	}

	var claimSpanOpts []telemetry.SpanOption
	claimSpanOpts = append(claimSpanOpts, telemetry.WithTag("job.id", oldest.ID), telemetry.WithTag("host.id", hostID))
	if oldest.TraceID != "" {
		claimSpanOpts = append(claimSpanOpts, telemetry.WithParentTrace(oldest.TraceID, ""))
	}
	_, claimSpan := telemetry.StartSpan(ctx, "global_queue.lease_and_dispatch", claimSpanOpts...)
	if claimSpan != nil {
		claimSpan.End()
	}

	return oldest, nil
}

// Heartbeat extends the lease expiration of an in-flight claimed job, validating the worker's fencing token.
func (q *InMemoryQueue) Heartbeat(ctx context.Context, jobID string, fencingToken int64) (*Job, error) {
	return q.HeartbeatWithStory(ctx, jobID, fencingToken, "")
}

// HeartbeatWithStory extends the lease expiration and optionally binds an allocated story ID atomically.
func (q *InMemoryQueue) HeartbeatWithStory(ctx context.Context, jobID string, fencingToken int64, storyID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if job.Status != JobStatusClaimed && job.Status != JobStatusLeased && job.Status != JobStatusRunning {
		return nil, fmt.Errorf("job %s is not currently claimed (status: %s)", jobID, job.Status)
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		return nil, ErrLeaseFenced
	}

	now := q.now()
	statusTransitioned := false
	if job.Status == JobStatusLeased || job.Status == JobStatusClaimed {
		job.Status = JobStatusRunning
		job.LeaseAckDeadlineAt = nil
		if job.AckedAt == nil {
			job.AckedAt = &now
		}
		statusTransitioned = true
	} else if job.LeaseAckDeadlineAt != nil {
		job.LeaseAckDeadlineAt = nil
		statusTransitioned = true
	}

	storyUpdated := false
	cleanStoryID := strings.TrimSpace(storyID)
	if cleanStoryID != "" && cleanStoryID != "pending-allocation" && cleanStoryID != "pending" {
		job.StoryID = cleanStoryID
		job.StoryAllocationStatus = "allocated"
		storyUpdated = true
		if len(job.Payload) > 0 {
			var payloadMap map[string]any
			if err := json.Unmarshal(job.Payload, &payloadMap); err == nil {
				payloadMap["story_id"] = cleanStoryID
				if updated, err := json.Marshal(payloadMap); err == nil {
					job.Payload = updated
				}
			}
		}
	}

	job.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	job.LeaseExpiresAt = &expires
	if q.store != nil {
		if err := q.store.UpdateQueueJobLease(ctx, job.ID, job.LeaseOwner, job.FencingToken, now, expires); err != nil {
			log.Printf("[dispatch-queue] ERROR: failed to persist heartbeat lease for %s: %v", jobID, err)
			return nil, fmt.Errorf("persist heartbeat lease: %w", err)
		}
		if statusTransitioned || storyUpdated {
			_ = q.store.SaveQueueJob(ctx, job)
		}
	}
	return job, nil
}

// ExtendLeasesForHost refreshes heartbeat timestamps and lease deadlines for all active jobs
// associated with the reporting host, preventing lease expiration timeouts while the host is healthy.
func (q *InMemoryQueue) ExtendLeasesForHost(ctx context.Context, hostID string, activeJobIDs []string) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if hostID == "" {
		return nil, nil
	}

	activeSet := make(map[string]bool, len(activeJobIDs))
	for _, id := range activeJobIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			activeSet[trimmed] = true
		}
	}

	now := q.now()
	var extended []string
	for _, j := range q.jobs {
		if j == nil {
			continue
		}
		// Match jobs allocated to this host
		hostMatch := (j.ClaimedHost == hostID || j.HostID == hostID || (j.AgentAffinity != nil && j.AgentAffinity.HostID == hostID))
		if !hostMatch {
			continue
		}

		// Auto-reconcile jobs marked failed due to lease expiration or timeout if reported active by agent
		if j.Status == JobStatusFailed && activeSet[j.ID] && isReconcilableFailure(j.Error) {
			j.Status = JobStatusRunning
			j.Error = ""
			j.CompletedAt = nil
		}

		// Only extend active jobs (leased, claimed, running)
		if j.Status != JobStatusClaimed && j.Status != JobStatusLeased && j.Status != JobStatusRunning {
			continue
		}

		// If specific active jobs were reported, require membership in the active set.
		// If no active jobs were reported, keep all active jobs on this host alive as long as host heartbeats.
		if len(activeSet) > 0 && !activeSet[j.ID] {
			continue
		}

		expires := now.Add(DefaultHeartbeatTimeout)
		if q.store != nil {
			if err := q.store.UpdateQueueJobLease(ctx, j.ID, hostID, j.FencingToken, now, expires); err != nil {
				log.Printf("[dispatch-queue] ERROR: failed to persist lease update for %s: %v", j.ID, err)
				continue
			}
		}

		j.HeartbeatAt = &now
		j.LeaseExpiresAt = &expires
		j.LeaseOwner = hostID
		j.LeaseVersion = j.FencingToken
		if j.Status == JobStatusLeased || j.Status == JobStatusClaimed {
			j.Status = JobStatusRunning
		}
		extended = append(extended, j.ID)
	}

	for jobID := range activeSet {
		if _, exists := q.jobs[jobID]; !exists {
			var rehydrated *Job
			if q.store != nil {
				if loaded, err := q.store.LoadQueueJobs(ctx); err == nil {
					for _, lj := range loaded {
						if lj.ID == jobID {
							rehydrated = lj
							break
						}
					}
				}
			}
			expires := now.Add(DefaultHeartbeatTimeout)
			if rehydrated != nil {
				rehydrated.Status = JobStatusRunning
				rehydrated.ClaimedHost = hostID
				rehydrated.HostID = hostID
				rehydrated.HeartbeatAt = &now
				rehydrated.LeaseExpiresAt = &expires
				rehydrated.LeaseOwner = hostID
				rehydrated.LeaseVersion = rehydrated.FencingToken
				if rehydrated.doneCh == nil {
					rehydrated.doneCh = make(chan struct{})
				}
				q.jobs[jobID] = rehydrated
			} else {
				q.jobs[jobID] = &Job{
					ID:             jobID,
					Status:         JobStatusRunning,
					ClaimedHost:    hostID,
					HostID:         hostID,
					HeartbeatAt:    &now,
					LeaseExpiresAt: &expires,
					LeaseOwner:     hostID,
					CreatedAt:      now,
					doneCh:         make(chan struct{}),
				}
			}
			if q.store != nil {
				if err := q.store.UpdateQueueJobLease(ctx, jobID, hostID, q.jobs[jobID].FencingToken, now, expires); err != nil {
					log.Printf("[dispatch-queue] ERROR: failed to persist rehydrated lease for %s: %v", jobID, err)
					delete(q.jobs, jobID)
					continue
				}
			}
			extended = append(extended, jobID)
		}
	}

	return extended, nil
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

// ClassifyDispatchFailure maps an error string to canonical failure taxonomy.
func ClassifyDispatchFailure(errStr string) (category, stage, summary string, retriable bool) {
	lower := strings.ToLower(strings.TrimSpace(errStr))
	switch {
	case strings.Contains(lower, "claim") || strings.Contains(lower, "startup_heartbeat") || strings.Contains(lower, "unacknowledged lease"):
		return FailureCategoryDispatchClaimTimeout, FailureStageQueueDispatch, "Agent lease claim timed out or lost before initial acknowledgement", true
	case strings.Contains(lower, "429") || strings.Contains(lower, "resource_exhausted") || strings.Contains(lower, "quota") || strings.Contains(lower, "auth") || strings.Contains(lower, "token limit") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "preflight_or_early_tool_exit"):
		return FailureCategoryToolQuotaOrAuth, FailureStageToolExecution, "Tool startup failed due to quota exhaustion, rate limit, or auth expiration", true
	case strings.Contains(lower, "crash") || strings.Contains(lower, "exit status") || strings.Contains(lower, "sigsegv") || strings.Contains(lower, "binary"):
		return FailureCategoryToolCrash, FailureStageToolExecution, "AI agent tool process crashed or exited non-zero during early execution", true
	case strings.Contains(lower, "dirty_worktree_post_exit") || strings.Contains(lower, "post_exit"):
		return FailureCategoryASEGateIncomplete, FailureStageCompletionGate, "Post-execution gate: child engine exited but left uncommitted worktree changes", true
	case strings.Contains(lower, "worktree") || strings.Contains(lower, "workspace_prep") || strings.Contains(lower, "lock") || strings.Contains(lower, "porcelain") || strings.Contains(lower, "submodule") || strings.Contains(lower, "dirty"):
		return FailureCategoryWorkspacePrepError, FailureStageWorkspacePrep, "Workspace preparation conflict: dirty worktree or stale git locks", true
	case strings.Contains(lower, "1h0m0s") || strings.Contains(lower, "background task") || strings.Contains(lower, "drain") || strings.Contains(lower, "lease_expired") || strings.Contains(lower, "heartbeat_timeout") || strings.Contains(lower, "execution_timeout") || strings.Contains(lower, "lease expired"):
		return FailureCategoryBackgroundDrainTimeout, FailureStageBackgroundDrain, errStr, true
	case strings.Contains(lower, "circuit") || strings.Contains(lower, "breaker") || strings.Contains(lower, "retry_capped"):
		return FailureCategoryCircuitBreakerTripped, FailureStageQueueDispatch, errStr, false
	case strings.Contains(lower, "story_not_found") || strings.Contains(lower, "not found in backlog") || strings.Contains(lower, "cannot execute nonexistent story"):
		return FailureCategoryStoryNotFound, FailureStageStoryValidation, "Story not found in backlog or control plane: cannot execute nonexistent story", false
	case strings.Contains(lower, "gate") || strings.Contains(lower, "dod") || strings.Contains(lower, "incomplete") || strings.Contains(lower, "ase_gate"):
		return FailureCategoryASEGateIncomplete, FailureStageCompletionGate, "Autonomous Story Execution DoD or review scheme validation incomplete", false
	default:
		return FailureCategoryToolCrash, FailureStageToolExecution, "Execution error: " + errStr, true
	}
}

// isReconcilableFailure checks whether a failure reason was caused by lease/heartbeat/reaper timeout.
func isReconcilableFailure(errStr string) bool {
	lower := strings.ToLower(errStr)
	return strings.Contains(lower, "lease_expired") ||
		strings.Contains(lower, "heartbeat_timeout") ||
		strings.Contains(lower, "execution_timeout") ||
		strings.Contains(lower, "stalled") ||
		strings.Contains(lower, "process_died") ||
		strings.Contains(lower, "lease expired")
}

// CompleteWithFencing validates the fencing token and updates the job with final result or error.
func (q *InMemoryQueue) CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusCanceled {
		q.mu.Unlock()
		return ErrJobAlreadyCompleted
	}
	if job.Status == JobStatusFailed {
		if !isReconcilableFailure(job.Error) {
			q.mu.Unlock()
			return ErrJobAlreadyCompleted
		}
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
		cat, stage, summary, retriable := ClassifyDispatchFailure(errStr)
		if job.FailureCategory == "" || job.FailureCategory == FailureCategoryNone {
			job.FailureCategory = cat
		}
		if job.FailureStage == "" {
			job.FailureStage = stage
		}
		if job.FailureReasonSummary == "" {
			job.FailureReasonSummary = summary
		}
		job.Retriable = retriable
	} else {
		job.Status = JobStatusCompleted
		job.Result = result
		job.Error = ""
	}

	if q.store != nil && job.CompletedAt != nil {
		if err := q.store.CompleteQueueJob(ctx, job.ID, job.Status, job.Result, job.Error, *job.CompletedAt); err != nil {
			log.Printf("[dispatch-queue] ERROR: failed to persist job completion for %s: %v", job.ID, err)
			q.mu.Unlock()
			return fmt.Errorf("persist job completion: %w", err)
		}
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
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
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

// SetFinalHarvest records the final verified turn count and transcript SHA256 onto the job.
func (q *InMemoryQueue) SetFinalHarvest(jobID string, turnCount int, sha string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if job, ok := q.jobs[jobID]; ok {
		updated := false
		if turnCount > 0 {
			job.FinalTurnCount = turnCount
			updated = true
		}
		if sha != "" {
			job.FinalTranscriptSHA256 = sha
			updated = true
		}
		if updated && q.store != nil {
			_ = q.store.SaveQueueJob(context.Background(), job)
		}
	}
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

func matchesJobStatusFilter(jobStatus, filterStatus string) bool {
	f := strings.ToLower(strings.TrimSpace(filterStatus))
	if f == "" || f == "all" {
		return true
	}
	s := strings.ToLower(strings.TrimSpace(jobStatus))
	switch f {
	case "queued", "pending":
		return s == JobStatusPending || s == "queued"
	case "claimed":
		return s == JobStatusClaimed || s == JobStatusLeased || s == JobStatusRunning
	case "cancelled", "canceled":
		return s == JobStatusCanceled || s == "cancelled"
	default:
		return strings.EqualFold(s, f)
	}
}

// Count returns the total number of jobs matching the filter before limit truncation.
func (q *InMemoryQueue) Count(ctx context.Context, filter JobFilter) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapExpiredLocked(q.now())

	count := 0
	for _, j := range q.jobs {
		if filter.Repo != "" && j.Repo != filter.Repo {
			continue
		}
		if !matchesJobStatusFilter(j.Status, filter.Status) {
			continue
		}
		if filter.Type != "" && j.Type != filter.Type {
			continue
		}
		count++
	}
	return count, nil
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
		if !matchesJobStatusFilter(j.Status, filter.Status) {
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
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}
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

	orig.Status = JobStatusRequeued
	newJob := &Job{
		ID:                      generateJobID(),
		Type:                    orig.Type,
		Repo:                    orig.Repo,
		TargetHost:              orig.TargetHost,
		TargetAgent:             orig.TargetAgent,
		StoryID:                 orig.StoryID,
		StoryTitle:              orig.StoryTitle,
		StoryPath:               orig.StoryPath,
		StoryAllocationStatus:   orig.StoryAllocationStatus,
		AllocationSource:        orig.AllocationSource,
		PriorityRankSelected:    orig.PriorityRankSelected,
		SourceConversationTitle: orig.SourceConversationTitle,
		NormalizedPrompt:        orig.NormalizedPrompt,
		EffectiveAction:         orig.EffectiveAction,
		AgentAffinity:           orig.AgentAffinity,
		Payload:                 orig.Payload,
		TraceID:                 orig.TraceID,
		SearchID:                orig.SearchID,
		ParentID:                orig.ID,
		Retriable:               orig.Retriable,
		Status:                  JobStatusPending,
		CreatedAt:               q.now(),
		doneCh:                  make(chan struct{}),
	}
	q.jobs[newJob.ID] = newJob
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, orig)
		_ = q.store.SaveQueueJob(ctx, newJob)
	}
	q.mu.Unlock()

	return newJob, nil
}

// AckLease completes Phase 2 of the two-phase lease handshake, transitioning the job to running.
func (q *InMemoryQueue) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if job.Status != JobStatusLeased && job.Status != JobStatusClaimed && job.Status != JobStatusRunning {
		return nil, fmt.Errorf("job %s is not in leased state (status: %s)", jobID, job.Status)
	}
	if job.LeaseToken != "" && (leaseToken == "" || job.LeaseToken != leaseToken) {
		return nil, errors.New("invalid or missing lease token")
	}

	now := q.now()
	job.Status = JobStatusRunning
	job.LeaseAckDeadlineAt = nil
	if job.AckedAt == nil {
		job.AckedAt = &now
	}
	if agentPID > 0 {
		job.AgentPID = agentPID
		if job.HarnessPID == 0 {
			job.HarnessPID = agentPID
		}
	}
	if activeConvID != "" {
		job.ActiveConversationID = activeConvID
		addConversationIDLocked(job, activeConvID)
	}
	job.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	job.LeaseExpiresAt = &expires
	if q.store != nil {
		if err := q.store.UpdateQueueJobLease(ctx, job.ID, job.LeaseOwner, job.FencingToken, now, expires); err != nil {
			log.Printf("[dispatch-queue] ERROR: failed to persist heartbeat lease for %s: %v", jobID, err)
			return nil, fmt.Errorf("persist heartbeat lease: %w", err)
		}
		_ = q.store.SaveQueueJob(ctx, job)
	}
	return job, nil
}

func addConversationIDLocked(job *Job, convID string) {
	cid := strings.TrimSpace(convID)
	if cid == "" || job == nil {
		return
	}
	for _, existing := range job.ConversationIDs {
		if existing == cid {
			return
		}
	}
	job.ConversationIDs = append(job.ConversationIDs, cid)
}

// UpdateRuntimeMetadata updates the delegated worktree path, active conversation, PIDs, and elapsed time on a job.
func (q *InMemoryQueue) UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists || job == nil {
		return nil, false
	}
	changed := false
	wt := strings.TrimSpace(worktreePath)
	if wt != "" && wt != "." && wt != "root" && job.Repo != wt {
		origSubmodule := job.Repo
		job.Repo = wt
		changed = true
		if len(job.Payload) > 0 {
			var payloadMap map[string]any
			if err := json.Unmarshal(job.Payload, &payloadMap); err == nil && payloadMap != nil {
				if _, hasSub := payloadMap["submodule"]; !hasSub && origSubmodule != "" {
					payloadMap["submodule"] = origSubmodule
				}
				payloadMap["worktree_path"] = wt
				payloadMap["repo"] = wt
				if updated, err := json.Marshal(payloadMap); err == nil {
					job.Payload = updated
				}
			}
		}
	}
	if cid := strings.TrimSpace(activeConvID); cid != "" {
		if job.ActiveConversationID != cid {
			job.ActiveConversationID = cid
			changed = true
		}
		prevLen := len(job.ConversationIDs)
		addConversationIDLocked(job, cid)
		if len(job.ConversationIDs) != prevLen {
			changed = true
		}
	}
	if harnessPID > 0 && job.HarnessPID != harnessPID {
		job.HarnessPID = harnessPID
		if job.AgentPID == 0 {
			job.AgentPID = harnessPID
		}
		changed = true
	}
	if toolPID > 0 && job.ToolPID != toolPID {
		job.ToolPID = toolPID
		changed = true
	}
	if elapsedSec > 0 && job.ElapsedSeconds != elapsedSec {
		job.ElapsedSeconds = elapsedSec
		changed = true
	}
	if job.Status == JobStatusRunning || job.Status == JobStatusClaimed || job.Status == JobStatusLeased {
		now := q.now()
		job.HeartbeatAt = &now
		expires := now.Add(DefaultHeartbeatTimeout)
		job.LeaseExpiresAt = &expires
	}
	if changed && q.store != nil {
		_ = q.store.SaveQueueJob(context.Background(), job)
	}
	return job, true
}

// Failover records an attempt failure and resets the job to pending for re-dispatch.
func (q *InMemoryQueue) Failover(ctx context.Context, jobID string, req AttemptFailure) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}

	if req.AttemptNumber <= 0 {
		req.AttemptNumber = len(job.AttemptHistory) + 1
	}
	if req.Timestamp == "" {
		req.Timestamp = q.now().UTC().Format(time.RFC3339)
	}
	if req.TriggerReason == "" {
		req.TriggerReason = req.Reason
	}
	if req.OutcomeSummary == "" {
		req.OutcomeSummary = fmt.Sprintf("Tool %s on %s failed (exit %d): %s", req.Tool, req.Host, req.ExitCode, req.StderrTail)
	}
	job.AttemptHistory = append(job.AttemptHistory, req)
	job.FailureCategory = req.FailureCategory
	job.FailureStage = FailureStageToolExecution
	job.FailureReasonSummary = req.OutcomeSummary

	// Cap retries if consecutive attempts fail with identical error or identical failure category/exit code
	consecutiveFailures := false
	if len(job.AttemptHistory) >= 2 {
		prev := job.AttemptHistory[len(job.AttemptHistory)-2]
		if (prev.FailureCategory == req.FailureCategory && prev.ExitCode == req.ExitCode && prev.Reason == req.Reason) || len(job.AttemptHistory) >= DefaultMaxAttempts {
			consecutiveFailures = true
		}
	}

	if consecutiveFailures {
		job.Status = JobStatusFailed
		job.Error = fmt.Sprintf("RETRY_CAPPED: consecutive attempts failed with identical error (%s)", req.Reason)
		job.Retriable = false
		completedAt := q.now()
		job.CompletedAt = &completedAt
		if job.doneCh != nil {
			select {
			case <-job.doneCh:
			default:
				close(job.doneCh)
			}
		}
		if q.store != nil {
			_ = q.store.SaveQueueJob(ctx, job)
		}
		return job, nil
	}

	failedTool := strings.ToLower(strings.TrimSpace(req.Tool))
	if failedTool == "" {
		failedTool = strings.ToLower(strings.TrimSpace(job.TargetAgent))
	}
	if failedTool != "" && req.FailureCategory != "WORKSPACE_PREP_ERROR" {
		priorityTools := []string{"agy", "opencode", "claude", "cursor"}
		failedSet := make(map[string]bool)
		for _, att := range job.AttemptHistory {
			if t := strings.ToLower(strings.TrimSpace(att.Tool)); t != "" {
				failedSet[t] = true
			}
		}
		failedSet[failedTool] = true
		nextTool := ""
		for _, cand := range priorityTools {
			if !failedSet[cand] {
				nextTool = cand
				break
			}
		}
		if nextTool == "" {
			for _, cand := range priorityTools {
				if cand != failedTool {
					nextTool = cand
					break
				}
			}
		}
		if nextTool != "" {
			job.TargetAgent = nextTool
			job.TargetHost = ""
			if len(job.Payload) > 0 {
				var payloadMap map[string]any
				if err := json.Unmarshal(job.Payload, &payloadMap); err == nil && payloadMap != nil {
					payloadMap["target_agent"] = nextTool
					payloadMap["target_host"] = ""
					if updated, err := json.Marshal(payloadMap); err == nil {
						job.Payload = updated
					}
				}
			}
		}
	}

	job.TargetHost = ""
	job.Status = JobStatusPending
	job.ClaimedHost = ""
	job.HostID = ""
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.LeaseAckDeadlineAt = nil
	job.Retriable = true
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}

	return job, nil
}

// RetryRetriable retries all failed jobs that are marked retriable.
func (q *InMemoryQueue) RetryRetriable(ctx context.Context) ([]*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var retried []*Job
	for _, job := range q.jobs {
		if (job.Status == JobStatusFailed || job.Status == JobStatusCanceled) && job.Retriable {
			job.Status = JobStatusRequeued
			newJob := &Job{
				ID:                      generateJobID(),
				Type:                    job.Type,
				Repo:                    job.Repo,
				TargetHost:              job.TargetHost,
				TargetAgent:             job.TargetAgent,
				StoryID:                 job.StoryID,
				StoryTitle:              job.StoryTitle,
				StoryPath:               job.StoryPath,
				StoryAllocationStatus:   job.StoryAllocationStatus,
				AllocationSource:        job.AllocationSource,
				PriorityRankSelected:    job.PriorityRankSelected,
				SourceConversationTitle: job.SourceConversationTitle,
				NormalizedPrompt:        job.NormalizedPrompt,
				EffectiveAction:         job.EffectiveAction,
				AgentAffinity:           job.AgentAffinity,
				Payload:                 append(json.RawMessage(nil), job.Payload...),
				Status:                  JobStatusPending,
				CreatedAt:               q.now(),
				TraceID:                 job.TraceID,
				Traceparent:             job.Traceparent,
				ZipkinTraceID:           job.ZipkinTraceID,
				ZipkinTraceURL:          job.ZipkinTraceURL,
				SearchID:                job.SearchID,
				ParentID:                job.ID,
				Retriable:               true,
				doneCh:                  make(chan struct{}),
			}
			q.jobs[newJob.ID] = newJob
			if q.store != nil {
				_ = q.store.SaveQueueJob(ctx, job)
				_ = q.store.SaveQueueJob(ctx, newJob)
			}
			retried = append(retried, newJob)
		}
	}
	return retried, nil
}

// BindStory binds a newly allocated story ID and metadata to a job.
func (q *InMemoryQueue) BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error) {
	return q.BindStoryWithFencing(ctx, jobID, storyID, storyTitle, storyPath, 0)
}

// BindStoryWithFencing binds an allocated story ID and metadata to a job while enforcing monotonic fencing token CAS.
func (q *InMemoryQueue) BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}

	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		return nil, ErrLeaseFenced
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

	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}

	return job, nil
}

// Await blocks until the specified job completes, the context is cancelled, or the timeout expires.
func (q *InMemoryQueue) Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error) {
	var waitSpanOpts []telemetry.SpanOption
	waitSpanOpts = append(waitSpanOpts, telemetry.WithTag("job.id", jobID))
	_, waitSpan := telemetry.StartSpan(ctx, "global_queue.wait", waitSpanOpts...)
	if waitSpan != nil {
		defer waitSpan.End()
	}

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
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.LeaseAckDeadlineAt = nil
	job.FencingToken = 0
	job.LeaseVersion = 0
	job.Error = ""
	if job.DispatchDiagnostics == nil {
		job.DispatchDiagnostics = &DispatchDiagnostics{}
	}
	job.DispatchDiagnostics.QueueState = "dispatching"
	job.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}

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

	reclaimedIDs := q.reapExpiredLocked(now)
	for _, j := range q.jobs {
		if j.Status == JobStatusClaimed || j.Status == JobStatusLeased || j.Status == JobStatusRunning {
			stale := false
			if j.HeartbeatAt != nil && now.Sub(*j.HeartbeatAt) > staleGrace {
				stale = true
			}
			if j.LeaseExpiresAt != nil && now.After(*j.LeaseExpiresAt) {
				stale = true
			}
			if stale {
				prevHost := j.ClaimedHost
				if prevHost == "" {
					prevHost = j.HostID
				}
				if j.Attempts < DefaultMaxAttempts {
					j.Status = JobStatusPending
					j.ClaimedHost = ""
					j.HostID = ""
					j.LeaseOwner = ""
					j.LeaseToken = ""
					j.ClaimedAt = nil
					j.HeartbeatAt = nil
					j.LeaseExpiresAt = nil
					j.LeaseAckDeadlineAt = nil
					j.FencingToken = 0
					j.LeaseVersion = 0
					if j.DispatchDiagnostics == nil {
						j.DispatchDiagnostics = &DispatchDiagnostics{}
					}
					j.DispatchDiagnostics.QueueState = "queued"
					j.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)
					if j.DispatchDiagnostics.BlockedReasonsByHost == nil {
						j.DispatchDiagnostics.BlockedReasonsByHost = make(map[string]string)
					}
					j.DispatchDiagnostics.BlockedReasonsByHost["lease"] = fmt.Sprintf("reclaimed from %s after heartbeat timeout > %v", prevHost, staleGrace)
					reclaimedIDs = append(reclaimedIDs, j.ID)
				} else {
					j.Status = JobStatusFailed
					j.Error = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, max retry attempts exceeded"
					j.FailureCategory = FailureCategoryBackgroundDrainTimeout
					j.FailureStage = FailureStageBackgroundDrain
					j.FailureReasonSummary = j.Error
					j.Retriable = j.Attempts < DefaultMaxAttempts
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
				if q.store != nil {
					_ = q.store.SaveQueueJob(context.Background(), j)
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
