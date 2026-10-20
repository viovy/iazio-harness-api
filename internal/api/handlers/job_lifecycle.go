package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

// JobLifecycleHandler manages querying, listing, canceling, and retrying agent jobs.
type JobLifecycleHandler struct {
	queue     dispatch.Queue
	store     codebase.Store
	scheduler GlobalScheduler
}

// NewJobLifecycleHandler constructs a new JobLifecycleHandler.
func NewJobLifecycleHandler(queue dispatch.Queue, store ...codebase.Store) *JobLifecycleHandler {
	var s codebase.Store
	if len(store) > 0 {
		s = store[0]
	}
	return &JobLifecycleHandler{
		queue: queue,
		store: s,
	}
}

// SetScheduler sets the global scheduler.
func (h *JobLifecycleHandler) SetScheduler(scheduler GlobalScheduler) {
	h.scheduler = scheduler
}

// JobDetailResponse is the detailed JSON representation of a dispatch job.
type JobDetailResponse struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Repo        string          `json:"repo"`
	TargetHost  string          `json:"target_host,omitempty"`
	TargetAgent string          `json:"target_agent,omitempty"`
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	CreatedAt   string          `json:"created_at"`
	ClaimedAt   string          `json:"claimed_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	DurationMs  int64           `json:"duration_ms,omitempty"`
}

// ListQueueResponse contains the list of jobs matching a filter.
type ListQueueResponse struct {
	Items []JobDetailResponse `json:"items"`
	Total int                 `json:"total"`
}

// CancelJobRequest is the optional payload when canceling a job.
type CancelJobRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ExecuteStoryRequest specifies parameters for executing an existing story.
type ExecuteStoryRequest struct {
	Submodule   string `json:"submodule,omitempty"`
	TargetAgent string `json:"target_agent,omitempty"`
	Prompt      string `json:"prompt,omitempty"`
}

// ExecuteStoryResponse is returned after queuing a story for execution.
type ExecuteStoryResponse struct {
	JobID       string `json:"job_id"`
	StoryID     string `json:"story_id"`
	Submodule   string `json:"submodule"`
	TargetAgent string `json:"target_agent"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
}

// GetJob serves GET /api/v1/jobs/{id}.
func (h *JobLifecycleHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	job, found := h.queue.Get(jobID)
	if !found || job == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
		return
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ListQueue serves GET /api/v1/queue and GET /api/v1/jobs.
func (h *JobLifecycleHandler) ListQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	q := r.URL.Query()
	repo := strings.TrimSpace(q.Get("repo"))
	status := strings.TrimSpace(q.Get("status"))
	jobType := strings.TrimSpace(q.Get("type"))

	limit := 20
	if rawLimit := q.Get("limit"); rawLimit != "" {
		if l, err := strconv.Atoi(rawLimit); err == nil && l > 0 {
			limit = l
		}
	}

	jobs, err := h.queue.List(r.Context(), dispatch.JobFilter{
		Repo:   repo,
		Status: status,
		Type:   jobType,
		Limit:  limit,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list queue: %v", err))
		return
	}

	items := make([]JobDetailResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, formatJobDetail(j))
	}

	resp := ListQueueResponse{
		Items: items,
		Total: len(items),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CancelJob serves POST /api/v1/jobs/{id}/cancel.
func (h *JobLifecycleHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	reason := "cancelled by user"
	if r.Body != nil {
		var req CancelJobRequest
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
			if strings.TrimSpace(req.Reason) != "" {
				reason = strings.TrimSpace(req.Reason)
			}
		}
	}

	job, err := h.queue.Cancel(r.Context(), jobID, reason)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		if errors.Is(err, dispatch.ErrJobAlreadyCompleted) {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf("job %q has already completed or terminated", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to cancel job: %v", err))
		return
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// RetryJob serves POST /api/v1/jobs/{id}/retry.
func (h *JobLifecycleHandler) RetryJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	newJob, err := h.queue.Retry(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to retry job: %v", err))
		return
	}

	resp := formatJobDetail(newJob)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// ExecuteStory serves POST /api/v1/stories/{id}/execute.
func (h *JobLifecycleHandler) ExecuteStory(w http.ResponseWriter, r *http.Request) {
	traceID, parentID := telemetry.ExtractHTTP(r.Header)
	var spanOpts []telemetry.SpanOption
	if traceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(traceID, parentID))
	}
	spanOpts = append(spanOpts, telemetry.WithKind("SERVER"), telemetry.WithTag("http.method", r.Method), telemetry.WithTag("http.path", r.URL.Path))
	ctx, span := telemetry.StartSpan(r.Context(), "JobLifecycleHandler.ExecuteStory", spanOpts...)
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	storyID := strings.TrimSpace(r.PathValue("id"))
	if storyID == "" {
		writeJSONError(w, http.StatusBadRequest, "story id is required")
		return
	}

	var req ExecuteStoryRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	submodule := strings.TrimSpace(req.Submodule)
	if submodule == "" {
		submodule = "."
	}
	targetAgent := strings.TrimSpace(req.TargetAgent)
	if targetAgent == "" {
		targetAgent = "autopilot"
	}

	promptText := strings.TrimSpace(req.Prompt)
	if promptText == "" {
		promptText = fmt.Sprintf("# Execute Story %s\n\nExecute autonomous story flow for %s in %s.", storyID, storyID, submodule)
	}

	promptID := fmt.Sprintf("prompt_%s_%d", storyID, time.Now().Unix())
	execPayload, err := json.Marshal(map[string]any{
		"prompt_id":    promptID,
		"repo":         submodule,
		"story_id":     storyID,
		"title":        fmt.Sprintf("Execute story %s", storyID),
		"prompt":       promptText,
		"target_agent": targetAgent,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal execution payload: %v", err))
		return
	}

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        submodule,
		TargetAgent: targetAgent,
		Payload:     execPayload,
		TraceID:     span.TraceID,
		ParentID:    span.ID,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue story execution: %v", err))
		return
	}

	if h.scheduler != nil {
		_, _ = h.scheduler.EnqueueGlobalStorySchedule(storyID, submodule, targetAgent, promptText)
	}

	resp := ExecuteStoryResponse{
		JobID:       job.ID,
		StoryID:     storyID,
		Submodule:   submodule,
		TargetAgent: targetAgent,
		Status:      job.Status,
		CreatedAt:   job.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func formatJobDetail(j *dispatch.Job) JobDetailResponse {
	if j == nil {
		return JobDetailResponse{}
	}

	resp := JobDetailResponse{
		ID:          j.ID,
		Type:        j.Type,
		Repo:        j.Repo,
		TargetHost:  j.TargetHost,
		TargetAgent: j.TargetAgent,
		Status:      j.Status,
		Result:      j.Result,
		Error:       j.Error,
		CreatedAt:   j.CreatedAt.Format(time.RFC3339),
	}

	if j.ClaimedAt != nil {
		resp.ClaimedAt = j.ClaimedAt.Format(time.RFC3339)
	}
	if j.CompletedAt != nil {
		resp.CompletedAt = j.CompletedAt.Format(time.RFC3339)
		if !j.CreatedAt.IsZero() {
			resp.DurationMs = j.CompletedAt.Sub(j.CreatedAt).Milliseconds()
		}
	}
	return resp
}
