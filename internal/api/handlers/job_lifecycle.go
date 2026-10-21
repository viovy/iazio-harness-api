package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

// HistoryFinder provides lookup of historical job records.
type HistoryFinder interface {
	FindHistoryRow(jobID string) (control.HistoryRow, string, string, bool)
}

// StoryValidator validates story existence before execution dispatch.
type StoryValidator interface {
	StoryExists(storyID, submodule string) bool
}

// JobLifecycleHandler manages querying, listing, canceling, and retrying agent jobs.
type JobLifecycleHandler struct {
	queue          dispatch.Queue
	store          codebase.Store
	scheduler      GlobalScheduler
	historyFinder  HistoryFinder
	storyValidator StoryValidator
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

// SetHistoryFinder sets the historical job lookup handler.
func (h *JobLifecycleHandler) SetHistoryFinder(finder HistoryFinder) {
	h.historyFinder = finder
}

// SetStoryValidator sets the story validation provider.
func (h *JobLifecycleHandler) SetStoryValidator(v StoryValidator) {
	h.storyValidator = v
}

func (h *JobLifecycleHandler) isStoryAvailable(storyID, submodule string) bool {
	if h.storyValidator != nil {
		return h.storyValidator.StoryExists(storyID, submodule)
	}

	root := findStoriesRoot()
	all := loadIndexStories(root)
	for _, s := range all {
		if strings.EqualFold(s.ID, storyID) {
			return true
		}
	}

	// Also check if any .story.md file matches this storyID in docs/stories or submodule/docs/stories
	storyCandidates := []string{
		filepath.Join(root, "docs", "stories"),
	}
	if submodule != "" && submodule != "." {
		storyCandidates = append(storyCandidates,
			filepath.Join(root, submodule, "docs", "stories"),
			filepath.Join(submodule, "docs", "stories"),
		)
	}

	for _, dir := range storyCandidates {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if strings.Contains(strings.ToUpper(e.Name()), strings.ToUpper(storyID)) && strings.HasSuffix(e.Name(), ".story.md") {
				return true
			}
		}
	}

	return false
}

// JobDetailResponse is the detailed JSON representation of a dispatch job.
type JobDetailResponse struct {
	ID               string                  `json:"id"`
	Type             string                  `json:"type"`
	Repo             string                  `json:"repo"`
	TargetHost       string                  `json:"target_host,omitempty"`
	TargetAgent      string                  `json:"target_agent,omitempty"`
	Status           string                  `json:"status"`
	Result           json.RawMessage         `json:"result,omitempty"`
	Error            string                  `json:"error,omitempty"`
	TraceID          string                  `json:"trace_id,omitempty"`
	SearchID         string                  `json:"search_id,omitempty"`
	RelevanceEmitted bool                    `json:"relevance_emitted"`
	PromotedMemories []string                `json:"promoted_memories,omitempty"`
	FencingToken     int64                   `json:"fencing_token,omitempty"`
	Attempts         int                     `json:"attempts,omitempty"`
	HeartbeatAt      string                  `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt   string                  `json:"lease_expires_at,omitempty"`
	AgentAffinity    *dispatch.AgentAffinity `json:"agent_affinity,omitempty"`
	CreatedAt        string                  `json:"created_at"`
	ClaimedAt        string                  `json:"claimed_at,omitempty"`
	CompletedAt      string                  `json:"completed_at,omitempty"`
	DurationMs       int64                   `json:"duration_ms,omitempty"`
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
	TraceID     string `json:"trace_id,omitempty"`
	SearchID    string `json:"search_id,omitempty"`
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
		if h.historyFinder != nil {
			if row, host, path, hFound := h.historyFinder.FindHistoryRow(jobID); hFound {
				resp := formatHistoryRowToJobDetail(row, host, path)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
		}
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

	if !h.isStoryAvailable(storyID, submodule) {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("story %q not found in backlog: cannot execute nonexistent story. Create it first using create_story or verify it exists in docs/stories/.", storyID))
		return
	}

	targetAgent := strings.TrimSpace(req.TargetAgent)
	if targetAgent == "" {
		targetAgent = "autopilot"
	}

	promptText := strings.TrimSpace(req.Prompt)
	if promptText == "" {
		promptText = fmt.Sprintf("# Execute Story %s\n\nExecute autonomous story flow for %s in %s.", storyID, storyID, submodule)
	}

	if t := strings.TrimSpace(req.TraceID); t != "" {
		traceID = t
	}
	if traceID == "" {
		traceID = span.TraceID
	}

	promptID := fmt.Sprintf("prompt_%s_%d", storyID, time.Now().Unix())
	execPayload, err := json.Marshal(map[string]any{
		"prompt_id":    promptID,
		"repo":         submodule,
		"story_id":     storyID,
		"title":        fmt.Sprintf("Execute story %s", storyID),
		"prompt":       promptText,
		"target_agent": targetAgent,
		"trace_id":     traceID,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal execution payload: %v", err))
		return
	}

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:        dispatch.JobTypePromptExecute,
		Repo:        submodule,
		TargetAgent: targetAgent,
		TraceID:     traceID,
		ParentID:    span.ID,
		SearchID:    strings.TrimSpace(req.SearchID),
		Payload:     execPayload,
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

func formatHistoryRowToJobDetail(row control.HistoryRow, host, path string) JobDetailResponse {
	return JobDetailResponse{
		ID:               row.JobID,
		Type:             "story_execution",
		Repo:             path,
		TargetHost:       host,
		TargetAgent:      row.Engine,
		Status:           row.Status,
		Error:            row.ErrorMessage,
		CreatedAt:        row.CreatedAt.Format(time.RFC3339),
		PromotedMemories: []string{},
	}
}

func formatJobDetail(j *dispatch.Job) JobDetailResponse {
	if j == nil {
		return JobDetailResponse{}
	}

	promoted := j.PromotedMemories
	if promoted == nil {
		promoted = []string{}
	}

	resp := JobDetailResponse{
		ID:               j.ID,
		Type:             j.Type,
		Repo:             j.Repo,
		TargetHost:       j.TargetHost,
		TargetAgent:      j.TargetAgent,
		Status:           j.Status,
		Result:           j.Result,
		Error:            j.Error,
		TraceID:          j.TraceID,
		SearchID:         j.SearchID,
		RelevanceEmitted: j.RelevanceEmitted,
		PromotedMemories: promoted,
		FencingToken:     j.FencingToken,
		Attempts:         j.Attempts,
		AgentAffinity:    j.AgentAffinity,
		CreatedAt:        j.CreatedAt.Format(time.RFC3339),
	}

	if j.HeartbeatAt != nil {
		resp.HeartbeatAt = j.HeartbeatAt.Format(time.RFC3339)
	}
	if j.LeaseExpiresAt != nil {
		resp.LeaseExpiresAt = j.LeaseExpiresAt.Format(time.RFC3339)
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

// GetJobAttribution serves GET /api/v1/jobs/{id}/attribution.
func (h *JobLifecycleHandler) GetJobAttribution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	attr, err := h.queue.GetAttribution(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(attr)
}
