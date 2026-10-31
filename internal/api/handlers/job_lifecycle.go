package handlers

import (
	"context"
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

const (
	modeStoryCreate           = "story.create"
	modeStoryExecute          = "story.execute"
	modeStoryCreateAndExecute = "story.create_and_execute"
	defaultASECommand         = "/autonomous-story-execution"
)

// JobLifecycleHandler manages querying, listing, canceling, and retrying agent jobs.
type JobLifecycleHandler struct {
	queue          dispatch.Queue
	store          codebase.Store
	scheduler      GlobalScheduler
	historyFinder  HistoryFinder
	storyValidator StoryValidator
	engine         *control.Engine
	repoRoot       string
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

// SetRepoRoot overrides the workspace root directory used for story index resolution.
func (h *JobLifecycleHandler) SetRepoRoot(root string) {
	h.repoRoot = root
}

// SetEngine sets the control engine for dynamic tool priority allocation and story binding.
func (h *JobLifecycleHandler) SetEngine(engine *control.Engine) {
	h.engine = engine
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

	if h.engine != nil {
		if _, ok := h.engine.GetStory(storyID); ok {
			return true
		}
	}

	root := resolveStoriesRoot(h.repoRoot)
	all := loadIndexStories(root)
	for _, s := range all {
		if strings.EqualFold(s.ID, storyID) || strings.EqualFold(strings.TrimPrefix(s.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
			return true
		}
	}

	// Also check if any .story.md file matches this storyID in docs/stories, family hubs, or submodule/docs/stories
	storyCandidates := []string{
		filepath.Join(root, "docs", "stories"),
		filepath.Join(root, "iazio-infra", "docs", "stories"),
		filepath.Join(root, "autopilot-umbrella", "docs", "stories"),
		filepath.Join(root, "vigo", "docs", "stories"),
		filepath.Join(root, "eye-habit", "docs", "stories"),
	}
	if submodule != "" && submodule != "." {
		storyCandidates = append(storyCandidates,
			filepath.Join(root, submodule, "docs", "stories"),
			filepath.Join(submodule, "docs", "stories"),
		)
	}
	for _, hubDir := range []string{"iazio-infra", "autopilot-umbrella", "vigo", "eye-habit"} {
		for _, s := range loadIndexStories(filepath.Join(root, hubDir)) {
			if strings.EqualFold(s.ID, storyID) || strings.EqualFold(strings.TrimPrefix(s.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
				return true
			}
		}
	}

	for _, dir := range storyCandidates {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			eName := strings.ToUpper(e.Name())
			sID := strings.ToUpper(storyID)
			cleanID := strings.TrimPrefix(sID, "STORY-")
			if (strings.Contains(eName, sID) || (cleanID != "" && strings.Contains(eName, cleanID))) && (strings.HasSuffix(e.Name(), ".story.md") || strings.HasSuffix(e.Name(), ".md")) {
				return true
			}
		}
	}

	return false
}

// JobDetailResponse is the detailed JSON representation of a dispatch job.
type JobDetailResponse struct {
	ID                      string                        `json:"id"`
	Type                    string                        `json:"type"`
	Repo                    string                        `json:"repo"`
	TargetHost              string                        `json:"target_host,omitempty"`
	ClaimedHost             string                        `json:"claimed_host,omitempty"`
	HostID                  string                        `json:"host_id,omitempty"`
	TargetAgent             string                        `json:"target_agent,omitempty"`
	StoryID                 string                        `json:"story_id,omitempty"`
	StoryTitle              string                        `json:"story_title,omitempty"`
	StoryPath               string                        `json:"story_path,omitempty"`
	StoryAllocationStatus   string                        `json:"story_allocation_status,omitempty"`
	AllocationSource        string                        `json:"allocation_source,omitempty"`
	PriorityRankSelected    int                           `json:"priority_rank_selected,omitempty"`
	NormalizedPrompt        string                        `json:"normalized_prompt,omitempty"`
	EffectiveAction         string                        `json:"effective_action,omitempty"`
	SourceConversationTitle string                        `json:"source_conversation_title,omitempty"`
	LiveURL                 string                        `json:"live_url,omitempty"`
	Status                  string                        `json:"status"`
	Result                  json.RawMessage               `json:"result,omitempty"`
	Error                   string                        `json:"error,omitempty"`
	FailureCategory         string                        `json:"failure_category,omitempty"`
	FailureStage            string                        `json:"failure_stage,omitempty"`
	FailureReasonSummary    string                        `json:"failure_reason_summary,omitempty"`
	Retriable               bool                          `json:"retriable,omitempty"`
	ParentID                string                        `json:"parent_id,omitempty"`
	InfraRequeueCount       int                           `json:"infra_requeue_count,omitempty"`
	AttemptHistory          []dispatch.AttemptFailure     `json:"attempt_history,omitempty"`
	TraceID                 string                        `json:"trace_id,omitempty"`
	SearchID                string                        `json:"search_id,omitempty"`
	RelevanceEmitted        bool                          `json:"relevance_emitted"`
	PromotedMemories        []string                      `json:"promoted_memories,omitempty"`
	FencingToken            int64                         `json:"fencing_token,omitempty"`
	Attempts                int                           `json:"attempts,omitempty"`
	FinalTurnCount          int                           `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256   string                        `json:"final_transcript_sha256,omitempty"`
	HeartbeatAt             string                        `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt          string                        `json:"lease_expires_at,omitempty"`
	AgentAffinity           *dispatch.AgentAffinity       `json:"agent_affinity,omitempty"`
	DispatchDiagnostics     *dispatch.DispatchDiagnostics `json:"dispatch_diagnostics,omitempty"`
	CreatedAt               string                        `json:"created_at"`
	ClaimedAt               string                        `json:"claimed_at,omitempty"`
	CompletedAt             string                        `json:"completed_at,omitempty"`
	DurationMs              int64                         `json:"duration_ms,omitempty"`
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

// ExecuteStoryRequest specifies parameters for executing an existing or newly allocated story.
type ExecuteStoryRequest struct {
	Submodule               string `json:"submodule,omitempty"`
	Repo                    string `json:"repo,omitempty"`
	Backlog                 string `json:"backlog,omitempty"`
	StoryID                 string `json:"story_id,omitempty"`
	TargetAgent             string `json:"target_agent,omitempty"`
	Host                    string `json:"host,omitempty"`
	Prompt                  string `json:"prompt,omitempty"`
	Request                 string `json:"request,omitempty"`
	Specification           string `json:"specification,omitempty"`
	Description             string `json:"description,omitempty"`
	Action                  string `json:"action,omitempty"`
	Command                 string `json:"command,omitempty"`
	Mode                    string `json:"mode,omitempty"`
	SourceConversationTitle string `json:"source_conversation_title,omitempty"`
	SourceContext           string `json:"source_context,omitempty"`
	TraceID                 string `json:"trace_id,omitempty"`
	SearchID                string `json:"search_id,omitempty"`
}

// ExecuteStoryResponse is returned after queuing a story for execution.
type ExecuteStoryResponse struct {
	JobID                 string `json:"job_id"`
	StoryID               string `json:"story_id,omitempty"`
	Submodule             string `json:"submodule"`
	Backlog               string `json:"backlog,omitempty"`
	Mode                  string `json:"mode,omitempty"`
	TargetAgent           string `json:"target_agent"`
	TargetHost            string `json:"target_host,omitempty"`
	TargetRepo            string `json:"target_repo,omitempty"`
	Status                string `json:"status"`
	StoryAllocationStatus string `json:"story_allocation_status,omitempty"`
	AllocationSource      string `json:"allocation_source,omitempty"`
	PriorityRankSelected  int    `json:"priority_rank_selected,omitempty"`
	NormalizedPrompt      string `json:"normalized_prompt,omitempty"`
	EffectiveAction       string `json:"effective_action,omitempty"`
	LiveURL               string `json:"live_url,omitempty"`
	CreatedAt             string `json:"created_at"`
}

// BindStoryRequest is the payload for PATCH /api/v1/jobs/{id}/story-binding.
type BindStoryRequest struct {
	FencingToken int64  `json:"fencing_token,omitempty"`
	StoryID      string `json:"story_id"`
	StoryTitle   string `json:"story_title,omitempty"`
	StoryPath    string `json:"story_path,omitempty"`
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

	if job.DispatchDiagnostics == nil && h.engine != nil && (job.Status == dispatch.JobStatusPending || job.Status == dispatch.JobStatusClaimed || job.Status == dispatch.JobStatusLeased) {
		diag := h.engine.EvaluateDispatchDiagnostics(job.TargetHost, job.TargetAgent)
		if job.Status == dispatch.JobStatusClaimed || job.Status == dispatch.JobStatusLeased {
			diag.QueueState = "running"
		}
		_ = h.queue.SetDispatchDiagnostics(jobID, diag)
		job.DispatchDiagnostics = diag
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ForceDispatchJob serves POST /api/v1/jobs/{id}/force-dispatch.
func (h *JobLifecycleHandler) ForceDispatchJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	// 1. Reconcile any stale workspace slots across the fleet
	if h.engine != nil {
		_ = h.engine.ReconcileStaleWorkspaceSlots(time.Now().UTC(), 45*time.Second)
	}

	// 2. Break claim on the job and re-queue it
	job, err := h.queue.ForceDispatch(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		if errors.Is(err, dispatch.ErrJobAlreadyCompleted) {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf("job %q has already completed or terminated", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to force dispatch job: %v", err))
		return
	}

	// 3. Evaluate live candidate diagnostics
	if h.engine != nil {
		diag := h.engine.EvaluateDispatchDiagnostics(job.TargetHost, job.TargetAgent)
		diag.QueueState = "dispatching"
		_ = h.queue.SetDispatchDiagnostics(jobID, diag)
		job.DispatchDiagnostics = diag
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

	filter := dispatch.JobFilter{
		Repo:   repo,
		Status: status,
		Type:   jobType,
		Limit:  limit,
	}
	jobs, err := h.queue.List(r.Context(), filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list queue: %v", err))
		return
	}

	items := make([]JobDetailResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, formatJobDetail(j))
	}

	total := len(items)
	if counter, ok := h.queue.(interface {
		Count(ctx context.Context, filter dispatch.JobFilter) (int, error)
	}); ok {
		if c, err := counter.Count(r.Context(), filter); err == nil {
			total = c
		}
	}

	resp := ListQueueResponse{
		Items: items,
		Total: total,
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

// ExecuteStory serves POST /api/v1/stories/{id}/execute or POST /api/v1/stories/execute.
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

	var req ExecuteStoryRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	storyID := strings.TrimSpace(r.PathValue("id"))
	if storyID == "" {
		storyID = strings.TrimSpace(req.StoryID)
	}

	mode := strings.TrimSpace(req.Mode)
	switch mode {
	case modeStoryCreateAndExecute, modeStoryCreate:
		// Modes 1 and 3 never mock or accept a pre-assigned story ID.
		storyID = ""
	case modeStoryExecute:
		if storyID == "" {
			writeJSONError(w, http.StatusUnprocessableEntity, "story_id is mandatory for story.execute")
			return
		}
	default:
		if storyID != "" {
			mode = modeStoryExecute
		} else {
			mode = modeStoryCreateAndExecute
		}
	}

	backlog := strings.TrimSpace(req.Backlog)
	submodule := strings.TrimSpace(req.Submodule)
	if submodule == "" {
		submodule = strings.TrimSpace(req.Repo)
	}

	var storyTitle, storyPath string
	root := resolveBacklogStoriesRoot(h.repoRoot, backlog, submodule)
	if storyID != "" {
		if h.engine != nil {
			if st, ok := h.engine.GetStory(storyID); ok {
				storyID = st.ID
				storyTitle = st.Title
				if submodule == "" && st.Submodule != "" {
					submodule = st.Submodule
				}
			}
		}
		for _, idxSt := range loadIndexStories(root) {
			if strings.EqualFold(idxSt.ID, storyID) || strings.EqualFold(strings.TrimPrefix(idxSt.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
				if storyTitle == "" {
					storyTitle = idxSt.Title
				}
				if idxSt.Path != "" {
					storyPath = idxSt.Path
				}
				if submodule == "" && len(idxSt.Submodules) > 0 && idxSt.Submodules[0] != "" {
					submodule = idxSt.Submodules[0]
				}
				break
			}
		}
		if storyPath == "" {
			storyPath = fmt.Sprintf("docs/stories/%s.story.md", strings.ToLower(storyID))
		}
	}

	if submodule == "" {
		submodule = "."
	}

	storyAllocationStatus := "allocated"
	if storyID == "" {
		storyAllocationStatus = "pending_harness_creation"
	} else {
		if !h.isStoryAvailable(storyID, submodule) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("story %q not found in backlog: cannot execute nonexistent story. Create it first using create_story or verify it exists in docs/stories/.", storyID))
			return
		}
	}

	promptText := strings.TrimSpace(req.Prompt)
	if promptText == "" {
		promptText = strings.TrimSpace(req.Request)
	}
	if promptText == "" {
		promptText = strings.TrimSpace(req.Specification)
	}
	if promptText == "" {
		promptText = strings.TrimSpace(req.Description)
	}

	action := strings.TrimSpace(req.Command)
	if action == "" {
		action = strings.TrimSpace(req.Action)
	}
	if action == "" {
		if mode == modeStoryCreate {
			action = "/story-create"
		} else {
			action = defaultASECommand
		}
	}
	if !strings.HasPrefix(action, "/") {
		action = "/" + action
	}

	var normalizedPrompt string
	if mode == modeStoryExecute && storyID != "" {
		// Mode 2: concatenation of command ("/autonomous-story-execution") followed by story_id (and optional prompt)
		if strings.HasPrefix(promptText, action+" "+storyID) || strings.HasPrefix(promptText, action+"\n"+storyID) {
			normalizedPrompt = promptText
		} else {
			extra := strings.TrimSpace(strings.TrimPrefix(promptText, action))
			extra = strings.TrimSpace(strings.TrimPrefix(extra, storyID))
			if extra != "" {
				normalizedPrompt = action + " " + storyID + " " + extra
			} else {
				normalizedPrompt = action + " " + storyID
			}
		}
	} else {
		if promptText == "" {
			if mode == modeStoryCreate {
				promptText = fmt.Sprintf("Create story in backlog for %s.", submodule)
			} else {
				promptText = fmt.Sprintf("Execute autonomous story flow in %s.", submodule)
			}
		}
		normalizedPrompt = control.NormalizeASEPrompt(promptText, action)
	}

	targetAgent := strings.TrimSpace(req.TargetAgent)
	targetHost := strings.TrimSpace(req.Host)
	targetRepo := submodule
	allocationSource := "explicit_target"
	priorityRankSelected := 0

	if h.engine != nil {
		alloc := h.engine.ResolveToolPriorityAllocation(targetAgent, targetHost, submodule)
		targetAgent = alloc.TargetAgent
		targetHost = alloc.TargetHost
		if alloc.TargetRepo != "" && alloc.TargetRepo != "root" && (submodule == "" || submodule == "." || submodule == "root") {
			targetRepo = alloc.TargetRepo
		}
		allocationSource = alloc.AllocationSource
		priorityRankSelected = alloc.PriorityRankSelected
	} else if targetAgent == "" {
		targetAgent = "agy"
	}

	if t := strings.TrimSpace(req.TraceID); t != "" {
		traceID = t
	}
	if traceID == "" {
		traceID = span.TraceID
	}

	promptID := fmt.Sprintf("prompt_%s_%d", storyID, time.Now().Unix())
	if storyID == "" {
		promptID = fmt.Sprintf("prompt_ase_%d", time.Now().Unix())
	}
	execTitle := fmt.Sprintf("Execute story %s", storyID)
	if storyID == "" {
		execTitle = "Autonomous Story Execution"
	}

	execPayload, err := json.Marshal(map[string]any{
		"prompt_id":                 promptID,
		"repo":                      targetRepo,
		"submodule":                 submodule,
		"backlog":                   backlog,
		"mode":                      mode,
		"command":                   action,
		"worktree_path":             targetRepo,
		"story_id":                  storyID,
		"story_title":               storyTitle,
		"story_path":                storyPath,
		"story_allocation_status":   storyAllocationStatus,
		"allocation_source":         allocationSource,
		"priority_rank_selected":    priorityRankSelected,
		"source_conversation_title": req.SourceConversationTitle,
		"title":                     execTitle,
		"prompt":                    normalizedPrompt,
		"normalized_prompt":         normalizedPrompt,
		"effective_action":          action,
		"target_agent":              targetAgent,
		"target_host":               targetHost,
		"trace_id":                  traceID,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal execution payload: %v", err))
		return
	}

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:                    dispatch.JobTypePromptExecute,
		Repo:                    targetRepo,
		TargetHost:              targetHost,
		TargetAgent:             targetAgent,
		StoryID:                 storyID,
		StoryTitle:              storyTitle,
		StoryPath:               storyPath,
		StoryAllocationStatus:   storyAllocationStatus,
		AllocationSource:        allocationSource,
		PriorityRankSelected:    priorityRankSelected,
		SourceConversationTitle: req.SourceConversationTitle,
		NormalizedPrompt:        normalizedPrompt,
		EffectiveAction:         action,
		TraceID:                 traceID,
		ParentID:                span.ID,
		SearchID:                strings.TrimSpace(req.SearchID),
		Payload:                 execPayload,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue story execution: %v", err))
		return
	}

	if storyID != "" {
		if h.engine != nil {
			_ = h.engine.BindStoryToJob(job.ID, storyID, storyTitle, storyPath)
		}
		updateIndexStoryStatus(resolveStoriesRootForWrite(h.repoRoot), storyID, "IN_PROGRESS")
	}

	if h.queue == nil && h.scheduler != nil {
		_, _ = h.scheduler.EnqueueGlobalStorySchedule(storyID, submodule, targetAgent, normalizedPrompt)
	}

	liveURL := ""
	if targetHost != "" {
		liveRepo := submodule
		if liveRepo == "" || liveRepo == "." {
			liveRepo = "root"
		}
		webBase := os.Getenv("IAZIO_WEB_BASE_URL")
		if webBase == "" {
			webBase = "https://example.com/iazio-web"
		}
		liveURL = fmt.Sprintf("%s/agents/%s/%s/live?job=%s", strings.TrimRight(webBase, "/"), targetHost, liveRepo, job.ID)
	}

	resp := ExecuteStoryResponse{
		JobID:                 job.ID,
		StoryID:               storyID,
		Submodule:             submodule,
		Backlog:               backlog,
		Mode:                  mode,
		TargetAgent:           targetAgent,
		TargetHost:            targetHost,
		TargetRepo:            submodule,
		Status:                job.Status,
		StoryAllocationStatus: storyAllocationStatus,
		AllocationSource:      allocationSource,
		PriorityRankSelected:  priorityRankSelected,
		NormalizedPrompt:      normalizedPrompt,
		EffectiveAction:       action,
		LiveURL:               liveURL,
		CreatedAt:             job.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// BindStory serves PATCH /api/v1/jobs/{id}/story-binding and POST /api/v1/jobs/{id}/story-binding.
func (h *JobLifecycleHandler) BindStory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch && r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req BindStoryRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	storyID := strings.TrimSpace(req.StoryID)
	if storyID == "" {
		writeJSONError(w, http.StatusBadRequest, "story_id is required")
		return
	}

	var job *dispatch.Job
	var err error
	if fencer, ok := h.queue.(interface {
		BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*dispatch.Job, error)
	}); ok {
		job, err = fencer.BindStoryWithFencing(r.Context(), jobID, storyID, req.StoryTitle, req.StoryPath, req.FencingToken)
	} else {
		job, err = h.queue.BindStory(r.Context(), jobID, storyID, req.StoryTitle, req.StoryPath)
	}
	if err != nil {
		if errors.Is(err, dispatch.ErrLeaseFenced) {
			writeJSONError(w, http.StatusConflict, "LEASE_FENCED: stale fencing token presented")
			return
		}
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if h.engine != nil {
		_ = h.engine.BindStoryToJob(jobID, storyID, req.StoryTitle, req.StoryPath)
	}
	updateIndexStoryStatus(resolveStoriesRootForWrite(h.repoRoot), storyID, "IN_PROGRESS")

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(formatJobDetail(job))
}

func formatHistoryRowToJobDetail(row control.HistoryRow, host, path string) JobDetailResponse {
	return JobDetailResponse{
		ID:                    row.JobID,
		Type:                  "story_execution",
		Repo:                  path,
		TargetHost:            host,
		TargetAgent:           row.Engine,
		StoryID:               row.StoryID,
		StoryAllocationStatus: row.StoryAllocationStatus,
		AllocationSource:      row.AllocationSource,
		PriorityRankSelected:  row.PriorityRankSelected,
		Status:                row.Status,
		Error:                 row.ErrorMessage,
		CreatedAt:             row.CreatedAt.Format(time.RFC3339),
		PromotedMemories:      []string{},
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

	effectiveHost := strings.TrimSpace(j.TargetHost)
	if effectiveHost == "" || strings.EqualFold(effectiveHost, "auto") {
		if strings.TrimSpace(j.ClaimedHost) != "" {
			effectiveHost = strings.TrimSpace(j.ClaimedHost)
		} else if strings.TrimSpace(j.HostID) != "" {
			effectiveHost = strings.TrimSpace(j.HostID)
		}
	}

	resp := JobDetailResponse{
		ID:                      j.ID,
		Type:                    j.Type,
		Repo:                    j.Repo,
		TargetHost:              effectiveHost,
		ClaimedHost:             j.ClaimedHost,
		HostID:                  j.HostID,
		TargetAgent:             j.TargetAgent,
		StoryID:                 j.StoryID,
		StoryTitle:              j.StoryTitle,
		StoryPath:               j.StoryPath,
		StoryAllocationStatus:   j.StoryAllocationStatus,
		AllocationSource:        j.AllocationSource,
		PriorityRankSelected:    j.PriorityRankSelected,
		NormalizedPrompt:        j.NormalizedPrompt,
		EffectiveAction:         j.EffectiveAction,
		SourceConversationTitle: j.SourceConversationTitle,
		Status:                  j.Status,
		Result:                  j.Result,
		Error:                   j.Error,
		FailureCategory:         j.FailureCategory,
		FailureStage:            j.FailureStage,
		FailureReasonSummary:    j.FailureReasonSummary,
		Retriable:               j.Retriable,
		ParentID:                j.ParentID,
		InfraRequeueCount:       j.InfraRequeueCount,
		AttemptHistory:          j.AttemptHistory,
		TraceID:                 j.TraceID,
		SearchID:                j.SearchID,
		RelevanceEmitted:        j.RelevanceEmitted,
		PromotedMemories:        promoted,
		FencingToken:            j.FencingToken,
		Attempts:                j.Attempts,
		FinalTurnCount:          j.FinalTurnCount,
		FinalTranscriptSHA256:   j.FinalTranscriptSHA256,
		AgentAffinity:           j.AgentAffinity,
		DispatchDiagnostics:     j.DispatchDiagnostics,
		CreatedAt:               j.CreatedAt.Format(time.RFC3339),
	}

	if effectiveHost != "" && !strings.EqualFold(effectiveHost, "auto") {
		liveRepo := j.Repo
		if liveRepo == "" || liveRepo == "." {
			liveRepo = "root"
		}
		webBase := os.Getenv("IAZIO_WEB_BASE_URL")
		if webBase == "" {
			webBase = "https://example.com/iazio-web"
		}
		resp.LiveURL = fmt.Sprintf("%s/agents/%s/%s/live?job=%s", strings.TrimRight(webBase, "/"), effectiveHost, liveRepo, j.ID)
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

// AckLeaseRequest is the payload for Phase 2 lease handshake.
type AckLeaseRequest struct {
	LeaseToken           string `json:"lease_token"`
	AgentPID             int    `json:"agent_pid"`
	ToolPID              int    `json:"tool_pid,omitempty"`
	ActiveConversationID string `json:"active_conversation_id"`
	WorktreePath         string `json:"worktree_path,omitempty"`
	Repo                 string `json:"repo,omitempty"`
}

// AckLease serves POST /api/v1/global-queue/jobs/{id}/ack-lease and POST /api/v1/jobs/{id}/ack-lease.
func (h *JobLifecycleHandler) AckLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req AckLeaseRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	var qJob *dispatch.Job
	var errQueue error
	if h.queue != nil {
		qJob, errQueue = h.queue.AckLease(r.Context(), jobID, req.LeaseToken, req.AgentPID, req.ActiveConversationID)
	}

	var errEngine error
	if h.engine != nil {
		errEngine = h.engine.AckLease(jobID, req.LeaseToken, req.AgentPID, req.ActiveConversationID)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to acknowledge lease: %v", errQueue))
		return
	}

	effectiveWT := strings.TrimSpace(req.WorktreePath)
	if effectiveWT == "" {
		effectiveWT = strings.TrimSpace(req.Repo)
	}
	if qJob != nil {
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(qJob.Repo)
		}
		if h.engine != nil {
			hostID := qJob.ClaimedHost
			if hostID == "" {
				hostID = qJob.HostID
			}
			boundWT := h.engine.BindDispatchJobRepo(hostID, effectiveWT, jobID)
			if boundWT != "" {
				effectiveWT = boundWT
			}
		}
		if updater, ok := h.queue.(interface {
			UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*dispatch.Job, bool)
		}); ok {
			updater.UpdateRuntimeMetadata(jobID, effectiveWT, req.ActiveConversationID, req.AgentPID, req.ToolPID, 0)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":        "acknowledged",
		"job_id":        jobID,
		"worktree_path": effectiveWT,
		"message":       "Phase 2 lease handshake completed; job transitioned to running",
	})
}

// Heartbeat serves POST /api/v1/global-queue/jobs/{id}/heartbeat and POST /api/v1/jobs/{id}/heartbeat.
func (h *JobLifecycleHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req struct {
		LeaseToken           string `json:"lease_token"`
		FencingToken         int64  `json:"fencing_token"`
		WorktreePath         string `json:"worktree_path,omitempty"`
		Repo                 string `json:"repo,omitempty"`
		AgentPID             int    `json:"agent_pid,omitempty"`
		ToolPID              int    `json:"tool_pid,omitempty"`
		ActiveConversationID string `json:"active_conversation_id,omitempty"`
		ElapsedSeconds       int64  `json:"elapsed_seconds,omitempty"`
	}
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	var qJob *dispatch.Job
	var errQueue error
	if h.queue != nil {
		qJob, errQueue = h.queue.Heartbeat(r.Context(), jobID, req.FencingToken)
	}

	var errEngine error
	if h.engine != nil {
		errEngine = h.engine.HeartbeatJob(jobID, req.LeaseToken)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to heartbeat job: %v", errQueue))
		return
	}

	if qJob != nil {
		effectiveWT := strings.TrimSpace(req.WorktreePath)
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(req.Repo)
		}
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(qJob.Repo)
		}
		if h.engine != nil {
			hostID := qJob.ClaimedHost
			if hostID == "" {
				hostID = qJob.HostID
			}
			boundWT := h.engine.BindDispatchJobRepo(hostID, effectiveWT, jobID)
			if boundWT != "" {
				effectiveWT = boundWT
			}
		}
		if updater, ok := h.queue.(interface {
			UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*dispatch.Job, bool)
		}); ok {
			updater.UpdateRuntimeMetadata(jobID, effectiveWT, req.ActiveConversationID, req.AgentPID, req.ToolPID, req.ElapsedSeconds)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"job_id": jobID,
	})
}

// FailoverRequest carries details when a tool startup or pre-progress check fails.
type FailoverRequest struct {
	Host            string `json:"host"`
	Tool            string `json:"tool"`
	Workspace       string `json:"workspace"`
	ExitCode        int    `json:"exit_code"`
	StderrTail      string `json:"stderr_tail"`
	FailureCategory string `json:"failure_category"`
	FailureStage    string `json:"failure_stage"`
	Reason          string `json:"reason"`
	Timestamp       string `json:"timestamp"`
}

// Failover serves POST /api/v1/global-queue/jobs/{id}/failover and POST /api/v1/jobs/{id}/failover.
func (h *JobLifecycleHandler) Failover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req FailoverRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	attempt := dispatch.AttemptFailure{
		Host:            req.Host,
		Tool:            req.Tool,
		Workspace:       req.Workspace,
		ExitCode:        req.ExitCode,
		StderrTail:      req.StderrTail,
		FailureCategory: req.FailureCategory,
		FailureStage:    req.FailureStage,
		Reason:          req.Reason,
		Timestamp:       req.Timestamp,
	}

	var errQueue error
	if h.queue != nil {
		_, errQueue = h.queue.Failover(r.Context(), jobID, attempt)
	}

	var errEngine error
	if h.engine != nil {
		h.engine.ReleaseDispatchJobRepo(jobID)
		_, errEngine = h.engine.FailoverJob(jobID, attempt)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to failover job: %v", errQueue))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "failover_requeued",
		"job_id":  jobID,
		"message": "Tool failure recorded and cooled down; job requeued for next priority candidate",
	})
}

// RetryRetriable serves POST /api/v1/global-queue/jobs/retry-retriable.
func (h *JobLifecycleHandler) RetryRetriable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var retriedQueueCount int
	if h.queue != nil {
		retried, err := h.queue.RetryRetriable(r.Context())
		if err == nil {
			retriedQueueCount = len(retried)
		}
	}

	var retriedEngineCount int
	if h.engine != nil {
		retried, err := h.engine.RetryRetriableJobs()
		if err == nil {
			retriedEngineCount = len(retried)
		}
	}

	total := retriedQueueCount
	if retriedEngineCount > total {
		total = retriedEngineCount
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":        "ok",
		"retried_count": total,
		"message":       fmt.Sprintf("Re-enqueued %d retriable failed jobs", total),
	})
}
