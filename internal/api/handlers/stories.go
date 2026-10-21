package handlers

import (
	"bufio"
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

const defaultStoryDispatchTimeout = 2500 * time.Millisecond

// CreateStoryRequest defines the semantic parameters for scaffolding an autopilot-governed story.
type CreateStoryRequest struct {
	Submodule            string   `json:"submodule"`
	Title                string   `json:"title"`
	Priority             string   `json:"priority,omitempty"`
	Problem              string   `json:"problem"`
	AcceptanceCriteria   []string `json:"acceptance_criteria"`
	TargetFiles          []string `json:"target_files"`
	DependsOn            []string `json:"depends_on,omitempty"`
	VerificationCommands []string `json:"verification_commands,omitempty"`
	Enqueue              bool     `json:"enqueue,omitempty"`
	TargetAgent          string   `json:"target_agent,omitempty"`
	TraceID              string   `json:"trace_id,omitempty"`
	SearchID             string   `json:"search_id,omitempty"`
}

// CreateStoryResponse is the response emitted by POST /api/v1/stories.
type CreateStoryResponse struct {
	HarnessJobID     string `json:"harness_job_id"`
	Status           string `json:"status"`
	StoryID          string `json:"story_id,omitempty"`
	Submodule        string `json:"submodule"`
	StoryPath        string `json:"story_path,omitempty"`
	IndexUpdated     bool   `json:"index_updated,omitempty"`
	ExecutionJobID   string `json:"execution_job_id,omitempty"`
	ExecutionStatus  string `json:"execution_status,omitempty"`
	CreatedAt        string `json:"created_at"`
	IdempotentReplay bool   `json:"idempotent_replay"`
}

// StoryResultPayload matches the JSON result emitted by iazio-harness upon story creation.
type StoryResultPayload struct {
	StoryID      string `json:"story_id"`
	Submodule    string `json:"submodule"`
	StoryPath    string `json:"story_path"`
	IndexUpdated bool   `json:"index_updated"`
}

// GlobalScheduler enqueues autonomous story execution tasks into the global queue.
type GlobalScheduler interface {
	EnqueueGlobalStorySchedule(storyID, submodule, targetAgent, promptText string) (control.Schedule, error)
}

// StoriesHandler handles semantic story validation and dispatch.
type StoriesHandler struct {
	store          codebase.Store
	queue          dispatch.Queue
	idempotency    *IdempotencyStore
	dispatchWait   time.Duration
	scheduler      GlobalScheduler
}

// NewStoriesHandler constructs a new StoriesHandler.
func NewStoriesHandler(store codebase.Store, queue dispatch.Queue, idempotency *IdempotencyStore, scheduler ...GlobalScheduler) *StoriesHandler {
	if idempotency == nil {
		idempotency = NewIdempotencyStore(24 * time.Hour)
	}

	timeout := defaultStoryDispatchTimeout
	if env := os.Getenv("STORY_DISPATCH_WAIT_TIMEOUT"); env != "" {
		if sec, err := strconv.Atoi(env); err == nil && sec > 0 {
			timeout = time.Duration(sec) * time.Second
		} else if d, err := time.ParseDuration(env); err == nil && d > 0 {
			timeout = d
		}
	}

	var sched GlobalScheduler
	if len(scheduler) > 0 {
		sched = scheduler[0]
	}

	return &StoriesHandler{
		store:        store,
		queue:        queue,
		idempotency:  idempotency,
		dispatchWait: timeout,
		scheduler:    sched,
	}
}

// SetDispatchWaitTimeout allows overriding the await timeout for tests.
func (h *StoriesHandler) SetDispatchWaitTimeout(d time.Duration) {
	h.dispatchWait = d
}

func (h *StoriesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	traceID, parentID := telemetry.ExtractHTTP(r.Header)
	var spanOpts []telemetry.SpanOption
	if traceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(traceID, parentID))
	}
	spanOpts = append(spanOpts, telemetry.WithKind("SERVER"), telemetry.WithTag("http.method", r.Method), telemetry.WithTag("http.path", r.URL.Path))
	ctx, span := telemetry.StartSpan(r.Context(), "StoriesHandler.ServeHTTP", spanOpts...)
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	defer r.Body.Close()

	payloadHash := ComputePayloadHash(bodyBytes)
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	if idempotencyKey != "" {
		cachedResp, _, replay, conflict := h.idempotency.Check(idempotencyKey, payloadHash)
		if conflict {
			writeJSONError(w, http.StatusConflict, "idempotency key conflict: payload mismatch")
			return
		}
		if replay && cachedResp != nil {
			var replayObj CreateStoryResponse
			if err := json.Unmarshal(cachedResp, &replayObj); err == nil {
				replayObj.IdempotentReplay = true
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(replayObj)
				return
			}
		}
	}

	var req CreateStoryRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "malformed JSON request body")
		return
	}

	req.Submodule = strings.TrimSpace(req.Submodule)
	req.Title = strings.TrimSpace(req.Title)
	req.Problem = strings.TrimSpace(req.Problem)

	if req.Submodule == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "submodule must not be empty")
		return
	}
	if err := validatePath(req.Submodule); err != nil && req.Submodule != "." {
		writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid submodule path: %v", err))
		return
	}
	if req.Title == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "title must not be empty")
		return
	}
	if req.Problem == "" {
		writeJSONError(w, http.StatusUnprocessableEntity, "problem statement must not be empty")
		return
	}

	req.Priority = strings.ToUpper(strings.TrimSpace(req.Priority))
	if req.Priority == "" {
		req.Priority = "P2"
	}
	if req.Priority != "P0" && req.Priority != "P1" && req.Priority != "P2" && req.Priority != "P3" {
		writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid priority %q (must be P0, P1, P2, or P3)", req.Priority))
		return
	}

	if len(req.AcceptanceCriteria) == 0 {
		writeJSONError(w, http.StatusUnprocessableEntity, "acceptance_criteria must not be empty")
		return
	}
	if len(req.TargetFiles) == 0 {
		req.TargetFiles = []string{req.Submodule}
	}
	for _, tf := range req.TargetFiles {
		if tf != "." {
			if err := validatePath(tf); err != nil {
				writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid target_file %q: %v", tf, err))
				return
			}
		}
	}

	// Verify submodule exists in the registered fleet repository table
	if req.Submodule != "." {
		_, found, err := h.store.GetRepository(r.Context(), req.Submodule)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to lookup submodule")
			return
		}
		if !found {
			writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("unknown submodule %q", req.Submodule))
			return
		}
	}

	if h.queue == nil {
		writeJSONError(w, http.StatusInternalServerError, "dispatch queue not available")
		return
	}

	jobTraceID := strings.TrimSpace(req.TraceID)
	if jobTraceID == "" {
		jobTraceID = span.TraceID
	}

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:     dispatch.JobTypeStoryCreate,
		Repo:     req.Submodule,
		Payload:  bodyBytes,
		TraceID:  jobTraceID,
		ParentID: span.ID,
		SearchID: strings.TrimSpace(req.SearchID),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue story: %v", err))
		return
	}

	// Bounded Await with Async Fallback
	completedJob, err := h.queue.Await(r.Context(), job.ID, h.dispatchWait)
	if errors.Is(err, dispatch.ErrTimeout) {
		// Timeout reached -> 202 Accepted
		resp := CreateStoryResponse{
			HarnessJobID:     job.ID,
			Status:           "queued",
			Submodule:        req.Submodule,
			CreatedAt:        job.CreatedAt.Format(time.RFC3339),
			IdempotentReplay: false,
		}

		// When enqueue: true is requested, guarantee that execution is dispatched when the scaffolding job completes in the background
		if req.Enqueue {
			targetAgent := strings.TrimSpace(req.TargetAgent)
			if targetAgent == "" {
				targetAgent = "autopilot"
			}
			go func(jobID, submodule, title, agent, sID, pSpanID string) {
				watchCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
				defer cancel()

				cJob, aErr := h.queue.Await(watchCtx, jobID, 15*time.Minute)
				if aErr == nil && cJob != nil && cJob.Status == dispatch.JobStatusCompleted {
					var res StoryResultPayload
					if jErr := json.Unmarshal(cJob.Result, &res); jErr == nil && res.StoryID != "" {
						promptID := fmt.Sprintf("prompt_%s_%d", res.StoryID, time.Now().Unix())
						execPayload, _ := json.Marshal(map[string]any{
							"prompt_id":    promptID,
							"repo":         submodule,
							"story_id":     res.StoryID,
							"title":        fmt.Sprintf("Execute story %s: %s", res.StoryID, title),
							"prompt":       fmt.Sprintf("# Execute Story %s\n\nRun autonomous story flow for %s in %s.", res.StoryID, res.StoryID, submodule),
							"target_agent": agent,
						})
						_, _ = h.queue.Enqueue(context.Background(), &dispatch.Job{
							Type:        dispatch.JobTypePromptExecute,
							Repo:        submodule,
							TargetAgent: agent,
							Payload:     execPayload,
							TraceID:     jobTraceID,
							ParentID:    pSpanID,
							SearchID:    sID,
						})
						if h.scheduler != nil {
							_, _ = h.scheduler.EnqueueGlobalStorySchedule(res.StoryID, submodule, agent, fmt.Sprintf("# Execute Story %s\n\nRun autonomous story flow for %s in %s.", res.StoryID, res.StoryID, submodule))
						}
					}
				}
			}(job.ID, req.Submodule, req.Title, targetAgent, strings.TrimSpace(req.SearchID), span.ID)
		}

		respBytes, _ := json.Marshal(resp)
		if idempotencyKey != "" {
			h.idempotency.Store(idempotencyKey, payloadHash, http.StatusAccepted, respBytes)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(respBytes)
		return
	}

	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error awaiting story execution: %v", err))
		return
	}

	if completedJob.Status == dispatch.JobStatusFailed {
		if strings.Contains(completedJob.Error, "WORKSPACE_LOCKED") {
			writeJSONError(w, http.StatusLocked, fmt.Sprintf("workspace locked: %s", completedJob.Error))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("story execution failed: %s", completedJob.Error))
		return
	}

	var result StoryResultPayload
	_ = json.Unmarshal(completedJob.Result, &result)

	resp := CreateStoryResponse{
		HarnessJobID:     job.ID,
		Status:           "completed",
		StoryID:          result.StoryID,
		Submodule:        req.Submodule,
		StoryPath:        result.StoryPath,
		IndexUpdated:     result.IndexUpdated,
		CreatedAt:        job.CreatedAt.Format(time.RFC3339),
		IdempotentReplay: false,
	}

	if req.Enqueue && result.StoryID != "" {
		targetAgent := strings.TrimSpace(req.TargetAgent)
		if targetAgent == "" {
			targetAgent = "autopilot"
		}
		promptID := fmt.Sprintf("prompt_%s_%d", result.StoryID, time.Now().Unix())
		execPayload, _ := json.Marshal(map[string]any{
			"prompt_id":    promptID,
			"repo":         req.Submodule,
			"story_id":     result.StoryID,
			"title":        fmt.Sprintf("Execute story %s: %s", result.StoryID, req.Title),
			"prompt":       fmt.Sprintf("# Execute Story %s\n\nRun autonomous story flow for %s in %s.", result.StoryID, result.StoryID, req.Submodule),
			"target_agent": targetAgent,
		})
		execJob, _ := h.queue.Enqueue(r.Context(), &dispatch.Job{
			Type:        dispatch.JobTypePromptExecute,
			Repo:        req.Submodule,
			TargetAgent: targetAgent,
			Payload:     execPayload,
			TraceID:     jobTraceID,
			ParentID:    span.ID,
			SearchID:    strings.TrimSpace(req.SearchID),
		})
		if execJob != nil {
			resp.ExecutionJobID = execJob.ID
			resp.ExecutionStatus = execJob.Status
		}
		if h.scheduler != nil {
			go func() {
				_, _ = h.scheduler.EnqueueGlobalStorySchedule(result.StoryID, req.Submodule, targetAgent, fmt.Sprintf("# Execute Story %s\n\nRun autonomous story flow for %s in %s.", result.StoryID, result.StoryID, req.Submodule))
			}()
		}
	}

	respBytes, _ := json.Marshal(resp)
	if idempotencyKey != "" {
		h.idempotency.Store(idempotencyKey, payloadHash, http.StatusCreated, respBytes)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBytes)
}

func validatePath(p string) error {
	clean := strings.TrimSpace(p)
	if clean == "" {
		return errors.New("path must not be empty")
	}
	if strings.Contains(clean, "..") {
		return fmt.Errorf("path traversal not permitted: %q", p)
	}
	if strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "\\") {
		return fmt.Errorf("absolute paths not permitted: %q", p)
	}
	if len(clean) >= 2 && clean[1] == ':' && ((clean[0] >= 'a' && clean[0] <= 'z') || (clean[0] >= 'A' && clean[0] <= 'Z')) {
		return fmt.Errorf("volume prefixes not permitted: %q", p)
	}
	return nil
}

// StoryDetail describes a story for GET /api/v1/stories and GET /api/v1/stories/{id}.
type StoryDetail struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Status      string   `json:"status"`
	Priority    string   `json:"priority"`
	Path        string   `json:"path"`
	Submodules  []string `json:"submodules,omitempty"`
	CompletedAt string   `json:"completed_at,omitempty"`
	UpdatedAt   string   `json:"updated_at,omitempty"`
	Content     string   `json:"content,omitempty"`
}

func findStoriesRoot() string {
	for _, env := range []string{"STORIES_ROOT", "WORKSPACE_ROOT", "META_REPO_ROOT"} {
		if val := strings.TrimSpace(os.Getenv(env)); val != "" {
			if _, err := os.Stat(filepath.Join(val, "docs", "stories", "index.yml")); err == nil {
				return val
			}
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "docs", "stories", "index.yml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "."
}

func loadIndexStories(root string) []StoryDetail {
	indexPath := filepath.Join(root, "docs", "stories", "index.yml")
	f, err := os.Open(indexPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	var stories []StoryDetail
	var current *StoryDetail
	inStories := false
	inSubmodules := false

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "stories:" {
			inStories = true
			continue
		}
		if !inStories {
			continue
		}
		if strings.HasPrefix(trimmed, "- id:") {
			if current != nil {
				stories = append(stories, *current)
			}
			current = &StoryDetail{
				ID: strings.TrimSpace(strings.TrimPrefix(trimmed, "- id:")),
			}
			inSubmodules = false
			continue
		}
		if current == nil {
			continue
		}
		if strings.HasPrefix(trimmed, "title:") {
			current.Title = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "title:")), `"`)
			inSubmodules = false
		} else if strings.HasPrefix(trimmed, "status:") {
			current.Status = strings.TrimSpace(strings.TrimPrefix(trimmed, "status:"))
			inSubmodules = false
		} else if strings.HasPrefix(trimmed, "priority:") {
			current.Priority = strings.TrimSpace(strings.TrimPrefix(trimmed, "priority:"))
			inSubmodules = false
		} else if strings.HasPrefix(trimmed, "path:") {
			current.Path = strings.TrimSpace(strings.TrimPrefix(trimmed, "path:"))
			inSubmodules = false
		} else if strings.HasPrefix(trimmed, "submodules:") {
			inSubmodules = true
		} else if inSubmodules && strings.HasPrefix(trimmed, "- ") {
			current.Submodules = append(current.Submodules, strings.TrimSpace(strings.TrimPrefix(trimmed, "- ")))
		} else if strings.HasPrefix(trimmed, "completed_at:") {
			current.CompletedAt = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "completed_at:")), `"`)
			inSubmodules = false
		} else if strings.HasPrefix(trimmed, "updated_at:") {
			current.UpdatedAt = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "updated_at:")), `"`)
			inSubmodules = false
		}
	}
	if current != nil {
		stories = append(stories, *current)
	}
	return stories
}

// ListStories handles GET /api/v1/stories with query filters.
func (h *StoriesHandler) ListStories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	statusFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))
	categoryFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("category")))
	submoduleFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("submodule")))
	limit := 20
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}

	root := findStoriesRoot()
	all := loadIndexStories(root)

	var matched []StoryDetail
	for _, s := range all {
		if statusFilter != "" && strings.ToUpper(s.Status) != statusFilter {
			continue
		}
		if categoryFilter != "" && !strings.Contains(strings.ToUpper(s.ID), categoryFilter) {
			continue
		}
		if submoduleFilter != "" {
			hasSub := false
			for _, sub := range s.Submodules {
				if strings.ToLower(sub) == submoduleFilter {
					hasSub = true
					break
				}
			}
			if !hasSub {
				continue
			}
		}
		matched = append(matched, s)
	}

	total := len(matched)
	if len(matched) > limit {
		matched = matched[:limit]
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"items":      matched,
		"item_count": len(matched),
		"total":      total,
	})
}

// GetStory handles GET /api/v1/stories/{id}.
func (h *StoriesHandler) GetStory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/v1/stories/")
	id = strings.TrimSpace(strings.TrimSuffix(id, "/"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "story id is required")
		return
	}

	root := findStoriesRoot()
	all := loadIndexStories(root)

	var found *StoryDetail
	for _, s := range all {
		if strings.EqualFold(s.ID, id) {
			match := s
			found = &match
			break
		}
	}

	if found == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("story %q not found", id))
		return
	}

	if found.Path != "" {
		contentBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(found.Path)))
		if err == nil {
			found.Content = string(contentBytes)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(found)
}
