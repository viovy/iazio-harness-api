package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

const defaultStoryDispatchTimeout = 15 * time.Second

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
		writeJSONError(w, http.StatusUnprocessableEntity, "target_files must not be empty")
		return
	}
	for _, tf := range req.TargetFiles {
		if err := validatePath(tf); err != nil {
			writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid target_file %q: %v", tf, err))
			return
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

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:     dispatch.JobTypeStoryCreate,
		Repo:     req.Submodule,
		Payload:  bodyBytes,
		TraceID:  span.TraceID,
		ParentID: span.ID,
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
		execPayload, _ := json.Marshal(map[string]any{
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
			TraceID:     span.TraceID,
			ParentID:    span.ID,
		})
		if execJob != nil {
			resp.ExecutionJobID = execJob.ID
			resp.ExecutionStatus = execJob.Status
		}
		if h.scheduler != nil {
			_, _ = h.scheduler.EnqueueGlobalStorySchedule(result.StoryID, req.Submodule, targetAgent, fmt.Sprintf("# Execute Story %s\n\nRun autonomous story flow for %s in %s.", result.StoryID, result.StoryID, req.Submodule))
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
