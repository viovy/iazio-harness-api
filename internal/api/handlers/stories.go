package handlers

import (
	"bufio"
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

const defaultStoryDispatchTimeout = 10 * time.Second

// CreateStoryRequest defines the semantic parameters for scaffolding an autopilot-governed story.
type CreateStoryRequest struct {
	ID                   string   `json:"id,omitempty"`
	Repo                 string   `json:"repo,omitempty"`
	Submodule            string   `json:"submodule"`
	Title                string   `json:"title"`
	Priority             string   `json:"priority,omitempty"`
	Problem              string   `json:"problem"`
	AcceptanceCriteria   []string `json:"acceptance_criteria"`
	TargetFiles          []string `json:"target_files"`
	DependsOn            []string `json:"depends_on,omitempty"`
	VerificationCommands []string `json:"verification_commands,omitempty"`
	Body                 string   `json:"body,omitempty"`
	ContentPath          string   `json:"content_path,omitempty"`
	Prompt               string   `json:"prompt,omitempty"`
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
	SyncState        string `json:"sync_state,omitempty"`
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

func normalizeCanonicalStoryStatus(status, syncState string) string {
	s := strings.ToUpper(strings.TrimSpace(status))
	switch s {
	case "PENDING", "READY":
		if strings.EqualFold(syncState, "PENDING_WORKSPACE_MATERIALIZATION") {
			return "READY"
		}
		return "READY"
	case "PENDING_INDEX":
		return "PENDING_INDEX"
	case "IN_PROGRESS", "RUNNING":
		return "IN_PROGRESS"
	case "DONE", "COMPLETED":
		return "DONE"
	case "":
		return "READY"
	default:
		return s
	}
}

func matchesStoryStatusFilter(storyStatus, syncState, filter string) bool {
	if filter == "" {
		return true
	}
	f := strings.ToUpper(strings.TrimSpace(filter))
	norm := normalizeCanonicalStoryStatus(storyStatus, syncState)
	if strings.EqualFold(storyStatus, f) || norm == f {
		return true
	}
	if (f == "READY" || f == "PENDING" || f == "PENDING_INDEX") && (norm == "READY" || norm == "PENDING_INDEX" || strings.EqualFold(syncState, "PENDING_WORKSPACE_MATERIALIZATION")) {
		return true
	}
	return false
}

// StoriesHandler handles semantic story validation and dispatch.
type StoriesHandler struct {
	store        codebase.Store
	queue        dispatch.Queue
	idempotency  *IdempotencyStore
	dispatchWait time.Duration
	scheduler    GlobalScheduler
	engine       *control.Engine
	repoRoot     string
}

// SetEngine injects the control plane engine for story persistence.
func (h *StoriesHandler) SetEngine(e *control.Engine) {
	h.engine = e
}

// SetRepoRoot overrides the repository root directory used for docs/stories/index.yml resolution.
func (h *StoriesHandler) SetRepoRoot(root string) {
	h.repoRoot = root
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
	switch req.Priority {
	case "CRITICAL":
		req.Priority = "P0"
	case "HIGH":
		req.Priority = "P1"
	case "MEDIUM":
		req.Priority = "P2"
	case "LOW":
		req.Priority = "P3"
	}
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

	storyID := strings.TrimSpace(req.ID)
	body := strings.TrimSpace(req.Body)
	if body == "" {
		body = strings.TrimSpace(req.Problem)
	}

	if h.engine != nil && storyID != "" {
		st := control.Story{
			ID:                   storyID,
			Repo:                 req.Repo,
			Submodule:            req.Submodule,
			Title:                req.Title,
			Priority:             req.Priority,
			Status:               "READY",
			SyncState:            "PENDING_WORKSPACE_MATERIALIZATION",
			Body:                 body,
			Problem:              req.Problem,
			AcceptanceCriteria:   req.AcceptanceCriteria,
			TargetFiles:          req.TargetFiles,
			DependsOn:            req.DependsOn,
			VerificationCommands: req.VerificationCommands,
			CreatedAt:            time.Now().UTC(),
			UpdatedAt:            time.Now().UTC(),
		}
		_ = h.engine.SaveStory(st)
	}

	var execJobID string
	var executionStatus string
	if req.Enqueue || storyID == "" {
		targetAgent := strings.TrimSpace(req.TargetAgent)
		var alloc control.ToolPriorityAllocation
		if h.engine != nil {
			alloc = h.engine.ResolveToolPriorityAllocation(targetAgent, "", req.Submodule)
			if alloc.TargetAgent != "" {
				targetAgent = alloc.TargetAgent
			}
		} else if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
			targetAgent = "agy"
		}
		if targetAgent == "" {
			targetAgent = "agy"
		}
		promptID := fmt.Sprintf("prompt_%d", time.Now().UnixNano())
		if storyID != "" {
			promptID = fmt.Sprintf("prompt_%s_%d", storyID, time.Now().Unix())
		}
		promptText := strings.TrimSpace(req.Prompt)
		if promptText == "" {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("Title: %s\nSubmodule: %s\nPriority: %s\n\nProblem:\n%s\n", req.Title, req.Submodule, req.Priority, body))
			if len(req.AcceptanceCriteria) > 0 {
				sb.WriteString("\nAcceptance Criteria:\n- " + strings.Join(req.AcceptanceCriteria, "\n- ") + "\n")
			}
			if len(req.TargetFiles) > 0 {
				sb.WriteString("\nTarget Files:\n- " + strings.Join(req.TargetFiles, "\n- ") + "\n")
			}
			if len(req.VerificationCommands) > 0 {
				sb.WriteString("\nVerification Commands:\n- " + strings.Join(req.VerificationCommands, "\n- ") + "\n")
			}
			promptText = control.NormalizeASEPrompt(sb.String(), "/autonomous-story-execution")
		}
		normalizedPrompt := control.NormalizeASEPrompt(promptText, "/autonomous-story-execution")
		title := req.Title
		if storyID != "" {
			title = fmt.Sprintf("Execute story %s: %s", storyID, req.Title)
		}
		execPayload, _ := json.Marshal(map[string]any{
			"prompt_id":             promptID,
			"repo":                  req.Submodule,
			"story_id":              storyID,
			"title":                 title,
			"prompt":                promptText,
			"normalized_prompt":     normalizedPrompt,
			"target_agent":          targetAgent,
			"priority":              req.Priority,
			"problem":               req.Problem,
			"acceptance_criteria":   req.AcceptanceCriteria,
			"target_files":          req.TargetFiles,
			"depends_on":            req.DependsOn,
			"verification_commands": req.VerificationCommands,
		})
		if h.queue != nil {
			storyAllocStatus := "allocated"
			effectiveAction := "/autonomous-story-execution"
			if storyID == "" {
				storyAllocStatus = "pending_harness_creation"
				effectiveAction = "story.create_and_execute"
			}
			ej, _ := h.queue.Enqueue(r.Context(), &dispatch.Job{
				Type:                  dispatch.JobTypePromptExecute,
				Repo:                  req.Submodule,
				TargetHost:            alloc.TargetHost,
				TargetAgent:           targetAgent,
				StoryID:               storyID,
				StoryTitle:            req.Title,
				StoryPath:             strings.TrimSpace(req.ContentPath),
				StoryAllocationStatus: storyAllocStatus,
				EffectiveAction:       effectiveAction,
				NormalizedPrompt:      normalizedPrompt,
				AllocationSource:      alloc.AllocationSource,
				PriorityRankSelected:  alloc.PriorityRankSelected,
				Payload:               execPayload,
				TraceID:               jobTraceID,
				ParentID:              span.ID,
				SearchID:              strings.TrimSpace(req.SearchID),
			})
			if ej != nil {
				execJobID = ej.ID
				executionStatus = "pending"
			}
		} else if h.scheduler != nil {
			sch, err := h.scheduler.EnqueueGlobalStorySchedule(storyID, req.Submodule, targetAgent, promptText)
			if err == nil {
				execJobID = sch.ID
				executionStatus = "pending"
			}
		}
	}

	resp := CreateStoryResponse{
		HarnessJobID:     execJobID,
		Status:           "created",
		StoryID:          storyID,
		Submodule:        req.Submodule,
		StoryPath:        strings.TrimSpace(req.ContentPath),
		IndexUpdated:     false,
		SyncState:        "PENDING_WORKSPACE_MATERIALIZATION",
		ExecutionJobID:   execJobID,
		ExecutionStatus:  executionStatus,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		IdempotentReplay: false,
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
	ID                   string   `json:"id"`
	Title                string   `json:"title"`
	Status               string   `json:"status"`
	Priority             string   `json:"priority"`
	Path                 string   `json:"path"`
	Submodules           []string `json:"submodules,omitempty"`
	AcceptanceCriteria   []string `json:"acceptance_criteria,omitempty"`
	TargetFiles          []string `json:"target_files,omitempty"`
	DependsOn            []string `json:"depends_on,omitempty"`
	VerificationCommands []string `json:"verification_commands,omitempty"`
	CompletedAt          string   `json:"completed_at,omitempty"`
	UpdatedAt            string   `json:"updated_at,omitempty"`
	Content              string   `json:"content,omitempty"`
}

func resolveStoriesRoot(repoRoot string) string {
	if trimmed := strings.TrimSpace(repoRoot); trimmed != "" {
		return trimmed
	}
	if envRoot := strings.TrimSpace(os.Getenv("IAZIO_REPO_ROOT")); envRoot != "" {
		return envRoot
	}
	return findStoriesRoot()
}

func resolveStoriesRootForWrite(repoRoot string) string {
	if trimmed := strings.TrimSpace(repoRoot); trimmed != "" {
		return trimmed
	}
	if envRoot := strings.TrimSpace(os.Getenv("IAZIO_REPO_ROOT")); envRoot != "" {
		return envRoot
	}
	if len(os.Args) > 0 {
		exe := strings.ToLower(os.Args[0])
		if strings.HasSuffix(exe, ".test") || strings.HasSuffix(exe, ".test.exe") || strings.Contains(exe, "_test") {
			return ""
		}
	}
	return findStoriesRoot()
}

func normalizePriorityFilter(p string) string {
	norm := strings.ToUpper(strings.TrimSpace(p))
	switch norm {
	case "CRITICAL":
		return "P0"
	case "HIGH":
		return "P1"
	case "MEDIUM":
		return "P2"
	case "LOW":
		return "P3"
	default:
		return norm
	}
}

func synthesizeCanonicalStoryMarkdown(s control.Story, status string) string {
	trimmed := strings.TrimSpace(s.Body)
	if strings.HasPrefix(trimmed, "---") {
		return s.Body
	}
	if trimmed != "" && strings.HasPrefix(trimmed, "#") && strings.TrimSpace(s.Problem) == "" && len(s.AcceptanceCriteria) == 0 && len(s.TargetFiles) == 0 && len(s.VerificationCommands) == 0 {
		return s.Body
	}
	if trimmed == "" {
		trimmed = strings.TrimSpace(s.Problem)
	}
	priority := strings.TrimSpace(s.Priority)
	if priority == "" {
		priority = "P2"
	}
	category := "GOV"
	parts := strings.Split(strings.ToUpper(s.ID), "-")
	if len(parts) >= 2 && parts[0] == "STORY" {
		category = parts[1]
	} else if len(parts) >= 1 && parts[0] != "" {
		category = parts[0]
	}
	submodule := strings.TrimSpace(s.Submodule)
	if submodule == "" {
		submodule = strings.TrimSpace(s.Repo)
	}
	if submodule == "" {
		submodule = "."
	}
	var sb strings.Builder
	sb.WriteString("---\n")
	sb.WriteString(fmt.Sprintf("id: %s\n", s.ID))
	sb.WriteString(fmt.Sprintf("title: %q\n", s.Title))
	sb.WriteString(fmt.Sprintf("status: %s\n", status))
	sb.WriteString(fmt.Sprintf("priority: %s\n", priority))
	sb.WriteString(fmt.Sprintf("category: %s\n", category))
	sb.WriteString(fmt.Sprintf("submodule: %s\n", submodule))
	sb.WriteString("---\n\n")
	sb.WriteString(fmt.Sprintf("# %s: %s\n\n", s.ID, s.Title))
	if trimmed != "" {
		sb.WriteString("## Problem\n\n")
		sb.WriteString(trimmed + "\n\n")
	}
	if len(s.AcceptanceCriteria) > 0 {
		sb.WriteString("## Acceptance Criteria\n\n")
		for _, ac := range s.AcceptanceCriteria {
			sb.WriteString("- " + ac + "\n")
		}
		sb.WriteString("\n")
	}
	if len(s.TargetFiles) > 0 {
		sb.WriteString("## Target Files\n\n")
		for _, tf := range s.TargetFiles {
			sb.WriteString("- " + tf + "\n")
		}
		sb.WriteString("\n")
	}
	if len(s.VerificationCommands) > 0 {
		sb.WriteString("## Verification Commands\n\n")
		for _, vc := range s.VerificationCommands {
			sb.WriteString("- " + vc + "\n")
		}
		sb.WriteString("\n")
	}
	return sb.String()
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
	priorityFilter := normalizePriorityFilter(r.URL.Query().Get("priority"))
	limit := 20
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 100 {
		limit = 100
	}

	root := resolveStoriesRoot(h.repoRoot)
	indexStories := loadIndexStories(root)
	indexByID := make(map[string]StoryDetail, len(indexStories))
	for _, idxS := range indexStories {
		indexByID[strings.ToUpper(idxS.ID)] = idxS
	}

	var all []StoryDetail
	seen := make(map[string]bool)

	// 1. Database engine is authoritative:
	syncStates := make(map[string]string)
	if h.engine != nil {
		for _, s := range h.engine.ListStories("") {
			var subs []string
			if s.Submodule != "" {
				subs = []string{s.Submodule}
			}
			storyPath := fmt.Sprintf("docs/stories/%s.story.md", strings.ToLower(s.ID))
			prio := s.Priority
			if idxS, ok := indexByID[strings.ToUpper(s.ID)]; ok {
				if idxS.Path != "" {
					storyPath = idxS.Path
				}
				if prio == "" && idxS.Priority != "" {
					prio = idxS.Priority
				}
				if len(subs) == 0 && len(idxS.Submodules) > 0 {
					subs = idxS.Submodules
				}
			}
			if priorityFilter != "" && !strings.EqualFold(prio, priorityFilter) {
				seen[strings.ToUpper(s.ID)] = true
				continue
			}
			syncStates[strings.ToUpper(s.ID)] = s.SyncState
			all = append(all, StoryDetail{
				ID:                   s.ID,
				Title:                s.Title,
				Status:               normalizeCanonicalStoryStatus(s.Status, s.SyncState),
				Priority:             prio,
				Path:                 storyPath,
				Submodules:           subs,
				AcceptanceCriteria:   s.AcceptanceCriteria,
				TargetFiles:          s.TargetFiles,
				DependsOn:            s.DependsOn,
				VerificationCommands: s.VerificationCommands,
				Content:              s.Body,
			})
			seen[strings.ToUpper(s.ID)] = true
		}
	}

	// 2. Load disk index as fallback for legacy stories not yet in DB:
	for _, s := range indexStories {
		if !seen[strings.ToUpper(s.ID)] {
			if priorityFilter != "" && !strings.EqualFold(s.Priority, priorityFilter) {
				seen[strings.ToUpper(s.ID)] = true
				continue
			}
			s.Status = normalizeCanonicalStoryStatus(s.Status, "")
			all = append(all, s)
			seen[strings.ToUpper(s.ID)] = true
		}
	}

	var matched []StoryDetail
	for _, s := range all {
		if !matchesStoryStatusFilter(s.Status, syncStates[strings.ToUpper(s.ID)], statusFilter) {
			continue
		}
		if priorityFilter != "" && !strings.EqualFold(s.Priority, priorityFilter) {
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

	var found *StoryDetail
	root := resolveStoriesRoot(h.repoRoot)
	indexStories := loadIndexStories(root)

	// 1. Database engine is authoritative:
	if h.engine != nil {
		if s, ok := h.engine.GetStory(id); ok {
			var subs []string
			if s.Submodule != "" {
				subs = []string{s.Submodule}
			}
			storyPath := ""
			prio := s.Priority
			for _, idxS := range indexStories {
				if strings.EqualFold(idxS.ID, s.ID) || strings.EqualFold(idxS.ID, id) {
					if idxS.Path != "" {
						storyPath = idxS.Path
					}
					if prio == "" && idxS.Priority != "" {
						prio = idxS.Priority
					}
					if len(subs) == 0 && len(idxS.Submodules) > 0 {
						subs = idxS.Submodules
					}
					break
				}
			}
			if storyPath == "" {
				storyPath = fmt.Sprintf("docs/stories/%s.story.md", strings.ToLower(s.ID))
			}
			normStatus := normalizeCanonicalStoryStatus(s.Status, s.SyncState)
			s.Priority = prio
			content := synthesizeCanonicalStoryMarkdown(s, normStatus)
			found = &StoryDetail{
				ID:                   s.ID,
				Title:                s.Title,
				Status:               normStatus,
				Priority:             prio,
				Path:                 storyPath,
				Submodules:           subs,
				AcceptanceCriteria:   s.AcceptanceCriteria,
				TargetFiles:          s.TargetFiles,
				DependsOn:            s.DependsOn,
				VerificationCommands: s.VerificationCommands,
				Content:              content,
			}
		}
	}

	// 2. Fallback to disk index:
	if found == nil {
		for _, s := range indexStories {
			if strings.EqualFold(s.ID, id) {
				s.Status = normalizeCanonicalStoryStatus(s.Status, "")
				if s.Path == "" {
					s.Path = fmt.Sprintf("docs/stories/%s.story.md", strings.ToLower(s.ID))
				}
				match := s
				found = &match
				break
			}
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

// MaterializeStory handles POST /api/v1/stories/{id}/materialize.
func (h *StoriesHandler) MaterializeStory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/v1/stories/")
	id = strings.TrimSuffix(id, "/materialize")
	id = strings.TrimSpace(strings.TrimSuffix(id, "/"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "story id is required")
		return
	}

	if h.engine != nil {
		if err := h.engine.UpdateStorySyncState(id, "MATERIALIZED"); err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"story_id":   id,
		"sync_state": "MATERIALIZED",
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	})
}

// UpdateStoryStatusRequest represents a request to mutate story lifecycle status.
type UpdateStoryStatusRequest struct {
	Status string `json:"status"`
}

// UpdateStoryStatus handles PATCH /api/v1/stories/{id} and POST /api/v1/stories/{id}/status.
func (h *StoriesHandler) UpdateStoryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch && r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		path := r.URL.Path
		for _, prefix := range []string{"/api/v1/stories/", "/v1/stories/"} {
			if strings.HasPrefix(path, prefix) {
				id = strings.TrimPrefix(path, prefix)
				break
			}
		}
		id = strings.TrimSuffix(id, "/status")
		id = strings.TrimSpace(strings.TrimSuffix(id, "/"))
	}
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "story id is required")
		return
	}

	var req UpdateStoryStatusRequest
	if r.Body != nil {
		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &req); err != nil {
				writeJSONError(w, http.StatusBadRequest, "malformed JSON request body")
				return
			}
		}
	}

	normStatus := strings.ToUpper(strings.TrimSpace(req.Status))
	switch normStatus {
	case "READY", "IN_PROGRESS", "DONE", "BLOCKED", "DEFERRED", "PENDING_INDEX", "BACKLOG":
	default:
		writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid status %q (must be READY, IN_PROGRESS, DONE, BLOCKED, DEFERRED, PENDING_INDEX, or BACKLOG)", req.Status))
		return
	}

	updatedInEngine := false
	if h.engine != nil {
		if _, err := h.engine.UpdateStoryStatus(id, normStatus); err == nil {
			updatedInEngine = true
		}
	}

	root := resolveStoriesRoot(h.repoRoot)
	updatedInIndex := updateIndexStoryStatus(resolveStoriesRootForWrite(h.repoRoot), id, normStatus)
	if !updatedInEngine && updatedInIndex && h.engine != nil {
		for _, s := range loadIndexStories(root) {
			if strings.EqualFold(s.ID, id) {
				sub := ""
				if len(s.Submodules) > 0 {
					sub = s.Submodules[0]
				}
				_ = h.engine.SaveStory(control.Story{
					ID:        s.ID,
					Repo:      sub,
					Submodule: sub,
					Title:     s.Title,
					Priority:  s.Priority,
					Status:    normStatus,
					SyncState: "MATERIALIZED",
				})
				updatedInEngine = true
				break
			}
		}
	}

	if !updatedInEngine && !updatedInIndex {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("story %q not found", id))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"story_id":   id,
		"status":     normStatus,
		"updated":    true,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	})
}

func updateIndexStoryStatus(root, storyID, newStatus string) bool {
	if strings.TrimSpace(root) == "" {
		return false
	}
	indexPath := filepath.Join(root, "docs", "stories", "index.yml")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return false
	}
	lines := strings.Split(string(data), "\n")
	inTarget := false
	updated := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- id:") {
			currID := strings.TrimSpace(strings.TrimPrefix(trimmed, "- id:"))
			inTarget = strings.EqualFold(currID, storyID)
			continue
		}
		if inTarget && strings.HasPrefix(trimmed, "status:") {
			idx := strings.Index(line, "status:")
			if idx >= 0 {
				lines[i] = line[:idx] + "status: " + newStatus
				updated = true
				break
			}
		}
	}
	if !updated {
		return false
	}
	_ = os.WriteFile(indexPath, []byte(strings.Join(lines, "\n")), 0644)
	return true
}
