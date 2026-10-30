package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func validStoryPayload() map[string]any {
	return map[string]any{
		"submodule": "iazio-mcp",
		"title":     "Add Fleet Repository Discovery Tools",
		"priority":  "P1",
		"problem":   "External MCP clients lack native fleet discovery...",
		"acceptance_criteria": []string{
			"list_repos and search_codebase are registered in server.go",
		},
		"target_files": []string{
			"internal/harness/client.go",
			"internal/mcp/tools_codebase.go",
		},
		"depends_on": []string{},
		"verification_commands": []string{
			"go test -v -race ./...",
		},
	}
}

func TestStoriesHandler_SemanticValidation(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewStoriesHandler(store, q, nil)

	tests := []struct {
		name       string
		mutate     func(map[string]any)
		wantStatus int
	}{
		{
			name: "missing submodule",
			mutate: func(m map[string]any) {
				m["submodule"] = ""
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "path traversal in target_files",
			mutate: func(m map[string]any) {
				m["target_files"] = []string{"../sensitive/file.go"}
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "absolute path in target_files",
			mutate: func(m map[string]any) {
				m["target_files"] = []string{"/etc/passwd"}
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "empty acceptance_criteria",
			mutate: func(m map[string]any) {
				m["acceptance_criteria"] = []string{}
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "invalid priority",
			mutate: func(m map[string]any) {
				m["priority"] = "P99"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "unknown submodule",
			mutate: func(m map[string]any) {
				m["submodule"] = "non-existent-submodule"
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := validStoryPayload()
			tc.mutate(payload)
			body, _ := json.Marshal(payload)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d: %s", tc.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

func TestStoriesHandler_ControlPlaneFirstCreation(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "created" {
		t.Errorf("expected status created, got %s", resp.Status)
	}
	if resp.SyncState != "PENDING_WORKSPACE_MATERIALIZATION" {
		t.Errorf("expected sync_state PENDING_WORKSPACE_MATERIALIZATION, got %s", resp.SyncState)
	}
	if resp.StoryID != "" {
		t.Errorf("expected empty story_id when id is not provided (resolved by Canonical AI/autopilot), got %s", resp.StoryID)
	}
	if resp.StoryPath != "" {
		t.Errorf("expected empty story_path when id/content_path is not provided, got %s", resp.StoryPath)
	}
}

func TestStoriesHandler_ControlPlaneFirstWithEnqueue(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	payload := validStoryPayload()
	payload["enqueue"] = true
	payload["target_agent"] = "debian"
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created with enqueue, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "created" {
		t.Errorf("expected status created, got %s", resp.Status)
	}
	if resp.ExecutionJobID == "" {
		t.Fatal("expected non-empty execution_job_id")
	}
	if resp.ExecutionStatus != "pending" {
		t.Errorf("expected execution_status pending, got %s", resp.ExecutionStatus)
	}

	// Verify that the execution job was enqueued in dispatch queue
	job, ok := q.Get(resp.ExecutionJobID)
	if !ok || job == nil {
		t.Fatalf("expected execution job %s in queue", resp.ExecutionJobID)
	}
	if job.Type != dispatch.JobTypePromptExecute {
		t.Errorf("expected JobTypePromptExecute, got %s", job.Type)
	}
	if job.TargetAgent != "debian" {
		t.Errorf("expected target_agent debian, got %s", job.TargetAgent)
	}
}

func TestStoriesHandler_Idempotency(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	idemStore := NewIdempotencyStore(1 * time.Hour)
	handler := NewStoriesHandler(store, q, idemStore)
	handler.SetEngine(engine)

	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	// Call 1: Fast 201 Created
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", "story-key-999")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w1.Code, w1.Body.String())
	}

	// Call 2: Identical replay -> 200 OK replay
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", "story-key-999")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replay, got %d: %s", w2.Code, w2.Body.String())
	}

	var resp2 CreateStoryResponse
	_ = json.NewDecoder(w2.Body).Decode(&resp2)
	if !resp2.IdempotentReplay {
		t.Error("expected idempotent_replay=true on replay")
	}

	// Call 3: Mutated payload -> 409 Conflict
	payload["title"] = "Mutated Title"
	bodyMutated, _ := json.Marshal(payload)
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(bodyMutated))
	req3.Header.Set("Idempotency-Key", "story-key-999")
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)

	if w3.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d", w3.Code)
	}
}

func TestStoriesHandler_GetAndListStories_DBAuthoritative(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	testStory := control.Story{
		ID:        "STORY-DB-9999",
		Title:     "DB Authoritative Title",
		Submodule: "iazio-mcp",
		Status:    "in_progress",
		Priority:  "P0",
		Body:      "# DB Story Body\n\nMust take precedence over disk.",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := engine.SaveStory(testStory); err != nil {
		t.Fatalf("failed to save story in engine: %v", err)
	}

	// Test GetStory
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/stories/STORY-DB-9999", nil)
	wGet := httptest.NewRecorder()
	handler.GetStory(wGet, reqGet)

	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GetStory, got %d: %s", wGet.Code, wGet.Body.String())
	}

	var gotStory StoryDetail
	if err := json.NewDecoder(wGet.Body).Decode(&gotStory); err != nil {
		t.Fatalf("failed to decode GetStory response: %v", err)
	}
	if gotStory.ID != "STORY-DB-9999" {
		t.Errorf("expected ID STORY-DB-9999, got %s", gotStory.ID)
	}
	if gotStory.Title != "DB Authoritative Title" {
		t.Errorf("expected Title DB Authoritative Title, got %s", gotStory.Title)
	}
	if gotStory.Content != testStory.Body {
		t.Errorf("expected Content %s, got %s", testStory.Body, gotStory.Content)
	}

	// Test ListStories
	reqList := httptest.NewRequest(http.MethodGet, "/api/v1/stories", nil)
	wList := httptest.NewRecorder()
	handler.ListStories(wList, reqList)

	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from ListStories, got %d: %s", wList.Code, wList.Body.String())
	}

	var listResp struct {
		Stories []StoryDetail `json:"items"`
		Total   int           `json:"total"`
	}
	if err := json.NewDecoder(wList.Body).Decode(&listResp); err != nil {
		t.Fatalf("failed to decode ListStories response: %v", err)
	}

	var found bool
	for _, s := range listResp.Stories {
		if s.ID == "STORY-DB-9999" {
			found = true
			if s.Title != "DB Authoritative Title" {
				t.Errorf("expected listed story Title DB Authoritative Title, got %s", s.Title)
			}
			break
		}
	}
	if !found {
		t.Errorf("expected STORY-DB-9999 to be present in ListStories response")
	}
}

func TestStoriesHandler_MaterializeStory(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)
	handler.SetDispatchWaitTimeout(10 * time.Millisecond)

	// 1. Wrong method
	reqWrong := httptest.NewRequest(http.MethodGet, "/api/v1/stories/STORY-MAT-1/materialize", nil)
	wWrong := httptest.NewRecorder()
	handler.MaterializeStory(wWrong, reqWrong)
	if wWrong.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", wWrong.Code)
	}

	// 2. Empty story ID
	reqEmpty := httptest.NewRequest(http.MethodPost, "/api/v1/stories//materialize", nil)
	wEmpty := httptest.NewRecorder()
	handler.MaterializeStory(wEmpty, reqEmpty)
	if wEmpty.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for empty ID, got %d", wEmpty.Code)
	}

	// 3. Story not found in engine
	reqNotFound := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-NONEXISTENT/materialize", nil)
	wNotFound := httptest.NewRecorder()
	handler.MaterializeStory(wNotFound, reqNotFound)
	if wNotFound.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", wNotFound.Code)
	}

	// 4. Successful materialization
	testStory := control.Story{
		ID:        "STORY-MAT-1",
		Title:     "Materialize Me",
		Submodule: "iazio-mcp",
		SyncState: "PENDING_WORKSPACE_MATERIALIZATION",
	}
	if err := engine.SaveStory(testStory); err != nil {
		t.Fatalf("failed to save story: %v", err)
	}

	reqSuccess := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-MAT-1/materialize", nil)
	wSuccess := httptest.NewRecorder()
	handler.MaterializeStory(wSuccess, reqSuccess)
	if wSuccess.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wSuccess.Code, wSuccess.Body.String())
	}

	var res map[string]any
	if err := json.Unmarshal(wSuccess.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["sync_state"] != "MATERIALIZED" {
		t.Errorf("expected sync_state MATERIALIZED, got %v", res["sync_state"])
	}

	// Verify engine state was updated
	st, ok := engine.GetStory("STORY-MAT-1")
	if !ok || st.SyncState != "MATERIALIZED" {
		t.Errorf("expected engine story sync_state MATERIALIZED, got %s", st.SyncState)
	}
}

func TestIntegration_StoryCreationAndExecutionDispatch(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	idem := NewIdempotencyStore(10 * time.Minute)
	handler := NewStoriesHandler(store, q, idem)
	handler.SetEngine(engine)

	const promptDirective = `/autonomous-story-execution deep review entire vigo* and k8s* submodules, find omissions, gaps, architectural issues and misalignments. Everything should be enterprise grade well architected, enterprise grade product and enterprise grade implemented. ensure all discoveries from review are implemented.`

	payload := map[string]any{
		"submodule":           ".",
		"title":               "Deep review and enterprise-grade alignment for vigo* and k8s* submodules",
		"priority":            "P1",
		"problem":             "Review entire vigo* and k8s* fleet submodules for enterprise grade completeness",
		"acceptance_criteria": []string{"Complete deep review", "Implement all findings"},
		"target_files":        []string{"vigo", "k8s"},
		"prompt":              promptDirective,
		"enqueue":             true,
		"target_agent":        "autopilot",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode CreateStoryResponse: %v", err)
	}

	if resp.ExecutionJobID == "" {
		t.Fatalf("expected ExecutionJobID to be populated when enqueue=true")
	}

	// Verify job was enqueued in dispatch queue with the exact prompt
	claimed, err := q.Claim(context.Background(), "mac-mini", []string{dispatch.JobTypePromptExecute})
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	if claimed == nil {
		t.Fatalf("expected claimed job, got nil")
	}
	if claimed.ID != resp.ExecutionJobID {
		t.Errorf("expected claimed job ID %s, got %s", resp.ExecutionJobID, claimed.ID)
	}

	var claimedPayload struct {
		PromptID    string `json:"prompt_id"`
		Repo        string `json:"repo"`
		StoryID     string `json:"story_id"`
		Prompt      string `json:"prompt"`
		TargetAgent string `json:"target_agent"`
	}
	if err := json.Unmarshal(claimed.Payload, &claimedPayload); err != nil {
		t.Fatalf("failed to unmarshal claimed job payload: %v", err)
	}

	if claimedPayload.Prompt != promptDirective {
		t.Errorf("expected prompt in job payload to match promptDirective, got: %s", claimedPayload.Prompt)
	}
	if claimedPayload.TargetAgent != "autopilot" {
		t.Errorf("expected target agent 'autopilot', got: %s", claimedPayload.TargetAgent)
	}
	if claimedPayload.StoryID != resp.StoryID {
		t.Errorf("expected story ID %s, got: %s", resp.StoryID, claimedPayload.StoryID)
	}
}

func TestGov3785_NoStoryIDAllocationOrFrontmatterHandlingAndCaseInsensitiveStatusFilter(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	payload := validStoryPayload()
	payload["enqueue"] = true
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}
	var resp CreateStoryResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.StoryID != "" {
		t.Fatalf("expected iazio-harness-api not to mint StoryID (resolved by Canonical AI/autopilot), got %s", resp.StoryID)
	}
	if resp.IndexUpdated {
		t.Fatalf("expected IndexUpdated=false when SyncState is PENDING_WORKSPACE_MATERIALIZATION")
	}

	// Seed an indexed story to verify ListStories with status=READY matches pending/PENDING_INDEX stories
	_ = engine.SaveStory(control.Story{
		ID:        "STORY-MCP-3782",
		Title:     "Indexed Story",
		Submodule: "iazio-mcp",
		Status:    "pending",
		SyncState: "PENDING_WORKSPACE_MATERIALIZATION",
		Problem:   "Raw problem statement without synthesized frontmatter",
	})

	reqList := httptest.NewRequest(http.MethodGet, "/api/v1/stories?status=READY", nil)
	wList := httptest.NewRecorder()
	handler.ListStories(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from ListStories, got %d", wList.Code)
	}
	var listResp struct {
		Stories []StoryDetail `json:"items"`
	}
	_ = json.NewDecoder(wList.Body).Decode(&listResp)
	found := false
	for _, s := range listResp.Stories {
		if s.ID == "STORY-MCP-3782" {
			found = true
			if s.Status != "READY" {
				t.Errorf("expected canonical status READY for unmaterialized pending story, got %s", s.Status)
			}
		}
	}
	if !found {
		t.Fatalf("expected STORY-MCP-3782 to match status=READY filter")
	}
}

func TestStoriesHandlerCreateWithoutIDAlwaysEnqueuesASEPrompt(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	engine.SetToolPriority([]string{"opencode", "agy"})
	_ = engine.RegisterHost("mac-mini", "permanent", "generic")
	_ = engine.UpsertRepo(control.Repo{
		HostID:                       "mac-mini",
		WorktreePath:                 "iazio-mcp",
		CloneURL:                     "https://github.com/viovy/iazio-mcp.git",
		GlobalQueueAllocationEnabled: true,
	})
	engine.Heartbeat("mac-mini", []control.Tool{
		{Name: "opencode", Installed: true, Path: "/usr/local/bin/opencode", Status: "ok"},
	}, false)

	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	// Omit "id" and omit "enqueue": must still enqueue an ASE prompt_execute job (ADR-006 compliant)
	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.StoryID != "" {
		t.Fatalf("expected StoryID to remain empty (resolved by Canonical AI/autopilot), got %q", resp.StoryID)
	}
	if resp.HarnessJobID == "" || resp.ExecutionJobID == "" {
		t.Fatalf("expected non-empty HarnessJobID/ExecutionJobID when creating story without explicit id, got %+v", resp)
	}

	job, ok := q.Get(resp.HarnessJobID)
	if !ok || job == nil {
		t.Fatalf("expected enqueued job %s in queue", resp.HarnessJobID)
	}
	if job.TargetAgent != "opencode" {
		t.Fatalf("expected ResolveToolPriorityAllocation to assign TargetAgent=opencode, got %s", job.TargetAgent)
	}
	if job.TargetHost != "mac-mini" {
		t.Fatalf("expected ResolveToolPriorityAllocation to assign TargetHost=mac-mini, got %s", job.TargetHost)
	}
}

func TestStoriesHandler_UpdateStoryStatus(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	_ = engine.SaveStory(control.Story{
		ID:        "STORY-GOV-3799",
		Repo:      "meta-repo",
		Submodule: "iazio-harness-api",
		Title:     "Forensic v3 Remediation",
		Priority:  "P0",
		Status:    "IN_PROGRESS",
		SyncState: "MATERIALIZED",
	})

	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/stories/STORY-GOV-3799", bytes.NewBufferString(`{"status":"DONE"}`))
	w := httptest.NewRecorder()
	handler.UpdateStoryStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	updated, ok := engine.GetStory("STORY-GOV-3799")
	if !ok || updated.Status != "DONE" {
		t.Fatalf("expected story status updated to DONE in engine, got ok=%v status=%q", ok, updated.Status)
	}
}

func TestV4_StoriesHandler_PriorityFilterGetStoryFrontmatterAndDeferredStatus(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	engine := control.NewEngine(nil)
	tmpDir := t.TempDir()

	_ = engine.SaveStory(control.Story{
		ID:                   "STORY-GOV-3806",
		Repo:                 "meta-repo",
		Submodule:            "iazio-harness-api",
		Title:                "Forensic v4 Remediation",
		Priority:             "P0",
		Status:               "READY",
		SyncState:            "PENDING_WORKSPACE_MATERIALIZATION",
		Problem:              "Stories lack canonical frontmatter and metadata in GetStory.",
		AcceptanceCriteria:   []string{"GetStory returns frontmatter", "ListStories filters by priority"},
		TargetFiles:          []string{"internal/api/handlers/stories.go"},
		DependsOn:            []string{"STORY-GOV-3799"},
		VerificationCommands: []string{"go test ./..."},
	})
	_ = engine.SaveStory(control.Story{
		ID:        "STORY-GOV-3807",
		Repo:      "meta-repo",
		Submodule: "iazio-harness-api",
		Title:     "Low Priority Cleanup",
		Priority:  "P2",
		Status:    "READY",
	})

	handler := NewStoriesHandler(store, q, nil)
	handler.SetEngine(engine)
	handler.SetRepoRoot(tmpDir)

	// 1. ListStories with priority=P0
	reqList := httptest.NewRequest(http.MethodGet, "/api/v1/stories?priority=P0", nil)
	wList := httptest.NewRecorder()
	handler.ListStories(wList, reqList)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wList.Code, wList.Body.String())
	}
	var listResp struct {
		Items []StoryDetail `json:"items"`
		Total int           `json:"total"`
	}
	_ = json.NewDecoder(wList.Body).Decode(&listResp)
	if listResp.Total != 1 || len(listResp.Items) != 1 || listResp.Items[0].ID != "STORY-GOV-3806" {
		t.Fatalf("expected priority=P0 filter to return only STORY-GOV-3806, got %+v", listResp)
	}

	// 2. GetStory returns Path, AcceptanceCriteria, TargetFiles, DependsOn, VerificationCommands, and YAML frontmatter Content
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/stories/STORY-GOV-3806", nil)
	wGet := httptest.NewRecorder()
	handler.GetStory(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from GetStory, got %d: %s", wGet.Code, wGet.Body.String())
	}
	var detail StoryDetail
	_ = json.NewDecoder(wGet.Body).Decode(&detail)
	if detail.Path != "docs/stories/story-gov-3806.story.md" {
		t.Fatalf("expected fallback Path docs/stories/story-gov-3806.story.md, got %q", detail.Path)
	}
	if len(detail.AcceptanceCriteria) != 2 || len(detail.TargetFiles) != 1 || len(detail.VerificationCommands) != 1 {
		t.Fatalf("expected metadata slices populated on StoryDetail, got %+v", detail)
	}
	if len(detail.Content) == 0 || detail.Content[:3] != "---" {
		t.Fatalf("expected synthesized Content to start with YAML frontmatter '---', got %q", detail.Content)
	}

	// 3. UpdateStoryStatus accepts DEFERRED, PENDING_INDEX, and BACKLOG
	for _, status := range []string{"DEFERRED", "PENDING_INDEX", "BACKLOG"} {
		reqPatch := httptest.NewRequest(http.MethodPatch, "/api/v1/stories/STORY-GOV-3806", bytes.NewBufferString(`{"status":"`+status+`"}`))
		wPatch := httptest.NewRecorder()
		handler.UpdateStoryStatus(wPatch, reqPatch)
		if wPatch.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for status %s, got %d: %s", status, wPatch.Code, wPatch.Body.String())
		}
		st, _ := engine.GetStory("STORY-GOV-3806")
		if st.Status != status {
			t.Fatalf("expected engine story status %s, got %s", status, st.Status)
		}
	}
}
