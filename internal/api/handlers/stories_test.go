package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestStoriesHandler_CompletedWithinWindow(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewStoriesHandler(store, q, nil)
	handler.SetDispatchWaitTimeout(1 * time.Second)

	// Simulate background agent claiming and completing the job
	go func() {
		time.Sleep(30 * time.Millisecond)
		job, err := q.Claim(context.Background(), "", []string{dispatch.JobTypeStoryCreate})
		if err != nil || job == nil {
			return
		}
		result := StoryResultPayload{
			StoryID:      "STORY-TEST-001",
			Submodule:    "iazio-mcp",
			StoryPath:    "docs/stories/2026-10-19-add-fleet-discovery.story.md",
			IndexUpdated: true,
		}
		resBytes, _ := json.Marshal(result)
		_ = q.Complete(context.Background(), job.ID, resBytes, "")
	}()

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

	if resp.Status != "completed" {
		t.Errorf("expected status completed, got %s", resp.Status)
	}
	if resp.StoryID != "STORY-TEST-001" {
		t.Errorf("expected story_id STORY-TEST-001, got %s", resp.StoryID)
	}
	if !resp.IndexUpdated {
		t.Error("expected index_updated=true")
	}
}

func TestStoriesHandler_TimeoutFallback(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewStoriesHandler(store, q, nil)
	// Set very short wait to trigger 202 Accepted
	handler.SetDispatchWaitTimeout(30 * time.Millisecond)

	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on timeout, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateStoryResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "queued" {
		t.Errorf("expected status queued, got %s", resp.Status)
	}
	if resp.HarnessJobID == "" {
		t.Error("expected non-empty harness_job_id")
	}

	// Verify job remains in queue
	job, ok := q.Get(resp.HarnessJobID)
	if !ok || job == nil {
		t.Fatalf("expected job %s to remain in queue", resp.HarnessJobID)
	}
}

func TestStoriesHandler_Idempotency(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	idemStore := NewIdempotencyStore(1 * time.Hour)
	handler := NewStoriesHandler(store, q, idemStore)
	handler.SetDispatchWaitTimeout(10 * time.Millisecond)

	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	// Call 1
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", "story-key-999")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", w1.Code)
	}

	// Call 2: Identical replay -> 200 OK replay
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", "story-key-999")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replay, got %d", w2.Code)
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

func TestStoriesHandler_WorkspaceLocked(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewStoriesHandler(store, q, nil)
	handler.SetDispatchWaitTimeout(1 * time.Second)

	payload := validStoryPayload()
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(body))
	w := httptest.NewRecorder()

	go func() {
		time.Sleep(10 * time.Millisecond)
		job, err := q.Claim(context.Background(), "", []string{dispatch.JobTypeStoryCreate})
		if err == nil && job != nil {
			_ = q.Complete(context.Background(), job.ID, nil, "WORKSPACE_LOCKED: checkout iazio-mcp is held by background task")
		}
	}()

	handler.ServeHTTP(w, req)
	if w.Code != http.StatusLocked {
		t.Fatalf("expected 423 Locked for WORKSPACE_LOCKED, got %d (body: %s)", w.Code, w.Body.String())
	}
}
