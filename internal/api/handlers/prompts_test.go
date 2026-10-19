package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func setupTestStore(t *testing.T) *codebase.MemStore {
	store := codebase.NewMemStore()
	err := store.UpsertRepository(context.Background(), codebase.Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to seed repository: %v", err)
	}
	return store
}

func TestPromptsHandler_Success(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewPromptsHandler(store, q, nil)

	payload := map[string]any{
		"repo":         "iazio-mcp",
		"title":        "Refactor codebase search",
		"prompt":       "Ensure whole-chunk truncation preserves markdown...",
		"target_agent": "opencode",
		"enqueue":      true,
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreatePromptResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Repo != "iazio-mcp" {
		t.Errorf("expected repo iazio-mcp, got %s", resp.Repo)
	}
	if resp.Status != "queued" {
		t.Errorf("expected status queued, got %s", resp.Status)
	}
	if resp.HarnessJobID == "" {
		t.Error("expected non-empty harness_job_id")
	}
	if resp.IdempotentReplay {
		t.Error("expected idempotent_replay to be false on first call")
	}

	// Verify job was enqueued
	job, ok := q.Get(resp.HarnessJobID)
	if !ok || job == nil {
		t.Fatalf("expected job %s in queue", resp.HarnessJobID)
	}
	if job.Type != dispatch.JobTypePromptExecute {
		t.Errorf("expected job type %s, got %s", dispatch.JobTypePromptExecute, job.Type)
	}
}

func TestPromptsHandler_UnknownRepo(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	handler := NewPromptsHandler(store, q, nil)

	payload := map[string]any{
		"repo":    "unknown-repo",
		"title":   "Some title",
		"prompt":  "Some prompt",
		"enqueue": true,
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity, got %d", w.Code)
	}
}

func TestPromptsHandler_Idempotency(t *testing.T) {
	store := setupTestStore(t)
	q := dispatch.NewInMemoryQueue()
	idemStore := NewIdempotencyStore(1 * time.Hour)
	handler := NewPromptsHandler(store, q, idemStore)

	payload1 := map[string]any{
		"repo":   "iazio-mcp",
		"title":  "Test prompt",
		"prompt": "Test body",
	}
	body1, _ := json.Marshal(payload1)

	// Call 1
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(body1))
	req1.Header.Set("Idempotency-Key", "test-key-123")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", w1.Code)
	}

	// Call 2: Exact same payload and key -> 200 OK replay
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(body1))
	req2.Header.Set("Idempotency-Key", "test-key-123")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replay, got %d", w2.Code)
	}

	var resp2 CreatePromptResponse
	_ = json.NewDecoder(w2.Body).Decode(&resp2)
	if !resp2.IdempotentReplay {
		t.Error("expected idempotent_replay=true on replay")
	}

	// Call 3: Mutated payload on same key -> 409 Conflict
	payloadMutated := map[string]any{
		"repo":   "iazio-mcp",
		"title":  "Mutated title",
		"prompt": "Test body",
	}
	bodyMutated, _ := json.Marshal(payloadMutated)

	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(bodyMutated))
	req3.Header.Set("Idempotency-Key", "test-key-123")
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)

	if w3.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on payload mismatch, got %d", w3.Code)
	}
}
