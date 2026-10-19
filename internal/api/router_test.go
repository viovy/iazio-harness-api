package api

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

func TestRouter_AuthMiddleware(t *testing.T) {
	store := codebase.NewMemStore()
	_ = store.UpsertRepository(context.Background(), codebase.Repository{
		Name: "iazio-mcp",
		Path: "iazio-mcp",
	})

	validToken := "secret-bearer-token-123"
	router := NewRouter(RouterConfig{
		Store:     store,
		AuthToken: validToken,
	})

	// 1. Missing Authorization header -> 401
	reqNoAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	recNoAuth := httptest.NewRecorder()
	router.ServeHTTP(recNoAuth, reqNoAuth)

	if recNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", recNoAuth.Code)
	}

	// 2. Invalid Bearer token -> 401
	reqBadAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	reqBadAuth.Header.Set("Authorization", "Bearer wrong-token")
	recBadAuth := httptest.NewRecorder()
	router.ServeHTTP(recBadAuth, reqBadAuth)

	if recBadAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for bad token, got %d", recBadAuth.Code)
	}

	// 3. Valid Bearer token -> 200 OK
	reqValidAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	reqValidAuth.Header.Set("Authorization", "Bearer "+validToken)
	recValidAuth := httptest.NewRecorder()
	router.ServeHTTP(recValidAuth, reqValidAuth)

	if recValidAuth.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for valid token, got %d", recValidAuth.Code)
	}

	// 4. Codebase search with valid token
	searchPayload := map[string]any{"query": "mcp"}
	searchBody, _ := json.Marshal(searchPayload)
	reqSearch := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader(searchBody))
	reqSearch.Header.Set("Authorization", "Bearer "+validToken)
	recSearch := httptest.NewRecorder()
	router.ServeHTTP(recSearch, reqSearch)

	if recSearch.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on search with valid token, got %d", recSearch.Code)
	}

	// 5. POST /api/v1/prompts with valid token
	promptPayload := map[string]any{
		"repo":    "iazio-mcp",
		"title":   "Test title",
		"prompt":  "Test prompt instructions",
		"enqueue": true,
	}
	promptBody, _ := json.Marshal(promptPayload)
	reqPrompt := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewReader(promptBody))
	reqPrompt.Header.Set("Authorization", "Bearer "+validToken)
	recPrompt := httptest.NewRecorder()
	router.ServeHTTP(recPrompt, reqPrompt)

	if recPrompt.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on prompts route, got %d: %s", recPrompt.Code, recPrompt.Body.String())
	}

	// 6. POST /api/v1/agent/jobs/claim with valid token
	claimBody, _ := json.Marshal(map[string]any{"job_types": []string{dispatch.JobTypePromptExecute}})
	reqClaim := httptest.NewRequest(http.MethodPost, "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	reqClaim.Header.Set("Authorization", "Bearer "+validToken)
	recClaim := httptest.NewRecorder()
	router.ServeHTTP(recClaim, reqClaim)

	if recClaim.Code != http.StatusOK {
		t.Fatalf("expected 200 OK claiming queued prompt job, got %d: %s", recClaim.Code, recClaim.Body.String())
	}
}

func TestRouter_StoriesRoute(t *testing.T) {
	store := codebase.NewMemStore()
	_ = store.UpsertRepository(context.Background(), codebase.Repository{
		Name: "iazio-mcp",
		Path: "iazio-mcp",
	})
	q := dispatch.NewInMemoryQueue()

	router := NewRouter(RouterConfig{
		Store: store,
		Queue: q,
	})

	// Background agent complete
	go func() {
		time.Sleep(30 * time.Millisecond)
		job, _ := q.Claim(context.Background(), "", []string{dispatch.JobTypeStoryCreate})
		if job != nil {
			res, _ := json.Marshal(map[string]any{
				"story_id":      "STORY-001",
				"submodule":     "iazio-mcp",
				"story_path":    "docs/stories/story.md",
				"index_updated": true,
			})
			_ = q.Complete(context.Background(), job.ID, res, "")
		}
	}()

	storyPayload := map[string]any{
		"submodule":           "iazio-mcp",
		"title":               "Test story",
		"priority":            "P1",
		"problem":             "Some problem",
		"acceptance_criteria": []string{"done"},
		"target_files":        []string{"internal/test.go"},
	}
	storyBody, _ := json.Marshal(storyPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewReader(storyBody))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created on stories route, got %d: %s", rec.Code, rec.Body.String())
	}
}
