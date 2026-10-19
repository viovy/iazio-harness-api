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

func TestRouter_AgentSyncRoutes(t *testing.T) {
	ctx := context.Background()
	store := codebase.NewMemStore()
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-initial",
		UpdatedAt:     time.Now().UTC(),
	})

	token := "agent-secret-token"
	router := NewRouter(RouterConfig{
		Store:     store,
		AuthToken: token,
	})

	// 1. Repos sync without auth -> 401 Unauthorized
	reqNoAuth := httptest.NewRequest(http.MethodPost, "/api/v1/agent/repos/sync", bytes.NewReader([]byte("{}")))
	recNoAuth := httptest.NewRecorder()
	router.ServeHTTP(recNoAuth, reqNoAuth)
	if recNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized without auth, got %d", recNoAuth.Code)
	}

	// 2. Codebase sync without auth -> 401 Unauthorized
	reqSyncNoAuth := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader([]byte("{}")))
	recSyncNoAuth := httptest.NewRecorder()
	router.ServeHTTP(recSyncNoAuth, reqSyncNoAuth)
	if recSyncNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on codebase sync without auth, got %d", recSyncNoAuth.Code)
	}

	// 3. Repos sync with valid auth -> 200 OK
	syncReq := map[string]any{
		"host_id": "test-host",
		"repos": []map[string]any{
			{
				"name":         "iazio-mcp",
				"path":         "iazio-mcp",
				"git_tree_sha": "tree-promoted",
			},
		},
	}
	body, _ := json.Marshal(syncReq)
	reqRepos := httptest.NewRequest(http.MethodPost, "/api/v1/agent/repos/sync", bytes.NewReader(body))
	reqRepos.Header.Set("Authorization", "Bearer "+token)
	recRepos := httptest.NewRecorder()
	router.ServeHTTP(recRepos, reqRepos)

	if recRepos.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on repos sync, got %d: %s", recRepos.Code, recRepos.Body.String())
	}

	// 4. Codebase sync with invalid base tree -> 409 Conflict
	conflictReq := codebase.IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "wrong-base-tree",
		GitTreeSHA:   "tree-promoted",
		IsFinalBatch: true,
	}
	conflictBody, _ := json.Marshal(conflictReq)
	reqConflict := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(conflictBody))
	reqConflict.Header.Set("Authorization", "Bearer "+token)
	recConflict := httptest.NewRecorder()
	router.ServeHTTP(recConflict, reqConflict)

	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on base tree mismatch, got %d: %s", recConflict.Code, recConflict.Body.String())
	}

	// 5. Codebase sync with matching base tree -> 200 OK
	codebaseReq := codebase.IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-initial",
		GitTreeSHA:   "tree-promoted",
		IsFinalBatch: true,
		Chunks: []codebase.CodeChunk{
			{
				FilePath:    "main.go",
				StartLine:   1,
				EndLine:     10,
				Content:     "package main",
				FileBlobSHA: "blob-1",
			},
		},
	}
	cbBody, _ := json.Marshal(codebaseReq)
	reqCb := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(cbBody))
	reqCb.Header.Set("Authorization", "Bearer "+token)
	recCb := httptest.NewRecorder()
	router.ServeHTTP(recCb, reqCb)

	if recCb.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on codebase sync, got %d: %s", recCb.Code, recCb.Body.String())
	}

	var res codebase.IngestResult
	if err := json.Unmarshal(recCb.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal ingest result: %v", err)
	}
	if !res.TreePromoted || res.IndexedTreeSHA != "tree-promoted" {
		t.Fatalf("expected TreePromoted == true with IndexedTreeSHA == tree-promoted, got %+v", res)
	}
}
