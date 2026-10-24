package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

func TestAgentReposSync_HandshakeFiltering(t *testing.T) {
	ctx := context.Background()
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	// Pre-populate Repo A (in sync) and Repo B (will be modified)
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "repo-a",
		Path:          "repo-a",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-a",
		UpdatedAt:     time.Now().UTC(),
	})
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "repo-b",
		Path:          "repo-b",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-b-stored",
		UpdatedAt:     time.Now().UTC(),
	})

	reqBody := RepoSyncRequest{
		HostID: "test-host",
		Repos: []RepoManifest{
			{
				Name:          "repo-a",
				Path:          "repo-a",
				DefaultBranch: "main",
				GitTreeSHA:    "tree-a", // in sync
				Description:   "Repo A",
			},
			{
				Name:          "repo-b",
				Path:          "repo-b",
				DefaultBranch: "main",
				GitTreeSHA:    "tree-b-modified", // modified tree
				Description:   "Repo B",
			},
			{
				Name:          "repo-c",
				Path:          "repo-c",
				DefaultBranch: "main",
				GitTreeSHA:    "tree-c-fresh", // brand new repo
				Description:   "Repo C",
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	r := httptest.NewRequest(http.MethodPost, "/api/v1/agent/repos/sync", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	handler.SyncRepos(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp RepoSyncResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp.OutOfSync) != 2 {
		t.Fatalf("expected 2 out-of-sync repos, got %d: %+v", len(resp.OutOfSync), resp.OutOfSync)
	}

	outMap := make(map[string]OutOfSyncRepo)
	for _, item := range resp.OutOfSync {
		outMap[item.RepoName] = item
	}

	// Verify Repo A is not in outOfSync
	if _, ok := outMap["repo-a"]; ok {
		t.Fatalf("repo-a was up to date but returned in out_of_sync")
	}

	// Verify Repo B
	if b, ok := outMap["repo-b"]; !ok {
		t.Fatalf("repo-b was modified but not returned in out_of_sync")
	} else {
		if b.IndexedTreeSHA != "tree-b-stored" || b.TargetTreeSHA != "tree-b-modified" {
			t.Fatalf("unexpected repo-b sync values: %+v", b)
		}
	}

	// Verify Repo C
	if c, ok := outMap["repo-c"]; !ok {
		t.Fatalf("repo-c was new but not returned in out_of_sync")
	} else {
		if c.IndexedTreeSHA != "" || c.TargetTreeSHA != "tree-c-fresh" {
			t.Fatalf("unexpected repo-c sync values: %+v", c)
		}
	}

	// Verify Repo C was stored in registry
	repoC, found, _ := store.GetRepository(ctx, "repo-c")
	if !found || repoC == nil {
		t.Fatalf("repo-c was not persisted to registry store")
	}
	if repoC.HostID != "test-host" {
		t.Fatalf("expected HostID to be test-host, got %s", repoC.HostID)
	}
}

func TestAgentReposSync_InvalidInputs(t *testing.T) {
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	// GET -> 405 Method Not Allowed
	rGet := httptest.NewRequest(http.MethodGet, "/api/v1/agent/repos/sync", nil)
	wGet := httptest.NewRecorder()
	handler.SyncRepos(wGet, rGet)
	if wGet.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", wGet.Code)
	}

	// Invalid JSON -> 400 Bad Request
	rBad := httptest.NewRequest(http.MethodPost, "/api/v1/agent/repos/sync", bytes.NewReader([]byte("{invalid-json")))
	wBad := httptest.NewRecorder()
	handler.SyncRepos(wBad, rBad)
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", wBad.Code)
	}
}

func TestAgentCodebaseSync_SuccessAndTreePromotion(t *testing.T) {
	ctx := context.Background()
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-v1",
		UpdatedAt:     time.Now().UTC(),
	})

	// Batch 1: is_final_batch: false
	batch1 := codebase.IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-v1",
		GitTreeSHA:   "tree-v2",
		IsFinalBatch: false,
		Chunks: []codebase.CodeChunk{
			{
				FilePath:    "pkg/tool1.go",
				StartLine:   1,
				EndLine:     20,
				Symbol:      "Tool1",
				Content:     "func Tool1() {}",
				FileBlobSHA: "blob-1",
			},
		},
	}
	b1Bytes, _ := json.Marshal(batch1)
	r1 := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(b1Bytes))
	w1 := httptest.NewRecorder()
	handler.SyncCodebase(w1, r1)

	if w1.Code != http.StatusOK {
		t.Fatalf("batch 1 expected 200, got %d: %s", w1.Code, w1.Body.String())
	}
	var res1 codebase.IngestResult
	_ = json.Unmarshal(w1.Body.Bytes(), &res1)
	if res1.TreePromoted {
		t.Fatalf("expected TreePromoted == false in batch 1")
	}
	if res1.IndexedTreeSHA != "tree-v1" {
		t.Fatalf("expected IndexedTreeSHA == tree-v1, got %s", res1.IndexedTreeSHA)
	}

	// Batch 2: is_final_batch: true
	batch2 := codebase.IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-v1",
		GitTreeSHA:   "tree-v2",
		IsFinalBatch: true,
		Chunks: []codebase.CodeChunk{
			{
				FilePath:    "pkg/tool2.go",
				StartLine:   1,
				EndLine:     20,
				Symbol:      "Tool2",
				Content:     "func Tool2() {}",
				FileBlobSHA: "blob-2",
			},
		},
	}
	b2Bytes, _ := json.Marshal(batch2)
	r2 := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(b2Bytes))
	w2 := httptest.NewRecorder()
	handler.SyncCodebase(w2, r2)

	if w2.Code != http.StatusOK {
		t.Fatalf("batch 2 expected 200, got %d: %s", w2.Code, w2.Body.String())
	}
	var res2 codebase.IngestResult
	_ = json.Unmarshal(w2.Body.Bytes(), &res2)
	if !res2.TreePromoted {
		t.Fatalf("expected TreePromoted == true in batch 2")
	}
	if res2.IndexedTreeSHA != "tree-v2" {
		t.Fatalf("expected IndexedTreeSHA == tree-v2, got %s", res2.IndexedTreeSHA)
	}

	// Verify store state
	repo, _, _ := store.GetRepository(ctx, "iazio-mcp")
	if repo.GitTreeSHA != "tree-v2" {
		t.Fatalf("expected stored repo GitTreeSHA to advance to tree-v2, got %s", repo.GitTreeSHA)
	}
}

func TestAgentCodebaseSync_ConflictOnStaleBaseTree(t *testing.T) {
	ctx := context.Background()
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-current-456",
		UpdatedAt:     time.Now().UTC(),
	})

	req := codebase.IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-stale-123",
		GitTreeSHA:   "tree-target-789",
		IsFinalBatch: true,
	}
	reqBytes, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(reqBytes))
	w := httptest.NewRecorder()
	handler.SyncCodebase(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d: %s", w.Code, w.Body.String())
	}

	var conflictResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &conflictResp)
	if conflictResp["indexed_tree_sha"] != "tree-current-456" {
		t.Fatalf("expected indexed_tree_sha == tree-current-456, got %v", conflictResp["indexed_tree_sha"])
	}
	if conflictResp["base_tree_sha"] != "tree-stale-123" {
		t.Fatalf("expected base_tree_sha == tree-stale-123, got %v", conflictResp["base_tree_sha"])
	}
}

func TestAgentCodebaseSync_RepoNotFound(t *testing.T) {
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	req := codebase.IngestRequest{
		RepoName:     "non-existent-submodule",
		BaseTreeSHA:  "tree-1",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
	}
	reqBytes, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(reqBytes))
	w := httptest.NewRecorder()
	handler.SyncCodebase(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAgentCodebaseSync_ValidationsAndErrors(t *testing.T) {
	store := codebase.NewMemStore()
	handler := NewAgentSyncHandler(store, nil)

	// 1. Method not allowed
	r := httptest.NewRequest(http.MethodGet, "/api/v1/agent/codebase/sync", nil)
	w := httptest.NewRecorder()
	handler.SyncCodebase(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", w.Code)
	}

	// 2. Bad JSON
	r = httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader([]byte("{bad")))
	w = httptest.NewRecorder()
	handler.SyncCodebase(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", w.Code)
	}

	// 3. Missing repo_name
	reqMissingRepo := codebase.IngestRequest{RepoName: "   "}
	bMissing, _ := json.Marshal(reqMissingRepo)
	r = httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(bMissing))
	w = httptest.NewRecorder()
	handler.SyncCodebase(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty repo_name, got %d", w.Code)
	}

	// 4. Ingest generic internal error
	errHandler := &AgentSyncHandler{
		Store:    store,
		Ingestor: &mockErrIngestor{err: errors.New("unexpected storage error")},
	}
	reqValid := codebase.IngestRequest{RepoName: "iazio-api"}
	bValid, _ := json.Marshal(reqValid)
	r = httptest.NewRequest(http.MethodPost, "/api/v1/agent/codebase/sync", bytes.NewReader(bValid))
	w = httptest.NewRecorder()
	errHandler.SyncCodebase(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for generic ingest error, got %d", w.Code)
	}
}

type mockErrIngestor struct {
	err error
}

func (m *mockErrIngestor) Ingest(ctx context.Context, req codebase.IngestRequest) (*codebase.IngestResult, error) {
	return nil, m.err
}
