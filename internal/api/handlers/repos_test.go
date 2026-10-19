package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

func TestReposHandler_ListAndFiltering(t *testing.T) {
	store := codebase.NewMemStore()
	ctx := context.Background()

	now := time.Now().UTC()
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		Description:   "MCP server bridging iazio-api and iazio-harness-api",
		UpdatedAt:     now,
	})
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "iazio-harness-api",
		Path:          "iazio-harness-api",
		DefaultBranch: "main",
		GitTreeSHA:    "e79bfcdeb83bd35318088ddec4e4322170dac8f3",
		Description:   "Harness execution control plane",
		UpdatedAt:     now,
	})
	_ = store.UpsertRepository(ctx, codebase.Repository{
		Name:          "autopilot",
		Path:          "autopilot",
		DefaultBranch: "main",
		GitTreeSHA:    "f30127890ae37b94ffb7fa965702f4d32536e0ec",
		Description:   "Autonomous SDLC engine",
		UpdatedAt:     now,
	})

	handler := NewReposHandler(store)

	// 1. List all repos
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp codebase.ListReposResult
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 3 || resp.ItemCount != 3 {
		t.Fatalf("expected 3 repos, got %d (total: %d)", resp.ItemCount, resp.Total)
	}

	// 2. Query filter
	reqFilter := httptest.NewRequest(http.MethodGet, "/api/v1/repos?query=mcp", nil)
	recFilter := httptest.NewRecorder()
	handler.ServeHTTP(recFilter, reqFilter)

	if recFilter.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recFilter.Code)
	}

	var filterResp codebase.ListReposResult
	_ = json.Unmarshal(recFilter.Body.Bytes(), &filterResp)
	if filterResp.ItemCount != 1 || filterResp.Items[0].Name != "iazio-mcp" {
		t.Fatalf("expected only iazio-mcp, got %+v", filterResp.Items)
	}

	// 3. Keyset Pagination
	reqPage1 := httptest.NewRequest(http.MethodGet, "/api/v1/repos?limit=2", nil)
	recPage1 := httptest.NewRecorder()
	handler.ServeHTTP(recPage1, reqPage1)

	var p1 codebase.ListReposResult
	_ = json.Unmarshal(recPage1.Body.Bytes(), &p1)
	if p1.ItemCount != 2 || p1.NextCursor == "" {
		t.Fatalf("expected 2 items and non-empty next_cursor, got %d, %s", p1.ItemCount, p1.NextCursor)
	}

	reqPage2 := httptest.NewRequest(http.MethodGet, "/api/v1/repos?limit=2&cursor="+p1.NextCursor, nil)
	recPage2 := httptest.NewRecorder()
	handler.ServeHTTP(recPage2, reqPage2)

	var p2 codebase.ListReposResult
	_ = json.Unmarshal(recPage2.Body.Bytes(), &p2)
	if p2.ItemCount != 1 || p2.NextCursor != "" {
		t.Fatalf("expected 1 remaining item and empty next_cursor, got %d, %s", p2.ItemCount, p2.NextCursor)
	}

	// Verify no overlap between page 1 and page 2
	seen := make(map[string]bool)
	for _, item := range p1.Items {
		seen[item.Name] = true
	}
	for _, item := range p2.Items {
		if seen[item.Name] {
			t.Fatalf("duplicate item between pages: %s", item.Name)
		}
	}
}

func TestReposHandler_InvalidMethodAndCursor(t *testing.T) {
	store := codebase.NewMemStore()
	handler := NewReposHandler(store)

	// Non-GET
	reqPost := httptest.NewRequest(http.MethodPost, "/api/v1/repos", nil)
	recPost := httptest.NewRecorder()
	handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", recPost.Code)
	}

	// Invalid cursor
	reqBadCursor := httptest.NewRequest(http.MethodGet, "/api/v1/repos?cursor=invalid!base64", nil)
	recBadCursor := httptest.NewRecorder()
	handler.ServeHTTP(recBadCursor, reqBadCursor)
	if recBadCursor.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", recBadCursor.Code)
	}
}
