package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

func TestCodebaseSearchHandler_SearchAndFilters(t *testing.T) {
	store := codebase.NewMemStore()
	ctx := context.Background()

	chunks := []codebase.CodeChunk{
		{
			ID:          "chunk-1",
			RepoName:    "iazio-mcp",
			FilePath:    "internal/mcp/tools_codebase.go",
			StartLine:   42,
			EndLine:     88,
			Symbol:      "handleSearchCodebase",
			Content:     "func handleSearchCodebase(query string) string { return tokenBudgetClamp }",
			FileBlobSHA: "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			GitTreeSHA:  "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		},
		{
			ID:          "chunk-2",
			RepoName:    "iazio-mcp",
			FilePath:    "internal/client/codebase.go",
			StartLine:   10,
			EndLine:     30,
			Symbol:      "Client",
			Content:     "type Client struct { BaseURL string }",
			FileBlobSHA: "95d09f2b10159347eece71399a7e2e907ea3df4f",
			GitTreeSHA:  "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		},
		{
			ID:          "chunk-3",
			RepoName:    "autopilot",
			FilePath:    "cmd/main.go",
			StartLine:   1,
			EndLine:     20,
			Symbol:      "main",
			Content:     "func main() { fmt.Println(\"autopilot\") }",
			FileBlobSHA: "3b18e512dba79e4c8300dd08aeb37f8e728b8dad",
			GitTreeSHA:  "f30127890ae37b94ffb7fa965702f4d32536e0ec",
		},
	}
	_ = store.UpsertChunks(ctx, chunks)

	handler := NewCodebaseSearchHandler(store)

	// 1. Successful search with query matching symbol handleSearchCodebase
	payload := SearchPayload{
		Query:    "search_codebase token budget clamp",
		RepoName: "iazio-mcp",
		PathGlob: "internal/mcp/**",
	}
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp codebase.SearchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.HitCount != 1 {
		t.Fatalf("expected 1 hit, got %d", resp.HitCount)
	}

	matchedChunk := resp.Chunks[0]
	if matchedChunk.RepoName != "iazio-mcp" {
		t.Errorf("expected repo iazio-mcp, got %s", matchedChunk.RepoName)
	}
	if matchedChunk.FilePath != "internal/mcp/tools_codebase.go" {
		t.Errorf("expected normalized path internal/mcp/tools_codebase.go, got %s", matchedChunk.FilePath)
	}
	if matchedChunk.FileBlobSHA != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Errorf("expected blob sha e69de29bb2d1d6434b8b29ae775ad8c2e48c5391, got %s", matchedChunk.FileBlobSHA)
	}
	if matchedChunk.GitTreeSHA != "4b825dc642cb6eb9a060e54bf8d69288fbee4904" {
		t.Errorf("expected tree sha 4b825dc642cb6eb9a060e54bf8d69288fbee4904, got %s", matchedChunk.GitTreeSHA)
	}
	if matchedChunk.Score <= 0 {
		t.Errorf("expected positive score, got %f", matchedChunk.Score)
	}

	// 2. GitTreeSHA filter
	payloadTree := SearchPayload{
		Query:      "main",
		GitTreeSHA: "f30127890ae37b94ffb7fa965702f4d32536e0ec",
	}
	bodyTree, _ := json.Marshal(payloadTree)
	reqTree := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader(bodyTree))
	recTree := httptest.NewRecorder()
	handler.ServeHTTP(recTree, reqTree)

	if recTree.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recTree.Code)
	}

	var respTree codebase.SearchResult
	_ = json.Unmarshal(recTree.Body.Bytes(), &respTree)
	if respTree.HitCount != 1 || respTree.Chunks[0].RepoName != "autopilot" {
		t.Fatalf("expected 1 hit for autopilot, got %d", respTree.HitCount)
	}
}

func TestCodebaseSearchHandler_ValidationsAndErrors(t *testing.T) {
	store := codebase.NewMemStore()
	handler := NewCodebaseSearchHandler(store)

	// 1. Empty query -> 422 Unprocessable Entity
	badPayload := SearchPayload{Query: "   "}
	body, _ := json.Marshal(badPayload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity on blank query, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "query must not be empty") {
		t.Fatalf("expected error message in response body, got %s", rec.Body.String())
	}

	// 2. Malformed JSON -> 422 Unprocessable Entity
	reqMalformed := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader([]byte("{not-json")))
	recMalformed := httptest.NewRecorder()
	handler.ServeHTTP(recMalformed, reqMalformed)

	if recMalformed.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on malformed json, got %d", recMalformed.Code)
	}

	// 3. Non-POST method -> 405 Method Not Allowed
	reqGet := httptest.NewRequest(http.MethodGet, "/api/v1/codebase/search", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)

	if recGet.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", recGet.Code)
	}
}
