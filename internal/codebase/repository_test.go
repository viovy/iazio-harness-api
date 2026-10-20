package codebase_test

import (
	"context"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

func TestRepoCursorEncoding(t *testing.T) {
	// Empty cursor
	name, path, err := codebase.DecodeRepoCursor("")
	if err != nil || name != "" || path != "" {
		t.Errorf("expected empty name/path, got %q, %q, %v", name, path, err)
	}

	// Normal roundtrip
	c := codebase.EncodeRepoCursor("repo-a", "/path/a")
	name, path, err = codebase.DecodeRepoCursor(c)
	if err != nil || name != "repo-a" || path != "/path/a" {
		t.Errorf("roundtrip failed: got %q, %q, %v", name, path, err)
	}

	// Invalid base64
	_, _, err = codebase.DecodeRepoCursor("not-base64!@#$")
	if err == nil {
		t.Error("expected error for invalid base64")
	}
}

func TestOffsetCursorEncoding(t *testing.T) {
	c := codebase.EncodeOffsetCursor(42)
	offset, err := codebase.DecodeOffsetCursor(c)
	if err != nil || offset != 42 {
		t.Errorf("roundtrip failed: got %d, %v", offset, err)
	}

	// Invalid format
	_, err = codebase.DecodeOffsetCursor("invalid")
	if err == nil {
		t.Error("expected error for invalid base64")
	}

	// Non-offset string
	c2 := codebase.EncodeRepoCursor("foo", "bar")
	_, err = codebase.DecodeOffsetCursor(c2)
	if err == nil {
		t.Error("expected error for non-offset prefix")
	}
}

func TestListRepositories(t *testing.T) {
	store := codebase.NewMemStore()
	ctx := context.Background()

	repos := []codebase.Repository{
		{Name: "iazio-api", Path: "iazio-api", Description: "API Gateway", UpdatedAt: time.Now()},
		{Name: "iazio-agent", Path: "iazio-agent", Description: "Host Agent", UpdatedAt: time.Now()},
		{Name: "iazio-web", Path: "iazio-web", Description: "Web UI", UpdatedAt: time.Now()},
		{Name: "iazio-mcp", Path: "iazio-mcp", Description: "MCP Gateway", UpdatedAt: time.Now()},
	}
	for _, r := range repos {
		_ = store.UpsertRepository(ctx, r)
	}

	// Query search by description
	res, err := store.ListRepositories(ctx, codebase.ListReposRequest{
		Query: "Gateway",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListRepositories: %v", err)
	}
	if res.Total != 2 {
		t.Errorf("expected 2 matching repos, got %d", res.Total)
	}

	// Keyset pagination (page size 2)
	p1, err := store.ListRepositories(ctx, codebase.ListReposRequest{Limit: 2})
	if err != nil || p1.ItemCount != 2 || p1.NextCursor == "" {
		t.Fatalf("page 1 failed: count=%d, nextCursor=%s, err=%v", p1.ItemCount, p1.NextCursor, err)
	}

	p2, err := store.ListRepositories(ctx, codebase.ListReposRequest{
		Limit:  2,
		Cursor: p1.NextCursor,
	})
	if err != nil || p2.ItemCount != 2 {
		t.Fatalf("page 2 failed: count=%d, err=%v", p2.ItemCount, err)
	}

	// Delete and verify
	_ = store.DeleteRepository(ctx, "iazio-api")
	_, ok, _ := store.GetRepository(ctx, "iazio-api")
	if ok {
		t.Error("expected iazio-api to be deleted")
	}
}

func TestSearchChunks(t *testing.T) {
	store := codebase.NewMemStore()
	ctx := context.Background()

	// Empty query error
	_, err := store.SearchChunks(ctx, codebase.SearchRequest{Query: ""})
	if err == nil {
		t.Fatal("expected error for empty query")
	}

	// No candidates
	res, err := store.SearchChunks(ctx, codebase.SearchRequest{
		Query:    "func main()",
		RepoName: "nonexistent",
	})
	if err != nil || res.Total != 0 {
		t.Fatalf("expected 0 hits, got %d, err=%v", res.Total, err)
	}

	// Insert test chunks
	chunks := []codebase.CodeChunk{
		{
			ID:          "c1",
			RepoName:    "iazio-mcp",
			FilePath:    "cmd/main.go",
			StartLine:   1,
			EndLine:     15,
			Symbol:      "main",
			Content:     "package main\n\nfunc main() { run() }",
			FileBlobSHA: "b1",
			GitTreeSHA:  "tree1",
		},
		{
			ID:          "c2",
			RepoName:    "iazio-mcp",
			FilePath:    "pkg/auth/auth.go",
			StartLine:   1,
			EndLine:     20,
			Symbol:      "ValidateToken",
			Content:     "package auth\n\nfunc ValidateToken(t string) bool { return true }",
			FileBlobSHA: "b2",
			GitTreeSHA:  "tree1",
		},
		{
			ID:          "c3",
			RepoName:    "iazio-mcp",
			FilePath:    "pkg/auth/auth_test.go",
			StartLine:   1,
			EndLine:     10,
			Symbol:      "TestValidateToken",
			Content:     "func TestValidateToken(t *testing.T) {}",
			FileBlobSHA: "b3",
			GitTreeSHA:  "tree1",
		},
	}
	_ = store.UpsertChunks(ctx, chunks)

	// Search matching token
	sRes, err := store.SearchChunks(ctx, codebase.SearchRequest{
		Query:      "ValidateToken",
		RepoName:   "iazio-mcp",
		GitTreeSHA: "tree1",
		PathGlob:   "pkg/**",
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("SearchChunks: %v", err)
	}
	if sRes.HitCount != 1 || sRes.Total != 2 {
		t.Errorf("expected 1 hit from total 2, got %d hits / %d total", sRes.HitCount, sRes.Total)
	}
	if sRes.NextCursor == "" {
		t.Error("expected non-empty nextCursor")
	}

	// Paged search with nextCursor
	sRes2, err := store.SearchChunks(ctx, codebase.SearchRequest{
		Query:      "ValidateToken",
		RepoName:   "iazio-mcp",
		GitTreeSHA: "tree1",
		PathGlob:   "pkg/**",
		Limit:      1,
		Cursor:     sRes.NextCursor,
	})
	if err != nil || sRes2.HitCount != 1 {
		t.Fatalf("paged search failed: count=%d, err=%v", sRes2.HitCount, err)
	}
}

func TestErrorsAndDetectSymbols(t *testing.T) {
	errMismatch := &codebase.ErrBaseTreeMismatch{RepoName: "repo1", BaseTreeSHA: "b1", IndexedTreeSHA: "i1"}
	if errMismatch.Error() == "" {
		t.Error("expected non-empty error message")
	}

	errNotFound := &codebase.ErrRepoNotFound{RepoName: "repo2"}
	if errNotFound.Error() == "" {
		t.Error("expected non-empty error message")
	}

	// Test symbols across Go type, Python def, Python class, JS function, JS class
	codeSnippet := `
type ServerConfig struct {
    Port int
}

def calculate_hash(data):
    pass

class DataProcessor:
    pass

function executeCommand() {
    return true;
}

class WidgetManager {
}
`
	chunks := codebase.ChunkSourceFile("repo-test", "test.py", "tree123", []byte(codeSnippet))
	if len(chunks) == 0 {
		t.Fatal("expected chunks from codeSnippet")
	}
	var symbols []string
	for _, c := range chunks {
		if c.Symbol != "" {
			symbols = append(symbols, c.Symbol)
		}
	}
	if len(symbols) == 0 {
		t.Error("expected at least one detected symbol")
	}
}
