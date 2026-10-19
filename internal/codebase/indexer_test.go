package codebase

import (
	"context"
	"testing"
)

func TestCalculateGitBlobSHA(t *testing.T) {
	// In Git:
	// echo -n "hello world" | git hash-object --stdin -> 95d09f2b10159347eece71399a7e2e907ea3df4f
	content := []byte("hello world")
	expected := "95d09f2b10159347eece71399a7e2e907ea3df4f"

	actual := CalculateGitBlobSHA(content)
	if actual != expected {
		t.Fatalf("expected git blob SHA %s, got %s", expected, actual)
	}

	// Empty content:
	// echo -n "" | git hash-object --stdin -> e69de29bb2d1d6434b8b29ae775ad8c2e48c5391
	emptyContent := []byte("")
	expectedEmpty := "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
	actualEmpty := CalculateGitBlobSHA(emptyContent)
	if actualEmpty != expectedEmpty {
		t.Fatalf("expected empty git blob SHA %s, got %s", expectedEmpty, actualEmpty)
	}
}

func TestNormalizeFilePath(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"./internal/mcp/tools.go", "internal/mcp/tools.go"},
		{"/internal/mcp/tools.go", "internal/mcp/tools.go"},
		{"internal\\mcp\\tools.go", "internal/mcp/tools.go"},
		{"tools.go", "tools.go"},
		{"././src/foo.py", "src/foo.py"},
	}

	for _, c := range cases {
		actual := NormalizeFilePath(c.input)
		if actual != c.expected {
			t.Errorf("input %s: expected %s, got %s", c.input, c.expected, actual)
		}
	}
}

func TestChunkSourceFile(t *testing.T) {
	repo := "iazio-mcp"
	path := "internal/mcp/tools_codebase.go"
	treeSHA := "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

	code := `package mcp

import "fmt"

func handleSearchCodebase(query string) string {
	fmt.Println(query)
	return "result"
}
`
	chunks := ChunkSourceFile(repo, path, treeSHA, []byte(code))
	if len(chunks) == 0 {
		t.Fatal("expected at least 1 chunk")
	}

	chunk := chunks[0]
	if chunk.RepoName != repo {
		t.Errorf("expected repo %s, got %s", repo, chunk.RepoName)
	}
	if chunk.FilePath != path {
		t.Errorf("expected path %s, got %s", path, chunk.FilePath)
	}
	if chunk.Symbol != "handleSearchCodebase" {
		t.Errorf("expected symbol handleSearchCodebase, got %s", chunk.Symbol)
	}
	if chunk.FileBlobSHA != CalculateGitBlobSHA([]byte(code)) {
		t.Errorf("expected blob SHA %s, got %s", CalculateGitBlobSHA([]byte(code)), chunk.FileBlobSHA)
	}
	if chunk.GitTreeSHA != treeSHA {
		t.Errorf("expected tree SHA %s, got %s", treeSHA, chunk.GitTreeSHA)
	}
}

func TestRankChunksRRF_ExactSymbolBoost(t *testing.T) {
	chunks := []CodeChunk{
		{
			ID:          "1",
			RepoName:    "iazio-mcp",
			FilePath:    "internal/mcp/tools.go",
			Symbol:      "handleSearchCodebase",
			Content:     "func handleSearchCodebase(query string) string",
			FileBlobSHA: "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			GitTreeSHA:  "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		},
		{
			ID:          "2",
			RepoName:    "iazio-mcp",
			FilePath:    "internal/mcp/other.go",
			Symbol:      "otherFunc",
			Content:     "func otherFunc() string { return searchCodebase }",
			FileBlobSHA: "3b18e512dba79e4c8300dd08aeb37f8e728b8dad",
			GitTreeSHA:  "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		},
	}

	query := "handleSearchCodebase"
	queryEmb := GenerateEmbedding(query)

	ranked := RankChunksRRF(chunks, query, queryEmb)
	if len(ranked) != 2 {
		t.Fatalf("expected 2 ranked chunks, got %d", len(ranked))
	}

	// Chunk 1 has exact symbol match, so it should be ranked first with a boosted score
	if ranked[0].ID != "1" {
		t.Fatalf("expected chunk 1 to be first, got chunk %s", ranked[0].ID)
	}

	if ranked[0].Score <= ranked[1].Score {
		t.Fatalf("expected boosted chunk score (%f) to be higher than chunk 2 score (%f)", ranked[0].Score, ranked[1].Score)
	}
}

func TestMatchPathGlob(t *testing.T) {
	cases := []struct {
		pattern  string
		path     string
		expected bool
	}{
		{"internal/mcp/**", "internal/mcp/tools.go", true},
		{"internal/mcp/**", "internal/mcp/subdir/nested.go", true},
		{"internal/mcp/**", "internal/api/router.go", false},
		{"*.go", "main.go", true},
		{"*.go", "internal/api/router.go", true},
		{"*.go", "README.md", false},
		{"", "internal/api/router.go", true},
		{"**", "internal/api/router.go", true},
	}

	for _, c := range cases {
		matched := MatchPathGlob(c.pattern, c.path)
		if matched != c.expected {
			t.Errorf("pattern %q on path %q: expected %v, got %v", c.pattern, c.path, c.expected, matched)
		}
	}
}

func TestIncrementalIndexingAndPruning(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()

	tree1 := "1111111111111111111111111111111111111111"
	tree2 := "2222222222222222222222222222222222222222"

	files1 := map[string][]byte{
		"file1.go": []byte("package main\n\nfunc main() {}"),
		"file2.go": []byte("package main\n\nfunc helper() {}"),
	}

	err := IndexRepositoryFiles(ctx, store, "repo-a", tree1, files1)
	if err != nil {
		t.Fatalf("index files 1 failed: %v", err)
	}

	chunks1, err := store.GetChunksForRepo(ctx, "repo-a")
	if err != nil || len(chunks1) != 2 {
		t.Fatalf("expected 2 active chunks, got %d, err: %v", len(chunks1), err)
	}

	// Update repo to tree2 where file2.go is deleted and file1.go is updated
	files2 := map[string][]byte{
		"file1.go": []byte("package main\n\nfunc main() { fmt.Println() }"),
	}

	err = IndexRepositoryFiles(ctx, store, "repo-a", tree2, files2)
	if err != nil {
		t.Fatalf("index files 2 failed: %v", err)
	}

	chunks2, err := store.GetChunksForRepo(ctx, "repo-a")
	if err != nil || len(chunks2) != 1 {
		t.Fatalf("expected 1 active chunk for tree2, got %d, err: %v", len(chunks2), err)
	}
	if chunks2[0].GitTreeSHA != tree2 {
		t.Fatalf("expected active chunk tree SHA to be %s, got %s", tree2, chunks2[0].GitTreeSHA)
	}
}
