package codebase

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIngestor_OptimisticBaseTreeVerification(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	ingestor := NewIngestor(store, nil)

	// 1. Repo does not exist -> ErrRepoNotFound
	_, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:    "non-existent-repo",
		BaseTreeSHA: "",
		GitTreeSHA:  "tree-1",
	})
	var notFound *ErrRepoNotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("expected ErrRepoNotFound, got %v", err)
	}

	// 2. Setup repo with indexed tree sha "tree-1"
	_ = store.UpsertRepository(ctx, Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-1",
		UpdatedAt:     time.Now().UTC(),
	})

	// 3. Mismatched base_tree_sha -> ErrBaseTreeMismatch
	_, err = ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "wrong-base-tree",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
	})
	var mismatch *ErrBaseTreeMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected ErrBaseTreeMismatch, got %v", err)
	}
	if mismatch.IndexedTreeSHA != "tree-1" || mismatch.BaseTreeSHA != "wrong-base-tree" {
		t.Fatalf("unexpected mismatch details: %+v", mismatch)
	}

	// 4. Correct base_tree_sha -> success
	res, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-1",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
	})
	if err != nil {
		t.Fatalf("expected success on matching base_tree_sha, got %v", err)
	}
	if !res.TreePromoted || res.IndexedTreeSHA != "tree-2" {
		t.Fatalf("expected tree promoted to tree-2, got %+v", res)
	}
}

func TestIngestor_MultiBatchTreePromotion(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	ingestor := NewIngestor(store, nil)

	_ = store.UpsertRepository(ctx, Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-base",
		UpdatedAt:     time.Now().UTC(),
	})

	chunk1 := CodeChunk{
		FilePath:    "internal/pkg/one.go",
		StartLine:   1,
		EndLine:     10,
		Symbol:      "FuncOne",
		Content:     "func FuncOne() string { return \"one\" }",
		FileBlobSHA: "blob-1",
	}
	chunk2 := CodeChunk{
		FilePath:    "internal/pkg/two.go",
		StartLine:   1,
		EndLine:     10,
		Symbol:      "FuncTwo",
		Content:     "func FuncTwo() string { return \"two\" }",
		FileBlobSHA: "blob-2",
	}

	// Batch 1: is_final_batch: false
	res1, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-base",
		GitTreeSHA:   "tree-target",
		IsFinalBatch: false,
		Chunks:       []CodeChunk{chunk1},
	})
	if err != nil {
		t.Fatalf("batch 1 failed: %v", err)
	}
	if res1.TreePromoted {
		t.Fatalf("expected TreePromoted == false on intermediate batch")
	}
	if res1.IndexedTreeSHA != "tree-base" {
		t.Fatalf("expected IndexedTreeSHA == tree-base, got %s", res1.IndexedTreeSHA)
	}

	// Verify repo in store has not advanced
	repo, _, _ := store.GetRepository(ctx, "iazio-mcp")
	if repo.GitTreeSHA != "tree-base" {
		t.Fatalf("expected store repo GitTreeSHA to stay tree-base, got %s", repo.GitTreeSHA)
	}

	// Batch 2: is_final_batch: true
	res2, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-base",
		GitTreeSHA:   "tree-target",
		IsFinalBatch: true,
		Chunks:       []CodeChunk{chunk2},
	})
	if err != nil {
		t.Fatalf("batch 2 failed: %v", err)
	}
	if !res2.TreePromoted {
		t.Fatalf("expected TreePromoted == true on final batch")
	}
	if res2.IndexedTreeSHA != "tree-target" {
		t.Fatalf("expected IndexedTreeSHA == tree-target, got %s", res2.IndexedTreeSHA)
	}

	// Verify repo in store has now advanced
	repo, _, _ = store.GetRepository(ctx, "iazio-mcp")
	if repo.GitTreeSHA != "tree-target" {
		t.Fatalf("expected store repo GitTreeSHA to be tree-target, got %s", repo.GitTreeSHA)
	}

	// Verify both chunks exist in store
	chunks, _ := store.GetChunksForRepo(ctx, "iazio-mcp")
	if len(chunks) != 2 {
		t.Fatalf("expected 2 active chunks in store, got %d", len(chunks))
	}
}

func TestIngestor_EmbeddingReuse(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	countingEmbedder := NewCountingEmbedder(nil)
	ingestor := NewIngestor(store, countingEmbedder)

	_ = store.UpsertRepository(ctx, Repository{
		Name:          "iazio-harness-api",
		Path:          "iazio-harness-api",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-0",
	})

	chunkA := CodeChunk{
		FilePath:    "pkg/a.go",
		StartLine:   1,
		EndLine:     5,
		Symbol:      "FuncA",
		Content:     "func FuncA() int { return 42 }",
		FileBlobSHA: "blob-a",
	}
	chunkB := CodeChunk{
		FilePath:    "pkg/b.go",
		StartLine:   1,
		EndLine:     5,
		Symbol:      "FuncB",
		Content:     "func FuncB() int { return 100 }",
		FileBlobSHA: "blob-b",
	}

	// 1. Initial ingestion of chunkA and chunkB: cache misses
	res1, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-harness-api",
		BaseTreeSHA:  "tree-0",
		GitTreeSHA:   "tree-1",
		IsFinalBatch: true,
		Chunks:       []CodeChunk{chunkA, chunkB},
	})
	if err != nil {
		t.Fatalf("initial ingest failed: %v", err)
	}
	if res1.EmbeddingsComputed != 2 || res1.EmbeddingsReused != 0 {
		t.Fatalf("expected 2 computed, 0 reused; got computed=%d reused=%d", res1.EmbeddingsComputed, res1.EmbeddingsReused)
	}
	if countingEmbedder.TotalTextsCount != 2 {
		t.Fatalf("expected countingEmbedder to have embedded 2 texts, got %d", countingEmbedder.TotalTextsCount)
	}

	// 2. Second ingestion: chunkA unchanged, chunkB unchanged, chunkC novel
	chunkC := CodeChunk{
		FilePath:    "pkg/c.go",
		StartLine:   1,
		EndLine:     5,
		Symbol:      "FuncC",
		Content:     "func FuncC() string { return \"novel\" }",
		FileBlobSHA: "blob-c",
	}

	res2, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-harness-api",
		BaseTreeSHA:  "tree-1",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
		Chunks:       []CodeChunk{chunkA, chunkB, chunkC},
	})
	if err != nil {
		t.Fatalf("second ingest failed: %v", err)
	}
	if res2.EmbeddingsReused != 2 {
		t.Fatalf("expected 2 reused embeddings, got %d", res2.EmbeddingsReused)
	}
	if res2.EmbeddingsComputed != 1 {
		t.Fatalf("expected 1 computed embedding, got %d", res2.EmbeddingsComputed)
	}
	if countingEmbedder.TotalTextsCount != 3 {
		t.Fatalf("expected total texts embedded to be 3 (2 from batch 1 + 1 from batch 2), got %d", countingEmbedder.TotalTextsCount)
	}
}

func TestIngestor_DeletedFilePruning(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	ingestor := NewIngestor(store, nil)

	_ = store.UpsertRepository(ctx, Repository{
		Name:          "iazio-agent",
		Path:          "iazio-agent",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-1",
	})

	// Add 2 files
	_, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-agent",
		BaseTreeSHA:  "tree-1",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
		Chunks: []CodeChunk{
			{
				FilePath:    "internal/worker/old_tool.go",
				StartLine:   1,
				EndLine:     10,
				Content:     "package worker",
				FileBlobSHA: "blob-old",
			},
			{
				FilePath:    "internal/worker/kept_tool.go",
				StartLine:   1,
				EndLine:     10,
				Content:     "package worker",
				FileBlobSHA: "blob-kept",
			},
		},
	})
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	chunks, _ := store.GetChunksForRepo(ctx, "iazio-agent")
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks initially, got %d", len(chunks))
	}

	// Now delete old_tool.go
	res, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-agent",
		BaseTreeSHA:  "tree-2",
		GitTreeSHA:   "tree-3",
		IsFinalBatch: true,
		DeletedFiles: []string{"internal/worker/old_tool.go"},
	})
	if err != nil {
		t.Fatalf("delete ingest failed: %v", err)
	}
	if res.FilesDeleted != 1 {
		t.Fatalf("expected 1 file deleted, got %d", res.FilesDeleted)
	}

	chunksAfter, _ := store.GetChunksForRepo(ctx, "iazio-agent")
	if len(chunksAfter) != 1 {
		t.Fatalf("expected 1 chunk remaining after deletion, got %d", len(chunksAfter))
	}
	if chunksAfter[0].FilePath != "internal/worker/kept_tool.go" {
		t.Fatalf("unexpected remaining chunk: %s", chunksAfter[0].FilePath)
	}
}

func TestIngestor_SupersededBlobPruning(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	ingestor := NewIngestor(store, nil)

	_ = store.UpsertRepository(ctx, Repository{
		Name:          "iazio-mcp",
		Path:          "iazio-mcp",
		DefaultBranch: "main",
		GitTreeSHA:    "tree-1",
	})

	// File originally had 2 chunks under blob-v1
	_, err := ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-1",
		GitTreeSHA:   "tree-2",
		IsFinalBatch: true,
		Chunks: []CodeChunk{
			{
				FilePath:    "internal/mcp/tool.go",
				StartLine:   1,
				EndLine:     50,
				Symbol:      "FuncAlpha",
				Content:     "func FuncAlpha() {}",
				FileBlobSHA: "blob-v1",
			},
			{
				FilePath:    "internal/mcp/tool.go",
				StartLine:   51,
				EndLine:     100,
				Symbol:      "FuncBeta",
				Content:     "func FuncBeta() {}",
				FileBlobSHA: "blob-v1",
			},
		},
	})
	if err != nil {
		t.Fatalf("initial ingest failed: %v", err)
	}

	chunks, _ := store.GetChunksForRepo(ctx, "iazio-mcp")
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	// Refactored file is smaller: now only 1 chunk under blob-v2
	_, err = ingestor.Ingest(ctx, IngestRequest{
		RepoName:     "iazio-mcp",
		BaseTreeSHA:  "tree-2",
		GitTreeSHA:   "tree-3",
		IsFinalBatch: true,
		Chunks: []CodeChunk{
			{
				FilePath:    "internal/mcp/tool.go",
				StartLine:   1,
				EndLine:     40,
				Symbol:      "FuncCombined",
				Content:     "func FuncCombined() {}",
				FileBlobSHA: "blob-v2",
			},
		},
	})
	if err != nil {
		t.Fatalf("updated blob ingest failed: %v", err)
	}

	chunksAfter, _ := store.GetChunksForRepo(ctx, "iazio-mcp")
	if len(chunksAfter) != 1 {
		t.Fatalf("expected exactly 1 chunk (no ghost chunk from blob-v1), got %d", len(chunksAfter))
	}
	if chunksAfter[0].Symbol != "FuncCombined" || chunksAfter[0].FileBlobSHA != "blob-v2" {
		t.Fatalf("unexpected chunk content: %+v", chunksAfter[0])
	}
}

func TestIngestorEmbeddingReuse(t *testing.T) {
	TestIngestor_EmbeddingReuse(t)
}
