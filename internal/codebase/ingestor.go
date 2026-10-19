// Package codebase provides fleet repository discovery, hybrid RAG chunk stores, and ingestion pipelines.
package codebase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ComputeContentSHA256 returns the hexadecimal SHA-256 hash of symbol + "\x00" + content.
func ComputeContentSHA256(symbol, content string) string {
	h := sha256.Sum256([]byte(symbol + "\x00" + content))
	return hex.EncodeToString(h[:])
}

// IngestRequest represents an incremental chunk sync batch from an agent.
type IngestRequest struct {
	RepoName     string      `json:"repo_name"`
	BaseTreeSHA  string      `json:"base_tree_sha"`
	GitTreeSHA   string      `json:"git_tree_sha"`
	IsFinalBatch bool        `json:"is_final_batch"`
	DeletedFiles []string    `json:"deleted_files,omitempty"`
	Chunks       []CodeChunk `json:"chunks"`
}

// IngestResult reports the outcome of the chunk ingestion batch.
type IngestResult struct {
	RepoName           string `json:"repo_name"`
	IndexedTreeSHA     string `json:"indexed_tree_sha"`
	ChunksUpserted     int    `json:"chunks_upserted"`
	IndexedChunks      int    `json:"indexed_chunks"`
	EmbeddingsReused   int    `json:"embeddings_reused"`
	EmbeddingsComputed int    `json:"embeddings_computed"`
	FilesDeleted       int    `json:"files_deleted"`
	TreePromoted       bool   `json:"tree_promoted"`
	Status             string `json:"status"`
}

// ErrBaseTreeMismatch indicates that the request's base_tree_sha does not match the stored indexed_tree_sha.
type ErrBaseTreeMismatch struct {
	RepoName       string
	BaseTreeSHA    string
	IndexedTreeSHA string
}

func (e *ErrBaseTreeMismatch) Error() string {
	return fmt.Sprintf("base_tree_sha mismatch for %s: expected %q, got %q", e.RepoName, e.IndexedTreeSHA, e.BaseTreeSHA)
}

// ErrRepoNotFound indicates that the specified repository is not registered in the store.
type ErrRepoNotFound struct {
	RepoName string
}

func (e *ErrRepoNotFound) Error() string {
	return fmt.Sprintf("repository %q not found", e.RepoName)
}

// Ingestor processes incremental chunk ingestion batches with optimistic locking and embedding deduplication.
type Ingestor interface {
	Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error)
}

// DefaultIngestor implements Ingestor against a Store and Embedder.
type DefaultIngestor struct {
	Store    Store
	Embedder Embedder
	mu       sync.Mutex
}

// NewIngestor constructs a new DefaultIngestor. If embedder is nil, DeterministicEmbedder is used.
func NewIngestor(store Store, embedder Embedder) *DefaultIngestor {
	if embedder == nil {
		embedder = NewDeterministicEmbedder()
	}
	return &DefaultIngestor{
		Store:    store,
		Embedder: embedder,
	}
}

// Ingest coordinates optimistic base tree verification, file deletion pruning, superseded chunk replacement,
// content-addressed embedding deduplication, and final-batch tree promotion.
func (d *DefaultIngestor) Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error) {
	req.RepoName = strings.TrimSpace(req.RepoName)
	if req.RepoName == "" {
		return nil, fmt.Errorf("repo_name is required")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Optimistic Base Tree Verification
	repo, found, err := d.Store.GetRepository(ctx, req.RepoName)
	if err != nil {
		return nil, fmt.Errorf("get repository %s: %w", req.RepoName, err)
	}
	if !found || repo == nil {
		return nil, &ErrRepoNotFound{RepoName: req.RepoName}
	}

	// Base tree verification: verify base_tree_sha matches current indexed_tree_sha (repo.GitTreeSHA)
	if req.BaseTreeSHA != repo.GitTreeSHA {
		return nil, &ErrBaseTreeMismatch{
			RepoName:       req.RepoName,
			BaseTreeSHA:    req.BaseTreeSHA,
			IndexedTreeSHA: repo.GitTreeSHA,
		}
	}

	// 2. File-Level Deletion Pruning
	filesDeleted := 0
	if len(req.DeletedFiles) > 0 {
		_, err := d.Store.DeleteChunksForFiles(ctx, req.RepoName, req.DeletedFiles)
		if err != nil {
			return nil, fmt.Errorf("delete chunks for deleted files: %w", err)
		}
		filesDeleted = len(req.DeletedFiles)
	}

	// 3. Stale Blob Pruning for Modified Files
	filesWithIncoming := make(map[string]string)
	for _, c := range req.Chunks {
		norm := NormalizeFilePath(c.FilePath)
		if _, exists := filesWithIncoming[norm]; !exists && c.FileBlobSHA != "" {
			filesWithIncoming[norm] = c.FileBlobSHA
		}
	}
	for filePath, incomingBlobSHA := range filesWithIncoming {
		if _, err := d.Store.PruneFileSupersededChunks(ctx, req.RepoName, filePath, incomingBlobSHA); err != nil {
			return nil, fmt.Errorf("prune superseded chunks for %s: %w", filePath, err)
		}
	}

	// 4. Content-Addressed Embedding Reuse & Deduction
	var chunksToUpsert []CodeChunk
	var novelIndices []int
	var novelTexts []string
	embeddingsReused := 0
	embeddingsComputed := 0

	batchEmbeddings := make(map[string][]float64)

	for i := range req.Chunks {
		c := req.Chunks[i]
		c.RepoName = req.RepoName
		c.FilePath = NormalizeFilePath(c.FilePath)
		if c.GitTreeSHA == "" {
			c.GitTreeSHA = req.GitTreeSHA
		}
		if c.ID == "" {
			c.ID = fmt.Sprintf("%s:%s:%d-%d", c.RepoName, c.FilePath, c.StartLine, c.EndLine)
		}
		c.ContentSHA256 = ComputeContentSHA256(c.Symbol, c.Content)

		// If chunk already carries an embedding vector
		if len(c.Embedding) > 0 {
			batchEmbeddings[c.ContentSHA256] = c.Embedding
			chunksToUpsert = append(chunksToUpsert, c)
			continue
		}

		// Check batch-level cache
		if cached, ok := batchEmbeddings[c.ContentSHA256]; ok && len(cached) > 0 {
			c.Embedding = cached
			embeddingsReused++
			chunksToUpsert = append(chunksToUpsert, c)
			continue
		}

		// Check store content-addressed embedding cache
		cached, found, err := d.Store.FindEmbeddingByContentSHA(ctx, c.ContentSHA256)
		if err == nil && found && len(cached) > 0 {
			c.Embedding = cached
			batchEmbeddings[c.ContentSHA256] = cached
			embeddingsReused++
			chunksToUpsert = append(chunksToUpsert, c)
			continue
		}

		// Cache miss: needs embedding computation
		novelIndices = append(novelIndices, len(chunksToUpsert))
		novelTexts = append(novelTexts, c.Content)
		chunksToUpsert = append(chunksToUpsert, c)
	}

	// Batch compute novel embeddings
	if len(novelTexts) > 0 {
		computed, err := d.Embedder.Embed(ctx, novelTexts)
		if err != nil {
			return nil, fmt.Errorf("compute embeddings: %w", err)
		}
		for k, idx := range novelIndices {
			chunksToUpsert[idx].Embedding = computed[k]
			batchEmbeddings[chunksToUpsert[idx].ContentSHA256] = computed[k]
		}
		embeddingsComputed = len(novelTexts)
	}

	if len(chunksToUpsert) > 0 {
		if err := d.Store.UpsertChunks(ctx, chunksToUpsert); err != nil {
			return nil, fmt.Errorf("upsert chunks: %w", err)
		}
	}

	// 5. Final-Batch Tree Promotion
	treePromoted := false
	indexedTreeSHA := repo.GitTreeSHA
	if req.IsFinalBatch {
		repo.GitTreeSHA = req.GitTreeSHA
		repo.UpdatedAt = time.Now().UTC()
		if err := d.Store.UpsertRepository(ctx, *repo); err != nil {
			return nil, fmt.Errorf("promote repository tree: %w", err)
		}
		treePromoted = true
		indexedTreeSHA = req.GitTreeSHA
	}

	return &IngestResult{
		RepoName:           req.RepoName,
		IndexedTreeSHA:     indexedTreeSHA,
		ChunksUpserted:     len(chunksToUpsert),
		IndexedChunks:      len(chunksToUpsert),
		EmbeddingsReused:   embeddingsReused,
		EmbeddingsComputed: embeddingsComputed,
		FilesDeleted:       filesDeleted,
		TreePromoted:       treePromoted,
		Status:             "ok",
	}, nil
}
