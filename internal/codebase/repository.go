// Package codebase provides fleet repository discovery and hybrid codebase RAG data types and stores.
package codebase

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Repository describes a registered fleet submodule or repository known to the harness control plane.
type Repository struct {
	Name          string    `json:"name"`
	Path          string    `json:"path"`
	DefaultBranch string    `json:"default_branch"`
	GitTreeSHA    string    `json:"git_tree_sha,omitempty"`
	Description   string    `json:"description,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// CodeChunk describes a snippet of source code indexed for hybrid RAG search.
type CodeChunk struct {
	ID          string    `json:"id"`
	RepoName    string    `json:"repo_name"`
	FilePath    string    `json:"file_path"`
	StartLine   int       `json:"start_line"`
	EndLine     int       `json:"end_line"`
	Symbol      string    `json:"symbol,omitempty"`
	Content     string    `json:"content"`
	FileBlobSHA string    `json:"file_blob_sha"`
	GitTreeSHA  string    `json:"git_tree_sha,omitempty"`
	Embedding   []float64 `json:"embedding,omitempty"`
	Superseded  bool      `json:"superseded,omitempty"`
	Score       float64   `json:"score"`
}

// ListReposRequest specifies query and pagination filters for GET /api/v1/repos.
type ListReposRequest struct {
	Query  string `json:"query,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// ListReposResult contains paginated repositories matching a query.
type ListReposResult struct {
	Items      []Repository `json:"items"`
	ItemCount  int          `json:"item_count"`
	Total      int          `json:"total"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// SearchRequest specifies parameters for hybrid codebase RAG search.
type SearchRequest struct {
	Query      string `json:"query"`
	RepoName   string `json:"repo_name,omitempty"`
	PathGlob   string `json:"path_glob,omitempty"`
	GitTreeSHA string `json:"git_tree_sha,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
}

// SearchResult contains the ranked code chunks and pagination cursor.
type SearchResult struct {
	Chunks     []CodeChunk `json:"chunks"`
	HitCount   int         `json:"hit_count"`
	Total      int         `json:"total"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// Store defines repository registry and codebase chunk operations.
type Store interface {
	ListRepositories(ctx context.Context, req ListReposRequest) (*ListReposResult, error)
	GetRepository(ctx context.Context, name string) (*Repository, bool, error)
	UpsertRepository(ctx context.Context, repo Repository) error
	DeleteRepository(ctx context.Context, name string) error

	SearchChunks(ctx context.Context, req SearchRequest) (*SearchResult, error)
	UpsertChunks(ctx context.Context, chunks []CodeChunk) error
	PruneStaleChunks(ctx context.Context, repoName, activeGitTreeSHA string) (int, error)
	GetChunksForRepo(ctx context.Context, repoName string) ([]CodeChunk, error)
}

// MemStore is a thread-safe, in-memory implementation of Store.
type MemStore struct {
	mu     sync.RWMutex
	repos  map[string]Repository
	chunks map[string]CodeChunk
}

// NewMemStore returns an empty in-memory codebase store.
func NewMemStore() *MemStore {
	return &MemStore{
		repos:  make(map[string]Repository),
		chunks: make(map[string]CodeChunk),
	}
}

// EncodeRepoCursor creates a deterministic base64 keyset cursor from repo name and path.
func EncodeRepoCursor(name, path string) string {
	raw := fmt.Sprintf("%s\x00%s", name, path)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeRepoCursor decodes a repo keyset cursor into name and path.
func DecodeRepoCursor(cursor string) (name, path string, err error) {
	if strings.TrimSpace(cursor) == "" {
		return "", "", nil
	}
	b, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(b), "\x00", 2)
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return parts[0], "", nil
}

// ListRepositories lists repositories with case-insensitive substring search and keyset pagination.
func (s *MemStore) ListRepositories(ctx context.Context, req ListReposRequest) (*ListReposResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := req.Limit
	if limit <= 0 {
		limit = 20
	} else if limit > 100 {
		limit = 100
	}

	cursorName, cursorPath, err := DecodeRepoCursor(req.Cursor)
	if err != nil {
		return nil, err
	}

	filterQuery := strings.ToLower(strings.TrimSpace(req.Query))

	all := make([]Repository, 0, len(s.repos))
	for _, r := range s.repos {
		if filterQuery != "" {
			nameMatch := strings.Contains(strings.ToLower(r.Name), filterQuery)
			pathMatch := strings.Contains(strings.ToLower(r.Path), filterQuery)
			descMatch := strings.Contains(strings.ToLower(r.Description), filterQuery)
			if !nameMatch && !pathMatch && !descMatch {
				continue
			}
		}
		all = append(all, r)
	}

	// Deterministic sort: name ASC, path ASC
	sort.Slice(all, func(i, j int) bool {
		if all[i].Name == all[j].Name {
			return all[i].Path < all[j].Path
		}
		return all[i].Name < all[j].Name
	})

	total := len(all)

	// Keyset skip
	startIndex := 0
	if cursorName != "" {
		for i, r := range all {
			if r.Name > cursorName || (r.Name == cursorName && r.Path > cursorPath) {
				startIndex = i
				break
			}
			if i == len(all)-1 {
				startIndex = len(all)
			}
		}
	}

	endIndex := startIndex + limit
	var nextCursor string
	if endIndex < total {
		lastItem := all[endIndex-1]
		nextCursor = EncodeRepoCursor(lastItem.Name, lastItem.Path)
	} else {
		endIndex = total
	}

	var items []Repository
	if startIndex < total {
		items = make([]Repository, endIndex-startIndex)
		copy(items, all[startIndex:endIndex])
	} else {
		items = []Repository{}
	}

	return &ListReposResult{
		Items:      items,
		ItemCount:  len(items),
		Total:      total,
		NextCursor: nextCursor,
	}, nil
}

// GetRepository retrieves a repository by exact name.
func (s *MemStore) GetRepository(ctx context.Context, name string) (*Repository, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.repos[name]
	if !ok {
		return nil, false, nil
	}
	cp := r
	return &cp, true, nil
}

// UpsertRepository adds or updates a repository.
func (s *MemStore) UpsertRepository(ctx context.Context, repo Repository) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if repo.UpdatedAt.IsZero() {
		repo.UpdatedAt = time.Now().UTC()
	}
	s.repos[repo.Name] = repo
	return nil
}

// DeleteRepository removes a repository by name.
func (s *MemStore) DeleteRepository(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.repos, name)
	return nil
}

// UpsertChunks stores or updates code chunks.
func (s *MemStore) UpsertChunks(ctx context.Context, chunks []CodeChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range chunks {
		if c.ID == "" {
			c.ID = fmt.Sprintf("%s:%s:%d-%d", c.RepoName, c.FilePath, c.StartLine, c.EndLine)
		}
		s.chunks[c.ID] = c
	}
	return nil
}

// PruneStaleChunks marks or deletes chunks for a repository that do not match the active tree SHA.
func (s *MemStore) PruneStaleChunks(ctx context.Context, repoName, activeGitTreeSHA string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pruned := 0
	for id, c := range s.chunks {
		if c.RepoName == repoName && c.GitTreeSHA != "" && c.GitTreeSHA != activeGitTreeSHA {
			c.Superseded = true
			s.chunks[id] = c
			pruned++
		}
	}
	return pruned, nil
}

// GetChunksForRepo returns all active chunks for a repository.
func (s *MemStore) GetChunksForRepo(ctx context.Context, repoName string) ([]CodeChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []CodeChunk
	for _, c := range s.chunks {
		if c.RepoName == repoName && !c.Superseded {
			result = append(result, c)
		}
	}
	return result, nil
}

// SearchChunks executes hybrid RRF search over code chunks with filters and pagination.
func (s *MemStore) SearchChunks(ctx context.Context, req SearchRequest) (*SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, fmt.Errorf("query must not be empty")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	} else if limit > 50 {
		limit = 50
	}

	// 1. Filter candidates
	candidates := make([]CodeChunk, 0, len(s.chunks))
	for _, c := range s.chunks {
		if c.Superseded {
			continue
		}
		if req.RepoName != "" && c.RepoName != req.RepoName {
			continue
		}
		if req.GitTreeSHA != "" && c.GitTreeSHA != req.GitTreeSHA {
			continue
		}
		if req.PathGlob != "" && !MatchPathGlob(req.PathGlob, c.FilePath) {
			continue
		}
		candidates = append(candidates, c)
	}

	if len(candidates) == 0 {
		return &SearchResult{
			Chunks:     []CodeChunk{},
			HitCount:   0,
			Total:      0,
			NextCursor: "",
		}, nil
	}

	// 2. Perform hybrid ranking
	queryEmbedding := GenerateEmbedding(query)
	scoredChunks := RankChunksRRF(candidates, query, queryEmbedding)

	total := len(scoredChunks)

	// Keyset / offset pagination
	startIndex := 0
	if req.Cursor != "" {
		if cursorIdx, err := DecodeOffsetCursor(req.Cursor); err == nil && cursorIdx > 0 {
			if cursorIdx < total {
				startIndex = cursorIdx
			} else {
				startIndex = total
			}
		}
	}

	endIndex := startIndex + limit
	var nextCursor string
	if endIndex < total {
		nextCursor = EncodeOffsetCursor(endIndex)
	} else {
		endIndex = total
	}

	var paged []CodeChunk
	if startIndex < total {
		paged = make([]CodeChunk, endIndex-startIndex)
		copy(paged, scoredChunks[startIndex:endIndex])
	} else {
		paged = []CodeChunk{}
	}

	return &SearchResult{
		Chunks:     paged,
		HitCount:   len(paged),
		Total:      total,
		NextCursor: nextCursor,
	}, nil
}

// EncodeOffsetCursor encodes an integer offset into a base64 cursor.
func EncodeOffsetCursor(offset int) string {
	raw := fmt.Sprintf("offset:%d", offset)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeOffsetCursor parses an offset cursor.
func DecodeOffsetCursor(cursor string) (int, error) {
	b, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	s := string(b)
	if !strings.HasPrefix(s, "offset:") {
		return 0, fmt.Errorf("unknown cursor format")
	}
	var offset int
	_, err = fmt.Sscanf(s, "offset:%d", &offset)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// MatchPathGlob matches a file path against a glob pattern supporting `**`, `*`, `?`.
func MatchPathGlob(pattern, filePath string) bool {
	cleanPath := filepath.ToSlash(filepath.Clean(filePath))
	cleanPattern := filepath.ToSlash(pattern)

	// If exact match or wildcard
	if cleanPattern == "" || cleanPattern == "**" || cleanPattern == "*" {
		return true
	}

	// Handle `dir/**` prefix patterns
	if strings.HasSuffix(cleanPattern, "/**") {
		prefix := strings.TrimSuffix(cleanPattern, "/**")
		return cleanPath == prefix || strings.HasPrefix(cleanPath, prefix+"/")
	}

	// Standard glob match
	matched, err := filepath.Match(cleanPattern, cleanPath)
	if err == nil && matched {
		return true
	}

	// Match base file name (e.g. *.go against internal/pkg/foo.go)
	base := filepath.Base(cleanPath)
	matched, err = filepath.Match(cleanPattern, base)
	return err == nil && matched
}
