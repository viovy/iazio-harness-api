package codebase

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const (
	// DefaultRRFConstant is the smoothing constant k in 1/(k + rank).
	DefaultRRFConstant = 60.0
	// ExactMatchBoost is the 1.25x boost for exact symbol or filename matches.
	ExactMatchBoost = 1.25
	// EmbeddingDimension is the default vector length for in-memory semantic representations.
	EmbeddingDimension = 128
)

var (
	goFuncRegex  = regexp.MustCompile(`(?m)^func\s+(?:\([^)]+\)\s+)?([A-Za-z0-9_]+)`)
	goTypeRegex  = regexp.MustCompile(`(?m)^type\s+([A-Za-z0-9_]+)\s+(?:struct|interface)`)
	pyFuncRegex  = regexp.MustCompile(`(?m)^\s*def\s+([A-Za-z0-9_]+)`)
	pyClassRegex = regexp.MustCompile(`(?m)^\s*class\s+([A-Za-z0-9_]+)`)
	jsFuncRegex  = regexp.MustCompile(`(?m)^\s*(?:export\s+)?(?:async\s+)?function\s+([A-Za-z0-9_]+)`)
	jsClassRegex = regexp.MustCompile(`(?m)^\s*(?:export\s+)?class\s+([A-Za-z0-9_]+)`)
)

// CalculateGitBlobSHA returns the 40-character hexadecimal Git blob SHA-1 for the given content.
// This matches `git hash-object` canonical format: sha1("blob <size>\0<content>").
func CalculateGitBlobSHA(content []byte) string {
	h := sha1.New()
	header := fmt.Sprintf("blob %d\x00", len(content))
	h.Write([]byte(header))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// NormalizeFilePath converts Windows separators, trims leading './' or '/', and returns a clean POSIX path.
func NormalizeFilePath(p string) string {
	clean := strings.ReplaceAll(p, "\\", "/")
	clean = filepath.ToSlash(filepath.Clean(clean))
	clean = strings.TrimPrefix(clean, "./")
	clean = strings.TrimPrefix(clean, "/")
	return clean
}

// ChunkSourceFile segments file content into semantic CodeChunks with line numbers and Git blob SHA.
func ChunkSourceFile(repoName, filePath, gitTreeSHA string, content []byte) []CodeChunk {
	normPath := NormalizeFilePath(filePath)
	blobSHA := CalculateGitBlobSHA(content)

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil
	}

	const maxChunkLines = 100
	const minChunkLines = 20

	var chunks []CodeChunk
	totalLines := len(lines)

	// Single small file: return as one chunk
	if totalLines <= maxChunkLines {
		symbol := detectPrimarySymbol(lines)
		chunk := CodeChunk{
			ID:          fmt.Sprintf("%s:%s:1-%d", repoName, normPath, totalLines),
			RepoName:    repoName,
			FilePath:    normPath,
			StartLine:   1,
			EndLine:     totalLines,
			Symbol:      symbol,
			Content:     string(content),
			FileBlobSHA: blobSHA,
			GitTreeSHA:  gitTreeSHA,
			Embedding:   GenerateEmbedding(string(content)),
		}
		return []CodeChunk{chunk}
	}

	// Multi-chunk sliding window
	start := 1
	for start <= totalLines {
		end := start + maxChunkLines - 1
		if end > totalLines {
			end = totalLines
		}

		chunkLines := lines[start-1 : end]
		chunkContent := strings.Join(chunkLines, "\n")
		symbol := detectPrimarySymbol(chunkLines)

		chunk := CodeChunk{
			ID:          fmt.Sprintf("%s:%s:%d-%d", repoName, normPath, start, end),
			RepoName:    repoName,
			FilePath:    normPath,
			StartLine:   start,
			EndLine:     end,
			Symbol:      symbol,
			Content:     chunkContent,
			FileBlobSHA: blobSHA,
			GitTreeSHA:  gitTreeSHA,
			Embedding:   GenerateEmbedding(chunkContent),
		}
		chunks = append(chunks, chunk)

		// Next start with minor overlap if not at end
		if end >= totalLines {
			break
		}
		start = end - 10
		if start < 1 {
			start = end + 1
		}
	}

	return chunks
}

func detectPrimarySymbol(lines []string) string {
	block := strings.Join(lines, "\n")
	if m := goFuncRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	if m := goTypeRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	if m := pyFuncRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	if m := pyClassRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	if m := jsFuncRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	if m := jsClassRegex.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	return ""
}

// GenerateEmbedding creates a deterministic, normalized vector embedding from text tokens.
func GenerateEmbedding(text string) []float64 {
	vec := make([]float64, EmbeddingDimension)
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return vec
	}

	for _, token := range tokens {
		h := fnv1a(token)
		dim := int(h % uint64(EmbeddingDimension))
		weight := 1.0 + float64(len(token))*0.1
		vec[dim] += weight

		// Subword 3-grams
		if len(token) >= 3 {
			for i := 0; i <= len(token)-3; i++ {
				ngram := token[i : i+3]
				nh := fnv1a(ngram)
				ndim := int(nh % uint64(EmbeddingDimension))
				vec[ndim] += 0.3
			}
		}
	}

	// L2 normalization
	norm := 0.0
	for _, val := range vec {
		norm += val * val
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range vec {
			vec[i] /= norm
		}
	}

	return vec
}

func fnv1a(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// CosineSimilarity computes the cosine similarity between two unit vectors.
func CosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}
	dot := 0.0
	for i := 0; i < len(a); i++ {
		dot += a[i] * b[i]
	}
	if dot < 0 {
		return 0.0
	}
	return dot
}

// LexicalScore computes token overlap and symbol matching score between query and chunk.
func LexicalScore(query string, chunk CodeChunk) float64 {
	queryTokens := tokenize(query)
	if len(queryTokens) == 0 {
		return 0.0
	}

	contentTokens := tokenize(chunk.Content)
	symbolTokens := tokenize(chunk.Symbol)
	pathTokens := tokenize(chunk.FilePath)

	contentMap := make(map[string]int)
	for _, t := range contentTokens {
		contentMap[t]++
	}

	symbolMap := make(map[string]bool)
	for _, t := range symbolTokens {
		symbolMap[t] = true
	}

	pathMap := make(map[string]bool)
	for _, t := range pathTokens {
		pathMap[t] = true
	}

	score := 0.0
	for _, qt := range queryTokens {
		// Content occurrences
		if count, ok := contentMap[qt]; ok {
			score += 1.0 + math.Log(1.0+float64(count))
		}
		// Symbol match bonus
		if symbolMap[qt] {
			score += 3.0
		}
		// Path match bonus
		if pathMap[qt] {
			score += 2.0
		}
	}

	return score
}

func tokenize(text string) []string {
	var tokens []string
	var buf strings.Builder

	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			buf.WriteRune(r)
		} else {
			if buf.Len() > 0 {
				tokens = append(tokens, buf.String())
				buf.Reset()
			}
		}
	}
	if buf.Len() > 0 {
		tokens = append(tokens, buf.String())
	}
	return tokens
}

type rankItem struct {
	index int
	score float64
}

// RankChunksRRF ranks chunks using Reciprocal Rank Fusion of vector and lexical scores,
// with a 1.25x boost for exact symbol or filename matches.
func RankChunksRRF(chunks []CodeChunk, query string, queryEmbedding []float64) []CodeChunk {
	n := len(chunks)
	if n == 0 {
		return nil
	}

	// 1. Vector ranking
	vecItems := make([]rankItem, n)
	for i, c := range chunks {
		emb := c.Embedding
		if len(emb) == 0 {
			emb = GenerateEmbedding(c.Content)
		}
		sim := CosineSimilarity(queryEmbedding, emb)
		vecItems[i] = rankItem{index: i, score: sim}
	}
	sort.Slice(vecItems, func(i, j int) bool {
		return vecItems[i].score > vecItems[j].score
	})

	vecRanks := make([]int, n)
	for rank, item := range vecItems {
		vecRanks[item.index] = rank + 1
	}

	// 2. Lexical ranking
	lexItems := make([]rankItem, n)
	for i, c := range chunks {
		lex := LexicalScore(query, c)
		lexItems[i] = rankItem{index: i, score: lex}
	}
	sort.Slice(lexItems, func(i, j int) bool {
		return lexItems[i].score > lexItems[j].score
	})

	lexRanks := make([]int, n)
	for rank, item := range lexItems {
		lexRanks[item.index] = rank + 1
	}

	// 3. Combine with RRF and 1.25x boost
	cleanQuery := strings.ToLower(strings.TrimSpace(query))
	results := make([]CodeChunk, n)

	for i := 0; i < n; i++ {
		rVec := float64(vecRanks[i])
		rLex := float64(lexRanks[i])

		rrfScore := (1.0 / (DefaultRRFConstant + rVec)) + (1.0 / (DefaultRRFConstant + rLex))

		c := chunks[i]
		// Check exact symbol or filename match
		isExactSymbol := c.Symbol != "" && strings.EqualFold(c.Symbol, cleanQuery)
		isExactFilename := strings.EqualFold(filepath.Base(c.FilePath), cleanQuery) ||
			strings.EqualFold(strings.TrimSuffix(filepath.Base(c.FilePath), filepath.Ext(c.FilePath)), cleanQuery)

		if isExactSymbol || isExactFilename {
			rrfScore *= ExactMatchBoost
		}

		c.Score = math.Round(rrfScore*100000) / 100000 // 5 decimal places
		results[i] = c
	}

	// Sort final results by Score DESC, then FilePath ASC, StartLine ASC
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			if results[i].FilePath == results[j].FilePath {
				return results[i].StartLine < results[j].StartLine
			}
			return results[i].FilePath < results[j].FilePath
		}
		return results[i].Score > results[j].Score
	})

	return results
}

// IndexRepositoryFiles chunks and indexes a set of files for a repository, pruning obsolete chunks.
func IndexRepositoryFiles(ctx context.Context, store Store, repoName, gitTreeSHA string, files map[string][]byte) error {
	var allChunks []CodeChunk

	for filePath, content := range files {
		chunks := ChunkSourceFile(repoName, filePath, gitTreeSHA, content)
		allChunks = append(allChunks, chunks...)
	}

	if err := store.UpsertChunks(ctx, allChunks); err != nil {
		return fmt.Errorf("upsert chunks: %w", err)
	}

	if _, err := store.PruneStaleChunks(ctx, repoName, gitTreeSHA); err != nil {
		return fmt.Errorf("prune stale chunks: %w", err)
	}

	return nil
}
