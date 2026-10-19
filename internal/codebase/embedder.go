// Package codebase provides fleet repository discovery, hybrid RAG chunk stores, and embedding services.
package codebase

import (
	"context"
)

// Embedder defines the interface for generating semantic vector embeddings for code or query text.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float64, error)
}

// DeterministicEmbedder generates deterministic, normalized vector embeddings locally using lexical tokens and n-grams.
type DeterministicEmbedder struct{}

// NewDeterministicEmbedder constructs a new DeterministicEmbedder.
func NewDeterministicEmbedder() *DeterministicEmbedder {
	return &DeterministicEmbedder{}
}

// Embed generates deterministic vector embeddings for the provided texts.
func (e *DeterministicEmbedder) Embed(ctx context.Context, texts []string) ([][]float64, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	out := make([][]float64, len(texts))
	for i, text := range texts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			out[i] = GenerateEmbedding(text)
		}
	}
	return out, nil
}

// CountingEmbedder wraps an Embedder to count invocations and computed embeddings, useful for testing and metric tracking.
type CountingEmbedder struct {
	Inner           Embedder
	InvocationCount int
	TotalTextsCount int
}

// NewCountingEmbedder wraps an existing embedder with counter tracking.
func NewCountingEmbedder(inner Embedder) *CountingEmbedder {
	if inner == nil {
		inner = NewDeterministicEmbedder()
	}
	return &CountingEmbedder{Inner: inner}
}

// Embed delegates to the underlying embedder and tracks invocation counts.
func (c *CountingEmbedder) Embed(ctx context.Context, texts []string) ([][]float64, error) {
	c.InvocationCount++
	c.TotalTextsCount += len(texts)
	return c.Inner.Embed(ctx, texts)
}
