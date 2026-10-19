// Package handlers implements REST HTTP handlers for iazio-harness-api.
package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

const defaultIdempotencyTTL = 24 * time.Hour

type idempotencyEntry struct {
	payloadHash string
	statusCode  int
	response    []byte
	createdAt   time.Time
}

// IdempotencyStore caches responses keyed by Idempotency-Key.
type IdempotencyStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]idempotencyEntry
}

// NewIdempotencyStore creates an in-memory thread-safe idempotency cache.
func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	return &IdempotencyStore{
		ttl:     ttl,
		entries: make(map[string]idempotencyEntry),
	}
}

// Check inspects whether key is cached.
// If key exists and payload hash matches within TTL, returns (cachedResp, statusCode, replay=true, conflict=false).
// If key exists and payload hash differs within TTL, returns (nil, 409, replay=false, conflict=true).
// Otherwise, returns (nil, 0, false, false).
func (s *IdempotencyStore) Check(key, payloadHash string) ([]byte, int, bool, bool) {
	if key == "" {
		return nil, 0, false, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.entries[key]
	if !exists {
		return nil, 0, false, false
	}

	if time.Since(entry.createdAt) > s.ttl {
		delete(s.entries, key)
		return nil, 0, false, false
	}

	if entry.payloadHash != payloadHash {
		return nil, 409, false, true
	}

	return entry.response, entry.statusCode, true, false
}

// Store caches a response for the given key and payload hash.
func (s *IdempotencyStore) Store(key, payloadHash string, statusCode int, response []byte) {
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[key] = idempotencyEntry{
		payloadHash: payloadHash,
		statusCode:  statusCode,
		response:    response,
		createdAt:   time.Now().UTC(),
	}
}

// ComputePayloadHash returns the hex-encoded SHA-256 hash of payload bytes.
func ComputePayloadHash(payload []byte) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
