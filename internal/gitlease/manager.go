// Package gitlease provides a lightweight distributed merge lease coordinator
// for synchronizing multi-workspace and multi-host git pushes and superproject
// gitlink reconciliations (STORY-GOV-3705).
package gitlease

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidRepo     = errors.New("repository name cannot be empty")
	ErrInvalidHolder   = errors.New("holder identifier cannot be empty")
	ErrLeaseNotFound   = errors.New("lease not found or expired")
	ErrLeaseIDMismatch = errors.New("lease id mismatch")
	ErrHolderMismatch  = errors.New("holder mismatch")
)

// Lease represents a distributed repository merge lease.
type Lease struct {
	Repo       string    `json:"repo"`
	Holder     string    `json:"holder"`
	LeaseID    string    `json:"lease_id"`
	TTLSeconds int       `json:"ttl_seconds"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// IsExpired reports whether the lease has passed its expiration time.
func (l *Lease) IsExpired(now time.Time) bool {
	return now.After(l.ExpiresAt)
}

// Manager coordinates concurrent git lease requests across fleet workspaces.
type Manager struct {
	mu     sync.Mutex
	leases map[string]*Lease // normalized repo -> lease
	nowFn  func() time.Time
}

// NewManager creates a new git lease manager.
func NewManager() *Manager {
	return &Manager{
		leases: make(map[string]*Lease),
		nowFn:  time.Now,
	}
}

func normalizeRepo(repo string) string {
	r := strings.TrimSpace(repo)
	r = strings.TrimPrefix(r, "/")
	r = strings.TrimSuffix(r, "/")
	r = strings.TrimSuffix(r, ".git")
	return strings.ToLower(r)
}

func generateLeaseID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b)
}

// Acquire attempts to acquire or renew a lease for repo by holder.
// Returns (lease, acquired, error). If acquired is false, the existing active lease is returned.
func (m *Manager) Acquire(repo, holder string, ttlSeconds int) (*Lease, bool, error) {
	normRepo := normalizeRepo(repo)
	if normRepo == "" {
		return nil, false, ErrInvalidRepo
	}
	holder = strings.TrimSpace(holder)
	if holder == "" {
		return nil, false, ErrInvalidHolder
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 60
	}
	if ttlSeconds > 300 {
		ttlSeconds = 300
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn().UTC()
	cur, exists := m.leases[normRepo]
	if exists && !cur.IsExpired(now) {
		if cur.Holder != holder {
			// Held by another active job/workspace
			return cur, false, nil
		}
		// Held by same holder: refresh lease
		cur.TTLSeconds = ttlSeconds
		cur.ExpiresAt = now.Add(time.Duration(ttlSeconds) * time.Second)
		return cur, true, nil
	}

	// Create new lease
	lease := &Lease{
		Repo:       normRepo,
		Holder:     holder,
		LeaseID:    generateLeaseID(),
		TTLSeconds: ttlSeconds,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(ttlSeconds) * time.Second),
	}
	m.leases[normRepo] = lease
	return lease, true, nil
}

// Renew extends the duration of an active lease if the lease ID and holder match.
func (m *Manager) Renew(repo, holder, leaseID string, ttlSeconds int) (*Lease, bool, error) {
	normRepo := normalizeRepo(repo)
	if normRepo == "" {
		return nil, false, ErrInvalidRepo
	}
	holder = strings.TrimSpace(holder)
	leaseID = strings.TrimSpace(leaseID)

	if ttlSeconds <= 0 {
		ttlSeconds = 60
	}
	if ttlSeconds > 300 {
		ttlSeconds = 300
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn().UTC()
	cur, exists := m.leases[normRepo]
	if !exists || cur.IsExpired(now) {
		return nil, false, ErrLeaseNotFound
	}
	if leaseID != "" && cur.LeaseID != leaseID {
		return nil, false, ErrLeaseIDMismatch
	}
	if holder != "" && cur.Holder != holder {
		return nil, false, ErrHolderMismatch
	}

	cur.TTLSeconds = ttlSeconds
	cur.ExpiresAt = now.Add(time.Duration(ttlSeconds) * time.Second)
	return cur, true, nil
}

// Release releases a held lease if the lease ID (and optional holder) matches.
func (m *Manager) Release(repo, holder, leaseID string) (bool, error) {
	normRepo := normalizeRepo(repo)
	if normRepo == "" {
		return false, ErrInvalidRepo
	}
	leaseID = strings.TrimSpace(leaseID)
	holder = strings.TrimSpace(holder)

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn().UTC()
	cur, exists := m.leases[normRepo]
	if !exists || cur.IsExpired(now) {
		delete(m.leases, normRepo)
		return true, nil
	}
	if leaseID != "" && cur.LeaseID != leaseID {
		return false, ErrLeaseIDMismatch
	}
	if holder != "" && cur.Holder != holder {
		return false, ErrHolderMismatch
	}

	delete(m.leases, normRepo)
	return true, nil
}

// Get returns the active lease for repo, or nil if none is active.
func (m *Manager) Get(repo string) *Lease {
	normRepo := normalizeRepo(repo)
	if normRepo == "" {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn().UTC()
	cur, exists := m.leases[normRepo]
	if !exists || cur.IsExpired(now) {
		return nil
	}
	return cur
}

// List returns all currently active non-expired leases.
func (m *Manager) List() []*Lease {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn().UTC()
	var list []*Lease
	for k, v := range m.leases {
		if v.IsExpired(now) {
			delete(m.leases, k)
		} else {
			list = append(list, v)
		}
	}
	return list
}
