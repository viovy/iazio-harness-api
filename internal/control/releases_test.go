package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type mockGitCredentialStore struct {
	creds map[string]GitCredential
}

func newMockGitCredentialStore() *mockGitCredentialStore {
	return &mockGitCredentialStore{creds: make(map[string]GitCredential)}
}

func (m *mockGitCredentialStore) SaveGitCredential(_ context.Context, cred GitCredential) error {
	m.creds[cred.ID] = cred
	return nil
}

func (m *mockGitCredentialStore) DeleteGitCredential(_ context.Context, id string) error {
	delete(m.creds, id)
	return nil
}

func (m *mockGitCredentialStore) LoadGitCredentials(_ context.Context) ([]GitCredential, error) {
	out := make([]GitCredential, 0, len(m.creds))
	for _, c := range m.creds {
		out = append(out, c)
	}
	return out, nil
}

func TestReleaseService_GitCredentialsPersistence(t *testing.T) {
	svc := NewReleaseService()
	store := newMockGitCredentialStore()
	svc.SetStore(store)

	cred, err := svc.RegisterCredential(GitCredential{
		ID:      "cred-1",
		Name:    "GitHub Personal Token",
		Pattern: "viovy/*",
		Token:   "ghp_test1234567890",
		Type:    "pat",
	})
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	if cred.ID != "cred-1" {
		t.Fatalf("expected ID cred-1, got %s", cred.ID)
	}
	if store.creds["cred-1"].Token != "ghp_test1234567890" {
		t.Fatalf("expected token stored in store")
	}

	// Token matching
	if tok := svc.tokenForRepo("viovy/autopilot"); tok != "ghp_test1234567890" {
		t.Errorf("expected token for viovy/autopilot, got %s", tok)
	}
	if tok := svc.tokenForRepo("other/repo"); tok == "ghp_test1234567890" {
		t.Errorf("did not expect token for other/repo")
	}

	// List masked
	list := svc.ListCredentials()
	if len(list) != 1 {
		t.Fatalf("expected 1 credential in list, got %d", len(list))
	}
	if list[0].Token == "ghp_test1234567890" {
		t.Errorf("expected masked token in ListCredentials")
	}

	// Re-load in a new service instance via SetStore
	svc2 := NewReleaseService()
	if err := svc2.SetStore(store); err != nil {
		t.Fatalf("unexpected SetStore error: %v", err)
	}
	if tok := svc2.tokenForRepo("viovy/autopilot"); tok != "ghp_test1234567890" {
		t.Errorf("expected token restored from store in svc2, got %s", tok)
	}

	// Delete
	if err := svc.DeleteCredential("cred-1"); err != nil {
		t.Fatalf("expected delete to succeed, got %v", err)
	}
	if _, exists := store.creds["cred-1"]; exists {
		t.Errorf("expected cred-1 deleted from store")
	}
}

func TestReleaseService_AuthenticatedFetch(t *testing.T) {
	var receivedAuthHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tag_name": "v1.5.0", "html_url": "https://github.com/viovy/test/releases/tag/v1.5.0"}`))
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()

	rel, err := svc.fetchReleaseURL(context.Background(), ts.URL, "viovy/test", "test", "latest", "secret-token-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rel.LatestVersion != "1.5.0" {
		t.Errorf("expected latest version 1.5.0, got %s", rel.LatestVersion)
	}
	if receivedAuthHeader != "Bearer secret-token-xyz" {
		t.Errorf("expected Bearer secret-token-xyz, got %s", receivedAuthHeader)
	}
}

type errCredentialStore struct{}

func (e *errCredentialStore) SaveGitCredential(ctx context.Context, cred GitCredential) error {
	return errors.New("db save error")
}
func (e *errCredentialStore) DeleteGitCredential(ctx context.Context, id string) error {
	return errors.New("db delete error")
}
func (e *errCredentialStore) LoadGitCredentials(ctx context.Context) ([]GitCredential, error) {
	return nil, errors.New("db load error")
}

func TestReleaseService_StoreFailure(t *testing.T) {
	svc := NewReleaseService()
	errStore := &errCredentialStore{}

	if err := svc.SetStore(errStore); err == nil {
		t.Fatalf("expected error from SetStore with errStore")
	}

	_, err := svc.RegisterCredential(GitCredential{ID: "fail-cred", Name: "Test", Token: "abc"})
	if err == nil || !strings.Contains(err.Error(), "db save error") {
		t.Fatalf("expected db save error, got %v", err)
	}

	// Pre-populate memory to test delete failure
	svc.credentials["fail-cred"] = GitCredential{ID: "fail-cred"}
	err = svc.DeleteCredential("fail-cred")
	if err == nil || !strings.Contains(err.Error(), "db delete error") {
		t.Fatalf("expected db delete error, got %v", err)
	}
}

func TestReleaseService_EmptyReleaseError(t *testing.T) {
	svc := NewReleaseService()
	// Mock client that returns 404
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	svc.client = ts.Client()

	_, err := svc.GetToolRelease(context.Background(), "repo-none", "tool-none", "")
	if err == nil {
		t.Fatalf("expected error when no release found and targetVersion is empty")
	}

	// Pinned targetVersion must also fail closed if no release or tag is found on GitHub
	_, err = svc.GetToolRelease(context.Background(), "repo-none", "tool-none", "1.0.0")
	if err == nil {
		t.Fatalf("expected error when no release found even if targetVersion is pinned")
	}
}

func TestReleaseService_GetProfileReleases_FailureSurfaced(t *testing.T) {
	svc := NewReleaseService()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	svc.client = ts.Client()

	profile := DistributionProfile{
		Name: "test-profile",
		Tools: []ProfileTool{
			{Name: "my-private-tool", Repository: "viovy/my-private-tool", TargetVersion: "1.2.0"},
		},
	}

	releases, err := svc.GetProfileReleases(context.Background(), profile)
	if err == nil {
		t.Fatalf("expected error from GetProfileReleases when tool lookup fails")
	}
	if len(releases) != 1 {
		t.Fatalf("expected 1 release entry returned, got %d", len(releases))
	}
	if releases[0].Error == "" {
		t.Fatalf("expected non-empty Error on release entry")
	}
}

func TestReleaseService_ExpiredCacheFailsClosed(t *testing.T) {
	svc := NewReleaseService()
	// Pre-populate cache with an expired entry
	fixedTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time {
		return fixedTime
	}

	repo := "viovy/stale-tool"
	targetVersion := "1.0.0"
	cacheKey := repo + ":" + targetVersion
	svc.cache[cacheKey] = cachedRelease{
		release: ToolRelease{
			Name:          "stale-tool",
			Repository:    repo,
			LatestVersion: "0.9.0",
		},
		expiresAt: fixedTime.Add(-1 * time.Minute), // Expired 1 minute ago
	}

	// Mock server that returns 500 error simulating network or credential error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	svc.client = ts.Client()

	rel, err := svc.GetToolRelease(context.Background(), repo, "stale-tool", targetVersion)
	if err == nil {
		t.Fatalf("expected error when cache is expired and live fetch fails, got release: %+v", rel)
	}
	if !strings.Contains(err.Error(), "lookup release") {
		t.Errorf("expected lookup release error message, got: %v", err)
	}
}
