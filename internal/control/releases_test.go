package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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

	svc.SetCachedRelease("viovy/iazio-agent", "v0.1.0", ToolRelease{Name: "iazio-agent", TagName: "v0.1.0"}, 5*time.Minute)

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

	rel, _, _, err := svc.fetchReleaseURL(context.Background(), ts.URL, "viovy/test", "test", "latest", "secret-token-xyz", "")
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
	svc.SetBaseURL(ts.URL)

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
	svc.SetBaseURL(ts.URL)

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
	svc.SetBaseURL(ts.URL)

	rel, err := svc.GetToolRelease(context.Background(), repo, "stale-tool", targetVersion)
	if err == nil {
		t.Fatalf("expected error when cache is expired and live fetch fails, got release: %+v", rel)
	}
	if !strings.Contains(err.Error(), "lookup release") {
		t.Errorf("expected lookup release error message, got: %v", err)
	}
}

func TestReleaseService_TokenForRepo(t *testing.T) {
	svc := NewReleaseService()
	svc.token = "default-token"
	if tok := svc.tokenForRepo("other/repo"); tok != "default-token" {
		t.Errorf("expected default-token, got %s", tok)
	}

	_, _ = svc.RegisterCredential(GitCredential{ID: "c1", Pattern: "viovy/*", Token: "viovy-token"})
	if tok := svc.tokenForRepo("viovy/app"); tok != "viovy-token" {
		t.Errorf("expected viovy-token, got %s", tok)
	}
	if tok := svc.tokenForRepo("other/app"); tok != "default-token" {
		t.Errorf("expected default-token, got %s", tok)
	}

	_, _ = svc.RegisterCredential(GitCredential{ID: "c2", Pattern: "exact/match", Token: "exact-token"})
	if tok := svc.tokenForRepo("exact/match"); tok != "exact-token" {
		t.Errorf("expected exact-token, got %s", tok)
	}
}

func TestReleaseService_GetCredentialAndTestCredential(t *testing.T) {
	svc := NewReleaseService()
	_, _ = svc.RegisterCredential(GitCredential{ID: "c1", Name: "token", Token: "tok-123"})
	c, ok := svc.GetCredential("c1")
	if !ok || c.Token != "tok-123" {
		t.Fatalf("expected c1, got %v", c)
	}
	_, ok = svc.GetCredential("nonexistent")
	if ok {
		t.Fatalf("expected not found")
	}

	// Test empty token
	res, err := svc.TestCredential(context.Background(), "")
	if err != nil || res.Valid {
		t.Fatalf("expected invalid for empty token")
	}
}

func TestControl_FleetStatusAndCommandsCoverage(t *testing.T) {
	e := NewEngine(time.Now)

	// 1. QueueCommand & ConsumePendingCommands
	e.RegisterHost("host-1", "linux", "generic")
	cmd := CommandEnvelope{ID: "cmd-1", Type: "PING"}
	err := e.QueueCommand("host-1", cmd)
	if err != nil {
		t.Fatalf("queue command: %v", err)
	}
	err = e.QueueCommand("unknown-host", cmd)
	if err == nil {
		t.Fatalf("expected error for unknown host")
	}

	cmds := e.ConsumePendingCommands("host-1")
	if len(cmds) != 1 || cmds[0].ID != "cmd-1" {
		t.Fatalf("expected cmd-1, got %v", cmds)
	}
	cmds2 := e.ConsumePendingCommands("host-1")
	if len(cmds2) != 0 {
		t.Fatalf("expected empty after consume, got %v", cmds2)
	}

	// 2. Ping waiter lifecycle
	ch := e.RegisterPingWaiter("ping-test-1")
	e.NotifyPingAck("ping-test-1", "1.2.3")
	select {
	case ack := <-ch:
		if ack.AgentVersion != "1.2.3" {
			t.Fatalf("expected 1.2.3, got %s", ack.AgentVersion)
		}
	default:
		t.Fatalf("expected ack received")
	}
	e.DeregisterPingWaiter("ping-test-1")

	// 3. RepoStatus waiter lifecycle
	key, statusCh := e.RegisterRepoStatusWaiter("host-1", "/test/repo")
	e.UpdateRepoRuntimeStatus("host-1", []RepoRuntimeStatus{
		{RepoPath: "/test/repo", Branch: "main", IsClean: true},
	})
	e.DeregisterRepoStatusWaiter(key)
	_ = statusCh

	// 4. Tool priority
	e.SetToolPriority([]string{"autopilot", "agy"})
	prios := e.GetToolPriority()
	if len(prios) != 2 {
		t.Fatalf("expected 2 prios, got %v", prios)
	}

	// 5. RefreshProfileReleases
	p := DistributionProfile{
		Name: "test-prof",
		Tools: []ProfileTool{
			{Name: "iazio-agent", Repository: "viovy/iazio-agent"},
		},
	}
	e.Releases.SetCachedRelease("viovy/iazio-agent", "latest", ToolRelease{Name: "iazio-agent", TagName: "v1.0.0"}, time.Hour)
	rels, _ := e.Releases.RefreshProfileReleases(context.Background(), p)
	_ = rels
}

func TestSemVerComparisonAndSorting(t *testing.T) {
	if compareSemVer("0.0.492", "0.0.482") <= 0 {
		t.Errorf("expected 0.0.492 > 0.0.482")
	}
	if compareSemVer("0.0.482", "0.0.492") >= 0 {
		t.Errorf("expected 0.0.482 < 0.0.492")
	}
	if compareSemVer("v0.0.492", "0.0.492") != 0 {
		t.Errorf("expected v0.0.492 == 0.0.492")
	}
	if compareSemVer("1.0.0", "0.9.99") <= 0 {
		t.Errorf("expected 1.0.0 > 0.9.99")
	}
	if compareSemVer("1.0.0", "1.0.0-rc1") <= 0 {
		t.Errorf("expected 1.0.0 > 1.0.0-rc1")
	}
	if CompareSemVer("0.0.492", "0.0.482") <= 0 {
		t.Errorf("expected exported CompareSemVer 0.0.492 > 0.0.482")
	}
}

func TestReleaseService_SemVerSortedReleasesAndTagsFallback(t *testing.T) {
	// Mock server returning releases where 0.0.492 is returned along with 0.0.482 out of order
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/releases/latest"):
			w.WriteHeader(http.StatusNotFound) // Simulate latest release object missing
		case strings.HasSuffix(r.URL.Path, "/releases"):
			// Return releases list where 0.0.482 is listed before 0.0.492 to verify SemVer sorting
			json.NewEncoder(w).Encode([]map[string]any{
				{"tag_name": "0.0.482", "html_url": "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.482", "draft": false},
				{"tag_name": "0.0.492", "html_url": "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492", "draft": false},
				{"tag_name": "0.0.493-draft", "html_url": "draft-url", "draft": true},
			})
		case strings.HasSuffix(r.URL.Path, "/tags"):
			json.NewEncoder(w).Encode([]map[string]any{
				{"name": "0.0.482"},
				{"name": "0.0.492"},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	// Calling GetToolRelease with "latest" should sort SemVer and pick 0.0.492
	rel, err := svc.GetToolRelease(context.Background(), "viovy/autopilot-plugins-internal", "autopilot-fleet", "latest")
	if err != nil {
		t.Fatalf("unexpected error fetching release: %v", err)
	}
	if rel.LatestVersion != "0.0.492" {
		t.Fatalf("expected latest version 0.0.492, got %s", rel.LatestVersion)
	}

	// Profile with old target_version 0.0.482 must resolve LatestVersion as 0.0.492
	p := DistributionProfile{
		Name: "generic",
		Tools: []ProfileTool{
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482"},
		},
	}
	releases, err := svc.GetProfileReleases(context.Background(), p)
	if err != nil {
		t.Fatalf("GetProfileReleases failed: %v", err)
	}
	if len(releases) != 1 {
		t.Fatalf("expected 1 release, got %d", len(releases))
	}
	if releases[0].LatestVersion != "0.0.492" {
		t.Fatalf("expected LatestVersion 0.0.492, got %s", releases[0].LatestVersion)
	}
	if releases[0].TargetVersion != "0.0.482" {
		t.Fatalf("expected TargetVersion 0.0.482, got %s", releases[0].TargetVersion)
	}
}

func TestReleaseService_SyncAllProfiles_AutoAdvanceTargetVersion(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			json.NewEncoder(w).Encode(map[string]any{
				"tag_name":     "0.0.492",
				"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
				"published_at": time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	_, err := e.UpsertProfile(DistributionProfile{
		Name: "generic",
		Tools: []ProfileTool{
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482", TrackLatest: BoolPtr(true)},
		},
	})
	if err != nil {
		t.Fatalf("UpsertProfile failed: %v", err)
	}

	var detectedProfile, detectedTool, detectedOld, detectedNew string
	svc.SyncAllProfiles(context.Background(), e, func(profileName string, tool ProfileTool, oldVer, newVer string) {
		detectedProfile = profileName
		detectedTool = tool.Name
		detectedOld = oldVer
		detectedNew = newVer
	})

	if detectedProfile != "generic" || detectedTool != "autopilot-fleet" {
		t.Fatalf("expected notification for generic autopilot-fleet, got %s/%s", detectedProfile, detectedTool)
	}
	if detectedOld != "0.0.482" || detectedNew != "0.0.492" {
		t.Fatalf("expected version advance 0.0.482 -> 0.0.492, got %s -> %s", detectedOld, detectedNew)
	}

	updatedProfile, _ := e.GetProfile("generic")
	if updatedProfile.Tools[0].TargetVersion != "0.0.492" {
		t.Fatalf("expected TargetVersion updated to 0.0.492, got %s", updatedProfile.Tools[0].TargetVersion)
	}
	if updatedProfile.Tools[0].LatestAvailableVersion != "0.0.492" {
		t.Fatalf("expected LatestAvailableVersion 0.0.492, got %s", updatedProfile.Tools[0].LatestAvailableVersion)
	}
}

func TestReleaseService_ETagConditionalRequestAnd304(t *testing.T) {
	etagHeaderSent := ""
	requestCount := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		etagHeaderSent = r.Header.Get("If-None-Match")

		if etagHeaderSent == `"etag-0.0.492"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("ETag", `"etag-0.0.492"`)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	p := DistributionProfile{
		Name: "generic",
		Tools: []ProfileTool{
			{Name: "autopilot", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "latest"},
		},
	}

	// First fetch: gets 200 and caches ETag
	rel1, err1 := svc.GetProfileReleases(context.Background(), p)
	if err1 != nil || len(rel1) != 1 || rel1[0].LatestVersion != "0.0.492" {
		t.Fatalf("first fetch failed: %v", err1)
	}

	// Second fetch via InvalidateProfileCache (retaining ETag)
	svc.InvalidateProfileCache(p)
	rel2, err2 := svc.GetProfileReleases(context.Background(), p)
	if err2 != nil || len(rel2) != 1 {
		t.Fatalf("second fetch failed: %v", err2)
	}
	if rel2[0].LastSyncStatus != "not_modified" {
		t.Fatalf("expected LastSyncStatus not_modified, got %s", rel2[0].LastSyncStatus)
	}
	if etagHeaderSent != `"etag-0.0.492"` {
		t.Fatalf("expected If-None-Match header %q sent on second request, got %q", `"etag-0.0.492"`, etagHeaderSent)
	}
}

func TestReleaseService_PinnedVersionDoesNotAutoAdvance(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	// Pinned tool: TargetVersion 0.0.482, TrackLatest false
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "pinned-profile",
		Tools: []ProfileTool{
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482", TrackLatest: BoolPtr(false)},
		},
	})

	var notifFired bool
	svc.SyncAllProfiles(context.Background(), e, func(profileName string, tool ProfileTool, oldVer, newVer string) {
		notifFired = true
	})

	if notifFired {
		t.Fatalf("expected no advance notification for pinned tool")
	}

	updated, _ := e.GetProfile("pinned-profile")
	if updated.Tools[0].TargetVersion != "0.0.482" {
		t.Fatalf("expected pinned TargetVersion 0.0.482 preserved, got %s", updated.Tools[0].TargetVersion)
	}
	if updated.Tools[0].LatestAvailableVersion != "0.0.492" {
		t.Fatalf("expected LatestAvailableVersion updated to 0.0.492, got %s", updated.Tools[0].LatestAvailableVersion)
	}
}

func TestReleaseService_FetchErrorRecordsSyncError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "error-profile",
		Tools: []ProfileTool{
			{Name: "missing-tool", Repository: "viovy/missing-tool", TargetVersion: "latest"},
		},
	})

	svc.SyncAllProfiles(context.Background(), e, nil)

	updated, _ := e.GetProfile("error-profile")
	if updated.Tools[0].LastSyncStatus != "error" {
		t.Fatalf("expected LastSyncStatus error, got %s", updated.Tools[0].LastSyncStatus)
	}
	if updated.Tools[0].LastSyncError == "" {
		t.Fatalf("expected LastSyncError populated on fetch failure")
	}
}

func TestReleaseService_ConcurrentListAndSync(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	for i := 0; i < 5; i++ {
		_, _ = e.UpsertProfile(DistributionProfile{
			Name: fmt.Sprintf("profile-%d", i),
			Tools: []ProfileTool{
				{Name: "tool-a", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "latest"},
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				profiles := e.ListProfiles()
				for _, p := range profiles {
					_ = len(p.Tools)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			svc.SyncAllProfiles(ctx, e, nil)
		}
	}()

	wg.Wait()
}

func TestReleaseService_ConsecutiveReleasesAutoAdvanceTracking(t *testing.T) {
	currentVersion := "0.0.492"
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ver := currentVersion
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     ver,
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/" + ver,
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	// Seed profile with implicit latest tracking (empty TargetVersion)
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "tracking-profile",
		Tools: []ProfileTool{
			{Name: "autopilot-plugins-internal", Repository: "viovy/autopilot-plugins-internal", TargetVersion: ""},
		},
	})

	// First sync: advances from "" to 0.0.492
	adv1 := svc.SyncAllProfiles(context.Background(), e, nil)
	if adv1 != 1 {
		t.Fatalf("expected 1 tool advanced on first sync, got %d", adv1)
	}
	p1, _ := e.GetProfile("tracking-profile")
	if p1.Tools[0].TargetVersion != "0.0.492" {
		t.Fatalf("expected target 0.0.492 after first sync, got %s", p1.Tools[0].TargetVersion)
	}
	if p1.Tools[0].TrackLatest == nil || !*p1.Tools[0].TrackLatest {
		t.Fatalf("expected TrackLatest true to be preserved after first advance")
	}

	// Bump upstream release to 0.0.493
	mu.Lock()
	currentVersion = "0.0.493"
	mu.Unlock()

	// Second sync: must auto-advance from 0.0.492 to 0.0.493 (N1 regression guard)
	adv2 := svc.SyncAllProfiles(context.Background(), e, nil)
	if adv2 != 1 {
		t.Fatalf("expected 1 tool advanced on second sync, got %d", adv2)
	}
	p2, _ := e.GetProfile("tracking-profile")
	if p2.Tools[0].TargetVersion != "0.0.493" {
		t.Fatalf("expected target 0.0.493 after second sync, got %s", p2.Tools[0].TargetVersion)
	}
	if p2.Tools[0].TrackLatest == nil || !*p2.Tools[0].TrackLatest {
		t.Fatalf("expected TrackLatest still true after second advance")
	}
}

func TestReleaseService_PrereleasesExcludedFromResolution(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/releases") {
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"tag_name": "0.0.493-rc1", "prerelease": true, "draft": false, "html_url": "url-rc"},
				{"tag_name": "0.0.492", "prerelease": false, "draft": false, "html_url": "url-stable"},
				{"tag_name": "0.0.494-beta", "prerelease": false, "draft": false, "html_url": "url-beta"},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	rel, err := svc.GetToolRelease(context.Background(), "viovy/autopilot-plugins-internal", "autopilot-plugins-internal", "latest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rel.LatestVersion != "0.0.492" {
		t.Fatalf("expected stable release 0.0.492, but got prerelease: %s", rel.LatestVersion)
	}
}

func TestReleaseService_FallbackConditionalETagAnd304(t *testing.T) {
	var requestCount int
	var receivedIfNoneMatch string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/releases") {
			requestCount++
			receivedIfNoneMatch = r.Header.Get("If-None-Match")
			if receivedIfNoneMatch == `"etag-releases-fallback"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `"etag-releases-fallback"`)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"tag_name": "0.0.492", "prerelease": false, "draft": false},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "fallback-profile",
		Tools: []ProfileTool{
			{Name: "test-tool", Repository: "viovy/test-tool", TargetVersion: "latest"},
		},
	})

	// First sync: fetches 200 with ETag
	svc.SyncAllProfiles(context.Background(), e, nil)
	if requestCount != 1 {
		t.Fatalf("expected 1 request to /releases fallback, got %d", requestCount)
	}

	// Second sync: sends If-None-Match and receives 304 Not Modified
	svc.SyncAllProfiles(context.Background(), e, nil)
	if requestCount != 2 {
		t.Fatalf("expected 2 requests, got %d", requestCount)
	}
	if receivedIfNoneMatch != `"etag-releases-fallback"` {
		t.Fatalf("expected If-None-Match header on fallback tier, got %q", receivedIfNoneMatch)
	}
	p, _ := e.GetProfile("fallback-profile")
	if p.Tools[0].LastSyncStatus != "not_modified" && p.Tools[0].LastSyncStatus != "ok" {
		t.Fatalf("expected valid sync status after 304, got %s", p.Tools[0].LastSyncStatus)
	}
}

func TestReleaseService_FallbackChain_ShortCircuitsOnListRateLimit(t *testing.T) {
	var tagsHit bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/releases") {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"rate limit exceeded"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/tags") {
			tagsHit = true
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"name": "0.0.492"},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	rel, err := svc.GetToolRelease(context.Background(), "viovy/test-rate-limit", "test-tool", "latest")
	if err == nil {
		t.Fatalf("expected error on rate limit, got nil")
	}
	if tagsHit {
		t.Fatalf("expected tags fallback NOT to be hit when releases list returned 429 rate limit (F-2)")
	}
	if rel.LastSyncStatus != "rate_limited" {
		t.Fatalf("expected status rate_limited, got %s", rel.LastSyncStatus)
	}
}

func TestReleaseService_FallbackChain_UnsortedSemVerAC1(t *testing.T) {
	unsortedToolReleases := map[string][]string{
		"autopilot-plugins-internal": {"0.0.482", "0.0.492", "0.0.480"},
		"iazio-agent":                {"0.0.60", "0.0.67", "0.0.65"},
		"iazio-harvester":            {"0.0.10", "0.0.98", "0.0.95"},
		"iazio-mcp":                  {"0.0.70", "0.0.79", "0.0.75"},
	}
	expectedVersions := map[string]string{
		"autopilot-plugins-internal": "0.0.492",
		"iazio-agent":                "0.0.67",
		"iazio-harvester":            "0.0.98",
		"iazio-mcp":                  "0.0.79",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/releases") {
			for tool, versions := range unsortedToolReleases {
				if strings.Contains(r.URL.Path, tool) {
					var raw []map[string]any
					for _, v := range versions {
						raw = append(raw, map[string]any{
							"tag_name":     v,
							"prerelease":   false,
							"draft":        false,
							"published_at": "2026-10-24T10:00:00Z",
						})
					}
					_ = json.NewEncoder(w).Encode(raw)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	for tool, expectedVer := range expectedVersions {
		rel, err := svc.GetToolRelease(context.Background(), "viovy/"+tool, tool, "latest")
		if err != nil {
			t.Fatalf("failed to resolve release for %s: %v", tool, err)
		}
		if rel.LatestVersion != expectedVer {
			t.Fatalf("tool %s: expected highest SemVer %s from unsorted list, got %s (AC1)", tool, expectedVer, rel.LatestVersion)
		}
	}
}

func TestReleaseService_MultiToolAC1Fixtures(t *testing.T) {
	fixtureReleases := map[string]string{
		"autopilot-plugins-internal": "0.0.492",
		"iazio-agent":                "0.0.67",
		"iazio-harvester":            "0.0.98",
		"iazio-mcp":                  "0.0.79",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for tool, ver := range fixtureReleases {
			if strings.Contains(r.URL.Path, tool) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"tag_name":     ver,
					"html_url":     "https://github.com/viovy/" + tool + "/releases/tag/" + ver,
					"published_at": "2026-10-24T12:00:00Z",
				})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	for tool, expectedVer := range fixtureReleases {
		rel, err := svc.GetToolRelease(context.Background(), "viovy/"+tool, tool, "latest")
		if err != nil {
			t.Fatalf("failed to resolve release for %s: %v", tool, err)
		}
		if rel.LatestVersion != expectedVer {
			t.Fatalf("expected %s version %s, got %s", tool, expectedVer, rel.LatestVersion)
		}
	}
}

func TestReleaseService_GenericBaselinePinnedVersionPreserved(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	// Generic default profile with an operator pin on a baseline tool (TrackLatest == false)
	_, _ = e.UpsertProfile(DistributionProfile{
		Name:      "generic",
		IsDefault: true,
		Tools: []ProfileTool{
			{
				Name:          "autopilot-plugins-internal",
				Repository:    "viovy/autopilot-plugins-internal",
				Tier:          "baseline",
				TargetVersion: "0.0.480",
				TrackLatest:   BoolPtr(false),
			},
		},
	})

	adv := svc.SyncAllProfiles(context.Background(), e, nil)
	if adv != 0 {
		t.Fatalf("expected 0 tools advanced when pinned with TrackLatest=false, got %d", adv)
	}

	p, err := e.GetProfile("generic")
	if err != nil {
		t.Fatalf("failed to get profile: %v", err)
	}
	if p.Tools[0].TargetVersion != "0.0.480" {
		t.Fatalf("expected operator pin 0.0.480 to be preserved, got %s", p.Tools[0].TargetVersion)
	}
	if p.Tools[0].TrackLatest != nil && *p.Tools[0].TrackLatest {
		t.Fatalf("expected TrackLatest to remain false for operator pin")
	}
	if p.Tools[0].LatestAvailableVersion != "0.0.492" {
		t.Fatalf("expected LatestAvailableVersion to be updated to 0.0.492, got %s", p.Tools[0].LatestAvailableVersion)
	}
}

func TestReleaseService_PerTickRepoDeduplication(t *testing.T) {
	var requestCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "shared-tool") {
			requestCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tag_name":     "1.0.0",
				"html_url":     "https://github.com/viovy/shared-tool/releases/tag/1.0.0",
				"published_at": time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	// Two distinct profiles sharing the same upstream repository
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "profile-a",
		Tools: []ProfileTool{
			{Name: "shared-tool", Repository: "viovy/shared-tool", TargetVersion: "latest"},
		},
	})
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "profile-b",
		Tools: []ProfileTool{
			{Name: "shared-tool", Repository: "viovy/shared-tool", TargetVersion: "latest"},
		},
	})

	svc.SyncAllProfiles(context.Background(), e, nil)
	if requestCount != 1 {
		t.Fatalf("expected exactly 1 upstream request across multiple profiles for shared repo, got %d", requestCount)
	}
}

func TestReleaseService_NegativeCaching404(t *testing.T) {
	var requestCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	// First request: hits upstream 404
	rel1, err1 := svc.GetToolRelease(context.Background(), "viovy/missing-repo", "missing-repo", "latest")
	if err1 == nil {
		t.Fatalf("expected error on first lookup for 404 repo")
	}
	if rel1.Error == "" {
		t.Fatalf("expected rel1.Error populated")
	}
	initialHits := requestCount

	// Second request: served from negative cache, upstream hit count must NOT increase
	rel2, err2 := svc.GetToolRelease(context.Background(), "viovy/missing-repo", "missing-repo", "latest")
	if requestCount != initialHits {
		t.Fatalf("expected negative cache hit with 0 additional upstream requests, got %d", requestCount-initialHits)
	}
	// Cached negative hit MUST return error (F4)
	if err2 == nil {
		t.Fatalf("expected error returned on cached negative hit (F4)")
	}
	if rel2.Error == "" {
		t.Fatalf("expected rel2.Error populated on cached negative hit")
	}

	// Verify error propagation in GetProfileReleases on cached negative hit
	prof := DistributionProfile{
		Name: "missing-prof",
		Tools: []ProfileTool{
			{Name: "missing-tool", Repository: "viovy/missing-repo", TargetVersion: "1.0.0"},
		},
	}
	releases, profErr := svc.GetProfileReleases(context.Background(), prof)
	if profErr == nil {
		t.Fatalf("expected GetProfileReleases to report error on cached negative hit (F4)")
	}
	if len(releases) != 1 || releases[0].LatestVersion != "1.0.0" {
		t.Fatalf("expected fallback to TargetVersion 1.0.0 on error, got %+v", releases)
	}
}

func TestEngine_UpdateProfileTools_PersistFailureDoesNotMutateMemory(t *testing.T) {
	e := NewEngine(time.Now)
	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "persist-fail-profile",
		Tools: []ProfileTool{
			{Name: "tool1", Repository: "viovy/tool1", TargetVersion: "1.0.0"},
		},
	})

	// Configure OnSaveProfile to simulate a storage/database failure
	e.OnSaveProfile = func(p DistributionProfile) error {
		return errors.New("simulated disk storage full")
	}

	_, _, err := e.UpdateProfileTools("persist-fail-profile", func(tools []ProfileTool) ([]ProfileTool, bool) {
		tools[0].TargetVersion = "2.0.0"
		return tools, true
	})
	if err == nil {
		t.Fatalf("expected error from UpdateProfileTools when OnSaveProfile fails")
	}

	// In-memory profile must NOT have been updated to 2.0.0 (E3)
	p, _ := e.GetProfile("persist-fail-profile")
	if p.Tools[0].TargetVersion != "1.0.0" {
		t.Fatalf("expected in-memory TargetVersion to remain 1.0.0 on save error, got %s", p.Tools[0].TargetVersion)
	}
}

func TestEngine_RepoRuntimeStatusTelemetryAndFleetCleanAggregation(t *testing.T) {
	e := NewEngine(time.Now)
	hostID := "host-alpha"
	e.RegisterHost(hostID, "linux", "generic")

	// Update with one dirty repo
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath:            "/workspace/repo-1",
			IsClean:             true,
			Branch:              "main",
			HeadSHA:             "abcdef1234",
			DirtyFilesCount:     0,
			UntrackedFilesCount: 0,
		},
		{
			RepoPath:            "/workspace/repo-2",
			IsClean:             false,
			Branch:              "feature",
			HeadSHA:             "9876543210",
			DirtyFilesCount:     3,
			UntrackedFilesCount: 1,
			Ahead:               2,
			Behind:              0,
			HasActiveGitLock:    false,
		},
	})

	h, ok := e.hosts[hostID]
	if !ok {
		t.Fatalf("host not found")
	}
	if h.FleetGitClean {
		t.Fatalf("expected FleetGitClean to be false when repo-2 is dirty")
	}
	if h.FleetCheckedAt == nil {
		t.Fatalf("expected FleetCheckedAt to be set")
	}
	st2 := h.RepoStatuses[filepath.Clean("/workspace/repo-2")]
	if st2.DirtyFilesCount != 3 || st2.UntrackedFilesCount != 1 || st2.Ahead != 2 {
		t.Fatalf("expected repo-2 telemetry to be ingested, got %+v", st2)
	}

	// Update repo-2 to clean
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath:            "/workspace/repo-2",
			IsClean:             true,
			Branch:              "feature",
			HeadSHA:             "9876543210",
			DirtyFilesCount:     0,
			UntrackedFilesCount: 0,
		},
	})

	hClean := e.hosts[hostID]
	if !hClean.FleetGitClean {
		t.Fatalf("expected FleetGitClean to be true when all repos are clean")
	}
}

func TestReleaseService_LegacyProfileMissingTrackLatestAutoAdvances(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	// Simulate legacy JSON row from DB with NO track_latest field and stale target 0.0.482
	legacyJSON := []byte(`{
		"name": "generic",
		"is_default": true,
		"tools": [
			{
				"name": "autopilot-plugins-internal",
				"repository": "viovy/autopilot-plugins-internal",
				"tier": "baseline",
				"target_version": "0.0.482"
			},
			{
				"name": "pinned-tool",
				"repository": "viovy/autopilot-plugins-internal",
				"tier": "baseline",
				"target_version": "0.0.480",
				"track_latest": false
			}
		]
	}`)
	var legacyProfile DistributionProfile
	if err := json.Unmarshal(legacyJSON, &legacyProfile); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	// Verify legacy row tool has TrackLatest == nil
	if legacyProfile.Tools[0].TrackLatest != nil {
		t.Fatalf("expected legacy tool 0 to decode with nil TrackLatest, got %v", *legacyProfile.Tools[0].TrackLatest)
	}
	// Verify explicit pin tool has TrackLatest != nil and false
	if legacyProfile.Tools[1].TrackLatest == nil || *legacyProfile.Tools[1].TrackLatest != false {
		t.Fatalf("expected explicit pin tool 1 to decode with TrackLatest=false")
	}

	_, _ = e.UpsertProfile(legacyProfile)

	adv := svc.SyncAllProfiles(context.Background(), e, nil)
	if adv != 1 {
		t.Fatalf("expected 1 legacy tool to auto-advance, got %d", adv)
	}

	p, err := e.GetProfile("generic")
	if err != nil {
		t.Fatalf("failed to get profile: %v", err)
	}
	// Legacy baseline tool: target must advance to 0.0.492 (R2)
	if p.Tools[0].TargetVersion != "0.0.492" {
		t.Fatalf("expected legacy tool to advance to 0.0.492, got %s", p.Tools[0].TargetVersion)
	}
	if p.Tools[0].TrackLatest == nil || !*p.Tools[0].TrackLatest {
		t.Fatalf("expected legacy tool to migrate TrackLatest to true")
	}

	// Explicit pinned tool: target must remain 0.0.480
	if p.Tools[1].TargetVersion != "0.0.480" {
		t.Fatalf("expected pinned tool to stay at 0.0.480, got %s", p.Tools[1].TargetVersion)
	}
	if p.Tools[1].TrackLatest == nil || *p.Tools[1].TrackLatest != false {
		t.Fatalf("expected pinned tool to remain TrackLatest=false")
	}
}

func TestEngine_RepoRuntimeStatus_MultiRepoBatchAggregation(t *testing.T) {
	e := NewEngine(time.Now)
	hostID := "host-gamma"
	e.RegisterHost(hostID, "linux", "generic")

	// Post 1: repo-1 is dirty
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath: "/workspace/repo-1",
			IsClean:  false,
		},
	})
	if e.hosts[hostID].FleetGitClean {
		t.Fatalf("expected FleetGitClean=false after repo-1 is dirty")
	}

	// Post 2: single-repo batch for repo-2 which is clean.
	// Must NOT flip host to clean because repo-1 remains dirty in inventory (R5)!
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath: "/workspace/repo-2",
			IsClean:  true,
		},
	})
	if e.hosts[hostID].FleetGitClean {
		t.Fatalf("expected FleetGitClean to REMAIN false when repo-1 is dirty and single clean repo-2 is reported (R5)")
	}

	// Post 3: repo-1 reports clean -> host flips to clean
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath: "/workspace/repo-1",
			IsClean:  true,
		},
	})
	if !e.hosts[hostID].FleetGitClean {
		t.Fatalf("expected FleetGitClean=true after all repos become clean")
	}
}

func TestEngine_HostHarnessActiveReconciliation(t *testing.T) {
	e := NewEngine(time.Now)
	hostID := "host-delta"
	e.RegisterHost(hostID, "linux", "generic")

	// Heartbeat with active jobs
	_ = e.HeartbeatDetailed(hostID, "ONLINE", nil, false, ActiveJob{JobID: "job-123"})
	if !e.hosts[hostID].HarnessActive {
		t.Fatalf("expected HarnessActive=true when active jobs are present")
	}

	// Heartbeat with no active jobs
	_ = e.HeartbeatDetailed(hostID, "ONLINE", nil, false)
	if e.hosts[hostID].HarnessActive {
		t.Fatalf("expected HarnessActive=false when active jobs are cleared")
	}
}

func TestReleaseService_SyncAllProfiles_ErrorPropagation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)
	e.Releases = svc

	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "err-profile",
		Tools: []ProfileTool{
			{Name: "err-tool", Repository: "viovy/err-tool", TargetVersion: "latest"},
		},
	})

	res := svc.SyncAllProfilesDetailed(context.Background(), e, nil)
	if len(res.Errors) == 0 {
		t.Fatalf("expected error recorded for err-profile in SyncAllProfilesDetailed (R7)")
	}
	if res.TotalAdvanced != 0 {
		t.Fatalf("expected 0 tools advanced on error, got %d", res.TotalAdvanced)
	}
}

func TestEngine_UpsertProfile_PreservesOperatorPinAndTelemetryWhenNil(t *testing.T) {
	e := NewEngine(time.Now)
	checkTime := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	// Initial profile creation with operator pin (track_latest=false) and telemetry
	_, err := e.UpsertProfile(DistributionProfile{
		Name: "pinned-service",
		Tools: []ProfileTool{
			{
				Name:                   "iazio-mcp",
				Repository:             "viovy/iazio-mcp",
				TargetVersion:          "0.0.95",
				TrackLatest:            BoolPtr(false),
				LatestAvailableVersion: "0.0.98",
				LatestReleaseURL:       "https://github.com/viovy/iazio-mcp/releases/tag/v0.0.98",
				LastCheckedAt:          &checkTime,
				LastSyncStatus:         "ok",
			},
		},
	})
	if err != nil {
		t.Fatalf("initial upsert failed: %v", err)
	}

	// Web UI or client submits an update with TrackLatest=nil (omitted in request body) and empty telemetry
	updated, err := e.UpsertProfile(DistributionProfile{
		Name:        "pinned-service",
		Description: "Updated description via web UI",
		Tools: []ProfileTool{
			{
				Name:          "iazio-mcp",
				Repository:    "viovy/iazio-mcp",
				TargetVersion: "0.0.95",
				TrackLatest:   nil, // omitted in web edit
			},
		},
	})
	if err != nil {
		t.Fatalf("secondary upsert failed: %v", err)
	}

	// Verify TrackLatest remains explicitly false (F1)
	if updated.Tools[0].TrackLatest == nil || *updated.Tools[0].TrackLatest != false {
		t.Fatalf("expected operator pin (TrackLatest=false) to be preserved, got %+v", updated.Tools[0].TrackLatest)
	}
	// Verify telemetry was preserved
	if updated.Tools[0].LatestAvailableVersion != "0.0.98" {
		t.Fatalf("expected LatestAvailableVersion=0.0.98 preserved, got %s", updated.Tools[0].LatestAvailableVersion)
	}
	if updated.Tools[0].LastSyncStatus != "ok" {
		t.Fatalf("expected LastSyncStatus=ok preserved, got %s", updated.Tools[0].LastSyncStatus)
	}
}

func TestEngine_RepoRuntimeStatus_StaleRepoPruning(t *testing.T) {
	currTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	e := NewEngine(func() time.Time { return currTime })
	hostID := "host-stale-test"
	e.RegisterHost(hostID, "linux", "generic")

	// Post 1: repo-1 is dirty at t=0
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath:      "/workspace/stale-dirty-repo",
			IsClean:       false,
			LastCheckedAt: currTime,
		},
	})
	if e.hosts[hostID].FleetGitClean {
		t.Fatalf("expected host to be dirty initially")
	}

	// Advance time by 15 minutes (> 10m StaleRepoThreshold)
	currTime = currTime.Add(15 * time.Minute)

	// Post 2: repo-2 is clean at t+15m
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath:      "/workspace/active-clean-repo",
			IsClean:       true,
			LastCheckedAt: currTime,
		},
	})

	// The stale dirty repo must have been pruned from h.RepoStatuses,
	// and the host FleetGitClean should now be true!
	h := e.hosts[hostID]
	if _, exists := h.RepoStatuses["/workspace/stale-dirty-repo"]; exists {
		t.Fatalf("expected stale dirty repo to be pruned from RepoStatuses (F7)")
	}
	if !h.FleetGitClean {
		t.Fatalf("expected FleetGitClean=true after stale dirty repo pruned (F7)")
	}
}

func TestReleaseService_RateLimitBackoffAndNoFormalReleasesSkipping(t *testing.T) {
	var latestCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		latestCalls++
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	currTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	svc := NewReleaseService()
	svc.now = func() time.Time { return currTime }
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	// 1. Initial call receives 403 with Retry-After 120s
	_, err := svc.GetToolRelease(context.Background(), "viovy/rate-limited-repo", "rate-tool", "latest")
	if err == nil {
		t.Fatalf("expected error from rate limited response")
	}
	initialCalls := latestCalls

	// 2. Immediate second call within backoff window must fail fast without HTTP hit
	_, err = svc.GetToolRelease(context.Background(), "viovy/rate-limited-repo", "rate-tool", "latest")
	if err == nil || !strings.Contains(err.Error(), "rate limit backoff active") {
		t.Fatalf("expected rate limit backoff error, got: %v", err)
	}
	if latestCalls != initialCalls {
		t.Fatalf("expected 0 HTTP requests during rate limit backoff window, got %d", latestCalls-initialCalls)
	}

	// 3. Test tags-only repo skipping /releases/latest after 404
	var tagsRepoLatestCalls int
	var tagsRepoTagsCalls int
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/releases/latest"):
			tagsRepoLatestCalls++
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/releases"):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/tags"):
			tagsRepoTagsCalls++
			w.Header().Set("ETag", `"tag-etag-2"`)
			w.Write([]byte(`[{"name": "v0.5.0"}]`))
		}
	}))
	defer ts2.Close()

	svc2 := NewReleaseService()
	svc2.now = func() time.Time { return currTime }
	svc2.client = ts2.Client()
	svc2.SetBaseURL(ts2.URL)

	// Pass 1: hits /releases/latest (404), falls back to tags (200)
	rel, err := svc2.GetToolRelease(context.Background(), "viovy/tags-only-repo", "tags-tool", "latest")
	if err != nil {
		t.Fatalf("expected success on tags fallback: %v", err)
	}
	if rel.LatestVersion != "0.5.0" {
		t.Fatalf("expected 0.5.0 from tags, got %s", rel.LatestVersion)
	}
	if tagsRepoLatestCalls != 1 {
		t.Fatalf("expected 1 call to /releases/latest on first pass")
	}

	// Expire cache TTL to simulate next poll tick
	svc2.InvalidateProfileCache(DistributionProfile{
		Tools: []ProfileTool{{Name: "tags-tool", Repository: "viovy/tags-only-repo"}},
	})

	// Pass 2: should skip /releases/latest because noFormalReleases is true (F5)
	_, err = svc2.GetToolRelease(context.Background(), "viovy/tags-only-repo", "tags-tool", "latest")
	if err != nil {
		t.Fatalf("expected success on second pass: %v", err)
	}
	if tagsRepoLatestCalls != 1 {
		t.Fatalf("expected /releases/latest to be skipped on second pass, but was called %d times (F5)", tagsRepoLatestCalls)
	}
}

func TestEngine_RepoRuntimeStatus_EmptyInventoryUnknown(t *testing.T) {
	e := NewEngine(time.Now)
	hostID := "host-empty-test"
	e.RegisterHost(hostID, "linux", "generic")

	// Post empty inventory
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{})

	h := e.hosts[hostID]
	if h.FleetCheckedAt != nil {
		t.Fatalf("expected FleetCheckedAt=nil when inventory is empty (G1)")
	}
	if h.FleetGitClean {
		t.Fatalf("expected FleetGitClean=false (unknown) when inventory is empty (G1)")
	}
}

func TestEngine_RepoRuntimeStatus_ClockSkewImmunity(t *testing.T) {
	currTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	e := NewEngine(func() time.Time { return currTime })
	hostID := "host-skew-test"
	e.RegisterHost(hostID, "linux", "generic")

	// Agent clock is 1 hour in the past (severe clock skew)
	agentSkewedTime := currTime.Add(-1 * time.Hour)
	e.UpdateRepoRuntimeStatus(hostID, []RepoRuntimeStatus{
		{
			RepoPath:      "/workspace/skewed-repo",
			IsClean:       true,
			LastCheckedAt: agentSkewedTime,
		},
	})

	h := e.hosts[hostID]
	if len(h.RepoStatuses) != 1 {
		t.Fatalf("expected skewed repo NOT to be pruned immediately (G1)")
	}
	if !h.FleetGitClean {
		t.Fatalf("expected FleetGitClean=true for clean repo despite agent clock skew")
	}
	if h.FleetCheckedAt == nil {
		t.Fatalf("expected FleetCheckedAt to be set by server receive time")
	}
}

func TestEngine_UpdateProfileTools_ConcurrentUpsertConsistency(t *testing.T) {
	e := NewEngine(time.Now)
	var persistedVersion uint64
	var saveMu sync.Mutex
	e.OnSaveProfile = func(p DistributionProfile) error {
		saveMu.Lock()
		persistedVersion = p.VersionSeq
		saveMu.Unlock()
		return nil
	}

	_, _ = e.UpsertProfile(DistributionProfile{
		Name: "race-profile",
		Tools: []ProfileTool{
			{Name: "tool1", Repository: "viovy/tool1", TargetVersion: "1.0.0"},
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			_, _, _ = e.UpdateProfileTools("race-profile", func(tools []ProfileTool) ([]ProfileTool, bool) {
				tools[0].TargetVersion = fmt.Sprintf("1.0.%d", idx)
				return tools, true
			})
		}(i)
		go func(idx int) {
			defer wg.Done()
			_, _ = e.UpsertProfile(DistributionProfile{
				Name:        "race-profile",
				Description: fmt.Sprintf("edit-%d", idx),
				Tools: []ProfileTool{
					{Name: "tool1", Repository: "viovy/tool1", TargetVersion: "1.0.0"},
				},
			})
		}(i)
	}
	wg.Wait()

	p, _ := e.GetProfile("race-profile")
	saveMu.Lock()
	lastSaved := persistedVersion
	saveMu.Unlock()

	if p.VersionSeq != lastSaved {
		t.Fatalf("expected in-memory VersionSeq (%d) to equal persisted VersionSeq (%d) (G3)", p.VersionSeq, lastSaved)
	}
}

func TestEngine_LoadProfile_PreservesTimestampsAndNoSave(t *testing.T) {
	e := NewEngine(time.Now)
	saved := false
	e.OnSaveProfile = func(p DistributionProfile) error {
		saved = true
		return nil
	}

	origCreated := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	origUpdated := time.Date(2025, 2, 1, 12, 0, 0, 0, time.UTC)
	e.LoadProfile(DistributionProfile{
		Name:        "loaded-profile",
		Description: "Loaded from DB",
		CreatedAt:   origCreated,
		UpdatedAt:   origUpdated,
		Tools: []ProfileTool{
			{Name: "tool1", Repository: "viovy/tool1"},
		},
	})

	if saved {
		t.Fatalf("expected LoadProfile not to invoke OnSaveProfile at startup (E2)")
	}

	p, err := e.GetProfile("loaded-profile")
	if err != nil {
		t.Fatalf("failed to get loaded profile: %v", err)
	}
	if !p.CreatedAt.Equal(origCreated) || !p.UpdatedAt.Equal(origUpdated) {
		t.Fatalf("expected CreatedAt and UpdatedAt to be preserved: got CreatedAt=%v, UpdatedAt=%v", p.CreatedAt, p.UpdatedAt)
	}
}

func TestReleaseService_ForbiddenShortCircuitsFallbacks(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusForbidden) // 403 without rate limit headers
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	rel, err := svc.GetToolRelease(context.Background(), "viovy/forbidden-repo", "forbidden-tool", "latest")
	if err == nil {
		t.Fatal("expected error on forbidden repo")
	}
	if rel.LastSyncStatus != "forbidden" {
		t.Fatalf("expected LastSyncStatus='forbidden', got %q", rel.LastSyncStatus)
	}
	// Crucial: should NOT fall back to /releases and /tags (should be exactly 1 call)
	if calls != 1 {
		t.Fatalf("expected forbidden error to short-circuit fallbacks (1 call), but made %d calls (E7)", calls)
	}
}

func TestReleaseService_BackoffNeverShortened(t *testing.T) {
	svc := NewReleaseService()
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }

	h1 := make(http.Header)
	h1.Set("Retry-After", "300") // 5 minutes = 12:05
	d1 := parseRateLimitBackoff(h1, now)
	if d1 != 300*time.Second {
		t.Fatalf("expected 300s, got %v", d1)
	}

	svc.mu.Lock()
	svc.rateLimits["repo-a"] = now.Add(d1)
	svc.globalRateLimitUntil = now.Add(d1)
	svc.mu.Unlock()

	// Shorter Retry-After arrives
	h2 := make(http.Header)
	h2.Set("Retry-After", "60") // 1 minute = 12:01
	d2 := parseRateLimitBackoff(h2, now)
	expiry2 := now.Add(d2)

	svc.mu.Lock()
	if expiry2.After(svc.rateLimits["repo-a"]) {
		svc.rateLimits["repo-a"] = expiry2
	}
	if expiry2.After(svc.globalRateLimitUntil) {
		svc.globalRateLimitUntil = expiry2
	}
	activeRepo := svc.rateLimits["repo-a"]
	activeGlobal := svc.globalRateLimitUntil
	svc.mu.Unlock()

	if !activeRepo.Equal(now.Add(300 * time.Second)) {
		t.Fatalf("expected rate limit for repo-a not to be shortened by smaller Retry-After (E7)")
	}
	if !activeGlobal.Equal(now.Add(300 * time.Second)) {
		t.Fatalf("expected global rate limit not to be shortened by smaller Retry-After (E7)")
	}
}

func TestReleaseService_SyncProfile_LegacyPinMigration(t *testing.T) {
	// H4, A1: Verify that a legacy pinned optional-tier tool in the default profile
	// is migrated to TrackLatest=true and its TargetVersion is auto-advanced to upstream latest,
	// while a legacy pinned tool in a custom profile preserves its pin and TrackLatest remains nil.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name": "v2.0.0",
			"html_url": "https://github.com/viovy/opt-tool/releases/tag/v2.0.0",
		})
	}))
	defer ts.Close()

	svc := NewReleaseService()
	svc.client = ts.Client()
	svc.SetBaseURL(ts.URL)

	e := NewEngine(time.Now)

	// 1. Register default profile "generic" with legacy pinned optional tool (TrackLatest == nil)
	_, err := e.UpsertProfile(DistributionProfile{
		Name:      "generic",
		IsDefault: true,
		Tools: []ProfileTool{
			{
				Name:          "opt-tool",
				Repository:    "viovy/opt-tool",
				BinaryName:    "opt-tool",
				TargetVersion: "1.0.0",
				Tier:          "optional",
				TrackLatest:   nil, // legacy un-migrated row
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to upsert generic profile: %v", err)
	}

	// 2. Register custom non-default profile with same legacy pinned optional tool
	_, err = e.UpsertProfile(DistributionProfile{
		Name:      "custom-env",
		IsDefault: false,
		Tools: []ProfileTool{
			{
				Name:          "opt-tool",
				Repository:    "viovy/opt-tool",
				BinaryName:    "opt-tool",
				TargetVersion: "1.0.0",
				Tier:          "optional",
				TrackLatest:   nil, // legacy pin in custom profile
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to upsert custom-env profile: %v", err)
	}

	// 3. Sync default profile
	n, err := svc.SyncProfile(context.Background(), e, "generic", nil)
	if err != nil {
		t.Fatalf("unexpected error syncing generic profile: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 tool auto-advanced in default profile, got %d", n)
	}

	pGen, err := e.GetProfile("generic")
	if err != nil {
		t.Fatalf("failed to get generic profile: %v", err)
	}
	if len(pGen.Tools) != 1 {
		t.Fatalf("expected 1 tool in generic profile, got %d", len(pGen.Tools))
	}
	toolGen := pGen.Tools[0]
	if toolGen.TargetVersion != "2.0.0" {
		t.Fatalf("expected default profile tool TargetVersion to advance to 2.0.0, got %s", toolGen.TargetVersion)
	}
	if toolGen.TrackLatest == nil || !*toolGen.TrackLatest {
		t.Fatalf("expected default profile tool TrackLatest to be migrated to true, got %v", toolGen.TrackLatest)
	}

	// 4. Sync custom profile
	nCustom, err := svc.SyncProfile(context.Background(), e, "custom-env", nil)
	if err != nil {
		t.Fatalf("unexpected error syncing custom profile: %v", err)
	}
	if nCustom != 0 {
		t.Fatalf("expected 0 tools auto-advanced in custom profile, got %d", nCustom)
	}

	pCust, err := e.GetProfile("custom-env")
	if err != nil {
		t.Fatalf("failed to get custom-env profile: %v", err)
	}
	toolCust := pCust.Tools[0]
	if toolCust.TargetVersion != "1.0.0" {
		t.Fatalf("expected custom profile tool TargetVersion to remain pinned at 1.0.0, got %s", toolCust.TargetVersion)
	}
	if toolCust.TrackLatest != nil {
		t.Fatalf("expected custom profile tool TrackLatest to remain nil, got %v", toolCust.TrackLatest)
	}
}
