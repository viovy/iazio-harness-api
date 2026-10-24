package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482", TrackLatest: true},
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
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482", TrackLatest: false},
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
	if !p1.Tools[0].TrackLatest {
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
	if !p2.Tools[0].TrackLatest {
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
