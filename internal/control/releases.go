package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ToolRelease represents the latest release information for a tool.
type ToolRelease struct {
	Name          string    `json:"name"`
	Repository    string    `json:"repository"`
	LatestVersion string    `json:"latest_version"`
	TagName       string    `json:"tag_name"`
	PublishedAt   time.Time `json:"published_at,omitempty"`
	HTMLURL       string    `json:"html_url,omitempty"`
	Error         string    `json:"error,omitempty"`
}

type cachedRelease struct {
	release   ToolRelease
	expiresAt time.Time
}

// GitCredential stores authentication tokens for private repository release lookups.
type GitCredential struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Pattern   string    `json:"pattern"` // e.g. "viovy/*" or "viovy/autopilot" or "*"
	Token     string    `json:"token"`
	Type      string    `json:"type"` // "pat", "token"
	CreatedAt time.Time `json:"created_at"`
}

// GitCredentialStore persists git credentials to external storage.
type GitCredentialStore interface {
	SaveGitCredential(ctx context.Context, cred GitCredential) error
	DeleteGitCredential(ctx context.Context, id string) error
	LoadGitCredentials(ctx context.Context) ([]GitCredential, error)
}

// ReleaseService queries and caches latest Git release versions for profile tools.
type ReleaseService struct {
	mu          sync.RWMutex
	cache       map[string]cachedRelease
	credentials map[string]GitCredential
	client      *http.Client
	now         func() time.Time
	token       string
	store       GitCredentialStore
}

// NewReleaseService creates a ReleaseService with in-memory caching.
func NewReleaseService() *ReleaseService {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		token = os.Getenv("GH_TOKEN")
	}
	if token == "" {
		if out, err := exec.Command("gh", "auth", "token").Output(); err == nil {
			token = strings.TrimSpace(string(out))
		}
	}
	return &ReleaseService{
		cache:       make(map[string]cachedRelease),
		credentials: make(map[string]GitCredential),
		client:      &http.Client{Timeout: 10 * time.Second},
		now:         time.Now,
		token:       token,
	}
}

// SetStore attaches a persistence store and loads existing credentials into memory.
func (s *ReleaseService) SetStore(st GitCredentialStore) error {
	s.mu.Lock()
	s.store = st
	s.mu.Unlock()
	if st != nil {
		creds, err := st.LoadGitCredentials(context.Background())
		if err != nil {
			return fmt.Errorf("load git credentials: %w", err)
		}
		s.mu.Lock()
		for _, c := range creds {
			s.credentials[c.ID] = c
		}
		s.mu.Unlock()
	}
	return nil
}

// RegisterCredential records a Git credential for authenticating with repositories.
func (s *ReleaseService) RegisterCredential(cred GitCredential) (GitCredential, error) {
	if cred.ID == "" {
		cred.ID = fmt.Sprintf("cred-%d", s.now().UnixNano())
	}
	if cred.CreatedAt.IsZero() {
		cred.CreatedAt = s.now()
	}
	s.mu.RLock()
	st := s.store
	s.mu.RUnlock()
	if st != nil {
		if err := st.SaveGitCredential(context.Background(), cred); err != nil {
			return GitCredential{}, fmt.Errorf("save git credential %s: %w", cred.ID, err)
		}
	}
	s.mu.Lock()
	s.credentials[cred.ID] = cred
	s.mu.Unlock()
	return cred, nil
}

// ListCredentials returns registered Git credentials with tokens masked.
func (s *ReleaseService) ListCredentials() []GitCredential {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]GitCredential, 0, len(s.credentials))
	for _, c := range s.credentials {
		masked := c
		if len(masked.Token) > 6 {
			masked.Token = masked.Token[:3] + "•••" + masked.Token[len(masked.Token)-3:]
		} else {
			masked.Token = "••••••"
		}
		out = append(out, masked)
	}
	return out
}

// DeleteCredential removes a registered credential by ID.
func (s *ReleaseService) DeleteCredential(id string) error {
	s.mu.Lock()
	st := s.store
	_, ok := s.credentials[id]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	s.mu.Unlock()

	if st != nil {
		if err := st.DeleteGitCredential(context.Background(), id); err != nil {
			return fmt.Errorf("delete git credential %s: %w", id, err)
		}
	}

	s.mu.Lock()
	delete(s.credentials, id)
	s.mu.Unlock()
	return nil
}

// GitCredentialTestResult contains the verification metrics from GitHub API.
type GitCredentialTestResult struct {
	Valid              bool     `json:"valid"`
	Username           string   `json:"username,omitempty"`
	Scopes             []string `json:"scopes,omitempty"`
	RateLimitRemaining int      `json:"rate_limit_remaining"`
	LatencyMs          int64    `json:"latency_ms"`
	Error              string   `json:"error,omitempty"`
}

// GetCredential returns a registered credential including unmasked token for testing.
func (s *ReleaseService) GetCredential(id string) (GitCredential, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.credentials[id]
	return c, ok
}

// TestCredential validates a token against GitHub /user and /rate_limit endpoints.
func (s *ReleaseService) TestCredential(ctx context.Context, token string) (GitCredentialTestResult, error) {
	start := s.now()
	cleanToken := strings.TrimSpace(token)
	if cleanToken == "" {
		return GitCredentialTestResult{Valid: false, Error: "token is empty"}, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		return GitCredentialTestResult{Valid: false, Error: err.Error()}, nil
	}
	req.Header.Set("Authorization", "Bearer "+cleanToken)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")

	resp, err := s.client.Do(req)
	latency := s.now().Sub(start).Milliseconds()
	if err != nil {
		return GitCredentialTestResult{Valid: false, LatencyMs: latency, Error: err.Error()}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return GitCredentialTestResult{
			Valid:     false,
			LatencyMs: latency,
			Error:     fmt.Sprintf("GitHub authentication failed: HTTP %d", resp.StatusCode),
		}, nil
	}

	var userPayload struct {
		Login string `json:"login"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&userPayload)

	scopesHeader := resp.Header.Get("X-OAuth-Scopes")
	var scopes []string
	if scopesHeader != "" {
		for _, sc := range strings.Split(scopesHeader, ",") {
			if trimmed := strings.TrimSpace(sc); trimmed != "" {
				scopes = append(scopes, trimmed)
			}
		}
	}

	rateLimitRemaining := 0
	if rem := resp.Header.Get("X-RateLimit-Remaining"); rem != "" {
		if n, err := strconv.Atoi(rem); err == nil {
			rateLimitRemaining = n
		}
	}

	return GitCredentialTestResult{
		Valid:              true,
		Username:           userPayload.Login,
		Scopes:             scopes,
		RateLimitRemaining: rateLimitRemaining,
		LatencyMs:          latency,
	}, nil
}

func (s *ReleaseService) tokenForRepo(repo string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := strings.ToLower(repo)
	for _, c := range s.credentials {
		pat := strings.ToLower(c.Pattern)
		if pat == "*" || pat == r || (strings.HasSuffix(pat, "*") && strings.HasPrefix(r, strings.TrimSuffix(pat, "*"))) {
			if c.Token != "" {
				return c.Token
			}
		}
	}
	if s.token != "" {
		return s.token
	}
	if tok := os.Getenv("GITHUB_TOKEN"); tok != "" {
		return tok
	}
	if tok := os.Getenv("GH_TOKEN"); tok != "" {
		return tok
	}
	return ""
}

// GetToolRelease returns the latest release for a repository.
func (s *ReleaseService) GetToolRelease(ctx context.Context, repo, toolName, targetVersion string) (ToolRelease, error) {
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}
	if targetVersion == "" {
		targetVersion = "latest"
	}

	cacheKey := repo + ":" + targetVersion
	s.mu.RLock()
	cached, ok := s.cache[cacheKey]
	s.mu.RUnlock()

	now := s.now()
	if ok && now.Before(cached.expiresAt) {
		r := cached.release
		r.Name = toolName
		return r, nil
	}

	token := s.tokenForRepo(repo)

	// Try releases/latest first
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", repo)
	if targetVersion != "" && targetVersion != "latest" {
		tag := targetVersion
		if !strings.HasPrefix(tag, "v") {
			tag = "v" + tag
		}
		url = fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)
	}

	var lastErr error
	rel, err := s.fetchReleaseURL(ctx, url, repo, toolName, targetVersion, token)
	if err == nil && rel.LatestVersion != "" {
		s.mu.Lock()
		s.cache[cacheKey] = cachedRelease{
			release:   rel,
			expiresAt: now.Add(10 * time.Minute),
		}
		s.mu.Unlock()
		return rel, nil
	}
	if err != nil {
		lastErr = err
	}

	// Fallback 1: check list of all releases (e.g. if latest flag not set)
	if targetVersion == "" || targetVersion == "latest" {
		listURL := fmt.Sprintf("https://api.github.com/repos/%s/releases?per_page=1", repo)
		listRel, listErr := s.fetchReleaseListURL(ctx, listURL, repo, toolName, token)
		if listErr == nil && listRel.LatestVersion != "" {
			s.mu.Lock()
			s.cache[cacheKey] = cachedRelease{
				release:   listRel,
				expiresAt: now.Add(10 * time.Minute),
			}
			s.mu.Unlock()
			return listRel, nil
		}
		if listErr != nil {
			lastErr = listErr
		}

		// Fallback 2: check repository tags
		tagsURL := fmt.Sprintf("https://api.github.com/repos/%s/tags?per_page=1", repo)
		tagRel, tagErr := s.fetchTagsURL(ctx, tagsURL, repo, toolName, token)
		if tagErr == nil && tagRel.LatestVersion != "" {
			s.mu.Lock()
			s.cache[cacheKey] = cachedRelease{
				release:   tagRel,
				expiresAt: now.Add(10 * time.Minute),
			}
			s.mu.Unlock()
			return tagRel, nil
		}
		if tagErr != nil {
			lastErr = tagErr
		}
	}

	if lastErr != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, fmt.Errorf("lookup release for %s: %w", repo, lastErr)
	}
	return ToolRelease{Name: toolName, Repository: repo}, fmt.Errorf("no releases or tags found for repository %s", repo)
}

func (s *ReleaseService) fetchReleaseURL(ctx context.Context, url, repo, toolName, targetVersion, token string) (ToolRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, fmt.Errorf("status %d", resp.StatusCode)
	}

	var ghRel struct {
		TagName     string    `json:"tag_name"`
		HTMLURL     string    `json:"html_url"`
		PublishedAt time.Time `json:"published_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ghRel); err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, err
	}

	ver := strings.TrimPrefix(ghRel.TagName, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       ghRel.TagName,
		PublishedAt:   ghRel.PublishedAt,
		HTMLURL:       ghRel.HTMLURL,
	}, nil
}

func (s *ReleaseService) fetchReleaseListURL(ctx context.Context, url, repo, toolName, token string) (ToolRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo}, fmt.Errorf("status %d", resp.StatusCode)
	}

	var list []struct {
		TagName     string    `json:"tag_name"`
		HTMLURL     string    `json:"html_url"`
		PublishedAt time.Time `json:"published_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || len(list) == 0 {
		return ToolRelease{Name: toolName, Repository: repo}, fmt.Errorf("no releases")
	}

	ver := strings.TrimPrefix(list[0].TagName, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       list[0].TagName,
		PublishedAt:   list[0].PublishedAt,
		HTMLURL:       list[0].HTMLURL,
	}, nil
}

func (s *ReleaseService) fetchTagsURL(ctx context.Context, url, repo, toolName, token string) (ToolRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo}, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tags []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil || len(tags) == 0 {
		return ToolRelease{Name: toolName, Repository: repo}, fmt.Errorf("no tags")
	}

	ver := strings.TrimPrefix(tags[0].Name, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       tags[0].Name,
		HTMLURL:       fmt.Sprintf("https://github.com/%s/releases/tag/%s", repo, tags[0].Name),
	}, nil
}

// GetProfileReleases queries latest releases for all tools in a profile.
func (s *ReleaseService) GetProfileReleases(ctx context.Context, p DistributionProfile) ([]ToolRelease, error) {
	out := make([]ToolRelease, 0, len(p.Tools))
	var errs []string
	for _, t := range p.Tools {
		repo := t.Repository
		if repo == "" {
			repo = "viovy/" + t.Name
		}
		rel, err := s.GetToolRelease(ctx, repo, t.Name, t.TargetVersion)
		if err != nil {
			rel.Error = err.Error()
			errs = append(errs, fmt.Sprintf("%s (%s): %v", t.Name, repo, err))
		}
		out = append(out, rel)
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("failed to fetch releases for %d tools: %s", len(errs), strings.Join(errs, "; "))
	}
	return out, nil
}

// RefreshProfileReleases purges cached releases for all tools in a profile and fetches fresh releases.
func (s *ReleaseService) RefreshProfileReleases(ctx context.Context, p DistributionProfile) ([]ToolRelease, error) {
	s.mu.Lock()
	for _, t := range p.Tools {
		repo := t.Repository
		if repo == "" {
			repo = "viovy/" + t.Name
		}
		if !strings.Contains(repo, "/") {
			repo = "viovy/" + repo
		}
		targetVersion := t.TargetVersion
		if targetVersion == "" {
			targetVersion = "latest"
		}
		delete(s.cache, repo+":"+targetVersion)
		delete(s.cache, repo+":latest")
	}
	s.mu.Unlock()
	return s.GetProfileReleases(ctx, p)
}

// SetCachedRelease manually injects a cached release (useful for testing and deterministic offline mocking).
func (s *ReleaseService) SetCachedRelease(repo, version string, release ToolRelease, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	if version == "" {
		version = "latest"
	}
	s.cache[repo+":"+version] = cachedRelease{
		release:   release,
		expiresAt: s.now().Add(ttl),
	}
}
