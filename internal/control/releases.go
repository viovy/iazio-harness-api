package control

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ToolRelease represents the latest release information for a tool.
type ToolRelease struct {
	Name           string    `json:"name"`
	Repository     string    `json:"repository"`
	LatestVersion  string    `json:"latest_version"`
	TargetVersion  string    `json:"target_version,omitempty"`
	TagName        string    `json:"tag_name"`
	PublishedAt    time.Time `json:"published_at,omitempty"`
	HTMLURL        string    `json:"html_url,omitempty"`
	LastCheckedAt  time.Time `json:"last_checked_at,omitempty"`
	LastSyncStatus string    `json:"last_sync_status,omitempty"`
	Error          string    `json:"error,omitempty"`
}

type semverTuple struct {
	Major      int
	Minor      int
	Patch      int
	PreRelease string
	Raw        string
}

func parseSemVer(tag string) (semverTuple, bool) {
	clean := strings.TrimSpace(tag)
	clean = strings.TrimPrefix(clean, "v")
	clean = strings.TrimPrefix(clean, "V")
	if clean == "" {
		return semverTuple{}, false
	}
	parts := strings.SplitN(clean, "-", 2)
	nums := strings.Split(parts[0], ".")
	if len(nums) < 2 {
		return semverTuple{}, false
	}
	major, err1 := strconv.Atoi(nums[0])
	minor, err2 := strconv.Atoi(nums[1])
	patch := 0
	var err3 error
	if len(nums) >= 3 {
		patch, err3 = strconv.Atoi(nums[2])
	}
	if err1 != nil || err2 != nil || err3 != nil {
		return semverTuple{}, false
	}
	pre := ""
	if len(parts) > 1 {
		pre = parts[1]
	}
	return semverTuple{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: pre,
		Raw:        clean,
	}, true
}

func compareSemVer(v1, v2 string) int {
	s1, ok1 := parseSemVer(v1)
	s2, ok2 := parseSemVer(v2)
	if !ok1 && !ok2 {
		return strings.Compare(v1, v2)
	}
	if !ok1 {
		return -1
	}
	if !ok2 {
		return 1
	}
	if s1.Major != s2.Major {
		return s1.Major - s2.Major
	}
	if s1.Minor != s2.Minor {
		return s1.Minor - s2.Minor
	}
	if s1.Patch != s2.Patch {
		return s1.Patch - s2.Patch
	}
	if s1.PreRelease == "" && s2.PreRelease != "" {
		return 1
	}
	if s1.PreRelease != "" && s2.PreRelease == "" {
		return -1
	}
	return strings.Compare(s1.PreRelease, s2.PreRelease)
}

// CompareSemVer compares two SemVer strings, returning >0 if v1 > v2, <0 if v1 < v2, and 0 if equal.
func CompareSemVer(v1, v2 string) int {
	return compareSemVer(v1, v2)
}

type cachedRelease struct {
	release   ToolRelease
	etag      string
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
	mu                   sync.RWMutex
	cache                map[string]cachedRelease
	endpointETags        map[string]string    // URL -> ETag mapping (prevents cross-endpoint ETag contamination, F5)
	rateLimits           map[string]time.Time // repo -> backoff expiry time (F5)
	globalRateLimitUntil time.Time            // shared backoff expiry time across repos (G6)
	noFormalReleases     map[string]time.Time // repo -> expiry time of 404 cache for /releases/latest (G6)
	credentials          map[string]GitCredential
	client               *http.Client
	now                  func() time.Time
	token                string
	store                GitCredentialStore
	baseURL              string
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
		cache:            make(map[string]cachedRelease),
		endpointETags:    make(map[string]string),
		rateLimits:       make(map[string]time.Time),
		noFormalReleases: make(map[string]time.Time),
		credentials:      make(map[string]GitCredential),
		client:           &http.Client{Timeout: 10 * time.Second},
		now:              time.Now,
		token:            token,
		baseURL:          "https://api.github.com",
	}
}

// SetBaseURL overrides the GitHub API base URL (useful for testing against mock servers).
func (s *ReleaseService) SetBaseURL(u string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseURL = strings.TrimSuffix(u, "/")
}

// SetHTTPClient overrides the HTTP client (useful for testing with mock servers).
func (s *ReleaseService) SetHTTPClient(c *http.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = c
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

	// Query /rate_limit endpoint to confirm full rate limits
	if rlReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/rate_limit", nil); err == nil {
		rlReq.Header.Set("Authorization", "Bearer "+cleanToken)
		rlReq.Header.Set("Accept", "application/vnd.github.v3+json")
		rlReq.Header.Set("User-Agent", "iazio-harness-api/1.0")
		if rlResp, err := s.client.Do(rlReq); err == nil {
			defer rlResp.Body.Close()
			var rlPayload struct {
				Rate struct {
					Remaining int `json:"remaining"`
				} `json:"rate"`
			}
			if json.NewDecoder(rlResp.Body).Decode(&rlPayload) == nil && rlPayload.Rate.Remaining > 0 {
				rateLimitRemaining = rlPayload.Rate.Remaining
			}
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

var (
	warnedReposMu sync.Mutex
	warnedRepos   = make(map[string]bool)
)

func warnUnauthenticatedOnce(repo string) {
	warnedReposMu.Lock()
	defer warnedReposMu.Unlock()
	if !warnedRepos[repo] {
		warnedRepos[repo] = true
		log.Printf("[ReleaseService] WARN: unauthenticated GitHub release requests for %s (rate limit: 60 req/hr). Set GITHUB_TOKEN or configure git credentials.", repo)
	}
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
	warnUnauthenticatedOnce(repo)
	return ""
}

func parseRateLimitBackoff(header http.Header, now time.Time) time.Duration {
	if retryAfter := header.Get("Retry-After"); retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	if reset := header.Get("X-RateLimit-Reset"); reset != "" {
		if unixSec, err := strconv.ParseInt(reset, 10, 64); err == nil {
			t := time.Unix(unixSec, 0)
			if t.After(now) {
				return t.Sub(now)
			}
		}
	}
	return 5 * time.Minute // default backoff
}

// GetToolRelease returns the latest release for a repository.
func (s *ReleaseService) GetToolRelease(ctx context.Context, repo, toolName, targetVersion string) (ToolRelease, error) {
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}

	isLatestLookup := targetVersion == "" || targetVersion == "latest"

	cacheKey := repo + ":" + targetVersion
	if isLatestLookup {
		cacheKey = repo + ":latest"
	}

	now := s.now()

	// Check per-repo and global rate limit backoff (F5, G6)
	s.mu.RLock()
	backoffUntil, isRateLimited := s.rateLimits[repo]
	if !isRateLimited || backoffUntil.Before(s.globalRateLimitUntil) {
		backoffUntil = s.globalRateLimitUntil
		if now.Before(backoffUntil) {
			isRateLimited = true
		}
	}
	s.mu.RUnlock()
	if isRateLimited && now.Before(backoffUntil) {
		err := fmt.Errorf("rate limit backoff active for %s until %s", repo, backoffUntil.Format(time.RFC3339))
		fallbackVer := targetVersion
		if fallbackVer == "" || fallbackVer == "latest" {
			fallbackVer = "0.0.0"
		}
		return ToolRelease{
			Name:           toolName,
			Repository:     repo,
			LatestVersion:  fallbackVer,
			TargetVersion:  targetVersion,
			TagName:        fallbackVer,
			LastCheckedAt:  now,
			LastSyncStatus: "rate_limited",
			Error:          err.Error(),
		}, err
	}

	s.mu.RLock()
	cached, ok := s.cache[cacheKey]
	s.mu.RUnlock()

	if ok && now.Before(cached.expiresAt) {
		r := cached.release
		r.Name = toolName
		if !isLatestLookup {
			r.TargetVersion = targetVersion
		}
		// Treat cached negative results as errors (F4)
		if r.Error != "" {
			return r, fmt.Errorf("lookup release for %s: %s", repo, r.Error)
		}
		return r, nil
	}

	token := s.tokenForRepo(repo)
	base := s.baseURL
	if base == "" {
		base = "https://api.github.com"
	}

	candidateURLs := make([]string, 0, 3)
	if !isLatestLookup {
		tag := targetVersion
		bareTag := strings.TrimPrefix(tag, "v")
		vTag := "v" + bareTag
		// Check both "v"-prefixed and bare tag names
		candidateURLs = append(candidateURLs,
			fmt.Sprintf("%s/repos/%s/releases/tags/%s", base, repo, vTag),
			fmt.Sprintf("%s/repos/%s/releases/tags/%s", base, repo, bareTag),
		)
	} else {
		s.mu.RLock()
		skipUntil := s.noFormalReleases[repo]
		skipLatest := !skipUntil.IsZero() && now.Before(skipUntil)
		s.mu.RUnlock()
		if !skipLatest {
			candidateURLs = append(candidateURLs, fmt.Sprintf("%s/repos/%s/releases/latest", base, repo))
		}
	}

	var lastErr error
	for _, candURL := range candidateURLs {
		s.mu.RLock()
		urlEtag := s.endpointETags[candURL]
		s.mu.RUnlock()

		rel, newEtag, notModified, err := s.fetchReleaseURL(ctx, candURL, repo, toolName, targetVersion, token, urlEtag)
		if notModified && ok {
			s.mu.Lock()
			cached.expiresAt = now.Add(10 * time.Minute)
			s.cache[cacheKey] = cached
			s.mu.Unlock()
			r := cached.release
			r.Name = toolName
			r.LastCheckedAt = now
			r.LastSyncStatus = "not_modified"
			if !isLatestLookup {
				r.TargetVersion = targetVersion
			}
			return r, nil
		}
		if newEtag != "" {
			s.mu.Lock()
			s.endpointETags[candURL] = newEtag
			s.mu.Unlock()
		}
		if err == nil && rel.LatestVersion != "" {
			// SemVer ordering guard: never downgrade LatestAvailableVersion if an existing cached release has higher SemVer (G7)
			if ok && cached.release.LatestVersion != "" && compareSemVer(rel.LatestVersion, cached.release.LatestVersion) < 0 {
				rel.LatestVersion = cached.release.LatestVersion
				rel.TagName = cached.release.TagName
				rel.HTMLURL = cached.release.HTMLURL
				rel.PublishedAt = cached.release.PublishedAt
			}
			rel.LastCheckedAt = now
			rel.LastSyncStatus = "ok"
			if !isLatestLookup {
				rel.TargetVersion = targetVersion
			}
			s.mu.Lock()
			s.cache[cacheKey] = cachedRelease{
				release:   rel,
				etag:      newEtag,
				expiresAt: now.Add(10 * time.Minute),
			}
			if isLatestLookup {
				s.cache[repo+":latest"] = s.cache[cacheKey]
			}
			s.mu.Unlock()
			return rel, nil
		}
		if err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "rate limited") {
				// Stop immediately on rate limit without spending quota on list or tags (G6)
				fallbackVer := targetVersion
				if fallbackVer == "" || fallbackVer == "latest" {
					fallbackVer = "0.0.0"
				}
				return ToolRelease{
					Name:           toolName,
					Repository:     repo,
					LatestVersion:  fallbackVer,
					TargetVersion:  targetVersion,
					TagName:        fallbackVer,
					LastCheckedAt:  now,
					LastSyncStatus: "rate_limited",
					Error:          err.Error(),
				}, err
			}
		}
	}

	// Fallback 1: check list of all releases with SemVer sorting
	listKey := repo + ":releases_list"
	s.mu.RLock()
	cachedList, okList := s.cache[listKey]
	s.mu.RUnlock()

	listURL := fmt.Sprintf("%s/repos/%s/releases?per_page=15", base, repo)
	s.mu.RLock()
	listEtag := s.endpointETags[listURL]
	s.mu.RUnlock()

	listRel, listNewEtag, listNotMod, listErr := s.fetchReleaseListURL(ctx, listURL, repo, toolName, token, listEtag)
	if listNewEtag != "" {
		s.mu.Lock()
		s.endpointETags[listURL] = listNewEtag
		s.mu.Unlock()
	}
	if listNotMod && okList {
		s.mu.Lock()
		cachedList.expiresAt = now.Add(10 * time.Minute)
		s.cache[listKey] = cachedList
		s.cache[cacheKey] = cachedList
		s.mu.Unlock()
		r := cachedList.release
		r.Name = toolName
		r.LastCheckedAt = now
		r.LastSyncStatus = "not_modified"
		if !isLatestLookup {
			r.TargetVersion = targetVersion
		}
		return r, nil
	}
	if listErr == nil && listRel.LatestVersion != "" {
		listRel.LastCheckedAt = now
		listRel.LastSyncStatus = "ok"
		if !isLatestLookup {
			listRel.TargetVersion = targetVersion
		}
		s.mu.Lock()
		s.cache[listKey] = cachedRelease{
			release:   listRel,
			etag:      listNewEtag,
			expiresAt: now.Add(10 * time.Minute),
		}
		s.cache[cacheKey] = s.cache[listKey]
		if isLatestLookup {
			s.cache[repo+":latest"] = s.cache[listKey]
		}
		s.mu.Unlock()
		return listRel, nil
	}
	if listErr != nil {
		lastErr = listErr
	}

	// Fallback 2: check repository tags with SemVer sorting
	tagsKey := repo + ":tags_list"
	s.mu.RLock()
	cachedTags, okTags := s.cache[tagsKey]
	s.mu.RUnlock()

	tagsURL := fmt.Sprintf("%s/repos/%s/tags?per_page=15", base, repo)
	s.mu.RLock()
	tagsEtag := s.endpointETags[tagsURL]
	s.mu.RUnlock()

	tagRel, tagsNewEtag, tagsNotMod, tagErr := s.fetchTagsURL(ctx, tagsURL, repo, toolName, token, tagsEtag)
	if tagsNewEtag != "" {
		s.mu.Lock()
		s.endpointETags[tagsURL] = tagsNewEtag
		s.mu.Unlock()
	}
	if tagsNotMod && okTags {
		s.mu.Lock()
		cachedTags.expiresAt = now.Add(10 * time.Minute)
		s.cache[tagsKey] = cachedTags
		s.cache[cacheKey] = cachedTags
		s.mu.Unlock()
		r := cachedTags.release
		r.Name = toolName
		r.LastCheckedAt = now
		r.LastSyncStatus = "not_modified"
		if !isLatestLookup {
			r.TargetVersion = targetVersion
		}
		return r, nil
	}
	if tagErr == nil && tagRel.LatestVersion != "" {
		tagRel.LastCheckedAt = now
		tagRel.LastSyncStatus = "ok"
		if !isLatestLookup {
			tagRel.TargetVersion = targetVersion
		}
		s.mu.Lock()
		s.cache[tagsKey] = cachedRelease{
			release:   tagRel,
			etag:      tagsNewEtag,
			expiresAt: now.Add(10 * time.Minute),
		}
		s.cache[cacheKey] = s.cache[tagsKey]
		if isLatestLookup {
			s.cache[repo+":latest"] = s.cache[tagsKey]
		}
		s.mu.Unlock()
		return tagRel, nil
	}
	if tagErr != nil {
		lastErr = tagErr
	}

	// Fallback 3: Return targetVersion as graceful fallback with error message on the struct
	fallbackVer := targetVersion
	if fallbackVer == "" || fallbackVer == "latest" {
		fallbackVer = "0.0.0"
	}
	errMsg := "no releases or tags found for repository " + repo
	if lastErr != nil {
		errMsg = lastErr.Error()
	}
	errToReturn := fmt.Errorf("lookup release for %s: %s", repo, errMsg)
	if lastErr != nil {
		errToReturn = fmt.Errorf("lookup release for %s: %w", repo, lastErr)
	}

	// Negatively cache 404/not found errors for 5 minutes (E4)
	if lastErr != nil && strings.Contains(lastErr.Error(), "404") {
		s.mu.Lock()
		s.cache[cacheKey] = cachedRelease{
			release: ToolRelease{
				Name:           toolName,
				Repository:     repo,
				LatestVersion:  fallbackVer,
				TargetVersion:  targetVersion,
				TagName:        fallbackVer,
				LastCheckedAt:  now,
				LastSyncStatus: "error",
				Error:          errMsg,
			},
			expiresAt: now.Add(5 * time.Minute),
		}
		s.mu.Unlock()
	}

	return ToolRelease{
		Name:           toolName,
		Repository:     repo,
		LatestVersion:  fallbackVer,
		TargetVersion:  targetVersion,
		TagName:        fallbackVer,
		LastCheckedAt:  now,
		LastSyncStatus: "error",
		Error:          errMsg,
	}, errToReturn
}

func (s *ReleaseService) fetchReleaseURL(ctx context.Context, url, repo, toolName, targetVersion, token, ifNoneMatch string) (ToolRelease, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, "", false, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return ToolRelease{Name: toolName, Repository: repo}, ifNoneMatch, true, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode == http.StatusForbidden && (resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "")) {
		backoffDur := parseRateLimitBackoff(resp.Header, s.now())
		s.mu.Lock()
		expiry := s.now().Add(backoffDur)
		s.rateLimits[repo] = expiry
		s.globalRateLimitUntil = expiry
		s.mu.Unlock()
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion, LastSyncStatus: "rate_limited"}, "", false, fmt.Errorf("rate limited (status %d)", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusForbidden {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, "", false, fmt.Errorf("forbidden (HTTP 403, check token scopes / SSO)")
	}

	if resp.StatusCode == http.StatusNotFound && strings.HasSuffix(url, "/releases/latest") {
		s.mu.Lock()
		s.noFormalReleases[repo] = s.now().Add(1 * time.Hour)
		s.mu.Unlock()
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, "", false, fmt.Errorf("status %d", resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")

	var ghRel struct {
		TagName     string    `json:"tag_name"`
		HTMLURL     string    `json:"html_url"`
		PublishedAt time.Time `json:"published_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ghRel); err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, "", false, err
	}

	ver := strings.TrimPrefix(ghRel.TagName, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       ghRel.TagName,
		PublishedAt:   ghRel.PublishedAt,
		HTMLURL:       ghRel.HTMLURL,
	}, etag, false, nil
}

func isPrereleaseTag(tag string) bool {
	lower := strings.ToLower(tag)
	if strings.Contains(lower, "-rc") ||
		strings.Contains(lower, "-beta") ||
		strings.Contains(lower, "-alpha") ||
		strings.Contains(lower, "-dev") ||
		strings.Contains(lower, "-preview") ||
		strings.Contains(lower, "-prerelease") {
		return true
	}
	parts := strings.SplitN(tag, "-", 2)
	return len(parts) > 1
}

func (s *ReleaseService) fetchReleaseListURL(ctx context.Context, url, repo, toolName, token, cachedEtag string) (ToolRelease, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if cachedEtag != "" {
		req.Header.Set("If-None-Match", cachedEtag)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return ToolRelease{Name: toolName, Repository: repo}, cachedEtag, true, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode == http.StatusForbidden && (resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "")) {
		backoffDur := parseRateLimitBackoff(resp.Header, s.now())
		s.mu.Lock()
		expiry := s.now().Add(backoffDur)
		s.rateLimits[repo] = expiry
		s.globalRateLimitUntil = expiry
		s.mu.Unlock()
		return ToolRelease{Name: toolName, Repository: repo, LastSyncStatus: "rate_limited"}, "", false, fmt.Errorf("rate limited (status %d)", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusForbidden {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("forbidden (HTTP 403, check token scopes / SSO)")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("status %d", resp.StatusCode)
	}

	newEtag := resp.Header.Get("ETag")

	type ghReleaseItem struct {
		TagName     string    `json:"tag_name"`
		HTMLURL     string    `json:"html_url"`
		PublishedAt time.Time `json:"published_at"`
		Draft       bool      `json:"draft"`
		Prerelease  bool      `json:"prerelease"`
	}
	var list []ghReleaseItem
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil || len(list) == 0 {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("no releases")
	}

	var candidates []ghReleaseItem
	for _, item := range list {
		if !item.Draft && !item.Prerelease && !isPrereleaseTag(item.TagName) {
			candidates = append(candidates, item)
		}
	}
	if len(candidates) == 0 {
		for _, item := range list {
			if !item.Draft {
				candidates = append(candidates, item)
			}
		}
	}
	if len(candidates) == 0 {
		candidates = list
	}

	// Sort descending by SemVer
	sort.SliceStable(candidates, func(i, j int) bool {
		return compareSemVer(candidates[i].TagName, candidates[j].TagName) > 0
	})

	best := candidates[0]
	ver := strings.TrimPrefix(best.TagName, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       best.TagName,
		PublishedAt:   best.PublishedAt,
		HTMLURL:       best.HTMLURL,
	}, newEtag, false, nil
}

func (s *ReleaseService) fetchTagsURL(ctx context.Context, url, repo, toolName, token, cachedEtag string) (ToolRelease, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if cachedEtag != "" {
		req.Header.Set("If-None-Match", cachedEtag)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return ToolRelease{Name: toolName, Repository: repo}, cachedEtag, true, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode == http.StatusForbidden && (resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "")) {
		backoffDur := parseRateLimitBackoff(resp.Header, s.now())
		s.mu.Lock()
		expiry := s.now().Add(backoffDur)
		s.rateLimits[repo] = expiry
		s.globalRateLimitUntil = expiry
		s.mu.Unlock()
		return ToolRelease{Name: toolName, Repository: repo, LastSyncStatus: "rate_limited"}, "", false, fmt.Errorf("rate limited (status %d)", resp.StatusCode)
	}

	if resp.StatusCode == http.StatusForbidden {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("forbidden (HTTP 403, check token scopes / SSO)")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("status %d", resp.StatusCode)
	}

	newEtag := resp.Header.Get("ETag")

	var tags []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil || len(tags) == 0 {
		return ToolRelease{Name: toolName, Repository: repo}, "", false, fmt.Errorf("no tags")
	}

	var candidateTags []struct {
		Name string `json:"name"`
	}
	for _, item := range tags {
		if !isPrereleaseTag(item.Name) {
			candidateTags = append(candidateTags, item)
		}
	}
	if len(candidateTags) == 0 {
		candidateTags = tags
	}

	sort.SliceStable(candidateTags, func(i, j int) bool {
		return compareSemVer(candidateTags[i].Name, candidateTags[j].Name) > 0
	})

	best := candidateTags[0]
	ver := strings.TrimPrefix(best.Name, "v")
	return ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       best.Name,
		HTMLURL:       fmt.Sprintf("https://github.com/%s/releases/tag/%s", repo, best.Name),
	}, newEtag, false, nil
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
		// Query the latest upstream release for the tool
		rel, err := s.GetToolRelease(ctx, repo, t.Name, "latest")
		rel.TargetVersion = t.TargetVersion
		if err != nil || rel.Error != "" {
			if rel.Error == "" && err != nil {
				rel.Error = err.Error()
			}
			if rel.LatestVersion == "" || rel.LatestVersion == "0.0.0" {
				rel.LatestVersion = t.TargetVersion
			}
			errs = append(errs, fmt.Sprintf("%s (%s): %s", t.Name, repo, rel.Error))
		}
		out = append(out, rel)
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("failed to fetch releases for %d tools: %s", len(errs), strings.Join(errs, "; "))
	}
	return out, nil
}

// InvalidateProfileCache expires the TTL of cached entries for tools in a profile,
// while intentionally preserving their cached ETags so subsequent requests use conditional HTTP (If-None-Match).
func (s *ReleaseService) InvalidateProfileCache(p DistributionProfile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := s.now().Add(-1 * time.Hour)
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
		bare := strings.TrimPrefix(targetVersion, "v")
		for _, key := range []string{
			repo + ":" + targetVersion,
			repo + ":" + bare,
			repo + ":v" + bare,
			repo + ":latest",
			repo + ":releases_list",
			repo + ":tags_list",
		} {
			if entry, exists := s.cache[key]; exists {
				entry.expiresAt = expired
				s.cache[key] = entry
			}
		}
	}
}

// RefreshProfileReleases expires cached release TTLs (preserving ETags) and queries fresh releases.
func (s *ReleaseService) RefreshProfileReleases(ctx context.Context, p DistributionProfile) ([]ToolRelease, error) {
	s.InvalidateProfileCache(p)
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
	etag := ""
	if release.TagName != "" {
		etag = `W/"` + release.TagName + `"`
	}
	s.cache[repo+":"+version] = cachedRelease{
		release:   release,
		etag:      etag,
		expiresAt: s.now().Add(ttl),
	}
}

// ReleaseNotificationCallback is invoked when a new release is detected and profile updated.
type ReleaseNotificationCallback func(profileName string, tool ProfileTool, oldVersion, newVersion string)

// StartBackgroundSync runs a background worker polling GitHub releases and auto-advancing tracking profiles.
func (s *ReleaseService) StartBackgroundSync(ctx context.Context, e *Engine, interval time.Duration, onReleaseDetected ReleaseNotificationCallback) {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	go func() {
		// Initial sync after 2 seconds
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			s.SyncAllProfiles(ctx, e, onReleaseDetected)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.SyncAllProfiles(ctx, e, onReleaseDetected)
			}
		}
	}()
}

// SyncProfile synchronizes release versions for a single profile.
// Updates tools atomically under Engine lock, respects TrackLatest, prevents downgrade,
// and invokes onReleaseDetected only after successful persistence.
func (s *ReleaseService) SyncProfile(
	ctx context.Context,
	e *Engine,
	profileName string,
	onReleaseDetected ReleaseNotificationCallback,
) (int, error) {
	return s.syncProfileWithCachePolicy(ctx, e, profileName, onReleaseDetected, true)
}

func (s *ReleaseService) syncProfileWithCachePolicy(
	ctx context.Context,
	e *Engine,
	profileName string,
	onReleaseDetected ReleaseNotificationCallback,
	invalidate bool,
) (int, error) {
	if e == nil {
		return 0, fmt.Errorf("engine required")
	}
	p, err := e.GetProfile(profileName)
	if err != nil {
		return 0, err
	}

	var releases []ToolRelease
	var fetchErr error
	if invalidate {
		releases, fetchErr = s.RefreshProfileReleases(ctx, *p)
	} else {
		releases, fetchErr = s.GetProfileReleases(ctx, *p)
	}
	if fetchErr != nil && len(releases) == 0 {
		return 0, fetchErr
	}

	totalAdvanced := 0
	type notif struct {
		tool        ProfileTool
		fromVersion string
		toVersion   string
	}
	var pendingNotifications []notif

	_, _, updateErr := e.UpdateProfileTools(profileName, func(tools []ProfileTool) ([]ProfileTool, bool) {
		updatedTools := make([]ProfileTool, len(tools))
		copy(updatedTools, tools)
		configChanged := false
		now := s.now()

		for i := range updatedTools {
			toolName := strings.ToLower(updatedTools[i].Name)
			repoName := strings.ToLower(updatedTools[i].Repository)

			for _, rel := range releases {
				relTool := strings.ToLower(rel.Name)
				relRepo := strings.ToLower(rel.Repository)
				if relTool == toolName || relRepo == repoName || strings.HasSuffix(relRepo, "/"+toolName) {
					updatedTools[i].LastCheckedAt = &now

					if rel.Error != "" {
						if rel.LastSyncStatus == "rate_limited" {
							updatedTools[i].LastSyncStatus = "rate_limited"
						} else {
							updatedTools[i].LastSyncStatus = "error"
						}
						updatedTools[i].LastSyncError = rel.Error
						break
					}

					if rel.LatestVersion != "" {
						updatedTools[i].LatestAvailableVersion = rel.LatestVersion
						updatedTools[i].LatestReleaseURL = rel.HTMLURL
						if !rel.PublishedAt.IsZero() {
							pub := rel.PublishedAt
							updatedTools[i].LatestPublishedAt = &pub
						}
						updatedTools[i].LastSyncStatus = "ok"
						updatedTools[i].LastSyncError = ""

						oldTarget := updatedTools[i].TargetVersion
						// Determine if tool is tracking latest (R2):
						// Respect explicit TrackLatest pointer (*bool), migrating legacy rows where TrackLatest is nil.
						isTrackingLatest := updatedTools[i].IsTrackingLatest(p.IsDefault || profileName == "generic")

						if isTrackingLatest {
							// Maintain tracking intent across auto-advances
							if updatedTools[i].TrackLatest == nil || !*updatedTools[i].TrackLatest {
								updatedTools[i].TrackLatest = BoolPtr(true)
								configChanged = true
							}

							// Advance target_version if target is empty, "latest", or upstream is strictly newer SemVer (N2)
							if oldTarget == "" || oldTarget == "latest" || compareSemVer(rel.LatestVersion, oldTarget) > 0 {
								updatedTools[i].TargetVersion = rel.LatestVersion
								configChanged = true
								totalAdvanced++
								pendingNotifications = append(pendingNotifications, notif{
									tool:        updatedTools[i],
									fromVersion: oldTarget,
									toVersion:   rel.LatestVersion,
								})
							}
						}
					}
					break
				}
			}
		}
		return updatedTools, configChanged
	})

	if updateErr != nil {
		return 0, updateErr
	}

	if onReleaseDetected != nil {
		for _, n := range pendingNotifications {
			onReleaseDetected(profileName, n.tool, n.fromVersion, n.toVersion)
		}
	}

	if fetchErr != nil {
		return totalAdvanced, fetchErr
	}
	return totalAdvanced, nil
}

// SyncAllProfilesResult details the outcome of syncing all profiles.
type SyncAllProfilesResult struct {
	TotalAdvanced int               `json:"tools_updated"`
	Errors        map[string]string `json:"errors,omitempty"`
}

// SyncAllProfiles iterates all registered distribution profiles and updates tool release information.
// Returns the number of tools whose target version was advanced.
func (s *ReleaseService) SyncAllProfiles(ctx context.Context, e *Engine, onReleaseDetected ReleaseNotificationCallback) int {
	res := s.SyncAllProfilesDetailed(ctx, e, onReleaseDetected)
	return res.TotalAdvanced
}

// SyncAllProfilesDetailed iterates all registered distribution profiles, updates tool releases,
// deduplicates requests across profiles (E4), and records per-profile error messages (R7).
func (s *ReleaseService) SyncAllProfilesDetailed(ctx context.Context, e *Engine, onReleaseDetected ReleaseNotificationCallback) SyncAllProfilesResult {
	res := SyncAllProfilesResult{
		Errors: make(map[string]string),
	}
	if e == nil {
		return res
	}
	profiles := e.ListProfiles()

	// Deduplicate unique repositories across all profiles and invalidate once per tick (E4)
	for _, p := range profiles {
		s.InvalidateProfileCache(p)
	}

	for _, p := range profiles {
		adv, err := s.syncProfileWithCachePolicy(ctx, e, p.Name, onReleaseDetected, false)
		if err != nil {
			log.Printf("[ReleaseService] ERROR syncing profile %s: %v", p.Name, err)
			res.Errors[p.Name] = err.Error()
		} else {
			res.TotalAdvanced += adv
		}
	}
	return res
}
