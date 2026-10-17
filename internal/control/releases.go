package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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
}

type cachedRelease struct {
	release   ToolRelease
	expiresAt time.Time
}

// ReleaseService queries and caches latest Git release versions for profile tools.
type ReleaseService struct {
	mu     sync.RWMutex
	cache  map[string]cachedRelease
	client *http.Client
	now    func() time.Time
	token  string
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
		cache:  make(map[string]cachedRelease),
		client: &http.Client{Timeout: 10 * time.Second},
		now:    time.Now,
		token:  token,
	}
}

// GetToolRelease returns the latest release for a repository.
func (s *ReleaseService) GetToolRelease(ctx context.Context, repo, toolName, targetVersion string) (ToolRelease, error) {
	if !strings.Contains(repo, "/") {
		repo = "viovy/" + repo
	}

	cacheKey := repo
	s.mu.RLock()
	cached, ok := s.cache[cacheKey]
	s.mu.RUnlock()

	now := s.now()
	if ok && now.Before(cached.expiresAt) {
		r := cached.release
		r.Name = toolName
		return r, nil
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", repo)
	if targetVersion != "" && targetVersion != "latest" {
		tag := targetVersion
		if !strings.HasPrefix(tag, "v") {
			tag = "v" + tag
		}
		url = fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "iazio-harness-api/1.0")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		if ok {
			return cached.release, nil
		}
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if ok {
			return cached.release, nil
		}
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, nil
	}

	var ghRel struct {
		TagName     string    `json:"tag_name"`
		HTMLURL     string    `json:"html_url"`
		PublishedAt time.Time `json:"published_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ghRel); err != nil {
		if ok {
			return cached.release, nil
		}
		return ToolRelease{Name: toolName, Repository: repo, LatestVersion: targetVersion, TagName: targetVersion}, nil
	}

	ver := strings.TrimPrefix(ghRel.TagName, "v")
	rel := ToolRelease{
		Name:          toolName,
		Repository:    repo,
		LatestVersion: ver,
		TagName:       ghRel.TagName,
		PublishedAt:   ghRel.PublishedAt,
		HTMLURL:       ghRel.HTMLURL,
	}

	s.mu.Lock()
	s.cache[cacheKey] = cachedRelease{
		release:   rel,
		expiresAt: now.Add(10 * time.Minute),
	}
	s.mu.Unlock()

	return rel, nil
}

// GetProfileReleases queries latest releases for all tools in a profile.
func (s *ReleaseService) GetProfileReleases(ctx context.Context, p DistributionProfile) ([]ToolRelease, error) {
	out := make([]ToolRelease, 0, len(p.Tools))
	for _, t := range p.Tools {
		repo := t.Repository
		if repo == "" {
			repo = "viovy/" + t.Name
		}
		rel, _ := s.GetToolRelease(ctx, repo, t.Name, t.TargetVersion)
		out = append(out, rel)
	}
	return out, nil
}
