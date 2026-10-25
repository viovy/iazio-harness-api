package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	// ErrProtectedProfile is returned when attempting to delete the default generic profile.
	ErrProtectedProfile = errors.New("the generic profile is protected and cannot be deleted")
	// ErrProfileNotFound is returned when a requested distribution profile does not exist.
	ErrProfileNotFound = errors.New("distribution profile not found")
)

// DistributionProfile defines a machine profile specifying the toolset and install roots.
type DistributionProfile struct {
	Name         string              `json:"name" yaml:"name"`
	Description  string              `json:"description" yaml:"description"`
	IsDefault    bool                `json:"is_default" yaml:"is_default"`
	InstallRoots ProfileInstallRoots `json:"install_roots" yaml:"install_roots"`
	Endpoints    map[string]string   `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	Tools        []ProfileTool       `json:"tools" yaml:"tools"`
	Version      string              `json:"version,omitempty" yaml:"version,omitempty"`
	VersionSeq   uint64              `json:"-" yaml:"-"`
	SourceRepo   string              `json:"source_repo,omitempty" yaml:"source_repo,omitempty"`
	SourceHost   string              `json:"source_host,omitempty" yaml:"source_host,omitempty"`
	CreatedAt    time.Time           `json:"created_at" yaml:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at" yaml:"updated_at"`
}

type ProfileInstallRoots struct {
	Unix    ProfileInstallRootPaths `json:"unix" yaml:"unix"`
	Windows ProfileInstallRootPaths `json:"windows" yaml:"windows"`
}

type ProfileInstallRootPaths struct {
	Primary  string `json:"primary" yaml:"primary"`
	Fallback string `json:"fallback" yaml:"fallback"`
}

type ProfileTool struct {
	Name                   string       `json:"name" yaml:"name"`
	Repository             string       `json:"repository" yaml:"repository"`
	BinaryName             string       `json:"binary_name" yaml:"binary_name"`
	Description            string       `json:"description" yaml:"description"`
	Tier                   string       `json:"tier" yaml:"tier"` // baseline | optional
	TargetVersion          string       `json:"target_version,omitempty" yaml:"target_version,omitempty"`
	TrackLatest            *bool        `json:"track_latest,omitempty" yaml:"track_latest,omitempty"`
	LatestAvailableVersion string       `json:"latest_available_version,omitempty" yaml:"latest_available_version,omitempty"`
	LatestReleaseURL       string       `json:"latest_release_url,omitempty" yaml:"latest_release_url,omitempty"`
	LatestPublishedAt      *time.Time   `json:"latest_published_at,omitempty" yaml:"latest_published_at,omitempty"`
	LastCheckedAt          *time.Time   `json:"last_checked_at,omitempty" yaml:"last_checked_at,omitempty"`
	LastSyncStatus         string       `json:"last_sync_status,omitempty" yaml:"last_sync_status,omitempty"`
	LastSyncError          string       `json:"last_sync_error,omitempty" yaml:"last_sync_error,omitempty"`
	Service                *ToolService `json:"service,omitempty" yaml:"service,omitempty"`
}

// BoolPtr returns a pointer to a bool value.
func BoolPtr(b bool) *bool {
	return &b
}

// IsTrackingLatest determines if the tool should auto-advance to latest upstream releases.
// If track_latest was omitted in storage (nil, legacy rows), it defaults to tracking latest for
// all tools in default/generic profiles or unpinned targets regardless of tool tier. Explicit false is strictly preserved.
func (t ProfileTool) IsTrackingLatest(isDefaultProfile bool) bool {
	if t.TrackLatest != nil {
		return *t.TrackLatest
	}
	if t.TargetVersion == "" || t.TargetVersion == "latest" {
		return true
	}
	if isDefaultProfile {
		return true
	}
	return false
}

type ToolService struct {
	Enabled       bool              `json:"enabled" yaml:"enabled"`
	Autostart     bool              `json:"autostart" yaml:"autostart"`
	Label         string            `json:"label" yaml:"label"`
	RunArgs       []string          `json:"run_args" yaml:"run_args"`
	RestartPolicy string            `json:"restart_policy" yaml:"restart_policy"`
	Environment   map[string]string `json:"environment,omitempty" yaml:"environment,omitempty"`
}

type ManifestResponse struct {
	Profile      string              `json:"profile"`
	InstallRoots ProfileInstallRoots `json:"install_roots"`
	Endpoints    map[string]string   `json:"endpoints,omitempty"`
	Tools        []ManifestToolItem  `json:"tools"`
	GeneratedAt  time.Time           `json:"generated_at"`
}

type ManifestToolItem struct {
	Name          string       `json:"name"`
	Repository    string       `json:"repository"`
	BinaryName    string       `json:"binary_name"`
	Description   string       `json:"description"`
	Tier          string       `json:"tier"`
	TargetVersion string       `json:"target_version,omitempty"`
	Service       *ToolService `json:"service,omitempty"`
}

// ListProfiles returns all distribution profiles sorted by name with deep-copied Tools.
func (e *Engine) ListProfiles() []DistributionProfile {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DistributionProfile, 0, len(e.profiles))
	for _, p := range e.profiles {
		cp := *p
		if len(p.Tools) > 0 {
			cp.Tools = make([]ProfileTool, len(p.Tools))
			copy(cp.Tools, p.Tools)
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDefault != out[j].IsDefault {
			return out[i].IsDefault // default first
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// GetReleaseService returns the registered ReleaseService in a thread-safe manner.
func (e *Engine) GetReleaseService() *ReleaseService {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Releases
}

// SetReleaseService registers the ReleaseService in a thread-safe manner.
func (e *Engine) SetReleaseService(r *ReleaseService) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Releases = r
}

// GetProfile returns a distribution profile by name with deep-copied Tools and Endpoints.
func (e *Engine) GetProfile(name string) (*DistributionProfile, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.profiles[strings.TrimSpace(name)]
	if !ok {
		return nil, ErrProfileNotFound
	}
	cp := *p
	if len(p.Tools) > 0 {
		cp.Tools = make([]ProfileTool, len(p.Tools))
		copy(cp.Tools, p.Tools)
	}
	if len(p.Endpoints) > 0 {
		cp.Endpoints = make(map[string]string, len(p.Endpoints))
		for k, v := range p.Endpoints {
			cp.Endpoints[k] = v
		}
	}
	return &cp, nil
}

// UpdateProfileTools atomically updates the tools slice of an existing profile.
// It holds a per-profile lock to ensure consistency between persistence and in-memory state (G3).
// If configChanged is true, UpdatedAt and VersionSeq are updated, and OnSaveProfile is invoked outside e.mu.
func (e *Engine) UpdateProfileTools(name string, updater func(tools []ProfileTool) ([]ProfileTool, bool)) (*DistributionProfile, bool, error) {
	trimmedName := strings.TrimSpace(name)
	pMu := e.getProfileLock(trimmedName)
	pMu.Lock()
	defer pMu.Unlock()

	e.mu.Lock()
	p, ok := e.profiles[trimmedName]
	if !ok {
		e.mu.Unlock()
		return nil, false, ErrProfileNotFound
	}

	clonedTools := make([]ProfileTool, len(p.Tools))
	copy(clonedTools, p.Tools)

	newTools, configChanged := updater(clonedTools)
	if !configChanged {
		p.Tools = newTools
		cp := *p
		if len(p.Tools) > 0 {
			cp.Tools = make([]ProfileTool, len(p.Tools))
			copy(cp.Tools, p.Tools)
		}
		e.mu.Unlock()
		return &cp, false, nil
	}

	candidate := *p
	candidate.Tools = newTools
	candidate.UpdatedAt = e.now()
	candidate.VersionSeq = p.VersionSeq + 1
	saveFn := e.OnSaveProfile
	e.mu.Unlock()

	// Persist outside e.mu while holding per-profile lock (G3)
	if saveFn != nil {
		if err := saveFn(candidate); err != nil {
			return nil, false, fmt.Errorf("persist profile %s: %w", name, err)
		}
	}

	// Commit updated candidate to in-memory profile
	e.mu.Lock()
	p, ok = e.profiles[trimmedName]
	if !ok {
		e.mu.Unlock()
		return nil, false, ErrProfileNotFound
	}
	p.Tools = candidate.Tools
	p.UpdatedAt = candidate.UpdatedAt
	p.VersionSeq = candidate.VersionSeq

	cp := *p
	if len(p.Tools) > 0 {
		cp.Tools = make([]ProfileTool, len(p.Tools))
		copy(cp.Tools, p.Tools)
	}
	e.mu.Unlock()
	return &cp, true, nil
}

// UpsertProfile inserts or updates a distribution profile.
// It holds a per-profile lock to ensure consistency between persistence and in-memory state (G3).
func (e *Engine) UpsertProfile(p DistributionProfile) (*DistributionProfile, error) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return nil, fmt.Errorf("profile name is required")
	}

	pMu := e.getProfileLock(name)
	pMu.Lock()
	defer pMu.Unlock()

	e.mu.Lock()
	now := e.now()
	existing, ok := e.profiles[name]
	if ok {
		p.CreatedAt = existing.CreatedAt
		p.UpdatedAt = now
		p.VersionSeq = existing.VersionSeq + 1
		if name == "generic" {
			p.IsDefault = true
		}
		// F1: Preserve existing tool TrackLatest and telemetry fields if incoming value is nil or empty
		existingToolsByName := make(map[string]ProfileTool, len(existing.Tools))
		for _, t := range existing.Tools {
			existingToolsByName[t.Name] = t
		}
		for i := range p.Tools {
			if exTool, found := existingToolsByName[p.Tools[i].Name]; found {
				if p.Tools[i].TrackLatest == nil {
					p.Tools[i].TrackLatest = exTool.TrackLatest
				}
				if p.Tools[i].LatestAvailableVersion == "" {
					p.Tools[i].LatestAvailableVersion = exTool.LatestAvailableVersion
				}
				if p.Tools[i].LatestReleaseURL == "" {
					p.Tools[i].LatestReleaseURL = exTool.LatestReleaseURL
				}
				if p.Tools[i].LatestPublishedAt == nil {
					p.Tools[i].LatestPublishedAt = exTool.LatestPublishedAt
				}
				if p.Tools[i].LastCheckedAt == nil {
					p.Tools[i].LastCheckedAt = exTool.LastCheckedAt
				}
				if p.Tools[i].LastSyncStatus == "" {
					p.Tools[i].LastSyncStatus = exTool.LastSyncStatus
				}
				if p.Tools[i].LastSyncError == "" {
					p.Tools[i].LastSyncError = exTool.LastSyncError
				}
			}
		}
	} else {
		p.CreatedAt = now
		p.UpdatedAt = now
		p.VersionSeq = 1
	}

	candidate := p
	saveFn := e.OnSaveProfile
	e.mu.Unlock()

	// Persist outside e.mu while holding per-profile lock (G3)
	if saveFn != nil {
		if err := saveFn(candidate); err != nil {
			return nil, fmt.Errorf("persist profile %s: %w", name, err)
		}
	}

	e.mu.Lock()
	e.profiles[name] = &candidate
	cp := candidate
	e.mu.Unlock()
	return &cp, nil
}

// LoadProfile loads a profile from persistent storage into memory during engine startup
// without triggering OnSaveProfile or modifying timestamps (E2).
func (e *Engine) LoadProfile(p DistributionProfile) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		name = "generic"
	}
	pMu := e.getProfileLock(name)
	pMu.Lock()
	defer pMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	cp := p
	if len(p.Tools) > 0 {
		cp.Tools = make([]ProfileTool, len(p.Tools))
		copy(cp.Tools, p.Tools)
	}
	e.profiles[name] = &cp
}

// DeleteProfile removes a distribution profile. Generic is protected.
func (e *Engine) DeleteProfile(name string) error {
	name = strings.TrimSpace(name)
	if name == "generic" {
		return ErrProtectedProfile
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.profiles[name]; !ok {
		return ErrProfileNotFound
	}
	delete(e.profiles, name)
	return nil
}

func parseFleetDistribution(raw []byte) (*DistributionProfile, error) {
	var parsed struct {
		FleetDistribution struct {
			SchemaVersion int `yaml:"schema_version" json:"schema_version"`
			InstallRoot   struct {
				Unix struct {
					Primary  string `yaml:"primary" json:"primary"`
					Fallback string `yaml:"fallback" json:"fallback"`
				} `yaml:"unix" json:"unix"`
				Windows struct {
					Primary  string `yaml:"primary" json:"primary"`
					Fallback string `yaml:"fallback" json:"fallback"`
				} `yaml:"windows" json:"windows"`
			} `yaml:"install_root" json:"install_root"`
			Endpoints    map[string]string `yaml:"endpoints" json:"endpoints"`
			Repositories map[string]struct {
				Repo      string `yaml:"repo" json:"repo"`
				Type      string `yaml:"type" json:"type"`
				IsPrivate bool   `yaml:"is_private" json:"is_private"`
			} `yaml:"repositories" json:"repositories"`
			Tools []struct {
				Name          string       `yaml:"name" json:"name"`
				Repository    string       `yaml:"repository" json:"repository"`
				BinaryName    string       `yaml:"binary_name" json:"binary_name"`
				Description   string       `yaml:"description" json:"description"`
				Tier          string       `yaml:"tier" json:"tier"`
				TargetVersion string       `yaml:"target_version" json:"target_version"`
				Service       *ToolService `yaml:"service" json:"service"`
			} `yaml:"tools" json:"tools"`
		} `yaml:"fleet_distribution" json:"fleet_distribution"`
	}

	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}

	fd := parsed.FleetDistribution
	if len(fd.Tools) == 0 && fd.InstallRoot.Unix.Primary == "" && fd.InstallRoot.Windows.Primary == "" {
		return nil, errors.New("fleet_distribution section not found or contains no tools")
	}

	profile := DistributionProfile{
		Name:        "generic",
		Description: "GitOps Default Fleet Distribution Profile",
		IsDefault:   true,
		InstallRoots: ProfileInstallRoots{
			Unix: ProfileInstallRootPaths{
				Primary:  fd.InstallRoot.Unix.Primary,
				Fallback: fd.InstallRoot.Unix.Fallback,
			},
			Windows: ProfileInstallRootPaths{
				Primary:  fd.InstallRoot.Windows.Primary,
				Fallback: fd.InstallRoot.Windows.Fallback,
			},
		},
		Endpoints: fd.Endpoints,
		Tools:     make([]ProfileTool, 0, len(fd.Tools)),
	}

	for _, t := range fd.Tools {
		repo := t.Repository
		if repoDef, ok := fd.Repositories[t.Repository]; ok && repoDef.Repo != "" {
			repo = repoDef.Repo
		}
		trackLatest := t.TargetVersion == "" || t.TargetVersion == "latest"
		profile.Tools = append(profile.Tools, ProfileTool{
			Name:          t.Name,
			Repository:    repo,
			BinaryName:    t.BinaryName,
			Description:   t.Description,
			Tier:          t.Tier,
			TargetVersion: t.TargetVersion,
			TrackLatest:   BoolPtr(trackLatest),
			Service:       t.Service,
		})
	}
	return &profile, nil
}

// SyncGenericProfile parses metadata.yml (or direct JSON/YAML) and updates the generic profile.
func (e *Engine) SyncGenericProfile(raw []byte) (*DistributionProfile, error) {
	p, err := parseFleetDistribution(raw)
	if err != nil {
		return nil, err
	}
	return e.UpsertProfile(*p)
}

// ScanRepoProfile inspects a configured repository on a host, reads its metadata.yml,
// autodetects its Git Flow version via git describe, updates the profile, and links the repo to it.
func (e *Engine) ScanRepoProfile(hostID, repoPath, profileName string) (*DistributionProfile, error) {
	hostID = strings.TrimSpace(hostID)
	repoPath = strings.TrimSpace(repoPath)
	if hostID == "" || repoPath == "" {
		return nil, errors.New("host_id and repo_path are required")
	}

	e.mu.Lock()
	if _, ok := e.hosts[hostID]; !ok {
		e.mu.Unlock()
		return nil, fmt.Errorf("requirement criteria not met: host %q is not registered", hostID)
	}
	repo, ok := e.repos[repoKey(hostID, repoPath)]
	if !ok || repo == nil {
		for _, candidate := range e.repos {
			if candidate.HostID == hostID && (candidate.EffectiveAlias() == repoPath || candidate.WorktreePath == repoPath || filepath.Base(candidate.WorktreePath) == repoPath) {
				repo = candidate
				repoPath = candidate.WorktreePath
				ok = true
				break
			}
		}
	}
	if !ok || repo == nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("requirement criteria not met: repository %q is not configured for host %q", repoPath, hostID)
	}
	existingProfile := repo.DistributionProfile
	repoCopy := *repo
	e.mu.Unlock()

	var raw []byte
	var version string
	var scanSource string

	// Tier 1: Local Filesystem Check
	metaFile := filepath.Join(repoPath, "metadata.yml")
	if b, err := os.ReadFile(metaFile); err == nil && len(b) > 0 {
		raw = b
		scanSource = fmt.Sprintf("local:%s", repoPath)
		cmd := exec.Command("git", "describe", "--tags", "--always")
		cmd.Dir = repoPath
		if out, err := cmd.Output(); err == nil && len(out) > 0 {
			version = strings.TrimSpace(string(out))
		}
	}

	// Tier 2: Agent Repo Snapshot Sync (cached or live RPC fetch)
	if len(raw) == 0 && repoCopy.MetadataYmlRaw != "" {
		raw = []byte(repoCopy.MetadataYmlRaw)
		version = repoCopy.GitVersion
		scanSource = fmt.Sprintf("agent-snapshot:%s:%s", hostID, filepath.Base(repoPath))
	}

	if len(raw) == 0 {
		e.mu.Lock()
		h := e.hosts[hostID]
		isOnline := h != nil && strings.HasPrefix(h.Presence, "ONLINE")
		e.mu.Unlock()

		if isOnline {
			probeID := fmt.Sprintf("fetch-meta-%d", time.Now().UnixNano())
			waiterKey, waiterCh := e.RegisterRepoStatusWaiter(hostID, repoCopy.WorktreePath)
			defer e.DeregisterRepoStatusWaiter(waiterKey)

			_ = e.QueueCommand(hostID, CommandEnvelope{
				ID:        probeID,
				Type:      "FETCH_METADATA_YML",
				Payload:   repoCopy.WorktreePath,
				CreatedAt: time.Now().UTC().Format(time.RFC3339),
			})

			select {
			case st := <-waiterCh:
				if st.MetadataYmlRaw != "" {
					raw = []byte(st.MetadataYmlRaw)
					version = st.GitVersion
					scanSource = fmt.Sprintf("agent-live:%s:%s", hostID, filepath.Base(repoPath))
				}
			case <-time.After(3 * time.Second):
			}
		}
	}

	// Tier 3: Upstream Git / GitHub API Fallback
	if len(raw) == 0 && strings.TrimSpace(repoCopy.CloneURL) != "" {
		cloneURL := strings.TrimSpace(repoCopy.CloneURL)
		if b, ver, err := fetchGitHubMetadataYml(context.Background(), cloneURL, e.Releases); err == nil && len(b) > 0 {
			raw = b
			version = ver
			scanSource = fmt.Sprintf("github-upstream:%s", cloneURL)
		}
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("requirement criteria not met: metadata.yml not found in %s (host %q may be remote and agent snapshot not yet received): ensure iazio-agent is online and has scanned repo", repoPath, hostID)
	}

	profile, err := parseFleetDistribution(raw)
	if err != nil {
		return nil, fmt.Errorf("requirement criteria not met: %w", err)
	}

	pName := strings.TrimSpace(profileName)
	if pName == "" {
		pName = existingProfile
	}
	if pName == "" {
		pName = "generic"
	}

	if version == "" {
		version = "unknown"
	}

	profile.Name = pName
	profile.Version = version
	profile.SourceRepo = repoPath
	profile.SourceHost = hostID
	profile.Description = fmt.Sprintf("Scanned from %s on host %s (%s, git flow: %s)", filepath.Base(repoPath), hostID, scanSource, version)
	profile.IsDefault = (pName == "generic")

	upserted, err := e.UpsertProfile(*profile)
	if err != nil {
		return nil, err
	}

	// Link repo to profile
	e.mu.Lock()
	if r, ok := e.repos[repoKey(hostID, repoPath)]; ok && r != nil {
		r.DistributionProfile = pName
	}
	e.mu.Unlock()

	// Trigger release reconciliation asynchronously if release service is available
	if e.Releases != nil {
		go func() {
			_, _ = e.Releases.SyncProfile(context.Background(), e, pName, nil)
		}()
	}

	return upserted, nil
}

func fetchGitHubMetadataYml(ctx context.Context, cloneURL string, relService *ReleaseService) ([]byte, string, error) {
	clean := strings.TrimSpace(cloneURL)
	repo := extractGitHubRepo(clean)
	if repo == "" {
		return nil, "", fmt.Errorf("unable to determine repository from clone url %q", cloneURL)
	}

	client := &http.Client{Timeout: 6 * time.Second}
	var token string
	if relService != nil {
		token = relService.tokenForRepo(repo)
	}
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
		if token == "" {
			token = os.Getenv("GH_TOKEN")
		}
	}

	for _, branch := range []string{"main", "master"} {
		rawURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/metadata.yml", repo, branch)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			continue
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body, rerr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
				if rerr == nil && len(body) > 0 {
					version := fetchGitHubLatestVersion(ctx, client, repo, token)
					return body, version, nil
				}
			}
		}
	}
	return nil, "", errors.New("metadata.yml not accessible via upstream GitHub")
}

func fetchGitHubLatestVersion(ctx context.Context, client *http.Client, repo, token string) string {
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err == nil {
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		req.Header.Set("Accept", "application/vnd.github.v3+json")
		if resp, err := client.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var r struct {
					TagName string `json:"tag_name"`
				}
				if json.NewDecoder(resp.Body).Decode(&r) == nil && r.TagName != "" {
					return r.TagName
				}
			}
		}
	}
	return "upstream"
}

func extractGitHubRepo(rawURL string) string {
	u := strings.TrimSpace(rawURL)
	u = strings.TrimSuffix(u, ".git")
	if strings.Contains(u, "github.com/") {
		parts := strings.Split(u, "github.com/")
		if len(parts) >= 2 {
			return strings.Trim(parts[1], "/")
		}
	} else if strings.Contains(u, "github.com:") {
		parts := strings.Split(u, "github.com:")
		if len(parts) >= 2 {
			return strings.Trim(parts[1], "/")
		}
	}
	if strings.Count(u, "/") == 1 && !strings.Contains(u, ":") {
		return u
	}
	return ""
}

// GetManifest produces a machine-targeted manifest for a profile.
func (e *Engine) GetManifest(profileName, targetOS, targetArch string) (*ManifestResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pName := strings.TrimSpace(profileName)
	if pName == "" {
		pName = "generic"
	}

	p, ok := e.profiles[pName]
	if !ok {
		p, ok = e.profiles["generic"]
		if !ok {
			return nil, ErrProfileNotFound
		}
	}

	isWindows := strings.EqualFold(targetOS, "windows")
	tools := make([]ManifestToolItem, 0, len(p.Tools))
	for _, t := range p.Tools {
		bin := t.BinaryName
		if isWindows && !strings.HasSuffix(strings.ToLower(bin), ".exe") {
			bin += ".exe"
		}
		tools = append(tools, ManifestToolItem{
			Name:          t.Name,
			Repository:    t.Repository,
			BinaryName:    bin,
			Description:   t.Description,
			Tier:          t.Tier,
			TargetVersion: t.TargetVersion,
			Service:       t.Service,
		})
	}

	return &ManifestResponse{
		Profile:      p.Name,
		InstallRoots: p.InstallRoots,
		Endpoints:    p.Endpoints,
		Tools:        tools,
		GeneratedAt:  e.now(),
	}, nil
}
