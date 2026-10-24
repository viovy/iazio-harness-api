package control

import (
	"errors"
	"fmt"
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
// If track_latest was omitted in storage (nil, legacy rows), it migrates baseline tools
// in default/generic profiles or unpinned targets to true (R2). Explicit false is strictly preserved.
func (t ProfileTool) IsTrackingLatest(isDefaultProfile bool) bool {
	if t.TrackLatest != nil {
		return *t.TrackLatest
	}
	if t.TargetVersion == "" || t.TargetVersion == "latest" {
		return true
	}
	if isDefaultProfile && (t.Tier == "baseline" || t.Tier == "") {
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
// If configChanged is true, UpdatedAt is updated and OnSaveProfile is invoked.
func (e *Engine) UpdateProfileTools(name string, updater func(tools []ProfileTool) ([]ProfileTool, bool)) (*DistributionProfile, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	p, ok := e.profiles[strings.TrimSpace(name)]
	if !ok {
		return nil, false, ErrProfileNotFound
	}

	clonedTools := make([]ProfileTool, len(p.Tools))
	copy(clonedTools, p.Tools)

	newTools, configChanged := updater(clonedTools)

	if configChanged {
		candidate := *p
		candidate.Tools = newTools
		candidate.UpdatedAt = e.now()
		if e.OnSaveProfile != nil {
			if err := e.OnSaveProfile(candidate); err != nil {
				return nil, false, fmt.Errorf("persist profile %s: %w", name, err)
			}
		}
		p.UpdatedAt = candidate.UpdatedAt
	}

	p.Tools = newTools

	cp := *p
	if len(p.Tools) > 0 {
		cp.Tools = make([]ProfileTool, len(p.Tools))
		copy(cp.Tools, p.Tools)
	}
	return &cp, configChanged, nil
}

// UpsertProfile inserts or updates a distribution profile.
func (e *Engine) UpsertProfile(p DistributionProfile) (*DistributionProfile, error) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return nil, fmt.Errorf("profile name is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	existing, ok := e.profiles[name]
	if ok {
		p.CreatedAt = existing.CreatedAt
		p.UpdatedAt = now
		if name == "generic" {
			p.IsDefault = true
		}
	} else {
		p.CreatedAt = now
		p.UpdatedAt = now
	}
	e.profiles[name] = &p
	cp := p
	return &cp, nil
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
		e.mu.Unlock()
		return nil, fmt.Errorf("requirement criteria not met: repository %q is not configured for host %q", repoPath, hostID)
	}
	existingProfile := repo.DistributionProfile
	e.mu.Unlock()

	// Read metadata.yml from repo
	metaFile := filepath.Join(repoPath, "metadata.yml")
	raw, err := os.ReadFile(metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("requirement criteria not met: metadata.yml not found in %s (host %q may be remote): %w", repoPath, hostID, err)
		}
		return nil, fmt.Errorf("requirement criteria not met: metadata.yml not found in %s: %w", repoPath, err)
	}

	profile, err := parseFleetDistribution(raw)
	if err != nil {
		return nil, fmt.Errorf("requirement criteria not met: %w", err)
	}

	// Autodetect version via Git Flow (git describe --tags --always)
	cmd := exec.Command("git", "describe", "--tags", "--always")
	cmd.Dir = repoPath
	out, err := cmd.Output()
	version := strings.TrimSpace(string(out))
	if err != nil || version == "" {
		version = "unknown"
	}

	pName := strings.TrimSpace(profileName)
	if pName == "" {
		pName = existingProfile
	}
	if pName == "" {
		pName = "generic"
	}

	profile.Name = pName
	profile.Version = version
	profile.SourceRepo = repoPath
	profile.SourceHost = hostID
	profile.Description = fmt.Sprintf("Scanned from %s on host %s (git flow: %s)", filepath.Base(repoPath), hostID, version)
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

	return upserted, nil
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
