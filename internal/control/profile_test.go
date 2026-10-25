package control

import (
	"strings"
	"testing"
	"time"
)

func TestProfile_CRUD(t *testing.T) {
	e := NewEngine(time.Now)

	// List initially empty
	if len(e.ListProfiles()) != 0 {
		t.Fatalf("expected 0 profiles initially, got %d", len(e.ListProfiles()))
	}

	// Create profile
	p := DistributionProfile{
		Name:        "ci-runner",
		Description: "Minimal CI runner profile",
		Tools: []ProfileTool{
			{Name: "autopilot", Repository: "viovy/autopilot", BinaryName: "autopilot", Tier: "baseline"},
		},
	}
	created, err := e.UpsertProfile(p)
	if err != nil {
		t.Fatalf("failed to create profile: %v", err)
	}
	if created.Name != "ci-runner" {
		t.Errorf("expected name ci-runner, got %s", created.Name)
	}

	// Get profile
	fetched, err := e.GetProfile("ci-runner")
	if err != nil {
		t.Fatalf("failed to get profile: %v", err)
	}
	if len(fetched.Tools) != 1 || fetched.Tools[0].Name != "autopilot" {
		t.Errorf("unexpected tools: %+v", fetched.Tools)
	}

	// List profiles
	list := e.ListProfiles()
	if len(list) != 1 {
		t.Fatalf("expected 1 profile in list, got %d", len(list))
	}

	// Delete profile
	if err := e.DeleteProfile("ci-runner"); err != nil {
		t.Fatalf("failed to delete profile: %v", err)
	}
	if _, err := e.GetProfile("ci-runner"); err != ErrProfileNotFound {
		t.Errorf("expected ErrProfileNotFound after delete, got %v", err)
	}
}

func TestProfile_GenericProtected(t *testing.T) {
	e := NewEngine(time.Now)
	p := DistributionProfile{
		Name:        "generic",
		Description: "Generic default profile",
		IsDefault:   true,
	}
	if _, err := e.UpsertProfile(p); err != nil {
		t.Fatalf("failed to create generic profile: %v", err)
	}

	err := e.DeleteProfile("generic")
	if err != ErrProtectedProfile {
		t.Fatalf("expected ErrProtectedProfile when deleting generic, got %v", err)
	}
}

func TestProfile_SyncGeneric(t *testing.T) {
	e := NewEngine(time.Now)
	sampleManifest := `
fleet_distribution:
  schema_version: 1
  install_root:
    unix:
      primary: ~/.local/bin
      fallback: ~/.iazio/bin
    windows:
      primary: "%USERPROFILE%/bin"
      fallback: "%USERPROFILE%/.iazio/bin"
  repositories:
    autopilot-core:
      repo: viovy/autopilot
      type: github_release
      is_private: true
    autopilot-plugins:
      repo: viovy/autopilot-plugins-internal
      type: github_release
      is_private: true
  tools:
    - name: autopilot
      repository: autopilot-core
      binary_name: autopilot
      description: Autopilot CLI orchestration harness
      tier: baseline
    - name: autopilot-fleet
      repository: autopilot-plugins
      binary_name: autopilot-fleet
      description: Autopilot fleet orchestration plugin
      tier: baseline
`
	profile, err := e.SyncGenericProfile([]byte(sampleManifest))
	if err != nil {
		t.Fatalf("SyncGenericProfile failed: %v", err)
	}
	if profile.Name != "generic" || !profile.IsDefault {
		t.Errorf("expected generic default profile, got %+v", profile)
	}
	if profile.InstallRoots.Unix.Primary != "~/.local/bin" {
		t.Errorf("expected unix primary ~/.local/bin, got %s", profile.InstallRoots.Unix.Primary)
	}
	if len(profile.Tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(profile.Tools))
	}
	if profile.Tools[0].Repository != "viovy/autopilot" {
		t.Errorf("expected resolved repository viovy/autopilot, got %s", profile.Tools[0].Repository)
	}
	if profile.Tools[1].Repository != "viovy/autopilot-plugins-internal" {
		t.Errorf("expected resolved repository viovy/autopilot-plugins-internal, got %s", profile.Tools[1].Repository)
	}
}

func TestProfile_GetManifest(t *testing.T) {
	e := NewEngine(time.Now)
	p := DistributionProfile{
		Name:      "generic",
		IsDefault: true,
		Tools: []ProfileTool{
			{Name: "autopilot", Repository: "viovy/autopilot", BinaryName: "autopilot", Tier: "baseline"},
		},
	}
	_, _ = e.UpsertProfile(p)

	// Target Windows
	manifestWin, err := e.GetManifest("generic", "windows", "amd64")
	if err != nil {
		t.Fatalf("GetManifest(windows) failed: %v", err)
	}
	if !strings.HasSuffix(manifestWin.Tools[0].BinaryName, ".exe") {
		t.Errorf("expected .exe suffix on Windows, got %s", manifestWin.Tools[0].BinaryName)
	}

	// Target Linux
	manifestLinux, err := e.GetManifest("generic", "linux", "amd64")
	if err != nil {
		t.Fatalf("GetManifest(linux) failed: %v", err)
	}
	if strings.HasSuffix(manifestLinux.Tools[0].BinaryName, ".exe") {
		t.Errorf("expected no .exe suffix on Linux, got %s", manifestLinux.Tools[0].BinaryName)
	}
}

func TestScanRepoProfile_Validation(t *testing.T) {
	e := NewEngine(time.Now)

	// Empty inputs
	_, err := e.ScanRepoProfile("", "", "")
	if err == nil {
		t.Error("expected error for empty inputs")
	}

	// Host not registered
	_, err = e.ScanRepoProfile("h-unknown", "/repo", "p1")
	if err == nil {
		t.Error("expected error for unknown host")
	}

	// Host registered but repo not configured
	e.RegisterHost("h-known", "permanent")
	_, err = e.ScanRepoProfile("h-known", "/repo-unconfigured", "p1")
	if err == nil {
		t.Error("expected error for unconfigured repo")
	}

	// Repo configured but metadata.yml not found
	_ = e.UpsertRepo(Repo{HostID: "h-known", WorktreePath: "/nonexistent/path"})
	_, err = e.ScanRepoProfile("h-known", "/nonexistent/path", "p1")
	if err == nil {
		t.Error("expected error for missing metadata.yml")
	}
}

func TestScanRepoProfile_AgentSnapshot(t *testing.T) {
	e := NewEngine(time.Now)
	e.RegisterHost("h-remote", "Host Remote", "permanent")
	rawYAML := `name: meta-repo
fleet_distribution:
  tools:
    - name: agy
      target_version: 1.0.0
      tier: required
`
	_ = e.UpsertRepo(Repo{
		HostID:         "h-remote",
		WorktreePath:   "/remote/work/meta-repo",
		MetadataYmlRaw: rawYAML,
		GitVersion:     "v1.2.3",
	})

	prof, err := e.ScanRepoProfile("h-remote", "/remote/work/meta-repo", "remote-prof")
	if err != nil {
		t.Fatalf("expected ScanRepoProfile to succeed via agent snapshot, got: %v", err)
	}
	if prof == nil || prof.Name != "remote-prof" {
		t.Fatalf("unexpected profile returned: %+v", prof)
	}
	if len(prof.Tools) != 1 || prof.Tools[0].Name != "agy" {
		t.Fatalf("expected tool agy in profile, got: %+v", prof.Tools)
	}
}

func TestProfileTool_IsTrackingLatest_OptionalTier(t *testing.T) {
	// F7: ProfileTool with tier: optional in default profile must auto-track latest
	toolOptional := ProfileTool{
		Name:          "iazio-mcp",
		Repository:    "viovy/iazio-mcp",
		BinaryName:    "iazio-mcp",
		TargetVersion: "0.0.42",
		Tier:          "optional",
	}

	if !toolOptional.IsTrackingLatest(true) {
		t.Errorf("expected optional tier tool in default profile to track latest, got false")
	}

	// In non-default profile with pinned version, it should not track latest unless explicit
	if toolOptional.IsTrackingLatest(false) {
		t.Errorf("expected optional tier tool in non-default profile with pinned version not to track latest, got true")
	}

	// Explicit false must always be honored
	explicitFalse := ProfileTool{
		Name:          "iazio-mcp",
		Repository:    "viovy/iazio-mcp",
		BinaryName:    "iazio-mcp",
		TargetVersion: "0.0.42",
		Tier:          "optional",
		TrackLatest:   BoolPtr(false),
	}
	if explicitFalse.IsTrackingLatest(true) {
		t.Errorf("expected explicit TrackLatest=false to be honored even in default profile, got true")
	}

	// Explicit true must always be honored
	explicitTrue := ProfileTool{
		Name:          "iazio-mcp",
		Repository:    "viovy/iazio-mcp",
		BinaryName:    "iazio-mcp",
		TargetVersion: "0.0.42",
		Tier:          "optional",
		TrackLatest:   BoolPtr(true),
	}
	if !explicitTrue.IsTrackingLatest(false) {
		t.Errorf("expected explicit TrackLatest=true to be honored in non-default profile, got false")
	}
}
