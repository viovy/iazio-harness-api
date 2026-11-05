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
    windows:
      primary: "%USERPROFILE%/bin"
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
	rawYAML := `name: test-repo
fleet_distribution:
  tools:
    - name: agy
      target_version: 1.0.0
      tier: required
`
	_ = e.UpsertRepo(Repo{
		HostID:         "h-remote",
		WorktreePath:   "/remote/work/test-repo",
		MetadataYmlRaw: rawYAML,
		GitVersion:     "v1.2.3",
	})

	prof, err := e.ScanRepoProfile("h-remote", "/remote/work/test-repo", "remote-prof")
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

func TestProfileTool_AuthStatusEnabled_ManifestAndHeartbeatEnrichment(t *testing.T) {
	e := NewEngine(time.Now)
	rawYAML := `fleet_distribution:
  schema_version: 1
  install_root:
    unix:
      primary: ~/.local/bin
  tools:
    - name: iazio-harvester
      repository: viovy/iazio-harvester
      binary_name: iazio-harvester
      tier: baseline
    - name: iazio-agent
      repository: viovy/iazio-agent
      binary_name: iazio-agent
      tier: baseline
      service:
        enabled: true
        autostart: true
        label: io.iazio.iazio-agent
        run_args: ["run"]
`
	prof, err := e.SyncGenericProfile([]byte(rawYAML))
	if err != nil {
		t.Fatalf("SyncGenericProfile failed: %v", err)
	}
	if len(prof.Tools) != 2 {
		t.Fatalf("expected 2 tools, got %d", len(prof.Tools))
	}

	// Enable AuthStatusEnabled on iazio-harvester via UpsertProfile (simulating UI checkbox tick)
	// Also clear Service on iazio-agent incoming payload to verify UpsertProfile preserves existing Service.
	prof.Tools[0].AuthStatusEnabled = true
	prof.Tools[1].Service = nil
	updated, err := e.UpsertProfile(*prof)
	if err != nil {
		t.Fatalf("UpsertProfile failed: %v", err)
	}
	if !updated.Tools[0].AuthStatusEnabled {
		t.Fatalf("expected AuthStatusEnabled=true on iazio-harvester")
	}
	if updated.Tools[1].Service == nil || updated.Tools[1].Service.Label != "io.iazio.iazio-agent" {
		t.Fatalf("expected UpsertProfile to preserve existing tool Service definition, got %+v", updated.Tools[1].Service)
	}

	// Verify GetManifest includes AuthStatusEnabled
	manifest, err := e.GetManifest("generic", "linux", "amd64")
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if !manifest.Tools[0].AuthStatusEnabled {
		t.Errorf("expected manifest tool iazio-harvester AuthStatusEnabled=true")
	}
	if manifest.Tools[1].AuthStatusEnabled {
		t.Errorf("expected manifest tool iazio-agent AuthStatusEnabled=false")
	}

	// Verify SyncGenericProfile preserves AuthStatusEnabled when re-synced from YAML
	resynced, err := e.SyncGenericProfile([]byte(rawYAML))
	if err != nil {
		t.Fatalf("re-SyncGenericProfile failed: %v", err)
	}
	if !resynced.Tools[0].AuthStatusEnabled {
		t.Errorf("expected SyncGenericProfile to preserve AuthStatusEnabled=true on iazio-harvester")
	}

	// Verify Host Heartbeat enriches AuthStatusEnabled from profile and preserves AuthStatus across intermediate heartbeats
	e.RegisterHost("debian", "debian", "permanent")
	hbErr := e.HeartbeatWithCapabilities("debian", "ONLINE", []Tool{
		{
			Name:    "iazio-harvester",
			Path:    "/usr/local/bin/iazio-harvester",
			Version: "0.2.0",
			Status:  "OK",
			AuthStatus: map[string]any{
				"token_status":          "available",
				"refresh_token_present": true,
				"api_base_url":          "https://tian.go.ro/iazio-harvester-api",
			},
			AuthCheckedAt: "2026-11-03T15:00:00Z",
		},
	}, false, nil, "1.0.0", "abc")
	if hbErr != nil {
		t.Fatalf("HeartbeatWithCapabilities failed: %v", hbErr)
	}

	detail, ok := e.GetHostDetail("debian")
	if !ok || len(detail.Host.Tools) != 1 {
		t.Fatalf("expected debian host detail with 1 tool")
	}
	if !detail.Host.Tools[0].AuthStatusEnabled {
		t.Errorf("expected debian tool iazio-harvester to have AuthStatusEnabled=true from generic profile")
	}
	if detail.Host.Tools[0].AuthStatus["token_status"] != "available" {
		t.Errorf("unexpected AuthStatus: %+v", detail.Host.Tools[0].AuthStatus)
	}

	// Intermediate heartbeat without AuthStatus should preserve cached AuthStatus while AuthStatusEnabled is true
	_ = e.HeartbeatWithCapabilities("debian", "ONLINE", []Tool{
		{
			Name:    "iazio-harvester",
			Path:    "/usr/local/bin/iazio-harvester",
			Version: "0.2.0",
			Status:  "OK",
		},
	}, false, nil, "1.0.0", "abc")
	detailAfter, _ := e.GetHostDetail("debian")
	if detailAfter.Host.Tools[0].AuthStatus["token_status"] != "available" {
		t.Errorf("expected intermediate heartbeat to preserve AuthStatus, got: %+v", detailAfter.Host.Tools[0].AuthStatus)
	}
}

func TestSanitizeProfileTools_AndAdhocASEQueuePause(t *testing.T) {
	e := NewEngine(time.Now)
	e.LoadProfile(DistributionProfile{
		Name: "generic",
		Tools: []ProfileTool{
			{Name: "autopilot-git-flow", BinaryName: "autopilot-git-flow", TargetVersion: "0.0.579"},
			{Name: "autopilot-git-flow", BinaryName: "autopilot-git-flow", TargetVersion: "0.0.579"},
			{Name: "git-flow", BinaryName: "git-flow", TargetVersion: "0.0.579"},
		},
	})
	manifest, err := e.GetManifest("generic", "darwin", "arm64")
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if len(manifest.Tools) != 1 || manifest.Tools[0].Name != "autopilot-git-flow" {
		t.Fatalf("expected sanitized manifest with 1 autopilot-git-flow entry, got %+v", manifest.Tools)
	}

	e.RegisterHost("mac-mini", "mac-mini", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: "/Users/romeo/work/meta-repo",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}); err != nil {
		t.Fatalf("UpsertRepo failed: %v", err)
	}

	// 1. Report active ad-hoc ASE flow (INDEPENDENT_USER_SESSION)
	e.UpdateRepoRuntimeStatus("mac-mini", []RepoRuntimeStatus{
		{
			RepoPath:            "/Users/romeo/work/meta-repo",
			IsClean:             false,
			Branch:              "fix/vigo-0235-bws",
			ASEState:            "RUNNING",
			ActiveStoryID:       "STORY-VIGO-0235",
			ActiveTool:          "autopilot-ase",
			ToolLiveness:        "LIVE",
			ExecutionProvenance: "INDEPENDENT_USER_SESSION",
		},
	})

	harnessExecs, adhocExecs, _ := e.GetHostExecutionCounts("mac-mini")
	if harnessExecs != 0 || adhocExecs != 1 {
		t.Fatalf("expected harnessExecs=0 adhocExecs=1, got harness=%d adhoc=%d", harnessExecs, adhocExecs)
	}
	repo, ok := e.GetRepo("mac-mini", "/Users/romeo/work/meta-repo")
	if !ok {
		t.Fatalf("expected repo to exist")
	}
	if repo.Queue != QueuePaused || repo.Reason != ReasonAdhocASE {
		t.Fatalf("expected repo queue PAUSED with reason %q, got queue=%q reason=%q", ReasonAdhocASE, repo.Queue, repo.Reason)
	}

	// 2. Partial status report (empty ASEState while still dirty) must preserve adhoc ASE state and queue pause
	e.UpdateRepoRuntimeStatus("mac-mini", []RepoRuntimeStatus{
		{
			RepoPath: "/Users/romeo/work/meta-repo",
			IsClean:  false,
			Branch:   "fix/vigo-0235-bws",
		},
	})
	_, adhocExecs2, _ := e.GetHostExecutionCounts("mac-mini")
	if adhocExecs2 != 1 {
		t.Fatalf("expected adhocExecs=1 preserved across partial dirty report, got %d", adhocExecs2)
	}

	// 3. Ad-hoc ASE completes and lands back on clean main -> queue auto-reopens
	e.UpdateRepoRuntimeStatus("mac-mini", []RepoRuntimeStatus{
		{
			RepoPath:            "/Users/romeo/work/meta-repo",
			IsClean:             true,
			Branch:              "main",
			ASEState:            "IDLE",
			ToolLiveness:        "NONE",
			ExecutionProvenance: "NONE",
		},
	})
	_, adhocExecs3, _ := e.GetHostExecutionCounts("mac-mini")
	if adhocExecs3 != 0 {
		t.Fatalf("expected adhocExecs=0 after completion, got %d", adhocExecs3)
	}
	repoAfter, _ := e.GetRepo("mac-mini", "/Users/romeo/work/meta-repo")
	if repoAfter.Queue != QueueOpen || repoAfter.Reason != "" {
		t.Fatalf("expected repo queue to auto-reopen to OPEN after ad-hoc ASE finishes, got queue=%q reason=%q", repoAfter.Queue, repoAfter.Reason)
	}
	if repoAfter.DirtyStoryID != "" {
		t.Fatalf("expected DirtyStoryID to be cleared after ad-hoc ASE finishes, got %q", repoAfter.DirtyStoryID)
	}

	// 4. Idle interactive shell (ToolLiveness=LIVE, ASEState=IDLE) on clean main must NOT trigger Adhoc ASE Active
	if IsAdhocASEActive("INDEPENDENT_USER_SESSION", "LIVE", "IDLE", false) {
		t.Fatalf("expected IsAdhocASEActive=false when ASEState=IDLE even if ToolLiveness=LIVE")
	}
}
