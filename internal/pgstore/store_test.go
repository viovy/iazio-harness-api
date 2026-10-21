package pgstore

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestNilStore(t *testing.T) {
	var s *Store
	ctx := context.Background()

	if err := s.SaveIdea(ctx, control.Idea{}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveIdeaArgs(ctx, "i1", "t", "u", "tr", "NEW", "s1"); err != nil {
		t.Fatal(err)
	}
	if ideas, err := s.LoadIdeas(ctx); err != nil || ideas != nil {
		t.Fatalf("expected nil ideas, got %v, %v", ideas, err)
	}

	if err := s.SavePrompt(ctx, control.Prompt{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePrompt(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if prompts, err := s.LoadPrompts(ctx); err != nil || prompts != nil {
		t.Fatalf("expected nil prompts, got %v, %v", prompts, err)
	}

	if err := s.SaveRepo(ctx, control.Repo{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRepo(ctx, "h1", "p1"); err != nil {
		t.Fatal(err)
	}
	if repos, err := s.LoadRepos(ctx); err != nil || repos != nil {
		t.Fatalf("expected nil repos, got %v, %v", repos, err)
	}

	if err := s.SaveHost(ctx, control.Host{}); err != nil {
		t.Fatal(err)
	}
	if hosts, err := s.LoadHosts(ctx); err != nil || hosts != nil {
		t.Fatalf("expected nil hosts, got %v, %v", hosts, err)
	}

	if err := s.SaveHistory(ctx, "h1", "/p1", control.HistoryRow{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteHistory(ctx, "job1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearRepoHistory(ctx, "h1", "/p1"); err != nil {
		t.Fatal(err)
	}
	if hist, err := s.LoadHistory(ctx); err != nil || hist != nil {
		t.Fatalf("expected nil history, got %v, %v", hist, err)
	}

	if err := s.SaveProfile(ctx, control.DistributionProfile{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteProfile(ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	if profs, err := s.LoadProfiles(ctx); err != nil || profs != nil {
		t.Fatalf("expected nil profiles, got %v, %v", profs, err)
	}

	if err := s.SaveGitCredential(ctx, control.GitCredential{}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteGitCredential(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if creds, err := s.LoadGitCredentials(ctx); err != nil || creds != nil {
		t.Fatalf("expected nil git credentials, got %v, %v", creds, err)
	}

	if err := s.SaveCodebaseRepo(ctx, codebase.Repository{}); err != nil {
		t.Fatal(err)
	}
	if cRepos, err := s.LoadCodebaseRepos(ctx); err != nil || cRepos != nil {
		t.Fatalf("expected nil codebase repos, got %v, %v", cRepos, err)
	}

	if err := s.SaveCodebaseChunks(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if chunks, err := s.LoadCodebaseChunks(ctx, "r1"); err != nil || chunks != nil {
		t.Fatalf("expected nil chunks, got %v, %v", chunks, err)
	}

	if err := s.SaveStory(ctx, control.Story{}); err != nil {
		t.Fatal(err)
	}
	if stories, err := s.LoadStories(ctx); err != nil || stories != nil {
		t.Fatalf("expected nil stories, got %v, %v", stories, err)
	}
	if err := s.UpdateStorySyncState(ctx, "s1", "MATERIALIZED"); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func getTestDSN() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	conn, err := net.DialTimeout("tcp", "127.0.0.1:55432", 300*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return "postgres://iazio:iazio@127.0.0.1:55432/iazio?sslmode=disable"
	}
	return ""
}

func TestLiveStoreIfConfigured(t *testing.T) {
	dsn := getTestDSN()
	if dsn == "" {
		t.Skip("DATABASE_URL not set and 127.0.0.1:55432 unreachable; skipping live postgres store tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// 1. Prompt round-trip
	testPrompt := control.Prompt{
		ID:       "test-p1",
		Title:    "Test Title",
		Body:     "Test Body",
		Engine:   "agent",
		Status:   "READY",
		Revision: 1,
	}
	if err := store.SavePrompt(ctx, testPrompt); err != nil {
		t.Fatalf("SavePrompt: %v", err)
	}
	prompts, err := store.LoadPrompts(ctx)
	if err != nil {
		t.Fatalf("LoadPrompts: %v", err)
	}
	var found bool
	for _, p := range prompts {
		if p.ID == "test-p1" && p.Title == "Test Title" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("saved prompt test-p1 not found in loaded prompts")
	}
	if err := store.DeletePrompt(ctx, "test-p1"); err != nil {
		t.Fatalf("DeletePrompt: %v", err)
	}

	// 2. Repo round-trip
	testRepo := control.Repo{
		HostID:        "test-host-1",
		WorktreePath:  "/test/repo/path",
		CloneURL:      "local://test-host-1/test/repo/path",
		DefaultBranch: "main",
		Queue:         control.QueueOpen,
		Lock:          control.LockIdle,
	}
	if err := store.SaveRepo(ctx, testRepo); err != nil {
		t.Fatalf("SaveRepo: %v", err)
	}
	repos, err := store.LoadRepos(ctx)
	if err != nil {
		t.Fatalf("LoadRepos: %v", err)
	}
	found = false
	for _, r := range repos {
		if r.HostID == "test-host-1" && r.WorktreePath == "/test/repo/path" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("saved repo not found in loaded repos")
	}
	if err := store.DeleteRepo(ctx, "test-host-1", "/test/repo/path"); err != nil {
		t.Fatalf("DeleteRepo: %v", err)
	}

	// 3. Idea round-trip
	testIdea := control.Idea{
		ID:             "test-idea-1",
		Title:          "Idea Title",
		ShareURL:       "https://example.com/share",
		Transcript:     "some text",
		Status:         "NEW",
		StoryID:        "STORY-123",
		PreviousStatus: "DRAFT",
	}
	if err := store.SaveIdea(ctx, testIdea); err != nil {
		t.Fatalf("SaveIdea: %v", err)
	}
	if err := store.SaveIdeaArgs(ctx, "test-idea-2", "Title2", "url2", "tr2", "NEW", "s2"); err != nil {
		t.Fatalf("SaveIdeaArgs: %v", err)
	}
	ideas, err := store.LoadIdeas(ctx)
	if err != nil {
		t.Fatalf("LoadIdeas: %v", err)
	}
	if len(ideas) == 0 {
		t.Fatal("expected at least 1 idea")
	}

	// 4. Host round-trip
	now := time.Now().UTC()
	testHost := control.Host{
		ID:                  "host-pg-test",
		Name:                "Host Test",
		Kind:                "permanent",
		Presence:            "ONLINE",
		LastSeen:            now,
		DistributionProfile: "generic",
		Tools:               []control.Tool{{Name: "git", Path: "/usr/bin/git", Version: "2.40"}},
	}
	if err := store.SaveHost(ctx, testHost); err != nil {
		t.Fatalf("SaveHost: %v", err)
	}
	hosts, err := store.LoadHosts(ctx)
	if err != nil {
		t.Fatalf("LoadHosts: %v", err)
	}
	if len(hosts) == 0 {
		t.Fatal("expected at least 1 host")
	}

	// 5. Distribution Profile round-trip
	testProfile := control.DistributionProfile{
		Name:        "test-profile-1",
		Description: "profile desc",
		IsDefault:   false,
		InstallRoots: control.ProfileInstallRoots{
			Unix: control.ProfileInstallRootPaths{Primary: "/opt"},
		},
		Endpoints: map[string]string{"api": "http://localhost"},
		Tools:     []control.ProfileTool{{Name: "agent"}},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.SaveProfile(ctx, testProfile); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}
	profiles, err := store.LoadProfiles(ctx)
	if err != nil {
		t.Fatalf("LoadProfiles: %v", err)
	}
	found = false
	for _, p := range profiles {
		if p.Name == "test-profile-1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected test-profile-1 in loaded profiles")
	}
	if err := store.DeleteProfile(ctx, "test-profile-1"); err != nil {
		t.Fatalf("DeleteProfile: %v", err)
	}

	// 6. History round-trip
	testHistory := control.HistoryRow{
		JobID:           "job-pg-1",
		ScheduleID:      "sched-1",
		PromptTitle:     "Prompt 1",
		Engine:          "agent",
		Status:          "SUCCESS",
		Clean:           true,
		ASEComplete:     true,
		ConversationIDs: []string{"conv-1"},
		Reason:          "ok",
		ErrorMessage:    "",
		DiagnosticCode:  "",
		Model:           "gemini",
		Remediation:     "",
		CreatedAt:       now,
	}
	if err := store.SaveHistory(ctx, "host-pg-test", "/test/repo", testHistory); err != nil {
		t.Fatalf("SaveHistory: %v", err)
	}
	histMap, err := store.LoadHistory(ctx)
	if err != nil {
		t.Fatalf("LoadHistory: %v", err)
	}
	if len(histMap) == 0 {
		t.Fatal("expected history rows")
	}
	if err := store.DeleteHistory(ctx, "job-pg-1"); err != nil {
		t.Fatalf("DeleteHistory: %v", err)
	}
	if err := store.ClearRepoHistory(ctx, "host-pg-test", "/test/repo"); err != nil {
		t.Fatalf("ClearRepoHistory: %v", err)
	}

	// 7. GitCredential round-trip
	testCred := control.GitCredential{
		ID:        "cred-1",
		Name:      "test-cred",
		Pattern:   "github.com/*",
		Token:     "ghp_secret",
		Type:      "pat",
		CreatedAt: now,
	}
	if err := store.SaveGitCredential(ctx, testCred); err != nil {
		t.Fatalf("SaveGitCredential: %v", err)
	}
	creds, err := store.LoadGitCredentials(ctx)
	if err != nil {
		t.Fatalf("LoadGitCredentials: %v", err)
	}
	if len(creds) == 0 {
		t.Fatal("expected git credentials")
	}
	if err := store.DeleteGitCredential(ctx, "cred-1"); err != nil {
		t.Fatalf("DeleteGitCredential: %v", err)
	}

	// 8. Codebase Repository round-trip
	cbRepo := codebase.Repository{
		Name:          "codebase-test-repo",
		Path:          "test/path",
		DefaultBranch: "main",
		GitTreeSHA:    "tree123",
		Description:   "desc",
		HostID:        "host-1",
		UpdatedAt:     now,
	}
	if err := store.SaveCodebaseRepo(ctx, cbRepo); err != nil {
		t.Fatalf("SaveCodebaseRepo: %v", err)
	}
	cRepos, err := store.LoadCodebaseRepos(ctx)
	if err != nil {
		t.Fatalf("LoadCodebaseRepos: %v", err)
	}
	if len(cRepos) == 0 {
		t.Fatal("expected codebase repos")
	}

	// 9. Codebase Chunks round-trip
	cbChunks := []codebase.CodeChunk{
		{
			ID:            "chunk-1",
			RepoName:      "codebase-test-repo",
			FilePath:      "main.go",
			StartLine:     1,
			EndLine:       10,
			Symbol:        "main",
			Content:       "package main",
			FileBlobSHA:   "blob123",
			GitTreeSHA:    "tree123",
			ContentSHA256: "sha256",
			Superseded:    false,
		},
	}
	if err := store.SaveCodebaseChunks(ctx, cbChunks); err != nil {
		t.Fatalf("SaveCodebaseChunks: %v", err)
	}
	chunks, err := store.LoadCodebaseChunks(ctx, "codebase-test-repo")
	if err != nil {
		t.Fatalf("LoadCodebaseChunks: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected codebase chunks")
	}

	// 10. Stories round-trip
	st := control.Story{
		ID:                   "story-pg-1",
		Repo:                 "app-repo",
		Submodule:            "iazio-mcp",
		Title:                "Test Story PG",
		Priority:             "P1",
		Status:               "pending",
		SyncState:            "PENDING_WORKSPACE_MATERIALIZATION",
		Body:                 "# Content",
		Problem:              "Problem desc",
		AcceptanceCriteria:   []string{"ac1"},
		TargetFiles:          []string{"f1.go"},
		DependsOn:            []string{"dep1"},
		VerificationCommands: []string{"make test"},
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if err := store.SaveStory(ctx, st); err != nil {
		t.Fatalf("SaveStory: %v", err)
	}
	stories, err := store.LoadStories(ctx)
	if err != nil {
		t.Fatalf("LoadStories: %v", err)
	}
	if len(stories) == 0 {
		t.Fatal("expected stories")
	}
	if err := store.UpdateStorySyncState(ctx, "story-pg-1", "MATERIALIZED"); err != nil {
		t.Fatalf("UpdateStorySyncState: %v", err)
	}
}
