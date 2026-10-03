package pgstore

import (
	"context"
	"os"
	"testing"
	"time"

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

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLiveStoreIfConfigured(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live postgres store tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	// Test Prompt round-trip
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

	// Test delete prompt
	if err := store.DeletePrompt(ctx, "test-p1"); err != nil {
		t.Fatalf("DeletePrompt: %v", err)
	}

	// Test Repo round-trip
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

	// Test delete repo
	if err := store.DeleteRepo(ctx, "test-host-1", "/test/repo/path"); err != nil {
		t.Fatalf("DeleteRepo: %v", err)
	}
}
