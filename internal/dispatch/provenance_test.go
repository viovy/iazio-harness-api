package dispatch

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNormalizeHarnessAndInferModelAuthor(t *testing.T) {
	harnessCases := []struct {
		raw  string
		want string
	}{
		{"agy", "agy"},
		{"antigravity", "agy"},
		{"antigravity-cli", "agy"},
		{"gemini-cli", "agy"},
		{"opencode", "opencode"},
		{"open-code", "opencode"},
		{"cursor", "cursor"},
		{"cursor-agent", "cursor"},
		{"agent", "cursor"},
		{"claude", "claude"},
		{"claude-code", "claude"},
		{"autopilot", "autopilot"},
		{"codex", "codex"},
		{"codex-cli", "codex"},
		{"auto", ""},
		{"", ""},
	}
	for _, tc := range harnessCases {
		if got := NormalizeHarnessName(tc.raw); got != tc.want {
			t.Errorf("NormalizeHarnessName(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	authorCases := []struct {
		model   string
		harness string
		want    string
	}{
		{"gemini-2.5-pro", "agy", "google"},
		{"google/gemini-2.5-flash", "opencode", "google"},
		{"claude-sonnet-4-20250514", "claude", "anthropic"},
		{"anthropic/claude-3-7-sonnet", "opencode", "anthropic"},
		{"gpt-5-codex", "codex", "openai"},
		{"o3-mini", "cursor", "openai"},
		{"o1-preview", "opencode", "openai"},
		{"deepseek-r1", "opencode", "deepseek"},
		{"qwen-2.5-coder-32b", "opencode", "qwen"},
		{"grok-3", "opencode", "xai"},
		{"", "agy", "google"},
		{"", "claude", "anthropic"},
		{"", "codex", "openai"},
	}
	for _, tc := range authorCases {
		if got := InferModelAuthor(tc.model, tc.harness); got != tc.want {
			t.Errorf("InferModelAuthor(%q, %q) = %q, want %q", tc.model, tc.harness, got, tc.want)
		}
	}
}

func TestQueueProvenance_RootAndChildReviewSession(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()

	rootPayload, _ := json.Marshal(map[string]any{
		"story_id":        "STORY-GOV-5200",
		"prompt":          "/autonomous-story-execution STORY-GOV-5200",
		"target_agent":    "antigravity",
		"model":           "gemini-2.5-pro",
		"worktree_path":   "/home/romeo/work/workspace",
		"conversation_id": "conv-root-5200",
	})

	rootJob, err := q.Enqueue(ctx, &Job{
		Type:                  JobTypeStoryExecute,
		Repo:                  "workspace",
		WorktreePath:          "/home/romeo/work/workspace",
		TargetHost:            "pc1",
		HostID:                "pc1",
		TargetAgent:           "antigravity",
		StoryID:               "STORY-GOV-5200",
		NormalizedPrompt:      "/autonomous-story-execution STORY-GOV-5200",
		EffectiveAction:       "/autonomous-story-execution",
		ActiveConversationID:  "conv-root-5200",
		InitialConversationID: "conv-root-5200",
		ConversationIDs:       []string{"conv-root-5200"},
		// Bare 16-hex Zipkin span ID should not be mistaken for a parent job ID
		ParentID: "0123456789abcdef",
		Payload:  rootPayload,
	})
	if err != nil {
		t.Fatalf("Enqueue rootJob failed: %v", err)
	}

	// 1. Lookup root ASE orchestrator job by conversation ID
	rootProv, found := q.FindProvenanceByConversationID("conv-root-5200")
	if !found {
		t.Fatalf("expected to find root provenance for conv-root-5200")
	}
	if rootProv.JobID != rootJob.ID {
		t.Errorf("root JobID = %q, want %q", rootProv.JobID, rootJob.ID)
	}
	if rootProv.StoryID != "STORY-GOV-5200" {
		t.Errorf("root StoryID = %q, want STORY-GOV-5200", rootProv.StoryID)
	}
	if rootProv.Harness != "agy" {
		t.Errorf("root Harness = %q, want agy", rootProv.Harness)
	}
	if rootProv.Model != "gemini-2.5-pro" || rootProv.ModelAuthor != "google" {
		t.Errorf("root Model/Author = %q/%q, want gemini-2.5-pro/google", rootProv.Model, rootProv.ModelAuthor)
	}
	if rootProv.ParentJobID != "" {
		t.Errorf("expected empty ParentJobID for root job, got %q", rootProv.ParentJobID)
	}
	if rootProv.RootJobID != rootJob.ID {
		t.Errorf("root RootJobID = %q, want %q", rootProv.RootJobID, rootJob.ID)
	}
	if rootProv.IsChildReviewSession {
		t.Errorf("expected IsChildReviewSession=false for root job")
	}

	// 2. Enqueue child Phase 4 review session referencing rootJob via ParentID, omitting StoryID to verify inheritance
	childPayload, _ := json.Marshal(map[string]any{
		"parent_job_id":          rootJob.ID,
		"role":                   "adversarial-reviewer",
		"prompt":                 "Run Phase 4 review-scheme adversarial-reviewer and write docs/reviews/2026-11-08-STORY-GOV-5200-iter-1-opencode.md",
		"review_model":           "claude-sonnet-4-20250514",
		"review_iteration":       1,
		"active_conversation_id": "conv-child-review-5200",
	})
	childJob, err := q.Enqueue(ctx, &Job{
		Type:        JobTypePromptExecute,
		Repo:        "iazio-harness-api",
		TargetAgent: "opencode",
		ParentID:    rootJob.ID,
		Payload:     childPayload,
	})
	if err != nil {
		t.Fatalf("Enqueue childJob failed: %v", err)
	}

	childProv, found := ResolveQueueProvenanceByConversationID(ctx, q, "conv-child-review-5200")
	if !found {
		t.Fatalf("expected to find child provenance for conv-child-review-5200")
	}
	if childProv.JobID != childJob.ID {
		t.Errorf("child JobID = %q, want %q", childProv.JobID, childJob.ID)
	}
	if childProv.ParentJobID != rootJob.ID {
		t.Errorf("child ParentJobID = %q, want %q", childProv.ParentJobID, rootJob.ID)
	}
	if childProv.RootJobID != rootJob.ID {
		t.Errorf("child RootJobID = %q, want %q", childProv.RootJobID, rootJob.ID)
	}
	if childProv.StoryID != "STORY-GOV-5200" {
		t.Errorf("child inherited StoryID = %q, want STORY-GOV-5200", childProv.StoryID)
	}
	if !childProv.IsChildReviewSession {
		t.Errorf("expected IsChildReviewSession=true for child Phase 4 review session")
	}
	if childProv.Harness != "opencode" {
		t.Errorf("child Harness = %q, want opencode", childProv.Harness)
	}
	if childProv.Model != "claude-sonnet-4-20250514" || childProv.ModelAuthor != "anthropic" {
		t.Errorf("child Model/Author = %q/%q, want claude-sonnet-4-20250514/anthropic", childProv.Model, childProv.ModelAuthor)
	}
	if childProv.HostID != "pc1" {
		t.Errorf("child inherited HostID = %q, want pc1", childProv.HostID)
	}
	if childProv.WorktreePath != "/home/romeo/work/workspace" {
		t.Errorf("child inherited WorktreePath = %q, want /home/romeo/work/workspace", childProv.WorktreePath)
	}
}

func TestStoryGov5223_ExtractStoryIDFromPromptIgnoresReviewArtifactPaths(t *testing.T) {
	prompt := `/autonomous-story-execution Please read docs/reviews/2026-11-08-STORY-VIGO-SECURITY-TRANSPORT-HYGIENE-iter-1-opencode.md and fix all issues.`
	if got := ExtractStoryIDFromPrompt(prompt); got != "" {
		t.Fatalf("expected empty story ID when prompt only contains non-canonical review artifact filename, got %q", got)
	}
	promptWithCanonical := `/autonomous-story-execution STORY-VIGO-0289 Please read docs/reviews/2026-11-08-STORY-VIGO-SECURITY-TRANSPORT-HYGIENE-iter-1-opencode.md`
	if got := ExtractStoryIDFromPrompt(promptWithCanonical); got != "STORY-VIGO-0289" {
		t.Fatalf("expected STORY-VIGO-0289, got %q", got)
	}
}

