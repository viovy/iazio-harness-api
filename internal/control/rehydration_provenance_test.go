package control

import (
	"testing"
	"time"
)

func TestRehydrationProvenanceAndCatalogCoverage(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	e := NewEngine(func() time.Time { return now })

	// Nil and empty edge cases
	var nilEngine *Engine
	if _, _, _, _, _, _, ok := nilEngine.FindJobMetadataByID("j1"); ok {
		t.Fatal("expected false on nil engine")
	}
	if _, ok := nilEngine.FindProvenanceByConversationID("c1"); ok {
		t.Fatal("expected false on nil engine")
	}
	if _, _, _, _, _, _, ok := e.FindJobMetadataByID("  "); ok {
		t.Fatal("expected false on empty jobID")
	}
	if _, ok := e.FindProvenanceByConversationID("  "); ok {
		t.Fatal("expected false on empty convID")
	}

	// Test isLineageNodeReviewSession branches
	if !isLineageNodeReviewSession(ConversationLineage{Role: "child_review"}, "c2", "c1") {
		t.Fatal("expected review role to match")
	}
	if !isLineageNodeReviewSession(ConversationLineage{ASEPhase: "PHASE_4_REVIEW_LOOP"}, "c2", "c1") {
		t.Fatal("expected phase_4 to match")
	}
	if !isLineageNodeReviewSession(ConversationLineage{Reason: "review-scheme run"}, "c2", "c1") {
		t.Fatal("expected reason review-scheme to match")
	}
	if !isLineageNodeReviewSession(ConversationLineage{Role: "legacy_unpinned_secondary"}, "c2", "c1") {
		t.Fatal("expected secondary unpinned to match")
	}
	if !isLineageNodeReviewSession(ConversationLineage{Role: "custom", ASEPhase: "4"}, "c2", "c1") {
		t.Fatal("expected secondary phase 4 to match")
	}
	if isLineageNodeReviewSession(ConversationLineage{Role: "initial_active"}, "c1", "c1") {
		t.Fatal("expected primary initial_active not to be review session")
	}

	// Test dedupeConversationList
	deduped := dedupeConversationList("c-init", "c-active", []string{"c-init", "c-extra", ""}, []ConversationLineage{{ConversationID: "c-lin"}, {ConversationID: "c-extra"}}, "c-match")
	if len(deduped) != 5 {
		t.Fatalf("expected 5 deduped conversation IDs, got %v", deduped)
	}

	// Populate history, jobs, schedules, repos, and hosts for 5-tier provenance resolution
	e.mu.Lock()
	e.schedules["sch-root"] = &Schedule{
		ID:                    "sch-root",
		StoryID:               "STORY-GOV-5200",
		HostID:                "host-a",
		WorktreePath:          "/work/iazio-harvester-api",
		Engine:                "opencode",
		CustomPrompt:          "Implement STORY-GOV-5200",
		EnvVars:               map[string]string{"OPENCODE_MODEL": "claude-sonnet-4-6"},
		InitialConversationID: "conv-sch-1",
		ActiveConversationID:  "conv-sch-1",
		Status:                "RUNNING",
	}
	e.schedules["sch-child"] = &Schedule{
		ID:                    "sch-child",
		ParentJobID:           "job-root",
		DependsOnJobID:        "job-root",
		RootJobID:             "job-root",
		HostID:                "host-a",
		TargetSubmodule:       "iazio-mcp",
		Engine:                "cursor",
		PromptTitle:           "review-scheme run STORY-GOV-5200",
		ActiveConversationID:  "conv-sch-review",
		AssociatedJobs:        []string{"job-sch-child"},
		Status:                "COMPLETED",
	}
	e.jobs["job-root"] = &Job{
		ID:                    "job-root",
		ScheduleID:            "sch-root",
		StoryID:               "STORY-GOV-5200",
		HostID:                "host-a",
		Status:                "COMPLETED",
		InitialConversationID: "conv-job-root",
		ActiveConversationID:  "conv-job-root",
	}
	e.jobs["job-active-review"] = &Job{
		ID:                    "job-active-review",
		ScheduleID:            "sch-root",
		ParentJobID:           "job-root",
		TargetHost:            "host-a",
		Status:                "RUNNING",
		NormalizedPrompt:      "Phase 4 review-scheme run for STORY-GOV-5200",
		InitialConversationID: "conv-job-root",
		ConversationIDs:       []string{"conv-job-active-child"},
		ConversationLineage: []ConversationLineage{
			{ConversationID: "conv-job-active-child", Role: "review_subagent", ASEPhase: "4a"},
		},
	}
	e.history["host-a\x00/work/iazio-harvester-api"] = []HistoryRow{
		{
			JobID:                 "job-hist-root",
			ScheduleID:            "sch-root",
			StoryID:               "STORY-GOV-5200",
			PromptTitle:           "Execute STORY-GOV-5200",
			Engine:                "claude",
			Model:                 "claude-opus-4-6",
			Status:                "SUCCESS",
			CreatedAt:             now.Add(-10 * time.Minute),
			InitialConversationID: "conv-hist-root",
			ActiveConversationID:  "conv-hist-root",
		},
		{
			JobID:                 "job-hist-child",
			ScheduleID:            "sch-root",
			ExecutionChain:        []string{"job-hist-root", "job-hist-child"},
			PromptTitle:           "Phase 4 review subagent STORY-GOV-5200",
			Engine:                "opencode",
			Status:                "SUCCESS",
			CreatedAt:             now.Add(-5 * time.Minute),
			InitialConversationID: "conv-hist-root",
			ConversationIDs:       []string{"conv-hist-child"},
			ConversationLineage: []ConversationLineage{
				{ConversationID: "conv-hist-child", Role: "child_review", Reason: "review-scheme"},
			},
		},
	}
	e.repos["host-a\x00/work/iazio-web"] = &Repo{
		HostID:                 "host-a",
		WorktreePath:           "/work/iazio-web",
		ActiveTool:             "agy",
		DirtyStoryID:           "STORY-GOV-5200",
		DirtyReviewFile:        "docs/reviews/review.md",
		DetectedConversationID: "conv-repo-detected",
	}
	e.hosts["host-a"] = &Host{
		ID:       "host-a",
		Name:     "Host A",
		Presence: "ONLINE",
		Tools:    []Tool{{Name: "git", Version: "2.40"}, {Name: "opencode", Version: "1.0"}},
		RepoStatuses: map[string]RepoRuntimeStatus{
			"iazio-mcp": {
				RepoPath:             "/work/iazio-mcp",
				Alias:                "iazio-mcp",
				ActiveTool:           "claude",
				ActiveStoryID:        "STORY-GOV-5200",
				ASEState:             "PHASE_4_REVIEW",
				ActiveConversationID: "conv-host-status",
			},
		},
	}
	e.mu.Unlock()

	// Verify FindJobMetadataByID across active jobs, history, and schedules
	sID, pID, rID, hID, repo, _, ok := e.FindJobMetadataByID("job-active-review")
	if !ok || sID != "STORY-GOV-5200" || pID != "job-root" || rID != "job-root" || hID != "host-a" || repo != "iazio-harvester-api" {
		t.Fatalf("unexpected FindJobMetadataByID for job-active-review: ok=%v sID=%q pID=%q rID=%q hID=%q repo=%q", ok, sID, pID, rID, hID, repo)
	}
	if sID2, _, _, _, _, _, ok2 := e.FindJobMetadataByID("job-hist-root"); !ok2 || sID2 != "STORY-GOV-5200" {
		t.Fatalf("unexpected FindJobMetadataByID for job-hist-root: ok=%v sID=%q", ok2, sID2)
	}
	if sID3, _, _, _, _, _, ok3 := e.FindJobMetadataByID("sch-root"); !ok3 || sID3 != "STORY-GOV-5200" {
		t.Fatalf("unexpected FindJobMetadataByID for sch-root: ok=%v sID=%q", ok3, sID3)
	}

	// Tier 1: History child review session
	prov1, ok := e.FindProvenanceByConversationID("conv-hist-child")
	if !ok || !prov1.IsChildReviewSession || prov1.ParentJobID != "job-hist-root" || prov1.StoryID != "STORY-GOV-5200" || prov1.Harness != "opencode" {
		t.Fatalf("unexpected Tier 1 provenance: ok=%v %+v", ok, prov1)
	}

	// Tier 2: Active job child review session
	prov2, ok := e.FindProvenanceByConversationID("conv-job-active-child")
	if !ok || !prov2.IsChildReviewSession || prov2.ParentJobID != "job-root" || prov2.Model != "claude-sonnet-4-6" {
		t.Fatalf("unexpected Tier 2 provenance: ok=%v %+v", ok, prov2)
	}

	// Tier 3: Schedule conversation
	prov3, ok := e.FindProvenanceByConversationID("conv-sch-review")
	if !ok || !prov3.IsChildReviewSession || prov3.JobID != "job-sch-child" || prov3.Harness != "cursor" {
		t.Fatalf("unexpected Tier 3 provenance: ok=%v %+v", ok, prov3)
	}

	// Tier 4: Repo runtime state
	prov4, ok := e.FindProvenanceByConversationID("conv-repo-detected")
	if !ok || !prov4.IsChildReviewSession || prov4.Repo != "iazio-web" || prov4.StoryID != "STORY-GOV-5200" {
		t.Fatalf("unexpected Tier 4 provenance: ok=%v %+v", ok, prov4)
	}

	// Tier 5: Host repo runtime status
	prov5, ok := e.FindProvenanceByConversationID("conv-host-status")
	if !ok || !prov5.IsChildReviewSession || prov5.Repo != "iazio-mcp" || prov5.Harness != "claude" {
		t.Fatalf("unexpected Tier 5 provenance: ok=%v %+v", ok, prov5)
	}

	// Catalog helper coverage
	e.SetHostDevBinaries("host-a", []string{"/tmp/dev-bin"})
	e.SetHostEagerUpdate("host-a", true, &now)
	e.UpdateEagerReadiness("host-a", EagerUpdateReadinessReport{})
	if _, ok := e.GetEagerReadiness("host-a"); !ok {
		t.Fatalf("expected eager readiness report, got %v", ok)
	}
	if err := e.EvaluateEagerUpdate("host-a"); err != nil {
		t.Fatalf("EvaluateEagerUpdate: %v", err)
	}
	if err := e.ForceEagerUpdate("host-a"); err != nil {
		t.Fatalf("ForceEagerUpdate: %v", err)
	}
	if err := e.DeleteTool("host-a", "opencode"); err != nil {
		t.Fatalf("DeleteTool: %v", err)
	}
	if err := e.DeleteTool("host-a", "nonexistent"); err == nil {
		t.Fatal("expected error deleting nonexistent tool")
	}
	e.ReleaseRepoLockIfMatchingJob("host-a", "")
}
