package control

import (
	"testing"
	"time"
)

func TestEngine_ReconcileHistoricalLineage_Job1311_1313(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConv := "aad902c8-bda6-44c7-8a68-c468a1562399"
	secConv1 := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"
	secConv2 := "14de9a1f-5679-4f11-a08f-f32bc7e47663"

	// Simulate dirty historical state for job-1311
	e.mu.Lock()
	e.history[repoKey("mac-mini", repoPath)] = []HistoryRow{
		{
			JobID:           "job-1311",
			Status:          "FAILED",
			Engine:          "agy",
			ConversationIDs: []string{initialConv},
			CreatedAt:       time.Now().Add(-2 * time.Hour),
		},
		{
			JobID:           "job-1313",
			Status:          "COMPLETED",
			Engine:          "agy",
			ConversationIDs: []string{secConv1, secConv2},
			CreatedAt:       time.Now().Add(-1 * time.Hour),
		},
	}
	e.mu.Unlock()

	// Run reconciliation
	count, err := e.ReconcileHistoricalLineage()
	if err != nil {
		t.Fatalf("ReconcileHistoricalLineage failed: %v", err)
	}
	if count < 2 {
		t.Fatalf("expected at least 2 reconciled rows, got %d", count)
	}

	detail, ok := e.GetRepoDetail("mac-mini", repoPath)
	if !ok {
		t.Fatal("expected repo detail to exist")
	}

	var row1311, row1313 *HistoryRow
	for i := range detail.History {
		if detail.History[i].JobID == "job-1311" {
			row1311 = &detail.History[i]
		} else if detail.History[i].JobID == "job-1313" {
			row1313 = &detail.History[i]
		}
	}

	if row1311 == nil || row1313 == nil {
		t.Fatalf("expected both job-1311 and job-1313 in history, got: %+v", detail.History)
	}

	if row1311.InitialConversationID != initialConv {
		t.Errorf("job-1311 InitialConversationID = %q, want %q", row1311.InitialConversationID, initialConv)
	}
	if row1313.ParentJobID != "job-1311" {
		t.Errorf("job-1313 ParentJobID = %q, want %q", row1313.ParentJobID, "job-1311")
	}
	if row1313.InitialConversationID != initialConv {
		t.Errorf("job-1313 InitialConversationID = %q, want %q", row1313.InitialConversationID, initialConv)
	}

	// Verify conversation roles
	var hasRoot, hasSec1, hasSec2 bool
	for _, node := range row1313.ConversationLineage {
		if node.ConversationID == initialConv && node.Role == "root_initial" {
			hasRoot = true
		}
		if node.ConversationID == secConv1 && node.Role == "legacy_unpinned_secondary" {
			hasSec1 = true
		}
		if node.ConversationID == secConv2 && node.Role == "legacy_unpinned_secondary" {
			hasSec2 = true
		}
	}
	if !hasRoot {
		t.Errorf("expected root_initial node for %s in job-1313 lineage: %+v", initialConv, row1313.ConversationLineage)
	}
	if !hasSec1 || !hasSec2 {
		t.Errorf("expected legacy_unpinned_secondary nodes for split-brain convs in job-1313 lineage: %+v", row1313.ConversationLineage)
	}

	// Now resume on job-1313
	resSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID: "job-1313",
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resSch.InitialConversationID != initialConv {
		t.Errorf("resumed schedule InitialConversationID = %q, want %q", resSch.InitialConversationID, initialConv)
	}
	if resSch.ParentJobID != "job-1313" {
		t.Errorf("resumed schedule ParentJobID = %q, want job-1313", resSch.ParentJobID)
	}
	if resSch.Engine != "agy" {
		t.Errorf("resumed schedule Engine = %q, want agy", resSch.Engine)
	}
}

func TestEngine_PreDispatchGate_CapabilityRequirement(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-legacy", "permanent")
	// Simulate legacy agent without deterministic_resume_4tuple_v1
	_ = e.HeartbeatWithCapabilities("host-legacy", "ONLINE", []Tool{
		{Name: "iazio-agent", Version: "0.1.0-legacy"},
	}, false, []string{"old_cap_v0"}, "0.1.0-legacy", "sha1")

	repoPath := "/workspace/legacy-repo"
	_ = e.UpsertRepo(Repo{
		HostID:       "host-legacy",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	// Queue a KindResume schedule
	e.mu.Lock()
	e.schedules["sch-resume-1"] = &Schedule{
		ID:                    "sch-resume-1",
		HostID:                "host-legacy",
		WorktreePath:          repoPath,
		Kind:                  KindResume,
		Status:                "QUEUED",
		InitialConversationID: "conv-init-123",
	}
	e.mu.Unlock()

	// 1. PollHost must be blocked by pre-dispatch capability gate
	_, _, _, ok := e.PollHost("host-legacy")
	if ok {
		t.Fatal("expected PollHost to be blocked due to missing deterministic_resume_4tuple_v1 capability")
	}

	// Verify pending tool update was queued
	e.mu.Lock()
	host := e.hosts["host-legacy"]
	e.mu.Unlock()
	if host == nil || host.PendingToolUpdate == nil {
		t.Fatal("expected PendingToolUpdate to be set on host lacking capability")
	}
	hasAgent := false
	for _, tool := range host.PendingToolUpdate.Tools {
		if tool == "iazio-agent" {
			hasAgent = true
			break
		}
	}
	if !hasAgent {
		t.Fatalf("expected iazio-agent in PendingToolUpdate, got %+v", host.PendingToolUpdate)
	}

	// 2. Host performs update and reports new capability in heartbeat
	_ = e.HeartbeatWithCapabilities("host-legacy", "ONLINE", []Tool{
		{
			Name:         "iazio-agent",
			Version:      "0.2.0-modern",
			Capabilities: []string{"deterministic_resume_4tuple_v1"},
		},
	}, false, []string{"user_service_watchdog_v2", "deterministic_resume_4tuple_v1"}, "0.2.0-modern", "sha2")

	// 3. PollHost now succeeds
	leasedJob, leasedSch, _, ok := e.PollHost("host-legacy")
	if !ok {
		t.Fatal("expected PollHost to succeed after capability advertisement")
	}
	if leasedSch.ID != "sch-resume-1" {
		t.Errorf("leased schedule ID = %q, want sch-resume-1", leasedSch.ID)
	}
	if leasedJob.InitialConversationID != "conv-init-123" {
		t.Errorf("leased job InitialConversationID = %q, want conv-init-123", leasedJob.InitialConversationID)
	}
}
