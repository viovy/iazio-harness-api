package dispatch

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func buildMockAgyBinary(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		batPath := filepath.Join(dir, "mock-agy.bat")
		content := "@echo off\r\necho {\"status\":\"completed\",\"runner\":\"mock-agy\",\"job_id\":\"%MOCK_JOB_ID%\",\"host_id\":\"%MOCK_HOST_ID%\"}\r\n"
		if err := os.WriteFile(batPath, []byte(content), 0o755); err != nil {
			t.Fatalf("write mock-agy.bat: %v", err)
		}
		return batPath
	}
	shPath := filepath.Join(dir, "mock-agy")
	content := "#!/bin/sh\necho \"{\\\"status\\\":\\\"completed\\\",\\\"runner\\\":\\\"mock-agy\\\",\\\"job_id\\\":\\\"${MOCK_JOB_ID}\\\",\\\"host_id\\\":\\\"${MOCK_HOST_ID}\\\"}\"\n"
	if err := os.WriteFile(shPath, []byte(content), 0o755); err != nil {
		t.Fatalf("write mock-agy: %v", err)
	}
	return shPath
}

func runMockAgyForClaimedJob(t *testing.T, ctx context.Context, mockAgyBin string, q *InMemoryQueue, job *Job, hostID string) {
	t.Helper()
	if _, err := q.AckLease(ctx, job.ID, job.LeaseToken, 4242, "conv-mock-"+job.ID); err != nil {
		t.Fatalf("AckLease(%s, %s): %v", job.ID, hostID, err)
	}
	cmd := exec.CommandContext(ctx, mockAgyBin, "chat", "--mode", "agent", "execute "+job.StoryID)
	cmd.Env = append(os.Environ(),
		"MOCK_JOB_ID="+job.ID,
		"MOCK_HOST_ID="+hostID,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mock agy execution failed for %s on %s: %v (%s)", job.ID, hostID, err, string(out))
	}
	resJSON := json.RawMessage(strings.TrimSpace(string(out)))
	if err := q.Complete(ctx, job.ID, resJSON, ""); err != nil {
		t.Fatalf("Complete(%s): %v", job.ID, err)
	}
}

// TestMockAgy_GlobalQueueIntegrityAndUnpinRemediation verifies all 4 global-queue discoveries:
// 1. A job stuck in `requeued` with `result.status == "completed_via_eager_readiness"` and no replacement child is reconciled to `completed`.
// 2. A parent job stuck in `pending` with stale `host_id`/`claimed_host` despite an active replacement child is reconciled to `requeued` and cleared.
// 3. `reapExpiredLocked` and `reconcileQueueIntegrityLocked` do NOT pin `target_host: "debian"` on global-queue jobs that never started an active/resumable conversation (`ResumeConversationID == ""`), allowing `mac-mini` and `pc1-wsl` to claim and execute them with mock `agy`.
// 4. `ForceDispatch` clears stale `target_host` on unstarted `global_queue_tool_priority` jobs.
func TestMockAgy_GlobalQueueIntegrityAndUnpinRemediation(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mockAgyBin := buildMockAgyBinary(t, tmpDir)

	now := time.Date(2026, 11, 12, 21, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.nowFunc = func() time.Time { return now }

	// Seed Discovery 4A: job_e28047b251eff446 stuck in `requeued` despite completed_via_eager_readiness
	eagerJob := &Job{
		ID:               "job_e28047b251eff446",
		Type:             "agent.prompt",
		Repo:             "meta-repo",
		StoryID:          "STORY-GOV-5259",
		AllocationSource: "global_queue_tool_priority",
		Status:           JobStatusRequeued,
		Result:           json.RawMessage(`{"status":"completed_via_eager_readiness","story_id":"STORY-GOV-5259"}`),
		CreatedAt:        now.Add(-30 * time.Minute),
		doneCh:           make(chan struct{}),
	}
	q.jobs[eagerJob.ID] = eagerJob

	// Seed Discovery 4B: duplicate parent job_65fe8da2229b6368 stuck in `pending` with stale debian claim + replacement child job_57ce7560f7e59336
	dupParent := &Job{
		ID:               "job_65fe8da2229b6368",
		Type:             "agent.prompt",
		Repo:             "iazio-infra",
		StoryID:          "STORY-IAZIO-0189",
		AllocationSource: "global_queue_tool_priority",
		TargetHost:       "debian",
		HostID:           "debian",
		ClaimedHost:      "debian",
		Status:           JobStatusPending,
		CreatedAt:        now.Add(-25 * time.Minute),
		doneCh:           make(chan struct{}),
	}
	q.jobs[dupParent.ID] = dupParent

	childJob := &Job{
		ID:               "job_57ce7560f7e59336",
		ParentID:         dupParent.ID,
		Type:             "agent.prompt",
		Repo:             "iazio-infra",
		StoryID:          "STORY-IAZIO-0189",
		AllocationSource: "global_queue_tool_priority",
		TargetHost:       "debian", // Falsely inherited from parent when no conversation existed
		Payload:          json.RawMessage(`{"prompt":"/autonomous-story-execution STORY-IAZIO-0189","target_host":"debian"}`),
		Status:           JobStatusPending,
		CreatedAt:        now.Add(-20 * time.Minute),
		doneCh:           make(chan struct{}),
	}
	q.jobs[childJob.ID] = childJob

	// Seed Discovery 4C: claimed job on `debian` whose lease expires BEFORE starting a conversation (ResumeConversationID == "")
	expiredClaimAt := now.Add(-10 * time.Minute)
	expiredLeaseAt := now.Add(-1 * time.Minute)
	reapJob := &Job{
		ID:               "job_819dedb61d8970c1",
		Type:             "agent.prompt",
		Repo:             "meta-repo",
		StoryID:          "STORY-GOV-5255",
		AllocationSource: "global_queue_tool_priority",
		HostID:           "debian",
		ClaimedHost:      "debian",
		LeaseOwner:       "debian",
		LeaseToken:       "lease-expired-1",
		Status:           JobStatusClaimed,
		ClaimedAt:        &expiredClaimAt,
		LeaseExpiresAt:   &expiredLeaseAt,
		CreatedAt:        now.Add(-15 * time.Minute),
		doneCh:           make(chan struct{}),
	}
	q.jobs[reapJob.ID] = reapJob

	// Run ReapExpired (which invokes reconcileQueueIntegrityLocked + lease reaper)
	q.ReapExpired(now)

	// Verify 4A: eagerJob is now completed
	gotEager, ok := q.Get(eagerJob.ID)
	if !ok {
		t.Fatalf("Get eagerJob: not found")
	}
	if gotEager.Status != JobStatusCompleted {
		t.Fatalf("expected eagerJob status %q, got %q", JobStatusCompleted, gotEager.Status)
	}

	// Verify 4B: dupParent transitioned to requeued and cleared stale HostID/ClaimedHost
	gotParent, ok := q.Get(dupParent.ID)
	if !ok {
		t.Fatalf("Get dupParent: not found")
	}
	if gotParent.Status != JobStatusRequeued {
		t.Fatalf("expected dupParent status %q, got %q", JobStatusRequeued, gotParent.Status)
	}
	if gotParent.HostID != "" || gotParent.ClaimedHost != "" {
		t.Fatalf("expected dupParent HostID/ClaimedHost cleared, got %q / %q", gotParent.HostID, gotParent.ClaimedHost)
	}

	// Verify 4C: childJob and reapJob both have TargetHost == "" so non-debian hosts can claim them
	gotChild, ok := q.Get(childJob.ID)
	if !ok {
		t.Fatalf("Get childJob: not found")
	}
	if gotChild.TargetHost != "" {
		t.Fatalf("expected childJob TargetHost unpinned, got %q", gotChild.TargetHost)
	}
	gotReap, ok := q.Get(reapJob.ID)
	if !ok {
		t.Fatalf("Get reapJob: not found")
	}
	if gotReap.Status != JobStatusPending || gotReap.TargetHost != "" {
		t.Fatalf("expected reapJob pending with empty TargetHost, got status=%q target_host=%q", gotReap.Status, gotReap.TargetHost)
	}

	// Claim and execute first unpinned job on `mac-mini` using mock `agy`
	claimed1, err := q.Claim(ctx, "mac-mini", []string{"agent.prompt"})
	if err != nil || claimed1 == nil {
		t.Fatalf("expected mac-mini to claim unpinned job, got job=%v err=%v", claimed1, err)
	}
	runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, claimed1, "mac-mini")

	// Claim and execute second unpinned job on `pc1-wsl` using mock `agy`
	claimed2, err := q.Claim(ctx, "pc1-wsl", []string{"agent.prompt"})
	if err != nil || claimed2 == nil {
		t.Fatalf("expected pc1-wsl to claim second unpinned job, got job=%v err=%v", claimed2, err)
	}
	runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, claimed2, "pc1-wsl")

	done1, _ := q.Get(claimed1.ID)
	done2, _ := q.Get(claimed2.ID)
	if done1.Status != JobStatusCompleted || done2.Status != JobStatusCompleted {
		t.Fatalf("expected both claimed jobs completed by mock agy, got %s=%s, %s=%s",
			done1.ID, done1.Status, done2.ID, done2.Status)
	}
}

// TestMockAgy_NeverAbandonSameJobIDAutoremediate (STORY-GOV-5275) verifies:
// 1. Remediable ASE-gate failure requeues the SAME job ID to pending (claimable).
// 2. Orphan requeued jobs without replacement children are promoted to pending and claimable.
// 3. TOOL_QUOTA_OR_AUTH / infra-down are terminal stops (Retriable=false); mock-agy never resumes them.
// 4. After remediable failure, mac-mini claims the original job ID and mock-agy completes it.
func TestMockAgy_NeverAbandonSameJobIDAutoremediate(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mockAgyBin := buildMockAgyBinary(t, tmpDir)
	now := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.nowFunc = func() time.Time { return now }

	origID := "job_3ffa5d0be1649f0e"
	job := &Job{
		ID:               origID,
		Type:             "agent.prompt",
		Repo:             "meta-repo",
		StoryID:          "STORY-GOV-5275",
		AllocationSource: "global_queue_tool_priority",
		Status:           JobStatusPending,
		TargetAgent:      "agy",
		CreatedAt:        now.Add(-time.Hour),
		Payload:          json.RawMessage(`{"prompt":"fix never abandon"}`),
		doneCh:           make(chan struct{}),
	}
	q.jobs[job.ID] = job

	claimed, err := q.Claim(ctx, "mac-mini", []string{"agent.prompt"})
	if err != nil || claimed == nil || claimed.ID != origID {
		t.Fatalf("initial claim of %s failed: job=%v err=%v", origID, claimed, err)
	}
	if _, err := q.AckLease(ctx, claimed.ID, claimed.LeaseToken, 4242, "conv-5275"); err != nil {
		t.Fatalf("AckLease: %v", err)
	}

	// Remediable ASE-gate incomplete must keep SAME id pending (never abandon).
	if err := q.Complete(ctx, claimed.ID, json.RawMessage(`{"status":"failed"}`), "ASE_GATE_INCOMPLETE: unmerged_feature_branch"); err != nil {
		t.Fatalf("Complete remediable: %v", err)
	}
	after, ok := q.Get(origID)
	if !ok || after.Status != JobStatusPending {
		t.Fatalf("expected %s pending in-place after remediable ASE gate, got ok=%v status=%v", origID, ok, after)
	}
	if IsTerminalStopFailure(FailureCategoryASEGateIncomplete, "ASE_GATE_INCOMPLETE") {
		t.Fatal("ASE_GATE_INCOMPLETE must not be terminal stop")
	}

	// Orphan requeued without replacement child → pending via reconcile.
	orphanID := "job_985bc2111a0944cd"
	orphan := &Job{
		ID:               orphanID,
		Type:             "agent.prompt",
		Repo:             "meta-repo",
		StoryID:          "STORY-GOV-5275-orphan",
		AllocationSource: "global_queue_tool_priority",
		Status:           JobStatusRequeued,
		CreatedAt:        now.Add(-2 * time.Hour),
		doneCh:           make(chan struct{}),
	}
	q.jobs[orphan.ID] = orphan
	_ = q.reconcileQueueIntegrityLocked(now)
	gotOrphan, _ := q.Get(orphanID)
	if gotOrphan.Status != JobStatusPending {
		t.Fatalf("expected orphan requeued %s promoted to pending, got %s", orphanID, gotOrphan.Status)
	}

	// Terminal stop: credits
	if !IsTerminalStopFailure(FailureCategoryToolQuotaOrAuth, "quota exhausted") {
		t.Fatal("TOOL_QUOTA_OR_AUTH must be terminal stop")
	}
	creditsID := "job_97d33fd156a88144"
	creditsJob := &Job{
		ID:     creditsID,
		Type:   "agent.prompt",
		Repo:   "meta-repo",
		Status: JobStatusPending,
		doneCh: make(chan struct{}),
	}
	q.jobs[creditsJob.ID] = creditsJob
	cClaim, err := q.Claim(ctx, "pc1", []string{"agent.prompt"})
	if err != nil || cClaim == nil {
		t.Fatalf("claim credits job: %v %v", cClaim, err)
	}
	if _, err := q.AckLease(ctx, cClaim.ID, cClaim.LeaseToken, 1, "conv-credits"); err != nil {
		t.Fatalf("ack credits: %v", err)
	}
	if err := q.Complete(ctx, cClaim.ID, nil, "quota exhausted: no AI credits remaining"); err != nil {
		t.Fatalf("complete credits: %v", err)
	}
	gotCredits, _ := q.Get(cClaim.ID)
	if gotCredits.Status != JobStatusFailed || gotCredits.Retriable {
		t.Fatalf("expected terminal credits stop failed+non-retriable, got status=%s retriable=%v cat=%s",
			gotCredits.Status, gotCredits.Retriable, gotCredits.FailureCategory)
	}

	// Original remediable job is claimable again and completes via mock agy.
	reclaimed, err := q.Claim(ctx, "mac-mini", []string{"agent.prompt"})
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim after remediate: %v %v", reclaimed, err)
	}
	if reclaimed.ID != origID && reclaimed.ID != orphanID {
		t.Fatalf("expected reclaim of remediable/orphan job, got %s", reclaimed.ID)
	}
	runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, reclaimed, "mac-mini")
	done, _ := q.Get(reclaimed.ID)
	if done.Status != JobStatusCompleted {
		t.Fatalf("expected mock-agy completion for %s, got %s", reclaimed.ID, done.Status)
	}
}

// TestMockAgy_RequeuedParentsWithCompletedChildrenSuperseded (STORY-IAZIO-0202 / RC-6)
// reproduces live jobs job_3ffa5d0be1649f0e, job_985bc2111a0944cd, job_97d33fd156a88144:
// parents stuck in requeued because completed replacement children were wrongly treated as
// "active". After reconcile they must become completed (superseded). True orphans without
// any child still promote to pending and are claimable by mock-agy through completion.
func TestMockAgy_RequeuedParentsWithCompletedChildrenSuperseded(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mockAgyBin := buildMockAgyBinary(t, tmpDir)
	now := time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC)
	q := NewInMemoryQueue()
	q.nowFunc = func() time.Time { return now }

	seedCompletedChild := func(id, parentID, storyID string) {
		completedAt := now.Add(-time.Hour)
		q.jobs[id] = &Job{
			ID:         id,
			ParentID:   parentID,
			Type:       JobTypePromptExecute,
			Repo:       "/home/romeo/work/meta-repo-02",
			StoryID:    storyID,
			Status:     JobStatusCompleted,
			Result:     json.RawMessage(`{"status":"completed","story_id":"` + storyID + `"}`),
			CreatedAt:  now.Add(-2 * time.Hour),
			CompletedAt: &completedAt,
			doneCh:     make(chan struct{}),
		}
		close(q.jobs[id].doneCh)
	}

	// Live job_3ffa5d0be1649f0e: requeued + BACKGROUND_DRAIN + two completed children.
	parent3ffa := &Job{
		ID:                   "job_3ffa5d0be1649f0e",
		Type:                 JobTypePromptExecute,
		Repo:                 "/home/romeo/work/meta-repo-02",
		StoryID:              "STORY-GOV-3850",
		TargetHost:           "debian",
		ClaimedHost:          "debian",
		HostID:               "debian",
		TargetAgent:          "agy",
		AllocationSource:     "global_queue_tool_priority",
		StoryAllocationStatus: "allocated",
		Status:               JobStatusRequeued,
		Retriable:            true,
		FailureCategory:      FailureCategoryBackgroundDrainTimeout,
		FailureStage:         "BACKGROUND_DRAIN",
		FailureReasonSummary: "LEASE_EXPIRED: execution lease expired without completion",
		Error:                "phantom_or_superseded_remediation_STORY_IAZIO_0192",
		CreatedAt:            now.Add(-14 * 24 * time.Hour),
		doneCh:               make(chan struct{}),
	}
	q.jobs[parent3ffa.ID] = parent3ffa
	seedCompletedChild("job_ad7d90d13dc1d719", parent3ffa.ID, "STORY-GOV-3850")
	seedCompletedChild("job_fec606cb5ad138a4", parent3ffa.ID, "STORY-GOV-3850")

	// Live job_985bc2111a0944cd: requeued, pending_harness_creation, deps satisfied, completed child.
	depAFD := &Job{
		ID:          "job_afddead322e5bee9",
		Type:        JobTypePromptExecute,
		Status:      JobStatusCompleted,
		Result:      json.RawMessage(`{"status":"completed"}`),
		CreatedAt:   now.Add(-3 * time.Hour),
		CompletedAt: func() *time.Time { t := now.Add(-90 * time.Minute); return &t }(),
		doneCh:      make(chan struct{}),
	}
	close(depAFD.doneCh)
	q.jobs[depAFD.ID] = depAFD
	parent985 := &Job{
		ID:                    "job_985bc2111a0944cd",
		Type:                  JobTypePromptExecute,
		Repo:                  ".",
		TargetAgent:           "agy",
		AllocationSource:      "global_queue_tool_priority",
		StoryAllocationStatus: "pending_harness_creation",
		DependsOnJobIDs:       []string{depAFD.ID},
		Status:                JobStatusRequeued,
		Retriable:             true,
		CreatedAt:             now.Add(-2 * 24 * time.Hour),
		DispatchDiagnostics:   &DispatchDiagnostics{QueueState: "queued"},
		doneCh:                make(chan struct{}),
	}
	q.jobs[parent985.ID] = parent985
	seedCompletedChild("job_d6e1c7fef4bf5e32", parent985.ID, "STORY-GOV-5264")

	// Live job_97d33fd156a88144: same pattern with completed Wave-4 child.
	depBF10 := &Job{
		ID:          "job_bf10a230d00d38df",
		Type:        JobTypePromptExecute,
		Status:      JobStatusCompleted,
		Result:      json.RawMessage(`{"status":"completed"}`),
		CreatedAt:   now.Add(-3 * time.Hour),
		CompletedAt: func() *time.Time { t := now.Add(-80 * time.Minute); return &t }(),
		doneCh:      make(chan struct{}),
	}
	close(depBF10.doneCh)
	q.jobs[depBF10.ID] = depBF10
	parent97d := &Job{
		ID:                    "job_97d33fd156a88144",
		Type:                  JobTypePromptExecute,
		Repo:                  ".",
		TargetAgent:           "agy",
		AllocationSource:      "global_queue_tool_priority",
		StoryAllocationStatus: "pending_harness_creation",
		DependsOnJobIDs:       []string{depBF10.ID},
		Status:                JobStatusRequeued,
		Error:                 "Re-binding depends_on_job_id to active Wave 3 job",
		CreatedAt:             now.Add(-2 * 24 * time.Hour),
		doneCh:                make(chan struct{}),
	}
	q.jobs[parent97d.ID] = parent97d
	seedCompletedChild("job_6a95cf152ce8039d", parent97d.ID, "STORY-GOV-5271")

	// True orphan (no children) must still promote to pending and complete via mock-agy.
	orphanID := "job_orphan_iazio_0202"
	q.jobs[orphanID] = &Job{
		ID:               orphanID,
		Type:             JobTypePromptExecute,
		Repo:             "meta-repo",
		StoryID:          "STORY-IAZIO-0202",
		AllocationSource: "global_queue_tool_priority",
		TargetAgent:      "agy",
		Status:           JobStatusRequeued,
		CreatedAt:        now.Add(-time.Hour),
		doneCh:           make(chan struct{}),
	}

	// Active pending child must still park its parent as requeued (not superseded).
	activeParent := &Job{
		ID:     "job_active_parent_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	q.jobs[activeParent.ID] = activeParent
	q.jobs["job_active_child_0202"] = &Job{
		ID:       "job_active_child_0202",
		ParentID: activeParent.ID,
		Type:     JobTypePromptExecute,
		Status:   JobStatusPending,
		CreatedAt: now.Add(-10 * time.Minute),
		doneCh:   make(chan struct{}),
	}

	_ = q.reconcileQueueIntegrityLocked(now)

	for _, id := range []string{"job_3ffa5d0be1649f0e", "job_985bc2111a0944cd", "job_97d33fd156a88144"} {
		got, ok := q.Get(id)
		if !ok || got.Status != JobStatusCompleted {
			t.Fatalf("expected %s completed (superseded by completed child), ok=%v status=%v", id, ok, got)
		}
		if got.DispatchDiagnostics == nil || got.DispatchDiagnostics.QueueState != "completed" {
			t.Fatalf("expected %s dispatch_diagnostics.queue_state=completed, got %+v", id, got.DispatchDiagnostics)
		}
	}
	gotActive, _ := q.Get(activeParent.ID)
	if gotActive.Status != JobStatusRequeued {
		t.Fatalf("parent with ACTIVE pending child must stay requeued, got %s", gotActive.Status)
	}
	gotOrphan, _ := q.Get(orphanID)
	if gotOrphan.Status != JobStatusPending {
		t.Fatalf("true orphan requeued must promote to pending, got %s", gotOrphan.Status)
	}

	// Claim path must not return the superseded parents; orphan is claimable and mock-agy completes it.
	claimed, err := q.Claim(ctx, "mac-mini", []string{JobTypePromptExecute})
	if err != nil || claimed == nil {
		t.Fatalf("expected claim of orphan or active child, got job=%v err=%v", claimed, err)
	}
	if claimed.ID == "job_3ffa5d0be1649f0e" || claimed.ID == "job_985bc2111a0944cd" || claimed.ID == "job_97d33fd156a88144" {
		t.Fatalf("must not reclaim superseded completed parents, got %s", claimed.ID)
	}
	if claimed.ID == orphanID {
		runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, claimed, "mac-mini")
		done, _ := q.Get(orphanID)
		if done.Status != JobStatusCompleted {
			t.Fatalf("expected mock-agy to complete orphan %s, got %s", orphanID, done.Status)
		}
	} else {
		// Active child claimed first — complete it, then orphan must be claimable.
		runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, claimed, "mac-mini")
		claimed2, err2 := q.Claim(ctx, "pc1-wsl", []string{JobTypePromptExecute})
		if err2 != nil || claimed2 == nil || claimed2.ID != orphanID {
			t.Fatalf("expected second claim of orphan %s, got job=%v err=%v", orphanID, claimed2, err2)
		}
		runMockAgyForClaimedJob(t, ctx, mockAgyBin, q, claimed2, "pc1-wsl")
		done, _ := q.Get(orphanID)
		if done.Status != JobStatusCompleted {
			t.Fatalf("expected mock-agy to complete orphan %s, got %s", orphanID, done.Status)
		}
	}

	// RetryRetriable must also supersede (not re-pending) when reconcile was skipped.
	stuck := &Job{
		ID:     "job_retry_supersede_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	q.jobs[stuck.ID] = stuck
	seedCompletedChild("job_retry_child_0202", stuck.ID, "STORY-IAZIO-0202")
	retried, err := q.RetryRetriable(ctx)
	if err != nil {
		t.Fatalf("RetryRetriable: %v", err)
	}
	found := false
	for _, j := range retried {
		if j.ID == stuck.ID {
			found = true
			if j.Status != JobStatusCompleted {
				t.Fatalf("RetryRetriable must supersede completed-child parent to completed, got %s", j.Status)
			}
		}
	}
	if !found {
		t.Fatalf("expected RetryRetriable to process superseded parent %s", stuck.ID)
	}

	// F1: remediable failed child must keep parent parked (no duplicate pending dispatch).
	failParent := &Job{
		ID:     "job_failed_child_parent_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	q.jobs[failParent.ID] = failParent
	q.jobs["job_failed_child_0202"] = &Job{
		ID:              "job_failed_child_0202",
		ParentID:        failParent.ID,
		Type:            JobTypePromptExecute,
		Status:          JobStatusFailed,
		Retriable:       true,
		FailureCategory: FailureCategoryASEGateIncomplete,
		Error:           "ASE_GATE_INCOMPLETE: unmerged",
		CreatedAt:       now.Add(-5 * time.Minute),
		doneCh:          make(chan struct{}),
	}
	_ = q.reconcileQueueIntegrityLocked(now)
	gotFailParent, _ := q.Get(failParent.ID)
	if gotFailParent.Status != JobStatusRequeued {
		t.Fatalf("parent with remediable failed child must stay requeued, got %s", gotFailParent.Status)
	}

	// F1: terminal-stop failed child must also keep parent parked (no credits bypass).
	termParent := &Job{
		ID:     "job_terminal_child_parent_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	q.jobs[termParent.ID] = termParent
	q.jobs["job_terminal_child_0202"] = &Job{
		ID:              "job_terminal_child_0202",
		ParentID:        termParent.ID,
		Type:            JobTypePromptExecute,
		Status:          JobStatusFailed,
		Retriable:       false,
		FailureCategory: FailureCategoryToolQuotaOrAuth,
		Error:           "quota exhausted: no AI credits",
		CreatedAt:       now.Add(-5 * time.Minute),
		doneCh:          make(chan struct{}),
	}
	_ = q.reconcileQueueIntegrityLocked(now)
	gotTermParent, _ := q.Get(termParent.ID)
	if gotTermParent.Status != JobStatusRequeued {
		t.Fatalf("parent with terminal-stop failed child must stay requeued, got %s", gotTermParent.Status)
	}

	// F2: superseded parent with BACKGROUND_DRAIN / Error must preserve AttemptHistory.
	histParent := &Job{
		ID:                   "job_hist_supersede_0202",
		Type:                 JobTypePromptExecute,
		Status:               JobStatusRequeued,
		HostID:               "debian",
		ClaimedHost:          "debian",
		TargetAgent:          "agy",
		FailureCategory:      FailureCategoryBackgroundDrainTimeout,
		FailureStage:         "BACKGROUND_DRAIN",
		FailureReasonSummary: "LEASE_EXPIRED: execution lease expired without completion",
		Error:                "LEASE_EXPIRED: execution lease expired without completion",
		CreatedAt:            now.Add(-time.Hour),
		doneCh:               make(chan struct{}),
	}
	q.jobs[histParent.ID] = histParent
	seedCompletedChild("job_hist_child_0202", histParent.ID, "STORY-IAZIO-0202")
	_ = q.reconcileQueueIntegrityLocked(now)
	gotHist, _ := q.Get(histParent.ID)
	if gotHist.Status != JobStatusCompleted {
		t.Fatalf("hist parent must complete superseded, got %s", gotHist.Status)
	}
	if len(gotHist.AttemptHistory) == 0 {
		t.Fatal("expected AttemptHistory entry preserving pre-supersede failure telemetry")
	}
	last := gotHist.AttemptHistory[len(gotHist.AttemptHistory)-1]
	if last.FailureCategory != FailureCategoryBackgroundDrainTimeout || last.Host != "debian" ||
		!strings.Contains(last.OutcomeSummary, "LEASE_EXPIRED") {
		t.Fatalf("AttemptHistory hollow: %+v", last)
	}

	// N2: completed bridge with active grandchild must keep root parked.
	bridgeParent := &Job{
		ID:     "job_bridge_parent_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	q.jobs[bridgeParent.ID] = bridgeParent
	completedAtBridge := now.Add(-30 * time.Minute)
	q.jobs["job_bridge_completed_0202"] = &Job{
		ID:          "job_bridge_completed_0202",
		ParentID:    bridgeParent.ID,
		Type:        JobTypePromptExecute,
		Status:      JobStatusCompleted,
		Result:      json.RawMessage(`{"status":"completed","exit_code":0}`),
		CreatedAt:   now.Add(-40 * time.Minute),
		CompletedAt: &completedAtBridge,
		doneCh:      make(chan struct{}),
	}
	close(q.jobs["job_bridge_completed_0202"].doneCh)
	q.jobs["job_bridge_grandchild_0202"] = &Job{
		ID:        "job_bridge_grandchild_0202",
		ParentID:  "job_bridge_completed_0202",
		Type:      JobTypePromptExecute,
		Status:    JobStatusPending,
		CreatedAt: now.Add(-10 * time.Minute),
		doneCh:    make(chan struct{}),
	}
	_ = q.reconcileQueueIntegrityLocked(now)
	gotBridge, _ := q.Get(bridgeParent.ID)
	if gotBridge.Status != JobStatusRequeued {
		t.Fatalf("root with active grandchild behind completed bridge must stay requeued, got %s", gotBridge.Status)
	}

	// N3: pending + phantom_or_superseded + completed child → superseded completed.
	phantomPending := &Job{
		ID:     "job_phantom_pending_0202",
		Type:   JobTypePromptExecute,
		Status: JobStatusPending,
		Error:  "phantom_or_superseded_remediation_STORY_IAZIO_0192",
		doneCh: make(chan struct{}),
	}
	q.jobs[phantomPending.ID] = phantomPending
	seedCompletedChild("job_phantom_child_0202", phantomPending.ID, "STORY-IAZIO-0202")
	_ = q.reconcileQueueIntegrityLocked(now)
	gotPhantom, _ := q.Get(phantomPending.ID)
	if gotPhantom.Status != JobStatusCompleted {
		t.Fatalf("pending phantom parent with completed child must supersede to completed, got %s", gotPhantom.Status)
	}
}

func TestRetryRetriable_DoesNotReviveCanceledOrParentWithReplacementChild(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue()
	canceled := &Job{
		ID:       "job_canceled_5275",
		Type:     "agent.prompt",
		Status:   JobStatusCanceled,
		Retriable: true,
		doneCh:   make(chan struct{}),
	}
	parent := &Job{
		ID:     "job_parent_5275",
		Type:   "agent.prompt",
		Status: JobStatusRequeued,
		doneCh: make(chan struct{}),
	}
	child := &Job{
		ID:       "job_child_5275",
		Type:     "agent.prompt",
		Status:   JobStatusPending,
		ParentID: parent.ID,
		doneCh:   make(chan struct{}),
	}
	q.jobs[canceled.ID] = canceled
	q.jobs[parent.ID] = parent
	q.jobs[child.ID] = child

	retried, err := q.RetryRetriable(ctx)
	if err != nil {
		t.Fatalf("RetryRetriable: %v", err)
	}
	for _, j := range retried {
		if j.ID == canceled.ID || j.ID == parent.ID {
			t.Fatalf("must not revive canceled or parent-with-child, got %s", j.ID)
		}
	}
	gotCanceled, _ := q.Get(canceled.ID)
	gotParent, _ := q.Get(parent.ID)
	if gotCanceled.Status != JobStatusCanceled {
		t.Fatalf("canceled must stay canceled, got %s", gotCanceled.Status)
	}
	if gotParent.Status != JobStatusRequeued {
		t.Fatalf("parent with replacement child must stay requeued, got %s", gotParent.Status)
	}
}

func TestClassifyDispatchFailure_TerminalStopOnlyCreditsAndInfra(t *testing.T) {
	cat, _, _, _, _, ret := ClassifyDispatchFailureWithDirection("DoD incomplete review scheme gate")
	if cat != FailureCategoryASEGateIncomplete || !ret {
		t.Fatalf("ASE/DoD incomplete must be remediable, got cat=%s retriable=%v", cat, ret)
	}
	cat, _, _, _, _, ret = ClassifyDispatchFailureWithDirection("quota exhausted rate limit")
	if cat != FailureCategoryToolQuotaOrAuth || ret {
		t.Fatalf("quota must be terminal, got cat=%s retriable=%v", cat, ret)
	}
	cat, _, _, _, _, ret = ClassifyDispatchFailureWithDirection("infrastructure down: host unreachable")
	if cat != FailureCategoryInfraDown || ret {
		t.Fatalf("infra down must be terminal, got cat=%s retriable=%v", cat, ret)
	}
	cat, _, _, _, _, ret = ClassifyDispatchFailureWithDirection("circuit breaker tripped")
	if cat != FailureCategoryCircuitBreakerTripped || !ret {
		t.Fatalf("circuit breaker must be remediable, got cat=%s retriable=%v", cat, ret)
	}
}
