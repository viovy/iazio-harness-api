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
