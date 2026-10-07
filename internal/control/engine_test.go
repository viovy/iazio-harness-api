package control

import (
	"sync"
	"testing"
	"time"
)

func TestRefinementAndDraft(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Unix(100, 0) })
	if err := e.BeginExtract(); err != nil {
		t.Fatal(err)
	}
	if err := e.BeginExtract(); err != ErrExtractorBusy {
		t.Fatalf("busy: %v", err)
	}
	e.EndExtract()
	idea, err := e.ImportIdea("https://gemini.google.com/share/abc", "transcript", "t")
	if err != nil {
		t.Fatal(err)
	}
	job, err := e.ScheduleRefinement(idea.ID, "host-1", "/repos/docs-hub")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ScheduleRefinement(idea.ID, "host-1", "/repos/docs-hub"); err != ErrIdeaState && err != ErrHubBusy {
		t.Fatalf("second schedule: %v", err)
	}
	if _, err := e.StoryDraft(job.ID, "other", "story-1", "body {{.StoryID}}"); err != ErrNotLeaseHolder {
		t.Fatalf("holder: %v", err)
	}
	p, err := e.StoryDraft(job.ID, "host-1", "story-1", "run {{.StoryID}}")
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "DRAFT" || p.StoryID != "story-1" {
		t.Fatalf("prompt: %+v", p)
	}
	got, _ := e.GetIdea(idea.ID)
	if got.Status != IdeaRefined {
		t.Fatalf("status %s", got.Status)
	}
}

func TestFailRefinementRestores(t *testing.T) {
	e := NewEngine(nil)
	idea, err := e.ImportIdea("https://gemini.google.com/share/abc", "t", "t")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Triage(idea.ID); err != nil {
		t.Fatal(err)
	}
	job, err := e.ScheduleRefinement(idea.ID, "h", "/hub")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.FailRefinement(job.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := e.GetIdea(idea.ID)
	if got.Status != IdeaTriaged {
		t.Fatalf("restored %s", got.Status)
	}
}

func TestCloneLeaseAndIntervention(t *testing.T) {
	e := NewEngine(nil)
	repo := Repo{HostID: "h1", WorktreePath: "/repos/leaf-01", CloneURL: "https://example.test/leaf.git", DocsHubPath: "/repos/docs-hub"}
	if err := e.UpsertRepo(repo); err != nil {
		t.Fatal(err)
	}
	if err := e.UpsertRepo(Repo{HostID: "h2", WorktreePath: "/repos/leaf-01", CloneURL: repo.CloneURL, DocsHubPath: "/repos/docs-hub"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.LeaseOrdinary("h1", repo.WorktreePath, repo.CloneURL); err != nil {
		t.Fatal(err)
	}
	if _, err := e.LeaseOrdinary("h2", repo.WorktreePath, repo.CloneURL); err != ErrCloneBusy {
		t.Fatalf("second host: %v", err)
	}
	e.ApplyPreflight("h1", repo.WorktreePath, Preflight{
		Kind: KindOrdinary, FreeBytes: 1, DocsHubOK: true, GitAuthOK: true, GitWorkTree: true,
		HeadAttached: true, Branch: "main", DefaultBranch: "main",
	})
	got, _ := e.GetRepo("h1", repo.WorktreePath)
	if got.Queue == QueuePaused {
		t.Fatal("disk halt must not pause")
	}
	e.ApplyFinish("h1", repo.WorktreePath, "", FinishInput{Kind: KindOrdinary, WorkPorcelain: " M x"})
	got, _ = e.GetRepo("h1", repo.WorktreePath)
	if got.Queue != QueuePaused || got.HealingAttempts != 0 {
		t.Fatalf("dirty finish: %+v", got)
	}
	if _, err := e.LeaseOrdinary("h1", repo.WorktreePath, repo.CloneURL); err != ErrQueueBlocked {
		t.Fatalf("ordinary on pause: %v", err)
	}
	if _, err := e.LeaseIntervention("h1", repo.WorktreePath, repo.CloneURL); err != nil {
		t.Fatal(err)
	}
}

func TestCorrelationAndPromptRevision(t *testing.T) {
	e := NewEngine(nil)
	if err := e.UpsertRepo(Repo{HostID: "h", WorktreePath: "/w", CloneURL: "https://example.test/a.git"}); err != nil {
		t.Fatal(err)
	}
	e.NoteCorrelation("h", "/w", false, time.Minute)
	got, _ := e.GetRepo("h", "/w")
	if got.Lock != LockCooling {
		t.Fatalf("cooling: %+v", got)
	}
	if _, err := e.LeaseIntervention("h", "/w", "https://example.test/a.git"); err != ErrQueueBlocked {
		t.Fatalf("intervention during cooling: %v", err)
	}
	e.NoteCorrelation("h", "/w", false, CorrelationWindow)
	got, _ = e.GetRepo("h", "/w")
	if got.Queue != QueueBlocked || got.Lock != LockIdle {
		t.Fatalf("blocked: %+v", got)
	}
	e.ForcePause("h", "/w")
	if _, err := e.LeaseIntervention("h", "/w", "https://example.test/a.git"); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "p", Body: "b", Status: "READY"})
	e.MarkPromptRunning(p.ID, true)
	if _, err := e.UpdatePrompt(p.ID, "new"); err != ErrPromptRunning {
		t.Fatalf("edit: %v", err)
	}
	if err := e.DeletePrompt(p.ID); err != ErrPromptRunning {
		t.Fatalf("delete: %v", err)
	}
}

func TestCancelFreezeAndEphemeral(t *testing.T) {
	start := time.Unix(1_000, 0)
	e := NewEngine(func() time.Time { return start })
	e.RegisterHost("box-1", "ephemeral")
	if err := e.UpsertRepo(Repo{HostID: "box-1", WorktreePath: "/w", CloneURL: "https://example.test/a.git"}); err != nil {
		t.Fatal(err)
	}
	job, err := e.LeaseOrdinary("box-1", "/w", "https://example.test/a.git")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(job.ID, "execution_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := e.PostExit(job.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(job.ID, "cancelled"); err != ErrCancelClosed {
		t.Fatalf("late cancel: %v", err)
	}
	e.MissHeartbeat("box-1", 2)
	e.now = func() time.Time { return start.Add(EphemeralGrace + time.Second) }
	if !e.ArchiveEphemeral("box-1") {
		t.Fatal("expected archive")
	}
	if e.ArchiveEphemeral("box-1") {
		t.Fatal("archived id must not revive")
	}
}

func TestRepoQueueDecrementsAndClearsOnFinish(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task prompt", Body: "do task", Status: "READY"})

	// 1. Create a schedule with 2 iterations
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Repo detail queue must have 1 schedule with iterations 2
	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue, got: %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 2 {
		t.Fatalf("expected 2 iterations remaining, got: %d", detail.Schedules[0].IterationsRemaining)
	}
	if len(detail.History) != 0 {
		t.Fatalf("expected 0 history items, got: %d", len(detail.History))
	}

	// 2. Poll host leases first iteration (triggering execution decreases iterations scheduled)
	job1, leasedSch, _, ok := e.PollHost("host-1")
	if !ok || job1.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s, got: %+v", sch.ID, job1)
	}
	if leasedSch.IterationsRemaining != 1 {
		t.Fatalf("expected 1 iteration remaining on lease, got: %d", leasedSch.IterationsRemaining)
	}
	// History immediately records in-flight entry
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.History) != 1 || detail.History[0].Status != "RUNNING" {
		t.Fatalf("expected 1 history item with status RUNNING, got: %+v", detail.History)
	}
	if len(detail.Schedules) != 1 || detail.Schedules[0].IterationsRemaining != 1 {
		t.Fatalf("expected 1 schedule with remaining 1 in queue, got: %+v", detail.Schedules)
	}

	// 3. Finish iteration 1 with ASEComplete: true
	e.ApplyFinish("host-1", "/work/app", job1.ID, FinishInput{ASEComplete: true})

	// After iteration 1, queue must still have 1 schedule, but with iterations remaining = 1
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue after iter 1, got: %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 1 {
		t.Fatalf("expected 1 iteration remaining, got: %d", detail.Schedules[0].IterationsRemaining)
	}
	if len(detail.History) != 1 || detail.History[0].Status != "RUN_FINISHED" {
		t.Fatalf("expected 1 history item after iter 1 with status RUN_FINISHED, got: %+v", detail.History)
	}

	// 4. Poll host leases second iteration (iterations reach 0, dequeued from queue)
	job2, leasedSch2, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s, got: %+v", sch.ID, job2)
	}
	if leasedSch2.IterationsRemaining != 0 {
		t.Fatalf("expected 0 iterations remaining on lease 2, got: %d", leasedSch2.IterationsRemaining)
	}
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 0 {
		t.Fatalf("expected schedule dequeued when iterations reach 0, got: %d", len(detail.Schedules))
	}
	if len(detail.History) != 2 || detail.History[1].Status != "RUNNING" {
		t.Fatalf("expected 2 history items, 2nd RUNNING, got: %+v", detail.History)
	}

	// 5. Finish iteration 2 with ASEComplete: true
	e.ApplyFinish("host-1", "/work/app", job2.ID, FinishInput{ASEComplete: true})

	// After iteration 2 (all iterations complete), queue must be EMPTY (0 schedules)!
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 schedules in queue after all iterations complete, got: %d", len(detail.Schedules))
	}
	if len(detail.History) != 2 || detail.History[1].Status != "RUN_FINISHED" {
		t.Fatalf("expected 2 history items after iter 2, got: %d", len(detail.History))
	}
}

func TestRepoQueueDecrementsAndClearsEvenIfDirty(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task prompt", Body: "do task", Status: "READY"})

	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue initially, got: %d", len(detail.Schedules))
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok || job.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s", sch.ID)
	}

	// Execution completes with dirty porcelain
	e.ApplyFinish("host-1", "/work/app", job.ID, FinishInput{WorkPorcelain: " M modified_file.go", ASEComplete: false})

	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok {
		t.Fatal("repo not found")
	}
	if len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 schedules in queue after execution completion even if dirty, got: %d", len(detail.Schedules))
	}
	if len(detail.History) != 1 {
		t.Fatalf("expected 1 history item, got: %d", len(detail.History))
	}
	if detail.History[0].ScheduleID != sch.ID {
		t.Fatalf("expected history schedule_id %s, got: %s", sch.ID, detail.History[0].ScheduleID)
	}
	if detail.History[0].Clean {
		t.Fatal("expected history item clean=false")
	}
}

func TestRepoQueueMultiIterationDecrement(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Multi-iter prompt", Body: "run 3 times", Status: "READY"})

	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 3, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Iteration 1
	job1, leased1, _, ok := e.PollHost("host-1")
	if !ok || job1.ScheduleID != sch.ID {
		t.Fatalf("poll 1 failed")
	}
	if leased1.IterationsRemaining != 2 {
		t.Fatalf("expected 2 remaining on poll 1, got %d", leased1.IterationsRemaining)
	}
	e.ApplyFinish("host-1", "/work/app", job1.ID, FinishInput{ASEComplete: true})

	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue after iter 1, got %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 2 || detail.Schedules[0].IterationsCompleted != 1 {
		t.Fatalf("expected remaining=2, completed=1, got: %+v", detail.Schedules[0])
	}
	if len(detail.History) != 1 || detail.History[0].Status != "RUN_FINISHED" {
		t.Fatalf("expected 1 history item, got %d", len(detail.History))
	}

	// Step 2: Iteration 2
	job2, leased2, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch.ID {
		t.Fatalf("poll 2 failed")
	}
	if leased2.IterationsRemaining != 1 {
		t.Fatalf("expected 1 remaining on poll 2, got %d", leased2.IterationsRemaining)
	}
	e.ApplyFinish("host-1", "/work/app", job2.ID, FinishInput{ASEComplete: true})

	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue after iter 2, got %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 1 || detail.Schedules[0].IterationsCompleted != 2 {
		t.Fatalf("expected remaining=1, completed=2, got: %+v", detail.Schedules[0])
	}
	if len(detail.History) != 2 || detail.History[1].Status != "RUN_FINISHED" {
		t.Fatalf("expected 2 history items, got %d", len(detail.History))
	}

	// Step 3: Iteration 3 (reaches 0, dequeued from queue)
	job3, leased3, _, ok := e.PollHost("host-1")
	if !ok || job3.ScheduleID != sch.ID {
		t.Fatalf("poll 3 failed")
	}
	if leased3.IterationsRemaining != 0 {
		t.Fatalf("expected 0 remaining on poll 3, got %d", leased3.IterationsRemaining)
	}
	e.ApplyFinish("host-1", "/work/app", job3.ID, FinishInput{ASEComplete: true})

	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 schedules in queue after iter 3 (all completed), got %d", len(detail.Schedules))
	}
	if len(detail.History) != 3 || detail.History[2].Status != "RUN_FINISHED" {
		t.Fatalf("expected 3 history items, got %d", len(detail.History))
	}
}

func TestPollHostSkipsBusyRepo(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/other", CloneURL: "https://example.test/other.git"}); err != nil {
		t.Fatal(err)
	}

	p1 := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	p2 := e.PutPrompt(Prompt{Title: "Task 2", Body: "do task 2", Status: "READY"})
	p3 := e.PutPrompt(Prompt{Title: "Task 3", Body: "do task 3", Status: "READY"})

	// Queue two schedules for /work/app and one for /work/other
	sch1, err := e.ExecutePrompt(p1.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	sch2, err := e.ExecutePrompt(p2.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	sch3, err := e.ExecutePrompt(p3.ID, "host-1", "/work/other", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// First poll leases sch1 for /work/app
	job1, leased1, _, ok := e.PollHost("host-1")
	if !ok || job1.ScheduleID != sch1.ID {
		t.Fatalf("expected lease of sch1, got job: %+v, ok: %v", job1, ok)
	}
	if leased1.WorktreePath != "/work/app" {
		t.Fatalf("expected /work/app, got %s", leased1.WorktreePath)
	}

	// Second poll: /work/app is LockRunning, so sch2 must be skipped!
	// It should lease sch3 on /work/other instead.
	job2, leased2, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch3.ID {
		t.Fatalf("expected lease of sch3 on /work/other, got job: %+v, ok: %v", job2, ok)
	}
	if leased2.WorktreePath != "/work/other" {
		t.Fatalf("expected /work/other, got %s", leased2.WorktreePath)
	}

	// Third poll: both /work/app and /work/other are LockRunning.
	// No other repos available, so PollHost must return false!
	_, _, _, ok = e.PollHost("host-1")
	if ok {
		t.Fatalf("expected PollHost to return false when all candidate repos are busy")
	}

	// Finish job1 on /work/app
	e.ApplyFinish("host-1", "/work/app", job1.ID, FinishInput{ASEComplete: true})

	// Fourth poll: /work/app is now LockIdle. sch2 should now be leased!
	job4, leased4, _, ok := e.PollHost("host-1")
	if !ok || job4.ScheduleID != sch2.ID {
		t.Fatalf("expected lease of sch2 after /work/app became idle, got: %+v, ok: %v", job4, ok)
	}
	if leased4.WorktreePath != "/work/app" {
		t.Fatalf("expected /work/app, got %s", leased4.WorktreePath)
	}
}

func TestReapExpiredTimeoutAndOrphanLock(t *testing.T) {
	fakeNow := time.Now()
	clock := func() time.Time { return fakeNow }
	e := NewEngine(clock)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok || job.ScheduleID != sch.ID {
		t.Fatalf("failed to lease job")
	}

	// Verify repo is locked
	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || detail.Repo.Lock != LockRunning || detail.Running == nil {
		t.Fatalf("expected running job and LockRunning: %+v", detail)
	}

	// Fast forward past DefaultMaxExec (60 min)
	fakeNow = fakeNow.Add(65 * time.Minute)

	// GetRepoDetail should trigger reapExpiredLocked
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok {
		t.Fatalf("failed to get repo detail")
	}
	if detail.Repo.Lock != LockIdle {
		t.Fatalf("expected LockIdle after timeout reap, got: %s", detail.Repo.Lock)
	}
	if detail.Running != nil {
		t.Fatalf("expected running job cleared after timeout reap, got: %+v", detail.Running)
	}

	// Verify job detail is FAILED
	jobDetail, ok := e.GetJob(job.ID)
	if !ok || jobDetail.Status != "FAILED" {
		t.Fatalf("expected job status FAILED, got: %s", jobDetail.Status)
	}
}

func TestForcePauseClearsActiveJobAndSchedule(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok || job.ScheduleID != sch.ID {
		t.Fatalf("failed to lease job")
	}

	// ForcePause repo
	e.ForcePause("host-1", "/work/app")

	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok {
		t.Fatalf("failed to get repo detail")
	}
	if detail.Repo.Lock != LockIdle {
		t.Fatalf("expected LockIdle, got: %s", detail.Repo.Lock)
	}
	if detail.Repo.Queue != QueuePaused {
		t.Fatalf("expected QueuePaused, got: %s", detail.Repo.Queue)
	}
	if detail.Running != nil {
		t.Fatalf("expected running job cleared, got: %+v", detail.Running)
	}
}

func TestDeclineJobRequeuesScheduleAndUnlocks(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok || job.ScheduleID != sch.ID {
		t.Fatalf("failed to lease job")
	}

	// Verify repo is locked while running
	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || detail.Repo.Lock != LockRunning {
		t.Fatalf("expected LockRunning during execution, got: %s", detail.Repo.Lock)
	}

	// Decline job (e.g. worktree busy on agent)
	if err := e.DeclineJob(job.ID, "worktree_busy"); err != nil {
		t.Fatalf("DeclineJob failed: %v", err)
	}

	// Job status must be DECLINED
	jobDetail, ok := e.GetJob(job.ID)
	if !ok || jobDetail.Status != "DECLINED" {
		t.Fatalf("expected job status DECLINED, got: %s", jobDetail.Status)
	}

	// Repo must be unlocked
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || detail.Repo.Lock != LockIdle {
		t.Fatalf("expected LockIdle after decline, got: %s", detail.Repo.Lock)
	}

	// Schedule must be requeued (Status == QUEUED)
	if len(detail.Schedules) != 1 || detail.Schedules[0].Status != "QUEUED" {
		t.Fatalf("expected 1 QUEUED schedule, got: %+v", detail.Schedules)
	}

	// Subsequent PollHost should re-lease the schedule
	job2, _, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch.ID {
		t.Fatalf("expected re-leasing declined schedule, but got ok=%v", ok)
	}
}

func TestPostExitRunningJobSuccessAndFailure(t *testing.T) {
	// Test failure exit (code != 0)
	{
		e := NewEngine(nil)
		e.RegisterHost("host-1", "permanent")
		if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
			t.Fatal(err)
		}
		p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
		sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
		if err != nil {
			t.Fatal(err)
		}

		job, _, _, ok := e.PollHost("host-1")
		if !ok || job.ScheduleID != sch.ID {
			t.Fatalf("failed to lease job")
		}

		if err := e.PostExit(job.ID, 1); err != nil {
			t.Fatalf("PostExit failed: %v", err)
		}

		jobDetail, ok := e.GetJob(job.ID)
		if !ok || jobDetail.Status != "FAILED" {
			t.Fatalf("expected job status FAILED on non-zero exit, got: %s", jobDetail.Status)
		}

		detail, ok := e.GetRepoDetail("host-1", "/work/app")
		if !ok || detail.Repo.Lock != LockIdle {
			t.Fatalf("expected LockIdle on exit, got: %s", detail.Repo.Lock)
		}
		if detail.Repo.Queue != QueuePaused {
			t.Fatalf("expected QueuePaused on non-zero exit, got: %s", detail.Repo.Queue)
		}
	}

	// Test success exit (code == 0)
	{
		e := NewEngine(nil)
		e.RegisterHost("host-1", "permanent")
		if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
			t.Fatal(err)
		}
		p := e.PutPrompt(Prompt{Title: "Task 2", Body: "do task 2", Status: "READY"})
		sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
		if err != nil {
			t.Fatal(err)
		}

		job, _, _, ok := e.PollHost("host-1")
		if !ok || job.ScheduleID != sch.ID {
			t.Fatalf("failed to lease job")
		}

		if err := e.PostExit(job.ID, 0); err != nil {
			t.Fatalf("PostExit failed: %v", err)
		}

		jobDetail, ok := e.GetJob(job.ID)
		if !ok || jobDetail.Status != "RUN_FINISHED" {
			t.Fatalf("expected job status RUN_FINISHED on zero exit, got: %s", jobDetail.Status)
		}

		detail, ok := e.GetRepoDetail("host-1", "/work/app")
		if !ok || detail.Repo.Lock != LockIdle {
			t.Fatalf("expected LockIdle on exit, got: %s", detail.Repo.Lock)
		}
		if len(detail.Schedules) != 0 {
			t.Fatalf("expected 0 remaining schedules after completed exit, got: %d", len(detail.Schedules))
		}
	}
}

func TestOnRepoChangeInvoked(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	var saved []Repo
	var mu sync.Mutex
	e.OnRepoChange = func(r Repo) {
		mu.Lock()
		defer mu.Unlock()
		saved = append(saved, r)
	}

	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected poll to succeed")
	}

	_ = e.DeclineJob(job.ID, "busy")

	// Allow goroutines to fire
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(saved) < 2 {
		t.Fatalf("expected at least 2 OnRepoChange invocations, got: %d", len(saved))
	}
}

func TestSchedulePrioritization(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}

	p1 := e.PutPrompt(Prompt{Title: "Low Priority", Body: "low", Status: "READY"})
	p2 := e.PutPrompt(Prompt{Title: "High Priority", Body: "high", Status: "READY"})

	schLow, err := e.ExecutePrompt(p1.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	schHigh, err := e.ExecutePrompt(p2.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Initially both priority 0, schLow created first so FIFO order
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if detail.Schedules[0].ID != schLow.ID {
		t.Fatalf("expected schLow first initially, got: %s", detail.Schedules[0].ID)
	}

	// Increase priority of schHigh
	updated, err := e.ChangeSchedulePriority(schHigh.ID, 5)
	if err != nil {
		t.Fatalf("failed to change priority: %v", err)
	}
	if updated.Priority != 5 {
		t.Fatalf("expected priority 5, got %d", updated.Priority)
	}

	// Verify GetRepoDetail puts schHigh first now
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.Schedules[0].ID != schHigh.ID {
		t.Fatalf("expected schHigh first after priority bump, got: %s", detail.Schedules[0].ID)
	}

	// Verify PollHost leases schHigh first!
	job, leasedSch, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected PollHost to lease")
	}
	if job.ScheduleID != schHigh.ID || leasedSch.ID != schHigh.ID {
		t.Fatalf("expected high priority schedule leased first, got job: %+v", job)
	}
}

func TestInFlightHistoryStatusTransitions(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "Status test prompt", Body: "test", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Lease job: in-flight history must immediately show RUNNING
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected lease")
	}
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.History) != 1 || detail.History[0].Status != "RUNNING" {
		t.Fatalf("expected 1 history entry with status RUNNING, got: %+v", detail.History)
	}

	// Post exit with error code 1: history transitions to FAILED
	if err := e.PostExit(job.ID, 1); err != nil {
		t.Fatal(err)
	}
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.History) != 1 || detail.History[0].Status != "FAILED" {
		t.Fatalf("expected history entry status FAILED, got: %+v", detail.History)
	}

	// Reset repo queue and lock for next test iteration
	_ = e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", Queue: QueueOpen, Lock: LockIdle})

	// Lease second iteration
	job2, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected second lease")
	}
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.History) != 2 || detail.History[1].Status != "RUNNING" {
		t.Fatalf("expected 2nd history item RUNNING, got: %+v", detail.History)
	}

	// Decline job: history transitions to DECLINED, iteration refunded
	if err := e.DeclineJob(job2.ID, "worktree_busy"); err != nil {
		t.Fatal(err)
	}
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.History[1].Status != "DECLINED" {
		t.Fatalf("expected 2nd history item DECLINED, got: %s", detail.History[1].Status)
	}
	if len(detail.Schedules) != 1 || detail.Schedules[0].ID != sch.ID {
		t.Fatalf("expected schedule refunded and retained in queue, got: %+v", detail.Schedules)
	}
}

func TestLeaseExpirationReapsStaleJob(t *testing.T) {
	curr := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curr }
	e := NewEngine(clock)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "Stale prompt", Body: "stale", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	// Initially running, lock running
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Lock != LockRunning {
		t.Fatalf("expected LockRunning, got: %s", detail.Repo.Lock)
	}

	// Advance clock past LeaseExpiry (60s)
	curr = curr.Add(LeaseInterval + 10*time.Second)

	// GetRepoDetail triggers reapExpiredLocked
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Lock != LockIdle {
		t.Fatalf("expected LockIdle after lease expiration, got: %s", detail.Repo.Lock)
	}
	if detail.Repo.Reason != "LEASE_EXPIRED" {
		t.Fatalf("expected reason LEASE_EXPIRED, got: %s", detail.Repo.Reason)
	}

	jobDetail, _ := e.GetJob(job.ID)
	if jobDetail.Status != "FAILED" {
		t.Fatalf("expected job status FAILED, got: %s", jobDetail.Status)
	}
	if len(detail.History) != 1 || detail.History[0].Status != "LEASE_EXPIRED" {
		t.Fatalf("expected history status LEASE_EXPIRED, got: %+v", detail.History)
	}
	_ = sch
}

func TestOrphanedRunningScheduleRecovery(t *testing.T) {
	curr := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curr }
	e := NewEngine(clock)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "Orphan prompt", Body: "recover me", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate schedule stuck in RUNNING status without any active job in e.jobs
	e.mu.Lock()
	e.schedules[sch.ID].Status = "RUNNING"
	e.mu.Unlock()

	// Prior to fix, PollHost would ignore it because Status != QUEUED
	// With fix, GetRepoDetail or PollHost triggers reapExpiredLocked and recovers it to QUEUED
	job, leasedSch, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected orphaned schedule to be recovered to QUEUED and polled, but PollHost returned false")
	}
	if leasedSch.ID != sch.ID {
		t.Fatalf("expected leased schedule %s, got %s", sch.ID, leasedSch.ID)
	}
	if job.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s, got %s", sch.ID, job.ScheduleID)
	}
}

func TestOrphanedCompletedScheduleRecovery(t *testing.T) {
	curr := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curr }
	e := NewEngine(clock)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "Exhausted prompt", Body: "done", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate schedule stuck in RUNNING with 0 iterations remaining and no job
	e.mu.Lock()
	e.schedules[sch.ID].Status = "RUNNING"
	e.schedules[sch.ID].IterationsRemaining = 0
	e.schedules[sch.ID].IterationsCompleted = 1
	e.mu.Unlock()

	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 active schedules after recovery, got %d", len(detail.Schedules))
	}

	e.mu.Lock()
	status := e.schedules[sch.ID].Status
	e.mu.Unlock()
	if status != "FINISHED" {
		t.Fatalf("expected FINISHED status, got %s", status)
	}
}

func TestCancelSchedule(t *testing.T) {
	curr := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curr }
	e := NewEngine(clock)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app"}); err != nil {
		t.Fatal(err)
	}

	p := e.PutPrompt(Prompt{Title: "To be cancelled", Body: "cancel me", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Cancel queued schedule
	if _, err := e.CancelSchedule(sch.ID); err != nil {
		t.Fatalf("failed to cancel schedule: %v", err)
	}

	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 0 {
		t.Fatalf("expected cancelled schedule to not appear in RepoDetail, got: %d", len(detail.Schedules))
	}

	// 2. Schedule another and poll it into RUNNING state, then cancel schedule
	sch2, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	if _, err := e.CancelSchedule(sch2.ID); err != nil {
		t.Fatalf("failed to cancel running schedule: %v", err)
	}

	jobDetail, _ := e.GetJob(job.ID)
	if jobDetail.Status != "CANCEL_REQUESTED" {
		t.Fatalf("expected running job to have CANCEL_REQUESTED status, got: %s", jobDetail.Status)
	}
}

func TestResumeRepoAndPreflightRecovery(t *testing.T) {
	e := NewEngine(time.Now)
	repo := Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		Queue:        QueuePaused,
		Lock:         LockIdle,
		Reason:       ReasonDirty,
		Porcelain:    "M dirty.txt",
	}
	_ = e.UpsertRepo(repo)

	// 1. ResumeRepo unpauses queue and clears reason/porcelain
	if err := e.ResumeRepo("host-1", "/work/app"); err != nil {
		t.Fatalf("expected ResumeRepo to succeed, got: %v", err)
	}
	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || detail.Repo.Queue != QueueOpen || detail.Repo.Reason != "" || detail.Repo.Porcelain != "" {
		t.Fatalf("expected repo to be QueueOpen and clean, got: %+v", detail.Repo)
	}

	// 2. Preflight dirty pauses the queue
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:     20 << 30,
		DocsHubOK:     true,
		GitAuthOK:     true,
		GitWorkTree:   true,
		WorkPorcelain: "M another.txt",
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
	})
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Queue != QueuePaused || detail.Repo.Reason != ReasonDirty {
		t.Fatalf("expected repo to be QueuePaused for ReasonDirty, got: %+v", detail.Repo)
	}

	// 3. Preflight clean automatically recovers QueueOpen and clears reason/porcelain
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:     20 << 30,
		DocsHubOK:     true,
		GitAuthOK:     true,
		GitWorkTree:   true,
		WorkPorcelain: "",
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
	})
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Queue != QueueOpen || detail.Repo.Reason != "" || detail.Repo.Porcelain != "" {
		t.Fatalf("expected clean preflight to restore QueueOpen, got: %+v", detail.Repo)
	}
}

func TestPollHostSkipsHaltedDiskRepo(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	if _, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil); err != nil {
		t.Fatal(err)
	}

	// 1. Preflight halts with low disk space
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:   2 << 30, // 2 GB < 10 GB
		GitWorkTree: true,
		DocsHubOK:   true,
		GitAuthOK:   true,
	})
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Reason != ReasonDisk {
		t.Fatalf("expected ReasonDisk, got: %s", detail.Repo.Reason)
	}

	// 2. PollHost MUST NOT lease schedule while repo is halted for disk
	_, _, _, ok := e.PollHost("host-1")
	if ok {
		t.Fatalf("expected PollHost to return ok=false while repo is halted with ReasonDisk")
	}

	// 3. Clear disk halt condition via clean preflight
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		DocsHubOK:     true,
		GitAuthOK:     true,
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
	})
	e.ResumeRepo("host-1", "/work/app")

	// 4. PollHost now leases the schedule
	job, sch, _, ok := e.PollHost("host-1")
	if !ok || job.ID == "" || sch.PromptID != p.ID {
		t.Fatalf("expected PollHost to lease schedule after disk condition cleared, got ok=%v, job=%+v", ok, job)
	}
}

func TestPollHostHaltedDirtySkipsOrdinaryAllowsIntervention(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	pOrd := e.PutPrompt(Prompt{Title: "Ordinary", Body: "do ord", Status: "READY"})
	pInt := e.PutPrompt(Prompt{Title: "Intervention", Body: "do int", Status: "READY"})

	// Queue ordinary schedule
	if _, err := e.ExecutePrompt(pOrd.ID, "host-1", "/work/app", 1, nil); err != nil {
		t.Fatal(err)
	}

	// Halt with dirty tree
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		DocsHubOK:     true,
		GitAuthOK:     true,
		WorkPorcelain: "M foo.go",
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
	})

	// Ordinary schedule is skipped
	_, _, _, ok := e.PollHost("host-1")
	if ok {
		t.Fatalf("expected ordinary schedule to be skipped on dirty repo")
	}

	// Queue intervention
	if _, err := e.ScheduleIntervention("host-1", "/work/app", pInt.ID); err != nil {
		t.Fatal(err)
	}

	// Intervention schedule IS leased
	job, sch, _, ok := e.PollHost("host-1")
	if !ok || sch.Kind != KindIntervention {
		t.Fatalf("expected intervention schedule to be leased on dirty repo, got: ok=%v, job=%+v", ok, job)
	}
}

func TestDeclineJobDeduplicatesConsecutiveHistory(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task", Body: "do task", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Poll 1 & Decline 1
	j1, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 1 failed")
	}
	if err := e.DeclineJob(j1.ID, "worktree_busy"); err != nil {
		t.Fatal(err)
	}
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.History) != 1 || detail.History[0].Status != "DECLINED" {
		t.Fatalf("expected 1 DECLINED history row, got: %d", len(detail.History))
	}

	// Poll 2 & Decline 2 (same schedule)
	j2, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 2 failed")
	}
	if err := e.DeclineJob(j2.ID, "worktree_busy"); err != nil {
		t.Fatal(err)
	}
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	// History should NOT grow to 2 DECLINED entries for the same schedule; it coalesces to 1!
	if len(detail.History) != 1 || detail.History[0].JobID != j2.ID || detail.History[0].Status != "DECLINED" {
		t.Fatalf("expected deduplicated 1 DECLINED history row with latest job %s, got: %+v", j2.ID, detail.History)
	}
	_ = sch
}



