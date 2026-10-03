package control

import (
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

	// 2. Poll host leases first iteration
	job1, leasedSch, _, ok := e.PollHost("host-1")
	if !ok || job1.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s, got: %+v", sch.ID, job1)
	}
	if leasedSch.IterationsRemaining != 2 {
		t.Fatalf("expected 2 iterations remaining on lease, got: %d", leasedSch.IterationsRemaining)
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
	if len(detail.History) != 1 {
		t.Fatalf("expected 1 history item after iter 1, got: %d", len(detail.History))
	}

	// 4. Poll host leases second iteration
	job2, _, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch.ID {
		t.Fatalf("expected job for schedule %s, got: %+v", sch.ID, job2)
	}

	// 5. Finish iteration 2 with ASEComplete: true
	e.ApplyFinish("host-1", "/work/app", job2.ID, FinishInput{ASEComplete: true})

	// After iteration 2 (all iterations complete), queue must be EMPTY (0 schedules)!
	detail, ok = e.GetRepoDetail("host-1", "/work/app")
	if !ok || len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 schedules in queue after all iterations complete, got: %d", len(detail.Schedules))
	}
	if len(detail.History) != 2 {
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
	job1, _, _, ok := e.PollHost("host-1")
	if !ok || job1.ScheduleID != sch.ID {
		t.Fatalf("poll 1 failed")
	}
	e.ApplyFinish("host-1", "/work/app", job1.ID, FinishInput{ASEComplete: true})

	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue after iter 1, got %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 2 || detail.Schedules[0].IterationsCompleted != 1 {
		t.Fatalf("expected remaining=2, completed=1, got: %+v", detail.Schedules[0])
	}
	if len(detail.History) != 1 {
		t.Fatalf("expected 1 history item, got %d", len(detail.History))
	}

	// Step 2: Iteration 2
	job2, _, _, ok := e.PollHost("host-1")
	if !ok || job2.ScheduleID != sch.ID {
		t.Fatalf("poll 2 failed")
	}
	e.ApplyFinish("host-1", "/work/app", job2.ID, FinishInput{ASEComplete: true})

	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 1 {
		t.Fatalf("expected 1 schedule in queue after iter 2, got %d", len(detail.Schedules))
	}
	if detail.Schedules[0].IterationsRemaining != 1 || detail.Schedules[0].IterationsCompleted != 2 {
		t.Fatalf("expected remaining=1, completed=2, got: %+v", detail.Schedules[0])
	}
	if len(detail.History) != 2 {
		t.Fatalf("expected 2 history items, got %d", len(detail.History))
	}

	// Step 3: Iteration 3
	job3, _, _, ok := e.PollHost("host-1")
	if !ok || job3.ScheduleID != sch.ID {
		t.Fatalf("poll 3 failed")
	}
	e.ApplyFinish("host-1", "/work/app", job3.ID, FinishInput{ASEComplete: true})

	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if len(detail.Schedules) != 0 {
		t.Fatalf("expected 0 schedules in queue after iter 3 (all completed), got %d", len(detail.Schedules))
	}
	if len(detail.History) != 3 {
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


