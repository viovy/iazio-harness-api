package control

import (
	"errors"
	"strings"
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

	// 3. Clear disk halt condition via clean preflight (self-healing, without manual ResumeRepo)
	e.ApplyPreflight("host-1", "/work/app", Preflight{
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		DocsHubOK:     true,
		GitAuthOK:     true,
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
	})
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	if detail.Repo.Reason != "" {
		t.Fatalf("expected Reason to be cleared by clean preflight, got: %s", detail.Repo.Reason)
	}

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

	delCh := make(chan string, 1)
	e.OnHistoryDelete = func(host, path, jobID string) {
		delCh <- jobID
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
	if len(detail.History) != 1 || detail.History[0].Status != "DECLINED" || detail.History[0].Reason != "worktree_busy" {
		t.Fatalf("expected 1 DECLINED history row with reason worktree_busy, got: %+v", detail.History)
	}

	// Poll 2 & Decline 2 (same schedule)
	j2, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 2 failed")
	}
	if err := e.DeclineJob(j2.ID, "spawn_failed"); err != nil {
		t.Fatal(err)
	}
	detail, _ = e.GetRepoDetail("host-1", "/work/app")
	// History should NOT grow to 2 DECLINED entries for the same schedule; it coalesces to 1 with latest reason!
	if len(detail.History) != 1 || detail.History[0].JobID != j2.ID || detail.History[0].Status != "DECLINED" || detail.History[0].Reason != "spawn_failed" {
		t.Fatalf("expected deduplicated 1 DECLINED history row with latest job %s and reason spawn_failed, got: %+v", j2.ID, detail.History)
	}

	// Verify that j1.ID was notified for deletion
	select {
	case gotID := <-delCh:
		if gotID != j1.ID {
			t.Fatalf("expected OnHistoryDelete for %s, got: %s", j1.ID, gotID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for OnHistoryDelete for %s", j1.ID)
	}
	_ = sch
}

func TestSetHistoryAdvancesSequenceAndConversationIDUpdatesLatest(t *testing.T) {
	e := NewEngine(nil)
	key := repoKey("host-1", "/work/app")
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}

	// 1. SetHistory with existing job-50 and sch-45
	e.SetHistory(key, []HistoryRow{
		{
			JobID:           "job-50",
			ScheduleID:      "sch-45",
			PromptTitle:     "Task 1",
			Status:          "LEASE_EXPIRED",
			ConversationIDs: []string{"old-conv-1"},
		},
	})

	// Sequence should now be at least 50
	p := e.PutPrompt(Prompt{Title: "Task 2", Body: "do task 2", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	j, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	// Job ID should be >= job-51, definitely not job-1..job-50
	if j.ID == "job-50" {
		t.Fatalf("expected job ID > job-50 to prevent collision, got %s", j.ID)
	}
	if len(j.ConversationIDs) != 0 {
		t.Fatalf("expected empty ConversationIDs for fresh job without resume conv, got: %+v", j.ConversationIDs)
	}

	// Add an active running history row with the same ID or multiple rows
	e.SetHistory(key, []HistoryRow{
		{
			JobID:           j.ID,
			ScheduleID:      "sch-old",
			PromptTitle:     "Old Prompt",
			Status:          "FAILED",
			ConversationIDs: []string{"old-conv-99"},
		},
		{
			JobID:           j.ID,
			ScheduleID:      sch.ID,
			PromptTitle:     p.Title,
			Status:          "RUNNING",
			ConversationIDs: []string{},
		},
	})

	// AddJobConversation should update the LATEST row (index 1), not index 0
	if err := e.AddJobConversation(j.ID, "new-active-conv"); err != nil {
		t.Fatal(err)
	}
	detail, _ := e.GetRepoDetail("host-1", "/work/app")
	if len(detail.History) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(detail.History))
	}
	if len(detail.History[0].ConversationIDs) != 1 || detail.History[0].ConversationIDs[0] != "old-conv-99" {
		t.Fatalf("older history row should not have been modified, got: %+v", detail.History[0].ConversationIDs)
	}
	if len(detail.History[1].ConversationIDs) != 1 || detail.History[1].ConversationIDs[0] != "new-active-conv" {
		t.Fatalf("latest history row should have received new conversation ID, got: %+v", detail.History[1].ConversationIDs)
	}
}

func TestAbandonAndResumeJob(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
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
	if err := e.AddJobConversation(job.ID, "conv-12345"); err != nil {
		t.Fatal(err)
	}

	repo, _ := e.GetRepo("host-1", "/work/app")
	if repo.Lock != LockRunning || repo.RunningJobID != job.ID {
		t.Fatalf("expected repo lock RUNNING with running job id, got lock=%s job=%s", repo.Lock, repo.RunningJobID)
	}

	// Abandon the job
	if err := e.AbandonJob(job.ID, "STALLED_SILENT"); err != nil {
		t.Fatal(err)
	}

	jd, ok := e.GetJob(job.ID)
	if !ok || jd.Status != "FAILED" || jd.Reason != "STALLED_SILENT" {
		t.Fatalf("expected abandoned job to be FAILED with reason STALLED_SILENT, got: %+v", jd)
	}

	repo, _ = e.GetRepo("host-1", "/work/app")
	if repo.Lock != LockIdle || repo.RunningJobID != "" {
		t.Fatalf("expected repo lock IDLE after abandon, got: lock=%s job=%s", repo.Lock, repo.RunningJobID)
	}

	// Resume the job
	newSch, err := e.ResumeJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if newSch.ResumeConversationID != "conv-12345" {
		t.Fatalf("expected resume conversation ID conv-12345, got %s", newSch.ResumeConversationID)
	}
	if newSch.Status != "QUEUED" {
		t.Fatalf("expected new schedule to be QUEUED, got %s", newSch.Status)
	}

	// Next poll leases the resumed schedule
	resumedJob, sch, _, ok := e.PollHost("host-1")
	if !ok || resumedJob.ID == job.ID {
		t.Fatalf("expected new resumed job from poll, got ok=%v id=%s", ok, resumedJob.ID)
	}
	if sch.ResumeConversationID != "conv-12345" {
		t.Fatalf("expected resumed job to carry conversation id conv-12345, got %s", sch.ResumeConversationID)
	}
	if sch.Kind != KindResume {
		t.Fatalf("expected resumed schedule to have KindResume, got %s", sch.Kind)
	}
}

func TestResumeJobUnhaltsDirtyQueueAndCustomConversationID(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		CloneURL:     "https://example.test/app.git",
		Queue:        QueueOpen,
	}); err != nil {
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

	// Simulate repo queue becoming paused with HALTED_DIRTY
	repo, _ := e.GetRepo("host-1", "/work/app")
	repo.Queue = QueuePaused
	repo.Reason = ReasonDirty
	_ = e.UpsertRepo(repo)

	// Resume job with custom conversation ID and unhalting dirty queue
	newSch, err := e.ResumeJob(job.ID, ResumeOptions{
		ConversationID: "custom-recovered-conv-999",
		AllowDirty:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if newSch.ResumeConversationID != "custom-recovered-conv-999" {
		t.Fatalf("expected custom conversation ID, got %s", newSch.ResumeConversationID)
	}
	if newSch.Kind != KindResume {
		t.Fatalf("expected KindResume, got %s", newSch.Kind)
	}

	// Verify repo queue was unpaused and reason cleared
	repo, _ = e.GetRepo("host-1", "/work/app")
	if repo.Queue != QueueOpen || repo.Reason != "" {
		t.Fatalf("expected repo queue OPEN and reason empty, got queue=%s reason=%s", repo.Queue, repo.Reason)
	}
}

func TestResumeJobAutoSwitchesAgentToAgyOnAntigravitySessionOrQuota(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		CloneURL:     "https://example.test/app.git",
		Queue:        QueueOpen,
	})
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Engine: "agent", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected poll to succeed")
	}

	// 1. Resume with Antigravity UUID conversation -> switches engine to agy
	resSch, err := e.ResumeJob(job.ID, ResumeOptions{
		ConversationID: "0ddc4208-8d7a-490b-86bb-1dee2e91ae82",
		AllowDirty:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resSch.Engine != "agy" {
		t.Fatalf("expected engine 'agy' for Antigravity conversation, got %s", resSch.Engine)
	}

	// 2. Job with ActionRequiredError / QUOTA_EXHAUSTED -> switches engine to agy
	p2 := e.PutPrompt(Prompt{Title: "Task 2", Body: "do task 2", Engine: "agent", Status: "READY"})
	_, _ = e.ExecutePrompt(p2.ID, "host-1", "/work/app", 1, nil)
	job2, _, _, _ := e.PollHost("host-1")
	_ = e.PostExit(job2.ID, 1, ExitOptions{
		Reason:         "ActionRequiredError: Increase limits for faster responses You're out of usage.",
		DiagnosticCode: "QUOTA_EXHAUSTED",
	})
	resSch2, err := e.ResumeJob(job2.ID, ResumeOptions{
		AllowDirty: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resSch2.Engine != "agy" {
		t.Fatalf("expected engine 'agy' on ActionRequiredError quota exhaustion, got %s", resSch2.Engine)
	}
}

func TestRemediateRepo(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
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

	if err := e.RemediateRepo("host-1", "/work/app"); err != nil {
		t.Fatal(err)
	}

	repo, _ := e.GetRepo("host-1", "/work/app")
	if repo.Lock != LockIdle || repo.Queue != QueueOpen || repo.Reason != "REMEDIATED" {
		t.Fatalf("expected repo to be remediated and idle, got: %+v", repo)
	}
	jd, _ := e.GetJob(job.ID)
	if jd.Status != "FAILED" || jd.Reason != "REMEDIATED" {
		t.Fatalf("expected job to be FAILED with REMEDIATED, got: %+v", jd)
	}
}

func TestHeartbeatConditionalLeaseRenewal(t *testing.T) {
	curr := time.Unix(1_700_000_000, 0)
	e := NewEngine(func() time.Time { return curr })
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, _ = e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	// Initial expiry is curr + 30s
	initialExpiry, _ := e.GetJobLeaseExpiry(job.ID)

	// Advance time by 40 seconds - without heartbeat renewal this would expire
	curr = curr.Add(40 * time.Second)

	// Heartbeat reporting NO active jobs -> should NOT renew lease for job
	err := e.Heartbeat("host-1", nil, false, ActiveJob{JobID: "other-job", WorktreePath: "/other"})
	if err != nil {
		t.Fatal(err)
	}
	expiryAfterMissed, _ := e.GetJobLeaseExpiry(job.ID)
	if !expiryAfterMissed.Equal(initialExpiry) {
		t.Fatalf("expected lease not renewed when job is missing from active_jobs")
	}

	// Trigger reaper - job should be reaped
	e.ReapExpired()
	jd, _ := e.GetJob(job.ID)
	if jd.Status != "FAILED" {
		t.Fatalf("expected dead job to be marked FAILED after lease expired, got %s", jd.Status)
	}
}

func TestDirtyStoryDetectionAndResumption(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/demo-repo-04",
		CloneURL:     "https://example.test/demo-repo-04.git",
		Queue:        QueueOpen,
	}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "iazio3", Body: "run iazio3", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/demo-repo-04", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected poll to succeed")
	}

	// 1. Report dirty preflight with detected story ID and review file
	pre := Preflight{
		Kind:                   "ordinary",
		FreeBytes:              20 << 30,
		GitWorkTree:            true,
		HeadAttached:           true,
		Branch:                 "main",
		DefaultBranch:          "main",
		GitAuthOK:              true,
		DocsHubOK:              true,
		WorkPorcelain:          " M sub-agent\n?? docs/reviews/2026-10-09-review-v0-44c767-STORY-APP-0098.md",
		DirtyStoryID:           "STORY-APP-0098",
		DirtyReviewFile:        "docs/reviews/2026-10-09-review-v0-44c767-STORY-APP-0098.md",
		DetectedConversationID: "4fc5bf81-6e70-4800-af36-8da466524b99",
		DetectedVerdict:        "CLEAN",
	}
	halt := e.ApplyPreflight("host-1", "/work/demo-repo-04", pre)
	if halt.Reason != ReasonDirty {
		t.Fatalf("expected halt reason HALTED_DIRTY, got: %s", halt.Reason)
	}

	detail, ok := e.GetRepoDetail("host-1", "/work/demo-repo-04")
	if !ok {
		t.Fatal("repo not found")
	}
	if detail.Repo.DirtyStoryID != "STORY-APP-0098" {
		t.Fatalf("expected DirtyStoryID STORY-APP-0098, got %q", detail.Repo.DirtyStoryID)
	}
	if detail.Repo.DetectedConversationID != "4fc5bf81-6e70-4800-af36-8da466524b99" {
		t.Fatalf("expected DetectedConversationID 4fc5bf81..., got %q", detail.Repo.DetectedConversationID)
	}
	if detail.Repo.DetectedVerdict != "CLEAN" {
		t.Fatalf("expected DetectedVerdict CLEAN, got %q", detail.Repo.DetectedVerdict)
	}

	// 2. Test porcelain parsing fallback
	preFallback := Preflight{
		Kind:          "ordinary",
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		HeadAttached:  true,
		Branch:        "main",
		DefaultBranch: "main",
		GitAuthOK:     true,
		DocsHubOK:     true,
		WorkPorcelain: " M sub-agent\n?? docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md",
	}
	e.ApplyPreflight("host-1", "/work/demo-repo-04", preFallback)
	detail, _ = e.GetRepoDetail("host-1", "/work/demo-repo-04")
	if detail.Repo.DirtyStoryID != "STORY-APP-0098" {
		t.Fatalf("expected fallback DirtyStoryID STORY-APP-0098, got %q", detail.Repo.DirtyStoryID)
	}
	if detail.Repo.DirtyReviewFile != "docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md" {
		t.Fatalf("expected DirtyReviewFile parsed from porcelain, got %q", detail.Repo.DirtyReviewFile)
	}

	// 3. Resume job using detected context
	newSch, err := e.ResumeJob(job.ID, ResumeOptions{AllowDirty: true})
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}
	if newSch.Kind != KindResume {
		t.Fatalf("expected KindResume, got %s", newSch.Kind)
	}
	if newSch.StoryID != "STORY-APP-0098" {
		t.Fatalf("expected resumed schedule to adopt story ID STORY-APP-0098, got %s", newSch.StoryID)
	}

	// 4. Verify repo queue is open and dirty context cleared
	detail, _ = e.GetRepoDetail("host-1", "/work/demo-repo-04")
	if detail.Repo.Queue != QueueOpen || detail.Repo.Reason != "" {
		t.Fatalf("expected repo queue open and reason cleared, got queue=%s reason=%s", detail.Repo.Queue, detail.Repo.Reason)
	}
	if detail.Repo.DirtyStoryID != "" {
		t.Fatalf("expected DirtyStoryID cleared after resume, got %q", detail.Repo.DirtyStoryID)
	}

	// 5. Verify next poll leases the resumed schedule
	resumedJob, sch, _, ok := e.PollHost("host-1")
	if !ok || resumedJob.ID == job.ID {
		t.Fatalf("expected resumed job from poll, got ok=%v id=%s", ok, resumedJob.ID)
	}
	if sch.Kind != KindResume || sch.StoryID != "STORY-APP-0098" {
		t.Fatalf("unexpected leased schedule: kind=%s story=%s", sch.Kind, sch.StoryID)
	}
}

func TestResumeRepoInPlace(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-mac-mini", "permanent")
	repoPath := "/Users/alice/work/test-repo-04"
	if err := e.UpsertRepo(Repo{
		HostID:                 "host-mac-mini",
		WorktreePath:           repoPath,
		CloneURL:               "https://example.test/repo.git",
		Queue:                  QueuePaused,
		Reason:                 ReasonDirty,
		DirtyStoryID:           "STORY-SAMPLE-0098",
		DetectedConversationID: "conv-mac-mini-0098",
	}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Ready ASE prompt", Body: "run {{.StoryID}}", Status: "READY"})

	sch, err := e.ResumeRepoInPlace("host-mac-mini", repoPath, ResumeOptions{
		AllowDirty: true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}

	if sch.Kind != KindResume {
		t.Fatalf("expected KindResume, got: %s", sch.Kind)
	}
	if sch.StoryID != "STORY-SAMPLE-0098" {
		t.Fatalf("expected StoryID STORY-SAMPLE-0098, got: %s", sch.StoryID)
	}
	if sch.ResumeConversationID != "conv-mac-mini-0098" {
		t.Fatalf("expected ResumeConversationID conv-mac-mini-0098, got: %s", sch.ResumeConversationID)
	}
	if sch.Priority < 100 {
		t.Fatalf("expected elevated priority >= 100, got: %d", sch.Priority)
	}
	if sch.PromptID != p.ID {
		t.Fatalf("expected PromptID %s, got: %s", p.ID, sch.PromptID)
	}

	detail, ok := e.GetRepoDetail("host-mac-mini", repoPath)
	if !ok {
		t.Fatal("repo detail not found")
	}
	if detail.Repo.Queue != QueueOpen || detail.Repo.Reason != "" {
		t.Fatalf("expected QueueOpen and cleared reason, got queue=%s reason=%s", detail.Repo.Queue, detail.Repo.Reason)
	}
	if detail.Repo.DirtyStoryID != "STORY-SAMPLE-0098" || detail.Repo.DetectedConversationID != "conv-mac-mini-0098" {
		t.Fatalf("expected dirty context retained on repo, got story=%q conv=%q", detail.Repo.DirtyStoryID, detail.Repo.DetectedConversationID)
	}

	if sch.MaxExecutionDuration < time.Minute {
		t.Fatalf("expected MaxExecutionDuration >= 1m, got: %v", sch.MaxExecutionDuration)
	}

	// Verify host can lease the resumed schedule
	job, leasedSch, _, ok := e.PollHost("host-mac-mini")
	if !ok || job.ScheduleID != sch.ID {
		t.Fatalf("expected poll to lease resumed schedule, got ok=%v", ok)
	}
	if leasedSch.Kind != KindResume || leasedSch.StoryID != "STORY-SAMPLE-0098" {
		t.Fatalf("unexpected leased schedule: %+v", leasedSch)
	}

	// Verify job does not immediately timeout on reap sweep (regression check for 1800ns bug)
	e.ReapExpired()
	leasedJob, ok := e.GetJob(job.ID)
	if !ok || leasedJob.Status != "RUNNING" {
		t.Fatalf("expected job to remain RUNNING, got: %+v", leasedJob)
	}
}

func TestResumeJobHistoricalFallbackWhenJobNotInActiveMemory(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-mac-mini", "permanent")
	repoPath := "/Users/alice/work/test-repo-04"
	if err := e.UpsertRepo(Repo{
		HostID:       "host-mac-mini",
		WorktreePath: repoPath,
		CloneURL:     "https://example.test/repo.git",
		Queue:        QueuePaused,
		Reason:       ReasonDirty,
		DirtyStoryID: "STORY-SAMPLE-0098",
	}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Ready ASE prompt", Body: "run {{.StoryID}}", Status: "READY"})

	// Simulate history after restart where in-memory e.jobs is empty
	key := repoKey("host-mac-mini", repoPath)
	e.SetHistory(key, []HistoryRow{
		{
			JobID:           "historical-job-404",
			ScheduleID:      "sch-old-1",
			PromptTitle:     p.Title,
			Engine:          "agent",
			Status:          "DECLINED",
			Reason:          "preflight_rejected: HALTED_DIRTY",
			ConversationIDs: []string{"conv-discovered-from-history"},
		},
	})

	// Calling ResumeJob on an in-memory-missing jobID should gracefully recover via history
	sch, err := e.ResumeJob("historical-job-404", ResumeOptions{
		AllowDirty: true,
	})
	if err != nil {
		t.Fatalf("ResumeJob failed to recover via history: %v", err)
	}

	if sch.Kind != KindResume {
		t.Fatalf("expected KindResume, got: %s", sch.Kind)
	}
	if sch.StoryID != "STORY-SAMPLE-0098" {
		t.Fatalf("expected StoryID STORY-SAMPLE-0098, got: %s", sch.StoryID)
	}
	if sch.ResumeConversationID != "conv-discovered-from-history" {
		t.Fatalf("expected ResumeConversationID conv-discovered-from-history, got: %s", sch.ResumeConversationID)
	}

	detail, ok := e.GetRepoDetail("host-mac-mini", repoPath)
	if !ok || detail.Repo.Queue != QueueOpen || detail.Repo.Reason != "" {
		t.Fatalf("expected QueueOpen and cleared reason, got: %+v", detail.Repo)
	}
}

func TestResumeRepoInPlaceResolvesStoryFromBranchAndPorcelain(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-mac-mini", "permanent")
	repoPath := "/work/test-repo-04"
	if err := e.UpsertRepo(Repo{
		HostID:       "host-mac-mini",
		WorktreePath: repoPath,
		CloneURL:     "https://example.test/repo.git",
		Queue:        QueuePaused,
		Reason:       ReasonDirty,
	}); err != nil {
		t.Fatal(err)
	}

	// Set branch on repo via preflight
	e.ApplyPreflight("host-mac-mini", repoPath, Preflight{
		GitWorkTree:  true,
		HeadAttached: true,
		Branch:       "feat/story-sample-0101-repo-in-place-resume",
	})

	sch, err := e.ResumeRepoInPlace("host-mac-mini", repoPath)
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if sch.StoryID != "STORY-SAMPLE-0101" {
		t.Fatalf("expected StoryID STORY-SAMPLE-0101 from branch, got: %s", sch.StoryID)
	}
}

func TestResumeRepoInPlaceEngineSelection(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-mac", "permanent")
	repoPath := "/work/app"
	if err := e.UpsertRepo(Repo{
		HostID:                 "host-mac",
		WorktreePath:           repoPath,
		CloneURL:               "https://example.test/app.git",
		DetectedConversationID: "0ddc4208-8d7a-490b-86bb-1dee2e91ae82",
	}); err != nil {
		t.Fatal(err)
	}

	// 1. Antigravity conversation ID should select "agy" engine
	sch1, err := e.ResumeRepoInPlace("host-mac", repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if sch1.Engine != "agy" {
		t.Fatalf("expected engine 'agy' for Antigravity conversation ID, got: %s", sch1.Engine)
	}

	// 2. Explicit engine override should be honored
	sch2, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		Engine: "custom-agent",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sch2.Engine != "custom-agent" {
		t.Fatalf("expected engine 'custom-agent', got: %s", sch2.Engine)
	}

	// 3. Cursor conversation ID should select "agent" engine
	sch3, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		ConversationID: "cursor-123456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sch3.Engine != "agent" {
		t.Fatalf("expected engine 'agent' for cursor conversation ID, got: %s", sch3.Engine)
	}

	// 4. agy- prefixed conversation ID should select "agy" engine
	sch4, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		ConversationID: "agy-0ddc4208-8d7a-490b-86bb-1dee2e91ae82",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sch4.Engine != "agy" {
		t.Fatalf("expected engine 'agy' for agy- prefixed conversation ID, got: %s", sch4.Engine)
	}

	// 5. Uppercase UUID should select "agy" engine
	sch5, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		ConversationID: "0DDC4208-8D7A-490B-86BB-1DEE2E91AE82",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sch5.Engine != "agy" {
		t.Fatalf("expected engine 'agy' for uppercase UUID, got: %s", sch5.Engine)
	}

	// 6. Unknown / empty conversation ID defaults to "agy"
	repoEmpty := "/work/app-empty"
	if err := e.UpsertRepo(Repo{
		HostID:       "host-mac",
		WorktreePath: repoEmpty,
		CloneURL:     "https://example.test/app-empty.git",
	}); err != nil {
		t.Fatal(err)
	}
	sch6, err := e.ResumeRepoInPlace("host-mac", repoEmpty)
	if err != nil {
		t.Fatal(err)
	}
	if sch6.Engine != "agy" {
		t.Fatalf("expected default engine 'agy' for empty conversation ID, got: %s", sch6.Engine)
	}
}

func TestResumeRepoInPlace_ConversationIsolationAcrossStories(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-mac", "permanent")
	repoPath := "/work/app-isolated"
	if err := e.UpsertRepo(Repo{
		HostID:                 "host-mac",
		WorktreePath:           repoPath,
		CloneURL:               "https://example.test/app-isolated.git",
		DirtyStoryID:           "STORY-OLD-001",
		DetectedConversationID: "0ddc4208-8d7a-490b-86bb-1dee2e91ae82",
	}); err != nil {
		t.Fatal(err)
	}

	// Also simulate history rows from STORY-OLD-001
	e.history[repoKey("host-mac", repoPath)] = []HistoryRow{
		{
			JobID:           "job-old-1",
			PromptTitle:     "Execute STORY-OLD-001",
			ConversationIDs: []string{"0ddc4208-8d7a-490b-86bb-1dee2e91ae82"},
			Status:          "FAILED",
		},
	}

	// 1. Resuming for a completely DIFFERENT story must NOT inherit the old conversation ID
	schNew, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		StoryID: "STORY-NEW-002",
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if schNew.ResumeConversationID != "" {
		t.Fatalf("expected empty ResumeConversationID for new story, got %q", schNew.ResumeConversationID)
	}
	if schNew.StoryID != "STORY-NEW-002" {
		t.Fatalf("expected StoryID STORY-NEW-002, got %q", schNew.StoryID)
	}

	// 2. Resuming for the SAME story SHOULD inherit the conversation ID
	schSame, err := e.ResumeRepoInPlace("host-mac", repoPath, ResumeOptions{
		StoryID: "STORY-OLD-001",
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if schSame.ResumeConversationID != "0ddc4208-8d7a-490b-86bb-1dee2e91ae82" {
		t.Fatalf("expected ResumeConversationID 0ddc4208..., got %q", schSame.ResumeConversationID)
	}
}

func TestDeclineJobCancellationSafetyAndCircuitBreaker(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", CloneURL: "https://example.test/app.git"}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Task", Body: "do task", Status: "READY"})

	// 1. Decline on preflight_rejected should immediately mark schedule DECLINED
	sch1, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	j1, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 1 failed")
	}
	if err := e.DeclineJob(j1.ID, "preflight_rejected: HALTED_DIRTY"); err != nil {
		t.Fatal(err)
	}
	schAfter, ok := e.schedules[sch1.ID]
	if !ok || schAfter.Status != "DECLINED" {
		t.Fatalf("expected preflight_rejected to mark schedule DECLINED, got: %s", schAfter.Status)
	}
	// Next poll should NOT re-lease the declined schedule
	_, _, _, ok = e.PollHost("host-1")
	if ok {
		t.Fatalf("expected no schedule leased after preflight rejection")
	}

	// 2. Cancellation safety: declining an in-flight job on a cancelled schedule should NOT resurrect it
	sch2, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	j2, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 2 failed")
	}
	// Cancel the schedule while job is running
	if _, err := e.CancelSchedule(sch2.ID); err != nil {
		t.Fatal(err)
	}
	// Agent declines the cancelled job
	if err := e.DeclineJob(j2.ID, "worktree_busy"); err != nil {
		t.Fatal(err)
	}
	sch2After, ok := e.schedules[sch2.ID]
	if !ok || sch2After.Status != "CANCELLED" {
		t.Fatalf("expected schedule to remain CANCELLED after decline, got: %s", sch2After.Status)
	}

	// 3. Circuit breaker: 3 consecutive transient declines (e.g. worktree_busy) marks schedule DECLINED
	sch3, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Decline 1
	j3_1, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 3.1 failed")
	}
	_ = e.DeclineJob(j3_1.ID, "worktree_busy")
	if e.schedules[sch3.ID].Status != "QUEUED" {
		t.Fatalf("expected QUEUED on retry 1, got: %s", e.schedules[sch3.ID].Status)
	}
	// Decline 2
	j3_2, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 3.2 failed")
	}
	_ = e.DeclineJob(j3_2.ID, "worktree_busy")
	if e.schedules[sch3.ID].Status != "QUEUED" {
		t.Fatalf("expected QUEUED on retry 2, got: %s", e.schedules[sch3.ID].Status)
	}
	// Decline 3 (exceeds MaxDeclineRetries)
	j3_3, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll 3.3 failed")
	}
	_ = e.DeclineJob(j3_3.ID, "worktree_busy")
	if e.schedules[sch3.ID].Status != "DECLINED" {
		t.Fatalf("expected DECLINED on retry 3, got: %s", e.schedules[sch3.ID].Status)
	}
	// Verify no fourth poll
	_, _, _, ok = e.PollHost("host-1")
	if ok {
		t.Fatalf("expected no schedule leased after circuit breaker tripped")
	}
}

func TestPollHostSchedulesNormalJobsWhenRemediated(t *testing.T) {
	curr := time.Unix(1_700_000_000, 0)
	e := NewEngine(func() time.Time { return curr })
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		Queue:        QueueOpen,
		Lock:         LockIdle,
		Reason:       "REMEDIATED",
	})

	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected normal schedule to be leased on REMEDIATED repo with QueueOpen")
	}
	if job.Status != "RUNNING" {
		t.Fatalf("expected job RUNNING, got %s", job.Status)
	}

	repo, _ := e.GetRepo("host-1", "/work/app")
	if repo.Lock != LockRunning || repo.RunningJobID != job.ID {
		t.Fatalf("expected repo lock RUNNING and RunningJobID set, got: %+v", repo)
	}
	if repo.Reason != "" {
		t.Fatalf("expected REMEDIATED reason to be cleared when job starts, got: %s", repo.Reason)
	}
}

func TestDeclineJobClearsRunningJobID(t *testing.T) {
	curr := time.Unix(1_700_000_000, 0)
	e := NewEngine(func() time.Time { return curr })
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	p := e.PutPrompt(Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	repo, _ := e.GetRepo("host-1", "/work/app")
	if repo.RunningJobID != job.ID || repo.Lock != LockRunning {
		t.Fatalf("expected running job %s, got: %+v", job.ID, repo)
	}

	if err := e.DeclineJob(job.ID, "preflight_rejected: HALTED_DIRTY"); err != nil {
		t.Fatal(err)
	}

	repo, _ = e.GetRepo("host-1", "/work/app")
	if repo.Lock != LockIdle {
		t.Fatalf("expected repo LockIdle after decline, got: %s", repo.Lock)
	}
	if repo.RunningJobID != "" {
		t.Fatalf("expected repo RunningJobID cleared after decline, got: %s", repo.RunningJobID)
	}
}

func TestExtractStoryIDFromBranchAndPorcelain(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"chore/app-0098-distribution-profile-remote-scan", "STORY-APP-0098"},
		{"feat/STORY-CORE-104-refactor", "STORY-CORE-104"},
		{"bug/vigo-0042", "STORY-VIGO-0042"},
		{"chore/app-0098-feature", "STORY-APP-0098"},
		{"clean-branch-name-no-id", ""},
	}
	for _, tc := range tests {
		got := extractStoryIDFromString(tc.input)
		if got != tc.expected {
			t.Errorf("extractStoryIDFromString(%q) = %q, expected %q", tc.input, got, tc.expected)
		}
	}

	porcelain := " M sub-agent\n?? docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md"
	sid, rfile := extractStoryIDFromPorcelain(porcelain)
	if sid != "STORY-APP-0098" {
		t.Errorf("expected sid STORY-APP-0098, got %q", sid)
	}
	if rfile != "docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md" {
		t.Errorf("expected review file docs/reviews/..., got %q", rfile)
	}
}

func TestResumeRepoInPlaceResilience(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app-tree",
		CloneURL:     "https://example.test/app-tree.git",
		Queue:        QueueOpen,
	}); err != nil {
		t.Fatal(err)
	}
	p := e.PutPrompt(Prompt{Title: "Ready ASE prompt", Body: "do work", Status: "READY"})
	_ = p

	// Preflight marks repo dirty on branch chore/app-0098-feature
	pre := Preflight{
		Kind:          "ordinary",
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		HeadAttached:  true,
		Branch:        "chore/app-0098-feature",
		DefaultBranch: "main",
		GitAuthOK:     true,
		DocsHubOK:     true,
		WorkPorcelain: " M sub-agent\n?? docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md",
	}
	halt := e.ApplyPreflight("host-1", "/work/app-tree", pre)
	if halt.Reason != ReasonDirty {
		t.Fatalf("expected ReasonDirty, got %q", halt.Reason)
	}

	detail, _ := e.GetRepoDetail("host-1", "/work/app-tree")
	if detail.Repo.DirtyStoryID != "STORY-APP-0098" {
		t.Fatalf("expected DirtyStoryID STORY-APP-0098, got %q", detail.Repo.DirtyStoryID)
	}

	// Trigger ResumeRepoInPlace without explicit story ID
	sch, err := e.ResumeRepoInPlace("host-1", "/work/app-tree", ResumeOptions{AllowDirty: true})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if sch.Kind != KindResume {
		t.Fatalf("expected KindResume, got %s", sch.Kind)
	}
	if sch.StoryID != "STORY-APP-0098" {
		t.Fatalf("expected sch.StoryID STORY-APP-0098, got %q", sch.StoryID)
	}
	if sch.PromptTitle != "Resume STORY-APP-0098 In-Place" {
		t.Fatalf("expected prompt title 'Resume STORY-APP-0098 In-Place', got %q", sch.PromptTitle)
	}

	// Host leases job
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed for resume job")
	}

	// Host sends preflight for resume job with dirty porcelain
	preResume := Preflight{
		Kind:          KindResume,
		FreeBytes:     20 << 30,
		GitWorkTree:   true,
		HeadAttached:  true,
		Branch:        "chore/app-0098-feature",
		DefaultBranch: "main",
		GitAuthOK:     true,
		DocsHubOK:     true,
		WorkPorcelain: " M sub-agent\n?? docs/reviews/2026-10-09-review-v0-26991d-STORY-APP-0098.md",
	}
	haltResume := e.ApplyPreflight("host-1", "/work/app-tree", preResume)
	if haltResume.Reason != "" {
		t.Fatalf("expected empty halt reason for KindResume on dirty worktree, got %q", haltResume.Reason)
	}

	// Verify DirtyStoryID was NOT wiped by preflight
	detailResume, _ := e.GetRepoDetail("host-1", "/work/app-tree")
	if detailResume.Repo.DirtyStoryID != "STORY-APP-0098" {
		t.Fatalf("expected DirtyStoryID to be preserved on resume, got %q", detailResume.Repo.DirtyStoryID)
	}
	_ = job
}

func TestPostExitDiagnosticOptionsAndResumeFallback(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent", "generic")
	p := e.PutPrompt(Prompt{
		Title:   "Story Execution",
		Body:    "execute story",
		Engine:  "agent",
		Status:  "READY",
		StoryID: "STORY-123",
	})
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app-tree",
		CloneURL:     "https://github.com/org/repo.git",
		Queue:        QueueOpen,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/app-tree", 1, nil)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	job, sch, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}
	if sch.Engine != "agent" {
		t.Fatalf("expected engine agent, got %s", sch.Engine)
	}

	// Harness reports exit with QUOTA_EXHAUSTED diagnostic options
	exitErr := e.PostExit(job.ID, 1, ExitOptions{
		ExitCode:       1,
		Reason:         "QUOTA_EXHAUSTED",
		DiagnosticCode: "QUOTA_EXHAUSTED",
		ErrorMessage:   "ActionRequiredError: Increase limits for faster responses You're out of usage. Switch to Auto, or ask your admin to increase your limit to continue.",
		Engine:         "agent",
		Model:          "auto",
		Remediation:    "switch_to_agy",
	})
	if exitErr != nil {
		t.Fatalf("PostExit failed: %v", exitErr)
	}

	detail, ok := e.GetRepoDetail("host-1", "/work/app-tree")
	if !ok {
		t.Fatal("repo not found")
	}
	if detail.Repo.Reason != "QUOTA_EXHAUSTED" {
		t.Fatalf("expected repo.Reason QUOTA_EXHAUSTED, got %q", detail.Repo.Reason)
	}
	if detail.Repo.Queue != QueuePaused {
		t.Fatalf("expected repo.Queue PAUSED, got %q", detail.Repo.Queue)
	}
	if len(detail.History) == 0 {
		t.Fatal("expected history row")
	}
	lastRow := detail.History[len(detail.History)-1]
	if lastRow.DiagnosticCode != "QUOTA_EXHAUSTED" {
		t.Fatalf("expected history DiagnosticCode QUOTA_EXHAUSTED, got %q", lastRow.DiagnosticCode)
	}
	if lastRow.ErrorMessage == "" || !strings.Contains(lastRow.ErrorMessage, "ActionRequiredError") {
		t.Fatalf("expected history ErrorMessage to contain ActionRequiredError, got %q", lastRow.ErrorMessage)
	}
	if lastRow.Model != "auto" {
		t.Fatalf("expected history Model auto, got %q", lastRow.Model)
	}
	if lastRow.Remediation != "switch_to_agy" {
		t.Fatalf("expected history Remediation switch_to_agy, got %q", lastRow.Remediation)
	}

	items := e.ListHistory("host-1", "/work/app-tree", "", 10)
	if len(items) == 0 || items[0].DiagnosticCode != "QUOTA_EXHAUSTED" {
		t.Fatalf("expected ListHistory to reflect diagnostic code, got %+v", items)
	}

	// Trigger ResumeRepoInPlace without explicit engine -> should automatically switch to "agy"
	resumeSch, err := e.ResumeRepoInPlace("host-1", "/work/app-tree", ResumeOptions{AllowDirty: true})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.Engine != "agy" {
		t.Fatalf("expected resume schedule engine 'agy' due to auto-fallback, got %q", resumeSch.Engine)
	}

	detailAfterResume, _ := e.GetRepoDetail("host-1", "/work/app-tree")
	if detailAfterResume.Repo.Queue != QueueOpen {
		t.Fatalf("expected repo queue OPEN after resume, got %q", detailAfterResume.Repo.Queue)
	}
	if detailAfterResume.Repo.Reason != "" {
		t.Fatalf("expected cleared repo reason after resume, got %q", detailAfterResume.Repo.Reason)
	}
}

func TestLeaseExternalAndRelease(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Now().UTC() })
	repo := Repo{HostID: "mac-mini", WorktreePath: "/work/sample-repo", Queue: QueueOpen, Lock: LockIdle}
	_ = e.UpsertRepo(repo)

	// 1. Acquire external lease
	leased, err := e.LeaseExternal("mac-mini", "/work/sample-repo", "autopilot-cli-1", "TASK-42", 10*time.Minute)
	if err != nil {
		t.Fatalf("LeaseExternal failed: %v", err)
	}
	if leased.Lock != LockAutopilot || leased.LockHolder != "autopilot-cli-1" {
		t.Fatalf("expected LockAutopilot and holder autopilot-cli-1, got lock=%s holder=%s", leased.Lock, leased.LockHolder)
	}

	// 2. Conflicting external lease from different holder should fail with ErrLockHeld
	_, err2 := e.LeaseExternal("mac-mini", "/work/sample-repo", "autopilot-cli-2", "STORY-OTHER", 10*time.Minute)
	if !errors.Is(err2, ErrLockHeld) {
		t.Fatalf("expected ErrLockHeld for conflicting external lease, got: %v", err2)
	}

	// 3. LeaseOrdinary should fail because repo is busy with LockAutopilot
	_, err3 := e.LeaseOrdinary("mac-mini", "/work/sample-repo", "https://github.com/example/sample-repo.git")
	if !errors.Is(err3, ErrQueueBlocked) {
		t.Fatalf("expected ErrQueueBlocked for ordinary lease while LockAutopilot active, got: %v", err3)
	}

	// 4. Release external lease
	released, err := e.ReleaseExternal("mac-mini", "/work/sample-repo", "autopilot-cli-1")
	if err != nil {
		t.Fatalf("ReleaseExternal failed: %v", err)
	}
	if released.Lock != LockIdle || released.LockHolder != "" {
		t.Fatalf("expected LockIdle after release, got lock=%s holder=%s", released.Lock, released.LockHolder)
	}

	// 5. Now another holder can acquire
	leased2, err := e.LeaseExternal("mac-mini", "/work/sample-repo", "autopilot-cli-2", "STORY-OTHER", 10*time.Minute)
	if err != nil {
		t.Fatalf("expected successful lease after release, got: %v", err)
	}
	if leased2.Lock != LockAutopilot || leased2.LockHolder != "autopilot-cli-2" {
		t.Fatalf("expected LockAutopilot for holder 2, got lock=%s", leased2.Lock)
	}

	// 6. Repo in LockASE must reject LeaseExternal
	repoASE := Repo{HostID: "mac-mini", WorktreePath: "/work/ase-repo", Queue: QueueOpen, Lock: LockASE}
	_ = e.UpsertRepo(repoASE)
	_, errASE := e.LeaseExternal("mac-mini", "/work/ase-repo", "autopilot-cli-1", "STORY-1", 10*time.Minute)
	if !errors.Is(errASE, ErrLockHeld) {
		t.Fatalf("expected ErrLockHeld for repo in LockASE, got: %v", errASE)
	}
}

func TestGlobalQueueAllocationHonoredByPollHost(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Unix(100, 0) })
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/repos/app-1",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetRepoQueueAllocation("host-1", "/repos/app-1", false); err != nil {
		t.Fatal(err)
	}

	pLocal := e.PutPrompt(Prompt{
		Title:    "Local task",
		Engine:   "agent",
		Status:   "READY",
		Revision: 1,
	})
	pGlobal := e.PutPrompt(Prompt{
		Title:    "Global task",
		Engine:   "agent",
		Status:   "READY",
		Revision: 1,
	})

	// 1. Enqueue Level 2 Global Schedule
	globalSch, err := e.EnqueueGlobalSchedule(pGlobal.ID, 1, nil)
	if err != nil {
		t.Fatalf("EnqueueGlobalSchedule failed: %v", err)
	}
	if !globalSch.IsGlobal {
		t.Fatalf("expected IsGlobal true on global schedule")
	}

	// 2. PollHost when GlobalQueueAllocationEnabled is false -> should NOT lease global schedule
	_, _, _, ok := e.PollHost("host-1")
	if ok {
		t.Fatal("expected PollHost to return false when GlobalQueueAllocationEnabled is false")
	}

	// 3. Enqueue Level 1 Local Schedule
	localSch, err := e.ExecutePrompt(pLocal.ID, "host-1", "/repos/app-1", 1, nil)
	if err != nil {
		t.Fatalf("ExecutePrompt failed: %v", err)
	}
	if localSch.IsGlobal {
		t.Fatalf("expected IsGlobal false on local schedule")
	}

	// 4. PollHost should now lease the local schedule even though GlobalQueueAllocationEnabled is false
	job, leasedSch, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected PollHost to lease Level 1 local schedule")
	}
	if leasedSch.ID != localSch.ID {
		t.Fatalf("expected leased schedule %s, got %s", localSch.ID, leasedSch.ID)
	}

	// Finish local job
	e.ApplyFinish("host-1", "/repos/app-1", job.ID, FinishInput{ASEComplete: true})

	// 5. Check GetRepoDetail exposes both local and global schedules
	detail, ok := e.GetRepoDetail("host-1", "/repos/app-1")
	if !ok {
		t.Fatal("GetRepoDetail returned false")
	}
	if len(detail.GlobalSchedules) != 1 || detail.GlobalSchedules[0].ID != globalSch.ID {
		t.Fatalf("expected 1 global schedule in detail, got %+v", detail.GlobalSchedules)
	}

	// 6. Enable global queue allocation
	if err := e.SetRepoQueueAllocation("host-1", "/repos/app-1", true); err != nil {
		t.Fatalf("SetRepoQueueAllocation failed: %v", err)
	}

	// 7. PollHost should now lease the global schedule
	globalJob, leasedGlobalSch, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected PollHost to lease Level 2 global schedule when enabled")
	}
	if leasedGlobalSch.ID != globalSch.ID {
		t.Fatalf("expected leased schedule %s, got %s", globalSch.ID, leasedGlobalSch.ID)
	}
	if globalJob.ID == "" {
		t.Fatal("expected non-empty job ID for leased global job")
	}
}

func TestLocalSchedulePrioritizedOverHigherPriorityGlobalSchedule(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Unix(100, 0) })
	e.RegisterHost("host-1", "permanent")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/repos/app-1",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}); err != nil {
		t.Fatal(err)
	}

	pGlobal := e.PutPrompt(Prompt{
		Title:    "Global high priority task",
		Engine:   "agent",
		Status:   "READY",
		Revision: 1,
	})
	pLocal := e.PutPrompt(Prompt{
		Title:    "Local normal priority task",
		Engine:   "agent",
		Status:   "READY",
		Revision: 1,
	})

	// Enqueue Level 2 Global Schedule with higher numeric priority
	globalSch, err := e.EnqueueGlobalSchedule(pGlobal.ID, 1, nil)
	if err != nil {
		t.Fatalf("EnqueueGlobalSchedule failed: %v", err)
	}
	e.mu.Lock()
	e.schedules[globalSch.ID].Priority = 999
	e.mu.Unlock()

	// Enqueue Level 1 Local Schedule with lower numeric priority (10)
	localSch, err := e.ExecutePrompt(pLocal.ID, "host-1", "/repos/app-1", 1, nil)
	if err != nil {
		t.Fatalf("ExecutePrompt failed: %v", err)
	}
	e.mu.Lock()
	e.schedules[localSch.ID].Priority = 10
	e.mu.Unlock()

	// PollHost must choose Level 1 Local schedule over Level 2 Global schedule
	_, leased, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected PollHost to return true")
	}
	if leased.ID != localSch.ID {
		t.Fatalf("expected Level 1 local schedule %s to take precedence over Level 2 global schedule %s, got %s", localSch.ID, globalSch.ID, leased.ID)
	}
}

func TestGlobalStoryScheduleDynamicAllocationMatching(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Now() })
	e.RegisterHost("host-idle", "linux")
	if err := e.UpsertRepo(Repo{
		HostID:       "host-idle",
		WorktreePath: "/workspace/fleet/iazio-web",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}); err != nil {
		t.Fatal(err)
	}
	_ = e.SetRepoQueueAllocation("host-idle", "/workspace/fleet/iazio-web", true)

	// Enqueue global story schedule with submodule "iazio-web"
	sch, err := e.EnqueueGlobalStorySchedule("STORY-GOV-3576", "iazio-web", "agy", "run story")
	if err != nil {
		t.Fatalf("failed to enqueue global story schedule: %v", err)
	}
	if sch.Status != "QUEUED" || !sch.IsGlobal {
		t.Fatalf("expected QUEUED and IsGlobal=true, got %+v", sch)
	}

	// PollHost on host-idle should match the repo /workspace/fleet/iazio-web
	job, leasedSch, _, ok := e.PollHost("host-idle")
	if !ok {
		t.Fatalf("expected host-idle to lease global schedule for iazio-web")
	}
	if leasedSch.ID != sch.ID {
		t.Fatalf("expected leased schedule %s, got %s", sch.ID, leasedSch.ID)
	}
	if leasedSch.WorktreePath != "/workspace/fleet/iazio-web" {
		t.Fatalf("expected resolved worktree /workspace/fleet/iazio-web, got %s", leasedSch.WorktreePath)
	}
	if job.ID == "" {
		t.Fatalf("expected job to be generated")
	}
}

func TestEngine_StoryOperations(t *testing.T) {
	e := NewEngine(nil)

	// 1. SaveStory with empty ID -> error
	err := e.SaveStory(Story{})
	if err == nil {
		t.Fatal("expected error saving story with empty ID")
	}

	// Channel for OnStoryChange verification
	changedCh := make(chan Story, 5)
	e.OnStoryChange = func(s Story) {
		changedCh <- s
	}

	// 2. Save valid story
	st1 := Story{
		ID:        "STORY-ENG-1",
		Repo:      "app-repo",
		Submodule: "iazio-harness",
		Title:     "Engine Story 1",
	}
	if err := e.SaveStory(st1); err != nil {
		t.Fatalf("failed to save story 1: %v", err)
	}

	select {
	case changed := <-changedCh:
		if changed.ID != "STORY-ENG-1" || changed.SyncState != "PENDING_WORKSPACE_MATERIALIZATION" || changed.Status != "pending" {
			t.Errorf("unexpected story in change callback: %+v", changed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for OnStoryChange")
	}

	// 3. GetStory
	got, ok := e.GetStory("STORY-ENG-1")
	if !ok || got.ID != "STORY-ENG-1" || got.SyncState != "PENDING_WORKSPACE_MATERIALIZATION" {
		t.Fatalf("expected to get story 1, got %+v (ok=%v)", got, ok)
	}

	_, ok = e.GetStory("STORY-NONEXISTENT")
	if ok {
		t.Fatal("expected ok=false for non-existent story")
	}

	// 4. Save second story with different repo
	st2 := Story{
		ID:        "STORY-ENG-2",
		Repo:      "other-repo",
		Submodule: "other-sub",
		Title:     "Engine Story 2",
		CreatedAt: time.Now().Add(-1 * time.Hour),
	}
	if err := e.SaveStory(st2); err != nil {
		t.Fatalf("failed to save story 2: %v", err)
	}
	<-changedCh

	// 5. ListStories (unfiltered and filtered)
	all := e.ListStories("")
	if len(all) != 2 {
		t.Fatalf("expected 2 stories, got %d", len(all))
	}
	if all[0].ID != "STORY-ENG-1" { // CreatedAt is newer
		t.Errorf("expected STORY-ENG-1 first, got %s", all[0].ID)
	}

	filtered := e.ListStories("app-repo")
	if len(filtered) != 1 || filtered[0].ID != "STORY-ENG-1" {
		t.Fatalf("expected 1 filtered story, got %+v", filtered)
	}

	// 6. UpdateStorySyncState
	if err := e.UpdateStorySyncState("STORY-ENG-1", "MATERIALIZED"); err != nil {
		t.Fatalf("failed to update sync state: %v", err)
	}
	select {
	case changed := <-changedCh:
		if changed.SyncState != "MATERIALIZED" {
			t.Errorf("expected SyncState MATERIALIZED, got %s", changed.SyncState)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for OnStoryChange after sync state update")
	}

	// Update non-existent story
	if err := e.UpdateStorySyncState("STORY-NONEXISTENT", "MATERIALIZED"); err == nil {
		t.Fatal("expected error updating non-existent story")
	}
}

func TestEngine_FindHistoryRow(t *testing.T) {
	e := NewEngine(nil)

	// Add host and repo
	e.RegisterHost("host-1", "linux", "generic")
	_ = e.UpsertRepo(Repo{HostID: "host-1", WorktreePath: "/work/app", Queue: QueueOpen, Lock: LockIdle})

	// Inject a history row directly into e.history
	e.mu.Lock()
	key := repoKey("host-1", "/work/app")
	e.history[key] = []HistoryRow{
		{
			JobID:        "job-find-999",
			Engine:       "opencode",
			Status:       "success",
			ErrorMessage: "",
			CreatedAt:    time.Now().UTC(),
		},
	}
	e.mu.Unlock()

	row, host, path, found := e.FindHistoryRow("job-find-999")
	if !found {
		t.Fatal("expected to find history row")
	}
	if row.JobID != "job-find-999" || host != "host-1" || path != "/work/app" {
		t.Fatalf("unexpected history row: %+v, host=%s, path=%s", row, host, path)
	}

	_, _, _, found = e.FindHistoryRow("job-unknown")
	if found {
		t.Fatal("expected found=false for unknown job ID")
	}
}

func TestEngine_GetRepo(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "linux")
	r := Repo{
		HostID:       "host-1",
		WorktreePath: "/work/repo1",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}
	e.UpsertRepo(r)

	// GetRepo hit and miss
	if got, ok := e.GetRepo("host-1", "/work/repo1"); !ok || got.HostID != "host-1" {
		t.Fatalf("expected repo host-1, got %v (ok=%v)", got, ok)
	}
	if _, ok := e.GetRepo("host-1", "/nonexistent"); ok {
		t.Fatal("expected ok=false for nonexistent repo")
	}
}

func TestAbandonJob_RepoBusyRefundsAndKeepsQueueOpen(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "linux")
	r := Repo{
		HostID:       "host-1",
		WorktreePath: "/work/repo1",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	}
	e.UpsertRepo(r)

	p := &Prompt{ID: "p1", Title: "test prompt", Engine: "agy", Body: "prompt text", Status: "READY"}
	e.prompts["p1"] = p

	sch, err := e.ExecutePrompt("p1", "host-1", "/work/repo1", 1, nil)
	if err != nil {
		t.Fatalf("ExecutePrompt failed: %v", err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("expected job to be leased")
	}

	// Host agent encounters busy lock and abandons with REPO_BUSY
	if err := e.AbandonJob(job.ID, "REPO_BUSY"); err != nil {
		t.Fatalf("AbandonJob failed: %v", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	j := e.jobs[job.ID]
	if j.Status != "FAILED" || j.Reason != "REPO_BUSY" {
		t.Fatalf("expected job FAILED/REPO_BUSY, got status=%s, reason=%s", j.Status, j.Reason)
	}
	s := e.schedules[sch.ID]
	if s.Status != "QUEUED" || s.IterationsRemaining != 1 {
		t.Fatalf("expected schedule retained QUEUED with 1 iteration remaining, got status=%s, remaining=%d", s.Status, s.IterationsRemaining)
	}
	repo := e.repos[repoKey("host-1", "/work/repo1")]
	if repo.Queue != QueueOpen {
		t.Fatalf("expected queue to remain OPEN, got: %s", repo.Queue)
	}
}

func TestReapExpiredLocked_OrphanedHistoryReconciled(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{HostID: "mac-mini", WorktreePath: "/work/meta-04"})

	// Inject an orphaned RUNNING history row
	key := repoKey("mac-mini", "/work/meta-04")
	e.history[key] = []HistoryRow{
		{
			JobID:       "job-1250",
			ScheduleID:  "sch-1249",
			PromptTitle: "iazio review",
			Engine:      "agy",
			Status:      "RUNNING",
			CreatedAt:   time.Now().Add(-2 * time.Hour),
		},
	}

	// Trigger reap
	e.mu.Lock()
	e.reapExpiredLocked()
	e.mu.Unlock()

	detail, ok := e.GetRepoDetail("mac-mini", "/work/meta-04")
	if !ok || len(detail.History) == 0 {
		t.Fatal("expected repo detail and history")
	}

	row := detail.History[0]
	if row.Status != "TIMED_OUT" {
		t.Fatalf("expected history row to transition to TIMED_OUT, got %s", row.Status)
	}
	if row.Reason != "ORPHANED_RUN_EXPIRED" {
		t.Fatalf("expected reason ORPHANED_RUN_EXPIRED, got %s", row.Reason)
	}
	if row.DiagnosticCode != "EXECUTION_TIMEOUT" {
		t.Fatalf("expected DiagnosticCode EXECUTION_TIMEOUT, got %s", row.DiagnosticCode)
	}
}

func TestPollHost_GlobalScheduleSkipsDirtyRepo(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("pc1", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:                       "pc1",
		WorktreePath:                 "C:\\work\\target-repo",
		GlobalQueueAllocationEnabled: true,
	})

	// Mark pc1 dirty
	e.repos[repoKey("pc1", "C:\\work\\target-repo")].Reason = "HALTED_DIRTY"

	// Enqueue global schedule
	sch, err := e.EnqueueGlobalStorySchedule("STORY-ROOT-45276", ".", "autopilot", "test prompt")
	if err != nil {
		t.Fatalf("EnqueueGlobalStorySchedule failed: %v", err)
	}

	// Poll pc1: should NOT be leased to the dirty repo
	job, _, _, ok := e.PollHost("pc1")
	if ok {
		t.Fatalf("expected PollHost on dirty repo to return false, leased job: %+v", job)
	}

	// Schedule must remain QUEUED and global
	e.mu.Lock()
	s := e.schedules[sch.ID]
	e.mu.Unlock()
	if s.Status != "QUEUED" || !s.IsGlobal {
		t.Fatalf("expected schedule to remain QUEUED/IsGlobal, got status=%s, isGlobal=%v", s.Status, s.IsGlobal)
	}
}

func TestDeclineJob_GlobalScheduleRequeuedOnPreflightDecline(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("pc1", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:                       "pc1",
		WorktreePath:                 "C:\\work\\target-repo",
		GlobalQueueAllocationEnabled: true,
	})

	sch, err := e.EnqueueGlobalStorySchedule("STORY-ROOT-45276", ".", "autopilot", "test prompt")
	if err != nil {
		t.Fatalf("EnqueueGlobalStorySchedule failed: %v", err)
	}

	job, _, _, ok := e.PollHost("pc1")
	if !ok {
		t.Fatal("expected PollHost to lease clean repo")
	}

	// Host declines due to preflight rejection
	if err := e.DeclineJob(job.ID, "preflight_rejected: HALTED_DIRTY"); err != nil {
		t.Fatalf("DeclineJob failed: %v", err)
	}

	e.mu.Lock()
	s := e.schedules[sch.ID]
	e.mu.Unlock()

	// Global schedule must be re-queued and unpinned from pc1
	if s.Status != "QUEUED" {
		t.Fatalf("expected global schedule to be re-queued, got %s", s.Status)
	}
	if s.HostID != "" {
		t.Fatalf("expected global schedule HostID to be unpinned, got %q", s.HostID)
	}
}

func TestPostExit_ClearsRunningJobIDOnCompletionAndFailure(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/target-repo-04"
	_ = e.UpsertRepo(Repo{
		HostID:                       "mac-mini",
		WorktreePath:                 repoPath,
		GlobalQueueAllocationEnabled: true,
	})

	_, err := e.EnqueueGlobalStorySchedule("STORY-ROOT-65584", ".", "autopilot", "run story")
	if err != nil {
		t.Fatalf("EnqueueGlobalStorySchedule failed: %v", err)
	}

	job, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected PollHost to lease clean repo")
	}

	// Verify running_job_id is set while running
	det, ok := e.GetRepoDetail("mac-mini", repoPath)
	if !ok || det.Repo.RunningJobID != job.ID {
		t.Fatalf("expected RunningJobID=%s, got %s", job.ID, det.Repo.RunningJobID)
	}

	// Post exit with code 0 (RUN_FINISHED)
	if err := e.PostExit(job.ID, 0); err != nil {
		t.Fatalf("PostExit failed: %v", err)
	}

	// Verify RunningJobID is cleared immediately
	hostDet, ok := e.GetHostDetail("mac-mini")
	if !ok {
		t.Fatal("expected GetHostDetail to succeed")
	}
	for _, r := range hostDet.Repos {
		if r.WorktreePath == repoPath {
			if r.RunningJobID != "" {
				t.Fatalf("expected RunningJobID to be cleared post-exit, got %s", r.RunningJobID)
			}
			if r.Lock != LockIdle {
				t.Fatalf("expected LockIdle post-exit, got %s", r.Lock)
			}
		}
	}
}

func TestReapExpired_ReconcilesIdleRepoRunningJobID(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/target-repo-04"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
	})

	// Manually inject a leaked running_job_id on an IDLE repo
	e.mu.Lock()
	r := e.repos[repoKey("mac-mini", repoPath)]
	r.Lock = LockIdle
	r.RunningJobID = "job-stale-999"
	e.mu.Unlock()

	// GetHostDetail triggers reapExpiredLocked and must reconcile
	hostDet, ok := e.GetHostDetail("mac-mini")
	if !ok {
		t.Fatal("expected GetHostDetail to succeed")
	}
	for _, rep := range hostDet.Repos {
		if rep.WorktreePath == repoPath {
			if rep.RunningJobID != "" {
				t.Fatalf("expected leaked RunningJobID to be cleaned by reaper, got %s", rep.RunningJobID)
			}
		}
	}
}

func TestEngine_DeterministicResumeConversationPinning_SameConversation(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConvID := "aad902c8-bda6-44c7-8a68-c468a1562399"

	// 1. Initial job-1311
	p := e.PutPrompt(Prompt{
		ID:     "prompt-ase-1",
		Title:  "Autonomous Story Execution",
		Engine: "agy",
		Status: "READY",
	})
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("execute sch1 failed: %v", err)
	}

	job1, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll failed for job1")
	}
	if job1.InitialConversationID != initialConvID || job1.ActiveConversationID != initialConvID {
		t.Fatalf("job1 conversation mismatch: init=%s active=%s", job1.InitialConversationID, job1.ActiveConversationID)
	}

	// Job1 fails/halts
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	// 2. Resume job-1311 -> creates job-1313
	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:                 job1.ID,
		ParentJobID:           job1.ID,
		InitialConversationID: initialConvID,
		StoryID:               "STORY-GOV-3678",
		Engine:                "agy",
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.ParentJobID != job1.ID {
		t.Fatalf("expected ParentJobID %s, got %s", job1.ID, resumeSch.ParentJobID)
	}
	if resumeSch.InitialConversationID != initialConvID {
		t.Fatalf("expected InitialConversationID %s, got %s", initialConvID, resumeSch.InitialConversationID)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected initial ResumeMode pending, got %s", resumeSch.ResumeMode)
	}

	// 3. Poll host leases resumed job (job-1313)
	jobResume, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll failed for resume job")
	}
	if jobResume.ParentJobID != job1.ID {
		t.Fatalf("resumed job ParentJobID %s want %s", jobResume.ParentJobID, job1.ID)
	}
	if jobResume.InitialConversationID != initialConvID {
		t.Fatalf("resumed job InitialConversationID %s want %s", jobResume.InitialConversationID, initialConvID)
	}
	if jobResume.ActiveConversationID != initialConvID {
		t.Fatalf("resumed job ActiveConversationID %s want %s", jobResume.ActiveConversationID, initialConvID)
	}

	// 4. Runner reattaches to same conversation
	err = e.AddJobConversationWithOptions(jobResume.ID, initialConvID, ConversationAttachOptions{
		Role:     "initial_resumed",
		CLIArgs:  "--conversation=" + initialConvID,
		ASEPhase: "RESUME",
	})
	if err != nil {
		t.Fatalf("AddJobConversationWithOptions failed: %v", err)
	}

	det, ok := e.GetJob(jobResume.ID)
	if !ok {
		t.Fatal("GetJob failed")
	}
	if len(det.ConversationIDs) != 1 || det.ConversationIDs[0] != initialConvID {
		t.Fatalf("expected only initial conversation ID %v, got %v", initialConvID, det.ConversationIDs)
	}
	if det.ResumeMode != "same_conversation" {
		t.Fatalf("expected confirmed ResumeMode same_conversation, got %s", det.ResumeMode)
	}

	// 5. If an unpinned tertiary conversation (e.g. 14de9a1f-...) is reported, it must be ignored!
	tertiaryConvID := "14de9a1f-5679-4f11-a08f-f32bc7e47663"
	_ = e.AddJobConversation(jobResume.ID, tertiaryConvID)

	detAfter, _ := e.GetJob(jobResume.ID)
	if len(detAfter.ConversationIDs) != 1 || detAfter.ConversationIDs[0] != initialConvID {
		t.Fatalf("unpinned tertiary conversation leaked into job! got %v", detAfter.ConversationIDs)
	}
}

func TestEngine_DeterministicResumeConversationPinning_ControlledFallback(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConvID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	fallbackConvID := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"

	p := e.PutPrompt(Prompt{
		ID:     "prompt-ase-2",
		Title:  "Autonomous Story Execution Fallback",
		Engine: "agy",
		Status: "READY",
	})
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("execute sch failed: %v", err)
	}

	job, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll failed")
	}

	// Initial conversation could not be attached -> runner triggers controlled fallback
	fallbackReason := "session not found on host disk for engine agy"
	err = e.AddJobConversationWithOptions(job.ID, fallbackConvID, ConversationAttachOptions{
		Fallback:       true,
		FallbackReason: fallbackReason,
		Role:           "fallback_primary",
		CLIArgs:        "--conversation=" + fallbackConvID,
		ASEPhase:       "FALLBACK_START",
	})
	if err != nil {
		t.Fatalf("AddJobConversationWithOptions fallback failed: %v", err)
	}

	det, ok := e.GetJob(job.ID)
	if !ok {
		t.Fatal("GetJob failed")
	}
	if det.ActiveConversationID != fallbackConvID {
		t.Fatalf("expected ActiveConversationID %s, got %s", fallbackConvID, det.ActiveConversationID)
	}
	if det.InitialConversationID != initialConvID {
		t.Fatalf("expected InitialConversationID %s, got %s", initialConvID, det.InitialConversationID)
	}
	if det.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode fallback_new_conversation, got %s", det.ResumeMode)
	}
	if det.ResumeFallbackReason != fallbackReason {
		t.Fatalf("expected ResumeFallbackReason %q, got %q", fallbackReason, det.ResumeFallbackReason)
	}
	if len(det.ConversationIDs) != 2 || det.ConversationIDs[0] != initialConvID || det.ConversationIDs[1] != fallbackConvID {
		t.Fatalf("expected conversation_ids [%s, %s], got %v", initialConvID, fallbackConvID, det.ConversationIDs)
	}

	// 6. Verify subsequent arbitrary conversation is ignored (pinned to fallbackConvID)
	arbitraryConvID := "14de9a1f-5679-4f11-a08f-f32bc7e47663"
	_ = e.AddJobConversation(job.ID, arbitraryConvID)

	detAfter, _ := e.GetJob(job.ID)
	if len(detAfter.ConversationIDs) != 2 {
		t.Fatalf("arbitrary conversation corrupted job! got: %v", detAfter.ConversationIDs)
	}
	if detAfter.ActiveConversationID != fallbackConvID {
		t.Fatalf("active conversation shifted unexpectedly to %s", detAfter.ActiveConversationID)
	}
}

func TestEngine_MultiHopChainResume_LineageAndAttempt(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConvID := "conv-hop-1-init"
	fallbackConvID := "conv-hop-2-fallback"

	p := e.PutPrompt(Prompt{
		ID:     "prompt-hop-1",
		Title:  "Multi-Hop Resume",
		Engine: "agy",
		Status: "READY",
	})

	// Job 1: Initial Run
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll job1 failed")
	}
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "HALTED_DIRTY"})

	// Job 2: First resume -> falls back to conversation B
	sch2, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace job2 failed: %v", err)
	}
	job2, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll job2 failed")
	}
	if sch2.ExecutionAttempt != 2 {
		t.Fatalf("expected attempt 2 for job2, got %d", sch2.ExecutionAttempt)
	}
	// Simulate fallback on job2
	_ = e.AddJobConversationWithOptions(job2.ID, fallbackConvID, ConversationAttachOptions{
		Fallback:       true,
		FallbackReason: "initial session corrupted",
		Role:           "fallback_primary",
		CLIArgs:        "--conversation=" + fallbackConvID,
		ASEPhase:       "FALLBACK_START",
	})
	_ = e.PostExit(job2.ID, 1, ExitOptions{Reason: "HALTED_DIRTY"})

	// Job 3: Second resume -> MUST resume conversation B (ActiveConversationID of Job 2) (F3)
	sch3, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job2.ID,
		ParentJobID: job2.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace job3 failed: %v", err)
	}
	if sch3.ResumeConversationID != fallbackConvID {
		t.Fatalf("Job 3 ResumeConversationID want %s, got %s", fallbackConvID, sch3.ResumeConversationID)
	}
	if sch3.ActiveConversationID != fallbackConvID {
		t.Fatalf("Job 3 ActiveConversationID want %s, got %s", fallbackConvID, sch3.ActiveConversationID)
	}
	if sch3.InitialConversationID != initialConvID {
		t.Fatalf("Job 3 InitialConversationID want %s, got %s", initialConvID, sch3.InitialConversationID)
	}
	// Verify attempt is 3 and chain contains job1 and job2 (F8)
	if sch3.ExecutionAttempt != 3 {
		t.Fatalf("Job 3 ExecutionAttempt want 3, got %d", sch3.ExecutionAttempt)
	}
	if len(sch3.ExecutionChain) != 2 || sch3.ExecutionChain[0] != job1.ID || sch3.ExecutionChain[1] != job2.ID {
		t.Fatalf("Job 3 ExecutionChain want [%s, %s], got %v", job1.ID, job2.ID, sch3.ExecutionChain)
	}
	if len(sch3.AssociatedJobs) != 2 || sch3.AssociatedJobs[0] != job1.ID || sch3.AssociatedJobs[1] != job2.ID {
		t.Fatalf("Job 3 AssociatedJobs want [%s, %s], got %v", job1.ID, job2.ID, sch3.AssociatedJobs)
	}
}

func TestEngine_DroppedUnpinnedLineageOnly_NoMutation(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConvID := "conv-active-pinned"
	p := e.PutPrompt(Prompt{
		ID:     "prompt-f5",
		Title:  "F5 Test",
		Engine: "agy",
		Status: "READY",
	})
	_, _ = e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	job, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll failed")
	}

	// Post dropped_unpinned out-of-order
	err := e.AddJobConversationWithOptions(job.ID, "conv-unpinned-garbage", ConversationAttachOptions{
		Role:     "dropped_unpinned",
		CLIArgs:  "--conversation=conv-unpinned-garbage",
		ASEPhase: "STEP_X",
	})
	if err != nil {
		t.Fatalf("unexpected error adding dropped_unpinned: %v", err)
	}

	det, _ := e.GetJob(job.ID)
	if det.ActiveConversationID != initialConvID {
		t.Fatalf("ActiveConversationID mutated! got %s want %s", det.ActiveConversationID, initialConvID)
	}
	if det.ResumeMode == "fallback_new_conversation" {
		t.Fatalf("ResumeMode unexpectedly mutated to fallback_new_conversation")
	}
	if len(det.ConversationIDs) != 1 || det.ConversationIDs[0] != initialConvID {
		t.Fatalf("ConversationIDs corrupted! got %v", det.ConversationIDs)
	}
	if len(det.ConversationLineage) != 2 || det.ConversationLineage[1].Role != "dropped_unpinned" {
		t.Fatalf("ConversationLineage missing dropped_unpinned! got %v", det.ConversationLineage)
	}
}

func TestEngine_CrossEngineResume_MismatchFallback(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialConvID := "cursor-conv-session-12345"
	p := e.PutPrompt(Prompt{
		ID:     "prompt-engine-mismatch",
		Title:  "Engine Mismatch Test",
		Engine: "agent",
		Status: "READY",
	})

	// Parent job executed with engine "agent"
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	parentJob, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll failed")
	}
	_ = e.PostExit(parentJob.ID, 1, ExitOptions{Reason: "FAILED"})

	// Resume requesting engine "agy" (mismatch with parent "agent")
	sch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       parentJob.ID,
		ParentJobID: parentJob.ID,
		Engine:      "agy",
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}

	if sch.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode fallback_new_conversation for cross-engine resume, got %s", sch.ResumeMode)
	}
	if sch.ResumeConversationID != "" {
		t.Fatalf("expected empty ResumeConversationID for cross-engine resume, got %s", sch.ResumeConversationID)
	}
	if sch.ResumeFallbackReason == "" || !strings.Contains(sch.ResumeFallbackReason, "cross-engine") {
		t.Fatalf("expected cross-engine fallback reason, got %q", sch.ResumeFallbackReason)
	}
}

func TestEngine_PendingResumeMode_UntilAttachmentConfirmed(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/meta-repo-03"
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	p := e.PutPrompt(Prompt{
		ID:     "prompt-pending-test",
		Title:  "Pending Mode Test",
		Engine: "agy",
		Status: "READY",
	})
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, convID)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	// Resumed schedule is created: ResumeMode MUST be "pending" before harness confirmation!
	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected initial ResumeMode 'pending', got %q", resumeSch.ResumeMode)
	}

	job2, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll job2 failed")
	}
	if job2.ResumeMode != "pending" {
		t.Fatalf("expected initial Job ResumeMode 'pending', got %q", job2.ResumeMode)
	}

	// Now confirm attachment:
	_ = e.AddJobConversationWithOptions(job2.ID, convID, ConversationAttachOptions{
		Role:     "initial_resumed",
		CLIArgs:  "--conversation=" + convID,
		ASEPhase: "RESUME_RUN",
	})

	det, _ := e.GetJob(job2.ID)
	if det.ResumeMode != "same_conversation" {
		t.Fatalf("expected confirmed ResumeMode 'same_conversation', got %q", det.ResumeMode)
	}
	if !det.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed true")
	}
}

func TestEngine_PendingToImplicitSameConversation_OnExit(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-1", Title: "Ready ASE prompt", Engine: "agy", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "aad902c8-bda6-44c7-8a68-c468a1562399")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected initial ResumeMode 'pending', got %q", resumeSch.ResumeMode)
	}

	job2, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll job2 failed")
	}

	// Exit 0 with no explicit marker: should map to same_conversation_unverified (Finding M3)
	_ = e.PostExit(job2.ID, 0)

	det, _ := e.GetJob(job2.ID)
	if det.ResumeMode != "same_conversation_unverified" {
		t.Fatalf("expected exit-time ResumeMode 'same_conversation_unverified', got %q", det.ResumeMode)
	}
	if det.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed false on exit without positive attach evidence")
	}
	if det.AttachConfirmation != "unverified" {
		t.Fatalf("expected AttachConfirmation 'unverified', got %q", det.AttachConfirmation)
	}

	// Verify history record also updated
	lastRow, _, _, found := e.FindHistoryRow(job2.ID)
	if !found {
		t.Fatal("expected history row for job2")
	}
	if lastRow.ResumeMode != "same_conversation_unverified" {
		t.Fatalf("expected history ResumeMode 'same_conversation_unverified', got %q", lastRow.ResumeMode)
	}
	if lastRow.AttachConfirmation != "unverified" {
		t.Fatalf("expected history AttachConfirmation 'unverified', got %q", lastRow.AttachConfirmation)
	}
}

func TestEngine_CrossEngineResume_QuotaOrRewriteFallback(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-1", Title: "Ready ASE prompt", Engine: "agent", Status: "READY"})

	// Parent job ran with engine "agent"
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "cursor-123")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{DiagnosticCode: "QUOTA_EXHAUSTED", Reason: "QUOTA_EXHAUSTED"})

	// Resume: repo is quota-exhausted so engine rewrites from agent to agy
	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.Engine != "agy" {
		t.Fatalf("expected rewritten engine 'agy', got %q", resumeSch.Engine)
	}
	if resumeSch.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode 'fallback_new_conversation' after engine rewrite, got %q", resumeSch.ResumeMode)
	}
	if !strings.Contains(resumeSch.ResumeFallbackReason, "cross-engine") {
		t.Fatalf("expected cross-engine fallback reason, got %q", resumeSch.ResumeFallbackReason)
	}
	if resumeSch.ResumeConversationID != "" {
		t.Fatalf("expected empty ResumeConversationID for cross-engine, got %q", resumeSch.ResumeConversationID)
	}
}

func TestEngine_ResumeMode_BodyOverrideIgnored(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-1", Title: "Ready ASE prompt", Engine: "agy", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "aad902c8-bda6-44c7-8a68-c468a1562399")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	// Client maliciously passes ResumeMode="same_conversation"
	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		ResumeMode:  "same_conversation",
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected client-passed same_conversation to be ignored and remain 'pending', got %q", resumeSch.ResumeMode)
	}
}

func TestEngine_FallbackTerminal_RePostActiveID_StaysFallback(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-1", Title: "Ready ASE prompt", Engine: "agy", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "aad902c8-bda6-44c7-8a68-c468a1562399")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	_, _ = e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})

	job2, _, _, _ := e.PollHost("mac-mini")

	// Trigger fallback:
	fallbackConvID := "11112222-3333-4444-5555-666677778888"
	_ = e.AddJobConversationWithOptions(job2.ID, fallbackConvID, ConversationAttachOptions{
		Fallback:       true,
		FallbackReason: "session unattachable",
		Role:           "fallback_primary",
	})

	det, _ := e.GetJob(job2.ID)
	if det.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected fallback_new_conversation, got %q", det.ResumeMode)
	}

	// Now re-post the active fallback ID without options (Finding A4)
	_ = e.AddJobConversation(job2.ID, fallbackConvID)

	detAfter, _ := e.GetJob(job2.ID)
	if detAfter.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode to remain 'fallback_new_conversation', but got relabelled to %q", detAfter.ResumeMode)
	}
	if detAfter.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed to remain false on fallback job")
	}
}

func TestEngine_PendingToExitNonZero_NotConfirmedSameConversation(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-1", Title: "Ready ASE prompt", Engine: "agy", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "aad902c8-bda6-44c7-8a68-c468a1562399")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	resumeSch, err := e.ResumeRepoInPlace("mac-mini", repoPath, ResumeOptions{
		JobID:       job1.ID,
		ParentJobID: job1.ID,
		AllowDirty:  true,
	})
	if err != nil {
		t.Fatalf("ResumeRepoInPlace failed: %v", err)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected initial ResumeMode 'pending', got %q", resumeSch.ResumeMode)
	}

	job2, _, _, ok := e.PollHost("mac-mini")
	if !ok {
		t.Fatal("poll job2 failed")
	}

	// Exit 1 without marker: must NOT confirm same_conversation (Finding B1)
	_ = e.PostExit(job2.ID, 1, ExitOptions{Reason: "FAILED"})

	det, _ := e.GetJob(job2.ID)
	if det.ResumeMode != "same_conversation_unverified" {
		t.Fatalf("expected non-zero exit ResumeMode 'same_conversation_unverified', got %q", det.ResumeMode)
	}
	if det.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed false on non-zero exit")
	}
	if det.AttachConfirmation != "unknown" {
		t.Fatalf("expected AttachConfirmation 'unknown', got %q", det.AttachConfirmation)
	}

	// Verify history record also unverified
	lastRow, _, _, found := e.FindHistoryRow(job2.ID)
	if !found {
		t.Fatal("expected history row for job2")
	}
	if lastRow.ResumeMode != "same_conversation_unverified" {
		t.Fatalf("expected history ResumeMode 'same_conversation_unverified', got %q", lastRow.ResumeMode)
	}
	if lastRow.SameConversationConfirmed {
		t.Fatalf("expected history SameConversationConfirmed false on non-zero exit")
	}
	if lastRow.AttachConfirmation != "unknown" {
		t.Fatalf("expected history AttachConfirmation 'unknown', got %q", lastRow.AttachConfirmation)
	}
}

func TestEngine_ResumeJob_SharedResolver_ParentHarnessAlias(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-harness-alias", Title: "Parent Harness Alias", Engine: "harness", Status: "READY"})

	convID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, convID)
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	// ResumeJob from parent with Engine="harness" (Finding B4)
	resumeSch, err := e.ResumeJob(job1.ID)
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}
	if resumeSch.Engine != "agy" {
		t.Fatalf("expected canonical engine 'agy', got %q", resumeSch.Engine)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected ResumeMode 'pending' (no false cross-engine fallback!), got %q", resumeSch.ResumeMode)
	}
	if resumeSch.ResumeConversationID != convID {
		t.Fatalf("expected ResumeConversationID %q to be preserved, got %q", convID, resumeSch.ResumeConversationID)
	}
	if resumeSch.ResumeFallbackReason != "" {
		t.Fatalf("expected empty ResumeFallbackReason, got %q", resumeSch.ResumeFallbackReason)
	}
}

func TestEngine_ResumeJob_QuotaExhausted_SharedResolverFallback(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-quota-job", Title: "Quota Job Test", Engine: "agent", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "cursor-123")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{DiagnosticCode: "QUOTA_EXHAUSTED", Reason: "QUOTA_EXHAUSTED"})

	// ResumeJob from parent that had quota exhausted on agent (Finding B3)
	resumeSch, err := e.ResumeJob(job1.ID)
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}
	if resumeSch.Engine != "agy" {
		t.Fatalf("expected rewritten engine 'agy', got %q", resumeSch.Engine)
	}
	if resumeSch.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode 'fallback_new_conversation' after quota switch, got %q", resumeSch.ResumeMode)
	}
	if !strings.Contains(resumeSch.ResumeFallbackReason, "cross-engine") {
		t.Fatalf("expected cross-engine fallback reason, got %q", resumeSch.ResumeFallbackReason)
	}
	if resumeSch.ResumeConversationID != "" {
		t.Fatalf("expected empty ResumeConversationID for cross-engine, got %q", resumeSch.ResumeConversationID)
	}
}

func TestEngine_ResumeJob_BodyOverrideIgnored(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	p := e.PutPrompt(Prompt{ID: "prompt-body-override", Title: "Body Override Test", Engine: "agy", Status: "READY"})

	_, err := e.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, "aad902c8-bda6-44c7-8a68-c468a1562399")
	if err != nil {
		t.Fatalf("ExecutePromptWithResume failed: %v", err)
	}
	job1, _, _, _ := e.PollHost("mac-mini")
	_ = e.PostExit(job1.ID, 1, ExitOptions{Reason: "FAILED"})

	// Client passes overrides to ResumeJob (Finding B3, C5)
	resumeSch, err := e.ResumeJob(job1.ID, ResumeOptions{
		ResumeMode:            "same_conversation",
		ParentJobID:           "job-attacker-override",
		RootJobID:             "job-attacker-root",
		InitialConversationID: "00000000-0000-0000-0000-000000000000",
		InterruptedASEState: map[string]any{
			"step": "injected-step-payload",
		},
	})
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}
	if resumeSch.ResumeMode != "pending" {
		t.Fatalf("expected client-passed same_conversation to be ignored and remain 'pending', got %q", resumeSch.ResumeMode)
	}
	if resumeSch.ParentJobID != job1.ID {
		t.Fatalf("expected ParentJobID to be %s, got %s", job1.ID, resumeSch.ParentJobID)
	}
	if resumeSch.InitialConversationID != "aad902c8-bda6-44c7-8a68-c468a1562399" {
		t.Fatalf("expected InitialConversationID to be preserved from parent job, got %s", resumeSch.InitialConversationID)
	}
}

func TestEngine_ResumeJob_HistoryFallbackPinsActiveInitialConversation(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	leakedID1 := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"
	leakedID2 := "14de9a1f-5679-4f11-a08f-f32bc7e47663"

	// Seed history entry with multiple conversation IDs where job is no longer in e.jobs (C1)
	key := repoKey("mac-mini", repoPath)
	e.history[key] = append(e.history[key], HistoryRow{
		JobID:                 "job-1311",
		PromptTitle:           "Story STORY-GOV-3678 deterministic resume",
		Engine:                "agy",
		Status:                "FAILED",
		Reason:                "HALTED_DIRTY",
		InitialConversationID: initialID,
		ActiveConversationID:  initialID,
		ConversationIDs:       []string{initialID, leakedID1, leakedID2},
		InterruptedASEState: map[string]any{
			"phase": "IMPLEMENTATION",
			"step":  2,
		},
	})

	// Job is NOT in e.jobs
	if _, ok := e.jobs["job-1311"]; ok {
		t.Fatal("expected job-1311 to be absent from e.jobs")
	}

	// Resume job-1311 from history
	sch, err := e.ResumeJob("job-1311")
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}

	// C1 Verification: must pin initialID, NOT leakedID2!
	if sch.ResumeConversationID != initialID {
		t.Fatalf("expected ResumeConversationID to be %s (active/initial), but got %s (leaked tertiary)", initialID, sch.ResumeConversationID)
	}
	if sch.InitialConversationID != initialID {
		t.Fatalf("expected InitialConversationID to be %s, but got %s", initialID, sch.InitialConversationID)
	}
	if sch.ParentJobID != "job-1311" {
		t.Fatalf("expected ParentJobID to be job-1311, but got %s", sch.ParentJobID)
	}
	if sch.InterruptedASEState["phase"] != "IMPLEMENTATION" {
		t.Fatalf("expected phase IMPLEMENTATION, got %v", sch.InterruptedASEState["phase"])
	}
}

func TestEngine_AddJobConversationWithOptions_EmptyParentDoesNotConfirmSameConversation(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	// Parent job had NO registered conversation
	jobParent := &Job{
		ID:     "job-parent-empty",
		HostID: "mac-mini",
		Status: "FAILED",
	}
	e.jobs[jobParent.ID] = jobParent

	// Resumed job starts pending with empty InitialConversationID
	jobResume := &Job{
		ID:          "job-resume-test",
		ParentJobID: jobParent.ID,
		HostID:      "mac-mini",
		Status:      "RUNNING",
		ResumeMode:  "pending",
	}
	e.jobs[jobResume.ID] = jobResume

	newConvID := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"
	// First conversation reported without fallback flag
	err := e.AddJobConversationWithOptions(jobResume.ID, newConvID, ConversationAttachOptions{Role: "initial_active"})
	if err != nil {
		t.Fatalf("AddJobConversationWithOptions failed: %v", err)
	}

	// Must NOT confirm same_conversation because parent had no initial conversation (Finding M1)
	if jobResume.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed to be false when parent has no initial conversation, got true")
	}
	if jobResume.ResumeMode == "same_conversation" {
		t.Fatalf("expected ResumeMode NOT to be same_conversation, got %q", jobResume.ResumeMode)
	}
}

func TestEngine_AddJobConversationWithOptions_DifferingNonFallbackDoesNotConfirmSameConversation(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	jobResume := &Job{
		ID:                    "job-resume-diff",
		ParentJobID:           "job-parent-001",
		InitialConversationID: initialID,
		HostID:                "mac-mini",
		Status:                "RUNNING",
		ResumeMode:            "pending",
	}
	e.jobs[jobResume.ID] = jobResume

	differingID := "99998888-7777-6666-5555-444433332222"
	// Non-fallback report with a differing conversation ID
	err := e.AddJobConversationWithOptions(jobResume.ID, differingID, ConversationAttachOptions{Role: "initial_active"})
	if err != nil {
		t.Fatalf("AddJobConversationWithOptions failed: %v", err)
	}

	// Must NOT confirm same_conversation because differingID != initialID (Finding M1, M2)
	if jobResume.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed to be false when differing conversation reported without fallback, got true")
	}
	if jobResume.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode to be fallback_new_conversation, got %q", jobResume.ResumeMode)
	}

	// Now call PostExit(jobResume.ID, 0) - must remain fallback_new_conversation and NOT become same_conversation (Finding M2)
	err = e.PostExit(jobResume.ID, 0)
	if err != nil {
		t.Fatalf("PostExit failed: %v", err)
	}

	if jobResume.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed to remain false after PostExit(0), got true")
	}
	if jobResume.ResumeMode != "fallback_new_conversation" {
		t.Fatalf("expected ResumeMode to remain fallback_new_conversation after PostExit(0), got %q", jobResume.ResumeMode)
	}
}

func TestEngine_PostExit_PendingWithoutMarker_MapsToUnverified(t *testing.T) {
	e := NewEngine(nil)
	repoPath := "/home/romeo/work/meta-repo"
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})

	initialID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	jobResume := &Job{
		ID:                    "job-resume-unverified",
		ParentJobID:           "job-parent-001",
		InitialConversationID: initialID,
		HostID:                "mac-mini",
		Status:                "RUNNING",
		ResumeMode:            "pending",
	}
	e.jobs[jobResume.ID] = jobResume

	// No conversation attach marker ever reported. Process exits with code 0.
	err := e.PostExit(jobResume.ID, 0)
	if err != nil {
		t.Fatalf("PostExit failed: %v", err)
	}

	// Finding M3: exit 0 without positive evidence MUST be mapped to same_conversation_unverified
	if jobResume.SameConversationConfirmed {
		t.Fatalf("expected SameConversationConfirmed to be false without positive attach evidence, got true")
	}
	if jobResume.ResumeMode != "same_conversation_unverified" {
		t.Fatalf("expected ResumeMode to be same_conversation_unverified, got %q", jobResume.ResumeMode)
	}
	if jobResume.AttachConfirmation != "unverified" {
		t.Fatalf("expected AttachConfirmation to be unverified, got %q", jobResume.AttachConfirmation)
	}
}

func TestEngine_AssociatedJobsAndHistoricalScheduleLineage(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Now() })

	t0 := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC) // 47 hours earlier (outside max correlation window)
	t1 := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)  // Prior completed run (terminal success boundary)
	t2 := time.Date(2026, 10, 24, 11, 0, 0, 0, time.UTC) // Initial attempt of current flow
	t3 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) // Resume attempt of current flow

	// Historical rows on mac-mini /work/fleet-repo-03
	keyMain := "mac-mini\x00/work/fleet-repo-03"
	e.history[keyMain] = []HistoryRow{
		{
			JobID:       "job-1309",
			ScheduleID:  "sch-1309",
			PromptTitle: "autonomous feature delivery",
			Status:      "failed",
			CreatedAt:   t0,
		},
		{
			JobID:       "job-1310",
			ScheduleID:  "sch-prior",
			PromptTitle: "autonomous feature delivery",
			Status:      "completed", // terminal success marks boundary
			CreatedAt:   t1,
		},
		{
			JobID:       "job-1311",
			ScheduleID:  "sch-1310", // exact incident shape: initial run was scheduled under sch-1310
			Engine:      "agy",
			PromptTitle: "autonomous feature delivery",
			Status:      "execution_timeout",
			CreatedAt:   t2,
		},
		{
			JobID:       "job-1313",
			ScheduleID:  "sch-1312", // resume run scheduled under sch-1312
			PromptTitle: "autonomous feature delivery",
			Status:      "running",
			CreatedAt:   t3,
		},
		{
			JobID:       "job-unrelated",
			ScheduleID:  "sch-9999",
			PromptTitle: "different prompt task",
			Status:      "success",
			CreatedAt:   t2,
		},
	}

	// Negative cases: same prompt on different host or repo
	e.history["other-host\x00/work/fleet-repo-03"] = []HistoryRow{
		{
			JobID:       "job-other-host",
			ScheduleID:  "sch-other-host",
			PromptTitle: "autonomous feature delivery",
			Status:      "failed",
			CreatedAt:   t1,
		},
	}
	e.history["mac-mini\x00/work/different-repo"] = []HistoryRow{
		{
			JobID:       "job-other-repo",
			ScheduleID:  "sch-other-repo",
			PromptTitle: "autonomous feature delivery",
			Status:      "failed",
			CreatedAt:   t1,
		},
	}

	// 1. GetAssociatedJobsForSchedule for legacy schedule sch-1312:
	// AC1: Exact chronological list ["job-1311", "job-1313"].
	// Must NOT contain job-1309 (>24h window), job-1310 (prior successful completion), or unrelated runs.
	jobs := e.GetAssociatedJobsForSchedule("sch-1312")
	expected := []string{"job-1311", "job-1313"}
	if len(jobs) != len(expected) {
		t.Fatalf("expected exact jobs %v, got %v", expected, jobs)
	}
	for i, exp := range expected {
		if jobs[i] != exp {
			t.Errorf("at index %d expected %q, got %q", i, exp, jobs[i])
		}
	}

	// Verify earlier schedule sch-prior isolates to its own cluster and does NOT scoop subsequent jobs
	jobsPrior := e.GetAssociatedJobsForSchedule("sch-prior")
	if len(jobsPrior) != 1 || jobsPrior[0] != "job-1310" {
		t.Fatalf("expected sch-prior to isolate to [job-1310], got %v", jobsPrior)
	}

	// 2. Fresh schedule in memory with recurring prompt must NOT scoop past historical jobs
	e.schedules["sch-fresh"] = &Schedule{
		ID:           "sch-fresh",
		HostID:       "mac-mini",
		WorktreePath: "/work/fleet-repo-03",
		PromptTitle:  "autonomous feature delivery",
		Status:       "queued",
	}
	freshJobs := e.GetAssociatedJobsForSchedule("sch-fresh")
	if len(freshJobs) != 0 {
		t.Fatalf("fresh unexecuted schedule must have empty associated jobs, got: %v", freshJobs)
	}

	// 3. Schedule with explicit lineage MUST NOT have legacy rows polluted
	e.schedules["sch-explicit"] = &Schedule{
		ID:           "sch-explicit",
		HostID:       "mac-mini",
		WorktreePath: "/work/fleet-repo-03",
		PromptTitle:  "autonomous feature delivery",
		ParentJobID:  "job-parent-explicit",
	}
	explicitJobs := e.GetAssociatedJobsForSchedule("sch-explicit")
	for _, j := range explicitJobs {
		if j == "job-1309" || j == "job-1310" || j == "job-1311" || j == "job-1313" {
			t.Fatalf("explicit lineage schedule must not be polluted by legacy correlation, got: %v", explicitJobs)
		}
	}

	// 4. ReconstructScheduleFromHistoryLocked deterministically selects latest row
	sch1312, ok := e.GetSchedule("sch-1312")
	if !ok {
		t.Fatalf("failed to retrieve historical schedule sch-1312")
	}
	if sch1312.Status != "running" {
		t.Errorf("expected latest row status 'running', got %q", sch1312.Status)
	}
	if len(sch1312.AssociatedJobs) != 2 || sch1312.AssociatedJobs[0] != "job-1311" || sch1312.AssociatedJobs[1] != "job-1313" {
		t.Errorf("expected sch1312 AssociatedJobs [job-1311, job-1313], got %v", sch1312.AssociatedJobs)
	}

	// 5. ListSchedules F2 check: active schedule in memory is NOT duplicated when status filter excludes it
	e.schedules["sch-active"] = &Schedule{
		ID:           "sch-active",
		HostID:       "mac-mini",
		WorktreePath: "/work/fleet-repo-03",
		PromptTitle:  "active prompt",
		Status:       "running",
	}
	e.history[keyMain] = append(e.history[keyMain], HistoryRow{
		JobID:       "job-active-old",
		ScheduleID:  "sch-active",
		PromptTitle: "active prompt",
		Status:      "failed",
		CreatedAt:   t1,
	})

	// Filter for status="failed". In-memory sch-active is "running" so it is excluded from out.
	// Its historical row "failed" must NOT be emitted as a phantom duplicate.
	listFailed := e.ListSchedules("mac-mini", "/work/fleet-repo-03", "failed")
	for _, s := range listFailed {
		if s.ID == "sch-active" {
			t.Fatalf("F2 violation: active schedule sch-active was resurrected as a phantom duplicate from history")
		}
	}

	// Verify status filter normalization: filtering for "failed" matches "execution_timeout"
	listFailedTimeout := e.ListSchedules("mac-mini", "/work/fleet-repo-03", "failed")
	foundTimeout := false
	for _, s := range listFailedTimeout {
		if s.ID == "sch-1310" {
			foundTimeout = true
			break
		}
	}
	if !foundTimeout {
		t.Errorf("expected ListSchedules with status=failed to match historical row with status=execution_timeout")
	}

	// 6. F3 check: failure chain spanning > 24 hours total does NOT form a single runaway cluster
	tDay1 := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	tDay2 := time.Date(2026, 10, 21, 6, 0, 0, 0, time.UTC) // +20h
	tDay3 := time.Date(2026, 10, 22, 2, 0, 0, 0, time.UTC) // +20h from Day 2, +40h from Day 1
	e.history["mac-mini\x00/work/span-repo"] = []HistoryRow{
		{JobID: "job-chain-1", ScheduleID: "sch-chain-1", PromptTitle: "daily cron", Status: "failed", CreatedAt: tDay1},
		{JobID: "job-chain-2", ScheduleID: "sch-chain-2", PromptTitle: "daily cron", Status: "failed", CreatedAt: tDay2},
		{JobID: "job-chain-3", ScheduleID: "sch-chain-3", PromptTitle: "daily cron", Status: "failed", CreatedAt: tDay3},
	}
	chainJobs := e.GetAssociatedJobsForSchedule("sch-chain-3")
	for _, j := range chainJobs {
		if j == "job-chain-1" {
			t.Fatalf("F3 violation: cluster exceeded max total span of 24h: %v", chainJobs)
		}
	}

	// 7. F9 check: in-memory pre-restart schedule with explicit parent pointer
	e.schedules["sch-pre-restart"] = &Schedule{
		ID:           "sch-pre-restart",
		HostID:       "mac-mini",
		WorktreePath: "/work/fleet-repo-03",
		PromptTitle:  "autonomous feature delivery",
		ParentJobID:  "job-1311",
		Status:       "running",
	}
	preRestartJobs := e.GetAssociatedJobsForSchedule("sch-pre-restart")
	foundParent := false
	for _, j := range preRestartJobs {
		if j == "job-1311" {
			foundParent = true
			break
		}
	}
	if !foundParent {
		t.Fatalf("expected in-memory schedule with ParentJobID to include parent job-1311, got: %v", preRestartJobs)
	}

	// 8. F2 check: In-memory schedule with AssociatedJobs seed but no parent pointer gives identical lineage before and after restart
	e.schedules["sch-1312-in-mem"] = &Schedule{
		ID:             "sch-1312-in-mem",
		HostID:         "mac-mini",
		WorktreePath:   "/work/fleet-repo-03",
		PromptTitle:    "autonomous feature delivery",
		Status:         "failed",
		AssociatedJobs: []string{"job-1313"},
	}
	inMemLineage := e.GetAssociatedJobsForSchedule("sch-1312-in-mem")
	if len(inMemLineage) != 2 || inMemLineage[0] != "job-1311" || inMemLineage[1] != "job-1313" {
		t.Fatalf("F2 violation: in-memory schedule failed to correlate legacy parent: got %v, expected [job-1311, job-1313]", inMemLineage)
	}

	// 9. F1 & F3 check: GetJob returns historical job from e.history with honest field mapping (empty Kind/StoryID) and deterministic selection
	histJobDetail, ok := e.GetJob("job-1311")
	if !ok {
		t.Fatalf("F1 violation: GetJob('job-1311') failed to find historical job in e.history")
	}
	if histJobDetail.ID != "job-1311" || histJobDetail.ScheduleID != "sch-1310" || histJobDetail.HostID != "mac-mini" {
		t.Fatalf("F1 violation: unexpected job detail from history: %+v", histJobDetail)
	}
	if histJobDetail.Kind != "" || histJobDetail.StoryID != "" {
		t.Fatalf("F3 violation: expected empty Kind and StoryID on unannotated historical job fallback, got Kind=%q, StoryID=%q", histJobDetail.Kind, histJobDetail.StoryID)
	}
	if histJobDetail.Engine != "agy" || histJobDetail.Prompt != "autonomous feature delivery" {
		t.Fatalf("F3 violation: unexpected engine or prompt: Engine=%q, Prompt=%q", histJobDetail.Engine, histJobDetail.Prompt)
	}

	// Test deterministic selection of latest row when multiple history keys contain the same JobID
	e.history["older-key"] = []HistoryRow{
		{
			JobID:       "job-1311",
			ScheduleID:  "sch-older",
			Engine:      "older-engine",
			CreatedAt:   t1.Add(-48 * time.Hour),
			PromptTitle: "older prompt",
		},
	}
	resolvedDetail, ok := e.GetJob("job-1311")
	if !ok || resolvedDetail.ScheduleID != "sch-1310" {
		t.Fatalf("F3 violation: GetJob should deterministically select latest history row, got: %+v", resolvedDetail)
	}
}

func TestHealingLifecycleAndOperatorControls(t *testing.T) {
	e := NewEngine(time.Now)
	e.RegisterHost("host-test", "Host Test", "permanent")
	err := e.UpsertRepo(Repo{
		HostID:       "host-test",
		WorktreePath: "/repos/healing-test",
		Queue:        QueueOpen,
		Lock:         LockIdle,
	})
	if err != nil {
		t.Fatalf("UpsertRepo failed: %v", err)
	}

	// 1. Initial state is QueueOpen
	repo, ok := e.GetRepo("host-test", "/repos/healing-test")
	if !ok || repo.Queue != QueueOpen {
		t.Fatalf("expected repo in QueueOpen, got %+v", repo)
	}

	// 2. Start a job, then finish with incomplete ASE -> transitions to QueueHealing
	p := e.PutPrompt(Prompt{
		ID:     "p-healing",
		Title:  "Test Story",
		Body:   "Implement stuff",
		Engine: "agent",
		Status: "READY",
	})
	sch, err := e.ExecutePrompt(p.ID, "host-test", "/repos/healing-test", 1, nil)
	if err != nil {
		t.Fatalf("ExecutePrompt failed: %v", err)
	}
	job, _, _, ok := e.PollHost("host-test")
	if !ok {
		t.Fatalf("PollHost failed to lease job")
	}

	// Finish incomplete
	e.ApplyFinish("host-test", "/repos/healing-test", job.ID, FinishInput{
		ASEComplete:   false,
		WorkPorcelain: "",
	})
	repo, _ = e.GetRepo("host-test", "/repos/healing-test")
	if repo.Queue != QueueHealing {
		t.Fatalf("expected QueueHealing after incomplete finish, got %s", repo.Queue)
	}
	if repo.HealingReasonCode != HealingReasonPostRunFailed {
		t.Fatalf("expected HealingReasonPostRunFailed, got %s", repo.HealingReasonCode)
	}
	if repo.HealingAttemptCount != 1 {
		t.Fatalf("expected HealingAttemptCount 1, got %d", repo.HealingAttemptCount)
	}

	// 3. Simulate healer runs and fails with non-zero exit code multiple times to trip circuit breaker
	repo.MaxHealingAttempts = 3
	e.repos[repoKey("host-test", "/repos/healing-test")] = &repo

	// Simulate 2 more failures to reach max attempts (total 3)
	for i := 2; i <= 3; i++ {
		hSch, err := e.ExecutePrompt(p.ID, "host-test", "/repos/healing-test", 1, nil)
		if err != nil {
			t.Fatalf("ExecutePrompt failed: %v", err)
		}
		_ = hSch
		hJob, _, _, leased := e.PollHost("host-test")
		if !leased {
			t.Fatalf("PollHost failed to lease healer job on attempt %d", i)
		}
		_ = e.PostExit(hJob.ID, 10, ExitOptions{Reason: "git clean failed"})
	}

	repo, _ = e.GetRepo("host-test", "/repos/healing-test")
	if repo.Queue != QueuePaused || repo.Reason != ReasonHealFailed {
		t.Fatalf("expected circuit breaker to pause queue with ReasonHealFailed, got queue=%s, reason=%s", repo.Queue, repo.Reason)
	}

	// 4. Operator RetryHealRepo
	err = e.RetryHealRepo("host-test", "/repos/healing-test")
	if err != nil {
		t.Fatalf("RetryHealRepo failed: %v", err)
	}
	repo, _ = e.GetRepo("host-test", "/repos/healing-test")
	if repo.Queue != QueueHealing {
		t.Fatalf("expected QueueHealing after retry, got %s", repo.Queue)
	}

	// Verify command queued
	hostObj := e.hosts["host-test"]
	if len(hostObj.PendingCommands) == 0 || hostObj.PendingCommands[len(hostObj.PendingCommands)-1].Type != "HEAL_GIT_REPO" {
		t.Fatalf("expected HEAL_GIT_REPO pending command, got %+v", hostObj.PendingCommands)
	}

	// 5. UpdateRepoRuntimeStatus arrives from agent with clean git state -> Auto-recovers to QueueOpen!
	cleanStatus := RepoRuntimeStatus{
		RepoPath:       "/repos/healing-test",
		IsClean:        true,
		Branch:         "main",
		Ahead:          0,
		Behind:         0,
		MetadataYmlRaw: "name: test-repo\nversion: 1.0.0\n",
		MetadataYmlSHA: "abc123sha",
	}
	e.UpdateRepoRuntimeStatus("host-test", []RepoRuntimeStatus{cleanStatus})

	repo, _ = e.GetRepo("host-test", "/repos/healing-test")
	if repo.Queue != QueueOpen {
		t.Fatalf("expected auto-recovery to QueueOpen when clean, got %s", repo.Queue)
	}
	if repo.Reason != "" || repo.HealingReasonCode != "" {
		t.Fatalf("expected cleared reason and healing code, got reason=%s, code=%s", repo.Reason, repo.HealingReasonCode)
	}
	if repo.MetadataYmlRaw != cleanStatus.MetadataYmlRaw {
		t.Fatalf("expected snapshot metadata.yml to be recorded on repo")
	}

	// 6. ForceCleanGitRepo queues FORCE_CLEAN_GIT command
	err = e.ForceCleanGitRepo("host-test", "/repos/healing-test")
	if err != nil {
		t.Fatalf("ForceCleanGitRepo failed: %v", err)
	}
	lastCmd := hostObj.PendingCommands[len(hostObj.PendingCommands)-1]
	if lastCmd.Type != "FORCE_CLEAN_GIT" {
		t.Fatalf("expected FORCE_CLEAN_GIT command, got %s", lastCmd.Type)
	}

	// 7. ClearHealingRepo sets Queue back to QueueOpen
	repo.Queue = QueueHealing
	e.repos[repoKey("host-test", "/repos/healing-test")] = &repo
	err = e.ClearHealingRepo("host-test", "/repos/healing-test")
	if err != nil {
		t.Fatalf("ClearHealingRepo failed: %v", err)
	}
	repo, _ = e.GetRepo("host-test", "/repos/healing-test")
	if repo.Queue != QueueOpen {
		t.Fatalf("expected QueueOpen after ClearHealingRepo, got %s", repo.Queue)
	}
	_ = sch
}

