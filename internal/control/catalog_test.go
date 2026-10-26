package control

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCatalogIdeasAndPrompts(t *testing.T) {
	e := NewEngine(nil)

	// ListIdeas
	idea := e.PutIdea(Idea{ID: "i1", Title: "Idea 1"})
	if idea.ID != "i1" {
		t.Errorf("unexpected idea: %+v", idea)
	}
	ideas := e.ListIdeas()
	if len(ideas) != 1 || ideas[0].ID != "i1" {
		t.Errorf("unexpected list ideas: %+v", ideas)
	}

	// ListPrompts
	p := e.PutPrompt(Prompt{ID: "p1", Title: "Prompt 1", Body: "run"})
	if p.ID != "p1" {
		t.Errorf("unexpected prompt: %+v", p)
	}
	prompts := e.ListPrompts()
	if len(prompts) != 1 || prompts[0].ID != "p1" {
		t.Errorf("unexpected list prompts: %+v", prompts)
	}

	// ReplacePrompt
	// Not found
	_, err := e.ReplacePrompt("nonexistent", Prompt{Title: "New"})
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Running
	e.runningRev["p1"] = true
	_, err = e.ReplacePrompt("p1", Prompt{Title: "New"})
	if err != ErrPromptRunning {
		t.Errorf("expected ErrPromptRunning, got %v", err)
	}
	e.runningRev["p1"] = false

	// Success
	replaced, err := e.ReplacePrompt("p1", Prompt{Title: "Updated Title", Body: "new body"})
	if err != nil || replaced.Title != "Updated Title" || replaced.Revision != 2 {
		t.Errorf("replace prompt failed: %+v, err=%v", replaced, err)
	}
}

func TestCatalogToolUpdatesAndHosts(t *testing.T) {
	e := NewEngine(nil)

	// Host not found
	err := e.QueueToolUpdate("host-unknown", []string{"git"}, false)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	h := e.RegisterHost("host-1", "permanent")
	if h.ID != "host-1" {
		t.Errorf("unexpected host: %+v", h)
	}

	// Queue and consume update
	if err := e.QueueToolUpdate("host-1", []string{"git", "go"}, true); err != nil {
		t.Fatalf("QueueToolUpdate: %v", err)
	}

	update := e.ConsumePendingToolUpdate("host-1")
	if update == nil || len(update.Tools) != 2 || !update.Force {
		t.Fatalf("unexpected update: %+v", update)
	}

	// Consuming again should be nil
	if e.ConsumePendingToolUpdate("host-1") != nil {
		t.Error("expected nil on second consume")
	}
	if e.ConsumePendingToolUpdate("host-unknown") != nil {
		t.Error("expected nil for unknown host")
	}

	// ListHosts
	hosts := e.ListHosts()
	if len(hosts) != 1 || hosts[0].Host.ID != "host-1" {
		t.Errorf("unexpected hosts: %+v", hosts)
	}

	// GetHostDetail
	detail, ok := e.GetHostDetail("host-1")
	if !ok || detail.Host.ID != "host-1" {
		t.Errorf("unexpected host detail: ok=%v, detail=%+v", ok, detail)
	}
	if _, ok := e.GetHostDetail("host-unknown"); ok {
		t.Error("expected false for unknown host")
	}
}

func TestCatalogReposAndDiscard(t *testing.T) {
	e := NewEngine(nil)
	e.RegisterHost("host-1", "permanent")

	// RequestCheckout
	err := e.RequestCheckout(RepoRequest{HostID: "", Path: ""})
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	err = e.RequestCheckout(RepoRequest{HostID: "host-1", Path: "/work/empty", Mode: "empty-folder", CloneURL: ""})
	if err == nil {
		t.Error("expected error for empty-folder without clone_url")
	}
	err = e.RequestCheckout(RepoRequest{HostID: "host-unknown", Path: "/work/a"})
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	err = e.RequestCheckout(RepoRequest{HostID: "host-1", Path: "/work/a", CloneURL: "https://git/a"})
	if err != nil {
		t.Fatalf("RequestCheckout: %v", err)
	}

	reqs := e.CheckoutRequests("host-1")
	if len(reqs) != 1 || reqs[0].Path != "/work/a" {
		t.Errorf("unexpected checkout requests: %+v", reqs)
	}

	// RequestDiscard
	if err := e.RequestDiscard("host-1", "nonexistent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	r, ok := e.GetRepo("host-1", "/work/a")
	if !ok {
		t.Fatal("repo not found")
	}
	if err := e.RequestDiscard("host-1", "/work/a"); err != nil {
		t.Fatalf("RequestDiscard: %v", err)
	}
	r, _ = e.GetRepo("host-1", "/work/a")
	if !r.DiscardPending {
		t.Error("expected DiscardPending to be true")
	}

	// RequestDiscard busy
	r.Lock = LockRunning
	_ = e.UpsertRepo(r)
	if err := e.RequestDiscard("host-1", "/work/a"); err != ErrDiscardBusy {
		t.Errorf("expected ErrDiscardBusy, got %v", err)
	}

	// RequestDiscardHub
	r.Lock = LockIdle
	r.DocsHubPath = "/docs/hub"
	_ = e.UpsertRepo(r)

	if err := e.RequestDiscardHub("host-1", "/docs/hub"); err != nil {
		t.Fatalf("RequestDiscardHub: %v", err)
	}
	r, _ = e.GetRepo("host-1", "/work/a")
	if r.Lock != LockCleaning {
		t.Errorf("expected LockCleaning, got %s", r.Lock)
	}

	// UnregisterRepo
	if err := e.UnregisterRepo("host-1", "nonexistent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	// Currently cleaning so blocked
	if err := e.UnregisterRepo("host-1", "/work/a"); err != ErrQueueBlocked {
		t.Errorf("expected ErrQueueBlocked, got %v", err)
	}
	r.Lock = LockIdle
	r.Queue = QueueOpen
	_ = e.UpsertRepo(r)
	if err := e.UnregisterRepo("host-1", "/work/a"); err != nil {
		t.Fatalf("UnregisterRepo: %v", err)
	}
}

func TestCatalogLogsAndStatus(t *testing.T) {
	e := NewEngine(nil)

	// AppendLog and SubscribeLogs
	subCh, cancel := e.SubscribeLogs("job-1")
	defer cancel()

	e.AppendLog("job-1", LogChunk{Text: "line 1"})
	select {
	case chunk := <-subCh:
		if chunk.Text != "line 1" {
			t.Errorf("expected line 1, got %s", chunk.Text)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for log chunk")
	}

	// Append more logs
	for i := 2; i <= 20; i++ {
		e.AppendLog("job-1", LogChunk{Text: "line"})
	}

	// JobLogs
	page := e.JobLogs("job-1", 1, "", 0, 0)
	if len(page.Chunks) == 0 {
		t.Error("expected non-empty page")
	}
	pagedRange := e.JobLogs("job-1", 1, "", 2, 5)
	if len(pagedRange.Chunks) != 4 {
		t.Errorf("expected 4 chunks in range 2..5, got %d", len(pagedRange.Chunks))
	}

	// TailLogs
	chunks, _, _ := e.TailLogs("job-1", 5, 0)
	if len(chunks) != 5 {
		t.Errorf("expected 5 tailed chunks, got %d", len(chunks))
	}

	// JobStatus
	if _, ok := e.JobStatus("nonexistent"); ok {
		t.Error("expected false for nonexistent job")
	}

	// ListSchedules and ListHistory
	e.schedules["s-1"] = &Schedule{ID: "s-1", HostID: "host-1", WorktreePath: "/work/a", Status: "RUNNING", Priority: 5}
	e.schedules["s-2"] = &Schedule{ID: "s-2", HostID: "host-1", WorktreePath: "/work/a", Status: "PAUSED", Priority: 1}
	scheds := e.ListSchedules("", "", "")
	if len(scheds) != 2 {
		t.Errorf("expected 2 schedules, got %d", len(scheds))
	}
	schedsFiltered := e.ListSchedules("host-1", "/work/a", "RUNNING")
	if len(schedsFiltered) != 1 {
		t.Errorf("expected 1 running schedule, got %d", len(schedsFiltered))
	}
	_, _ = e.SetSchedulePriority("s-1", 10)

	e.jobs["job-1"] = &Job{ID: "job-1", ScheduleID: "s-1", Status: "RUNNING"}
	e.repos["host-1\x00/work/a"] = &Repo{HostID: "host-1", WorktreePath: "/work/a"}
	if repo, ok := e.GetRepoForJob("job-1"); !ok || repo.HostID != "host-1" {
		t.Errorf("unexpected repo for job: ok=%v, repo=%+v", ok, repo)
	}

	e.history["host-1\x00/work/a"] = []HistoryRow{
		{JobID: "job-1", ScheduleID: "s-1", Status: "SUCCESS"},
		{JobID: "job-2", ScheduleID: "s-2", Status: "FAILED"},
	}
	hist := e.ListHistory("host-1", "/work/a", "", 10)
	if len(hist) != 2 {
		t.Errorf("expected 2 history items, got %d", len(hist))
	}
	histFiltered := e.ListHistory("host-1", "/work/a", "SUCCESS", 10)
	if len(histFiltered) != 1 {
		t.Errorf("expected 1 history item, got %d", len(histFiltered))
	}

	_ = e.DeleteHistoryRow("job-1")
	e.ClearHistory("host-1", "/work/a")

	e.SetHost(Host{ID: "host-saved", Name: "Saved"})

	// ForcePauseJob and RetryCorrelation
	if err := e.ForcePauseJob("nonexistent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	if err := e.ForcePauseJob("job-1"); err != nil {
		t.Errorf("ForcePauseJob: %v", err)
	}

	if err := e.RetryCorrelation("nonexistent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	if err := e.RetryCorrelation("job-1"); err != nil {
		t.Errorf("RetryCorrelation: %v", err)
	}
}

func TestCatalogAdditionalCoverage(t *testing.T) {
	e := NewEngine(nil)

	// 1. EnsureSeq
	e.EnsureSeq("idea-99")
	if e.seq < 99 {
		t.Errorf("expected seq >= 99, got %d", e.seq)
	}
	e.EnsureSeq("invalid-format")
	e.EnsureSeq("idea-1")

	// 2. EffectiveAlias
	r1 := Repo{RepoAlias: "explicit-alias"}
	if r1.EffectiveAlias() != "explicit-alias" {
		t.Errorf("expected explicit-alias, got %s", r1.EffectiveAlias())
	}
	r2 := Repo{WorktreePath: "/var/projects/repo-name"}
	if r2.EffectiveAlias() != "repo-name" {
		t.Errorf("expected repo-name, got %s", r2.EffectiveAlias())
	}
	r3 := Repo{WorktreePath: "/"}
	if r3.EffectiveAlias() != "/" {
		t.Errorf("expected /, got %s", r3.EffectiveAlias())
	}

	// 3. extractStoryIDFromPorcelain
	sID, rev := extractStoryIDFromPorcelain("")
	if sID != "" || rev != "" {
		t.Errorf("expected empty for blank porcelain")
	}
	sID, rev = extractStoryIDFromPorcelain("M  random/file.go\nA  docs/reviews/STORY-GOV-987.md\n")
	if sID != "STORY-GOV-987" || rev != "docs/reviews/STORY-GOV-987.md" {
		t.Errorf("expected STORY-GOV-987, got sid=%s rev=%s", sID, rev)
	}
	sID, rev = extractStoryIDFromPorcelain("R  old.md -> other/docs/reviews/gov-555.md\n")
	if sID != "STORY-GOV-555" || rev != "other/docs/reviews/gov-555.md" {
		t.Errorf("expected STORY-GOV-555, got sid=%s rev=%s", sID, rev)
	}

	// 4. ListSchedules filtering
	e.schedules["s1"] = &Schedule{ID: "s1", HostID: "h1", WorktreePath: "/work/1", Status: "PENDING"}
	e.schedules["s2"] = &Schedule{ID: "s2", HostID: "h1", WorktreePath: "/work/2", Status: "RUNNING"}
	e.schedules["s3"] = &Schedule{ID: "s3", HostID: "h2", WorktreePath: "/work/1", Status: "PENDING"}

	allSchedules := e.ListSchedules("", "", "")
	if len(allSchedules) != 3 {
		t.Errorf("expected 3 schedules, got %d", len(allSchedules))
	}
	h1Schedules := e.ListSchedules("h1", "", "")
	if len(h1Schedules) != 2 {
		t.Errorf("expected 2 schedules for h1, got %d", len(h1Schedules))
	}
	w1Schedules := e.ListSchedules("", "/work/1", "")
	if len(w1Schedules) != 2 {
		t.Errorf("expected 2 schedules for /work/1, got %d", len(w1Schedules))
	}
	pendingSchedules := e.ListSchedules("", "", "PENDING")
	if len(pendingSchedules) != 2 {
		t.Errorf("expected 2 pending schedules, got %d", len(pendingSchedules))
	}

	// 5. TailLogs
	chunks, gapFrom, gapTo := e.TailLogs("no-job", 10, 0)
	if len(chunks) != 0 || gapFrom != 0 || gapTo != 0 {
		t.Errorf("expected empty tail for no job")
	}
	for i := 1; i <= 5; i++ {
		e.AppendLog("j-tail", LogChunk{Seq: i, Text: "line"})
	}
	chunks, _, _ = e.TailLogs("j-tail", 2, 0)
	if len(chunks) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(chunks))
	}
	chunks, _, _ = e.TailLogs("j-tail", 10, 3)
	if len(chunks) != 2 {
		t.Errorf("expected 2 chunks after seq 3, got %d", len(chunks))
	}

	// 6. RequestDiscardHub
	err := e.RequestDiscardHub("nonexistent-host", "/docs")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for missing host, got %v", err)
	}

	e.RegisterHost("h-hub", "linux")
	_ = e.UpsertRepo(Repo{HostID: "h-hub", WorktreePath: "/work/repo-hub", DocsHubPath: "/docs/hub1", Lock: LockRunning})
	err = e.RequestDiscardHub("h-hub", "/docs/hub1")
	if err != ErrDiscardBusy {
		t.Errorf("expected ErrDiscardBusy for locked repo, got %v", err)
	}

	_ = e.UpsertRepo(Repo{HostID: "h-hub", WorktreePath: "/work/repo-hub", DocsHubPath: "/docs/hub1", Lock: LockIdle})
	err = e.RequestDiscardHub("h-hub", "/docs/hub1")
	if err != nil {
		t.Errorf("expected success for idle repo discard hub, got %v", err)
	}
}

func TestCatalog_SynthesizeLogsAndCoverage(t *testing.T) {
	e := NewEngine(nil)

	// 1. GetPrompt
	e.PutPrompt(Prompt{ID: "p-test", Title: "Title 1", Body: "Body 1"})
	p, ok := e.GetPrompt("p-test")
	if !ok || p.ID != "p-test" {
		t.Errorf("GetPrompt p-test failed: %v, %v", p, ok)
	}
	_, ok = e.GetPrompt("nonexistent-p")
	if ok {
		t.Errorf("expected false for missing prompt, got true")
	}

	// 2. RepoCounts & ActiveJobsCount
	_ = e.UpsertRepo(Repo{HostID: "h1", WorktreePath: "/work/r1", Lock: LockIdle})
	_ = e.UpsertRepo(Repo{HostID: "h1", WorktreePath: "/work/r2", Lock: LockRunning})
	idle, running, paused := e.RepoCounts()
	if idle+running+paused != 2 {
		t.Errorf("unexpected RepoCounts sum: %d, %d, %d", idle, running, paused)
	}

	ajc := e.ActiveJobsCount()
	if ajc != 0 {
		t.Errorf("expected 0 active jobs initially, got %d", ajc)
	}

	// 3. JobLogs when raw logs exist
	e.AppendLog("j-raw", LogChunk{Seq: 1, Stream: "stdout", Text: "raw log"})
	rawLogs := e.JobLogs("j-raw", 1, "", 0, 0)
	if len(rawLogs.Chunks) != 1 || rawLogs.Chunks[0].Text != "raw log" {
		t.Errorf("expected raw log, got %+v", rawLogs)
	}

	// 4. JobLogs synthesized from active job in e.jobs
	e.schedules["sch-active"] = &Schedule{
		ID:           "sch-active",
		WorktreePath: "/work/r1",
		Engine:       "agy",
		StoryID:      "STORY-111",
		PromptTitle:  "Run story 111",
	}
	e.jobs["j-active"] = &Job{
		ID:                   "j-active",
		ScheduleID:           "sch-active",
		ActiveConversationID: "conv-123",
		Status:               "RUNNING",
		TargetHost:           "h1",
		Kind:                 "resume",
		ParentJobID:          "j-parent",
		RootJobID:            "j-root",
		HarnessPID:           101,
		ToolPID:              102,
		ElapsedSeconds:       45,
		ResumeCount:          1,
		WorkspaceDirtyFiles:  []string{"file1.go"},
		InterruptedASEState:  map[string]any{"phase": "TDD", "story_id": "STORY-111"},
	}
	activeSynth := e.JobLogs("j-active", 1, "", 0, 0)
	if len(activeSynth.Chunks) != 1 || !strings.Contains(activeSynth.Chunks[0].Text, "[ASE-SUPERVISOR]") {
		t.Errorf("expected synthesized supervisor log for active job, got %+v", activeSynth)
	}

	// 5. JobLogs synthesized from history row
	historyKey := "h1\x00/work/r1"
	e.history[historyKey] = []HistoryRow{
		{
			JobID:                 "j-hist-auto",
			ParentJobID:           "j-parent-001",
			RootJobID:             "j-root-001",
			ActiveConversationID:  "conv-hist-456",
			Status:                "FAILED",
			Reason:                "watchdog_timeout",
			TerminationReasonCode: "TIMEOUT",
			ExitCode:              1,
			PromptTitle:           "Historic story prompt",
			CreatedAt:             time.Now(),
			AutoResumeCount:       2,
			HarnessPID:            201,
			ToolPID:               202,
			WorkspaceDirtyFiles:   []string{"dirty.go"},
			InterruptedASEState:   map[string]any{"phase": "REVIEW", "story_id": "STORY-HIST"},
			Engine:                "agy",
			Resumable:             true,
		},
	}
	histSynth := e.JobLogs("j-hist-auto", 1, "", 0, 0)
	if len(histSynth.Chunks) != 1 || !strings.Contains(histSynth.Chunks[0].Text, "Automatic Resume") {
		t.Errorf("expected synthesized auto-resume supervisor log for history job, got %+v", histSynth)
	}

	// 6. JobLogs for completely nonexistent job
	noneSynth := e.JobLogs("j-completely-unknown", 1, "", 0, 0)
	if len(noneSynth.Chunks) != 0 {
		t.Errorf("expected empty logs for unknown job, got %+v", noneSynth)
	}
}

func TestCatalog_AppendLog_PreservesInitialHeadAndAttemptHeadersOnRingCap(t *testing.T) {
	e := NewEngine(nil)
	jobID := "job-1466"

	// 1. Append initial Attempt 1 startup chunk (Seq 1)
	chunk1 := LogChunk{
		Stream: "stdout",
		Text:   "[ASE-SUPERVISOR] Starting execution for job job-1466 (kind: ordinary, engine: agy, worktree: /work/sample-repo)\n[ASE-SUPERVISOR] Initializing execution for job job-1466...\n",
	}
	e.AppendLog(jobID, chunk1)

	// 2. Append enough chunks to well exceed RingCap (e.g. 600 chunks)
	for i := 2; i <= 250; i++ {
		e.AppendLog(jobID, LogChunk{
			Stream: "stdout",
			Text:   fmt.Sprintf("Attempt 1 model output stream line %d\n", i),
		})
	}

	// 3. Append Attempt 2 separator banner in the middle
	attempt2Banner := LogChunk{
		Stream: "stdout",
		Text:   "\n================================================================================\n[iazio] Starting Attempt #2 (trigger_reason: lease_expired, previous_exit_code: 1, previous_error: LEASE_EXPIRED, conversation_id: 110d238e, timestamp: 2026-10-26T04:18:46Z)\n================================================================================\n",
	}
	e.AppendLog(jobID, attempt2Banner)

	// 4. Append more chunks so total chunks = 700 (well above RingCap 500)
	for i := 252; i <= 700; i++ {
		e.AppendLog(jobID, LogChunk{
			Stream: "stdout",
			Text:   fmt.Sprintf("Attempt 2 model output stream line %d\n", i),
		})
	}

	// 5. Verify stored logs in e.logs[jobID]
	e.mu.Lock()
	logs := e.logs[jobID]
	e.mu.Unlock()

	if len(logs) > RingCap {
		t.Errorf("expected logs to be bounded by RingCap %d, got %d", RingCap, len(logs))
	}

	// Verify Chunk 1 (Attempt 1 startup) was NOT dropped!
	foundChunk1 := false
	foundAttempt2Banner := false
	for _, c := range logs {
		if c.Seq == 1 && strings.Contains(c.Text, "[ASE-SUPERVISOR] Starting execution for job job-1466") {
			foundChunk1 = true
		}
		if strings.Contains(c.Text, "[iazio] Starting Attempt #2") {
			foundAttempt2Banner = true
		}
	}

	if !foundChunk1 {
		t.Errorf("Attempt 1 startup log (Seq 1) was lost after RingCap truncation!")
	}
	if !foundAttempt2Banner {
		t.Errorf("Attempt 2 banner in the middle was lost after RingCap truncation!")
	}

	// 6. Verify TailLogs returns the head chunk and attempt banner when tailing
	tailChunks, _, _ := e.TailLogs(jobID, 100, 0)
	tailHasChunk1 := false
	tailHasBanner := false
	for _, c := range tailChunks {
		if c.Seq == 1 {
			tailHasChunk1 = true
		}
		if strings.Contains(c.Text, "[iazio] Starting Attempt #2") {
			tailHasBanner = true
		}
	}
	if !tailHasChunk1 {
		t.Errorf("TailLogs with afterSeq=0 lost initial Attempt 1 chunk 1!")
	}
	if !tailHasBanner {
		t.Errorf("TailLogs with afterSeq=0 lost Attempt 2 banner!")
	}
}


