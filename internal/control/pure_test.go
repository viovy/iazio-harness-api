package control

import (
	"encoding/json"
	"testing"
)

func TestValidateShareURL(t *testing.T) {
	ok := "https://gemini.google.com/share/abc_DEF-1"
	if err := ValidateShareURL(ok); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"https://gemini.google.com/app/abc",
		"http://gemini.google.com/share/abc",
		"https://example.com/share/abc",
		"https://gemini.google.com/share/a/b",
		"",
	} {
		if err := ValidateShareURL(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

func TestDecidePreflight(t *testing.T) {
	t.Setenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES", "")
	base := Preflight{
		Kind: KindOrdinary, FreeBytes: RequiredMinFreeBytes(), DocsHubOK: true, GitAuthOK: true,
		GitWorkTree: true, HeadAttached: true, Branch: "main", DefaultBranch: "main",
	}
	if h := DecidePreflight(base); h.Reason != "" {
		t.Fatalf("clean ordinary halted: %+v", h)
	}
	low := base
	low.FreeBytes = RequiredMinFreeBytes() - 1
	low.Kind = KindIntervention
	if h := DecidePreflight(low); h.Reason != ReasonDisk || h.PauseQueue {
		t.Fatalf("disk: %+v", h)
	}
	dirty := base
	dirty.WorkPorcelain = " M file"
	if h := DecidePreflight(dirty); h.Reason != ReasonDirty || !h.PauseQueue || h.Heal {
		t.Fatalf("dirty: %+v", h)
	}
	det := base
	det.HeadAttached = false
	if h := DecidePreflight(det); h.Reason != ReasonDetached || !h.PauseQueue {
		t.Fatalf("detached: %+v", h)
	}
	unt := base
	unt.Branch = "feature/x"
	unt.HasUpstream = false
	if h := DecidePreflight(unt); h.Reason != ReasonUntracked || !h.PauseQueue {
		t.Fatalf("untracked: %+v", h)
	}
	iv := base
	iv.Kind = KindIntervention
	iv.WorkPorcelain = " M x"
	iv.HeadAttached = false
	if h := DecidePreflight(iv); h.Reason != "" {
		t.Fatalf("intervention: %+v", h)
	}
	res := base
	res.Kind = KindResume
	res.WorkPorcelain = " M x"
	res.HeadAttached = true
	if h := DecidePreflight(res); h.Reason != "" {
		t.Fatalf("resume with attached HEAD should permit dirty porcelain: %+v", h)
	}
	resDetached := res
	resDetached.HeadAttached = false
	if h := DecidePreflight(resDetached); h.Reason != ReasonDetached || !h.PauseQueue {
		t.Fatalf("resume with detached HEAD should pause queue: %+v", h)
	}
	ref := base
	ref.Kind = KindRefinement
	ref.WorkPorcelain = " M leaf"
	if h := DecidePreflight(ref); h.Reason != "" {
		t.Fatalf("refinement ignores worktree: %+v", h)
	}
	ref.HubPorcelain = " M hub"
	if h := DecidePreflight(ref); h.Reason != ReasonDirty || !h.PauseQueue {
		t.Fatalf("refinement hub: %+v", h)
	}
	auth := base
	auth.GitAuthOK = false
	if h := DecidePreflight(auth); h.Reason != ReasonGitAuth || h.PauseQueue {
		t.Fatalf("git auth: %+v", h)
	}
}

func TestDecideFinish(t *testing.T) {
	clean := FinishInput{Kind: KindOrdinary}
	d := DecideFinish(clean)
	if d.Queue != QueueHealing || !d.LeaseResume || d.HealingAttempts != 1 || d.HealingReasonCode != HealingReasonPostRunFailed {
		t.Fatalf("first heal: %+v", d)
	}
	clean.HealingAttempts = 1
	d = DecideFinish(clean)
	if d.Queue != QueuePaused || d.LeaseResume || d.Reason != ReasonHealFailed {
		t.Fatalf("second: %+v", d)
	}
	done := FinishInput{Kind: KindOrdinary, ASEComplete: true}
	d = DecideFinish(done)
	if d.Queue != QueueOpen || !d.Decrement || d.HealingAttempts != 0 {
		t.Fatalf("complete: %+v", d)
	}
	dirty := FinishInput{Kind: KindOrdinary, WorkPorcelain: "?? x", HealingAttempts: 0}
	d = DecideFinish(dirty)
	if d.Queue != QueuePaused || d.LeaseResume || d.HealingAttempts != 0 {
		t.Fatalf("dirty must not heal: %+v", d)
	}
	ahead := FinishInput{Kind: KindOrdinary, ASEComplete: true, HubAhead: 2}
	d = DecideFinish(ahead)
	if d.Queue != QueuePaused || d.LeaseResume || d.HealingAttempts != 0 {
		t.Fatalf("ahead: %+v", d)
	}
	ref := FinishInput{Kind: KindRefinement, HubPushOK: true, StoryDraftOK: true}
	d = DecideFinish(ref)
	if d.Queue != QueueOpen {
		t.Fatalf("refinement: %+v", d)
	}
	ref.HubAhead = 1
	d = DecideFinish(ref)
	if d.Queue != QueuePaused {
		t.Fatalf("refinement ahead: %+v", d)
	}
}

func TestRequiredMinFreeBytes(t *testing.T) {
	t.Setenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES", "")
	if got := RequiredMinFreeBytes(); got != DefaultMinFreeBytes {
		t.Fatalf("expected DefaultMinFreeBytes %d, got %d", DefaultMinFreeBytes, got)
	}

	t.Setenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES", "2147483648") // 2 GiB
	if got := RequiredMinFreeBytes(); got != 2147483648 {
		t.Fatalf("expected 2147483648, got %d", got)
	}
}

func TestPreflightUnmarshalJSON(t *testing.T) {
	// Standard JSON
	stdJSON := []byte(`{
		"kind": "ordinary",
		"free_bytes": 1000000,
		"work_porcelain": "clean",
		"branch": "main"
	}`)
	var p1 Preflight
	if err := json.Unmarshal(stdJSON, &p1); err != nil {
		t.Fatalf("unmarshal stdJSON: %v", err)
	}
	if p1.Kind != "ordinary" || p1.FreeBytes != 1000000 {
		t.Errorf("unexpected p1: %+v", p1)
	}

	// Alt PascalCase JSON
	altJSON := []byte(`{
		"Kind": "resume",
		"FreeBytes": 5000000,
		"WorkPorcelain": "M file",
		"HubPorcelain": "M hub",
		"HeadAttached": true,
		"Branch": "feat",
		"DefaultBranch": "main",
		"HasUpstream": true,
		"DocsHubOK": true,
		"GitAuthOK": true,
		"GitWorkTree": true,
		"DirtyStoryID": "S-1",
		"DirtyReviewFile": "review.md",
		"DetectedConversationID": "c-1",
		"DetectedVerdict": "PASS"
	}`)
	var p2 Preflight
	if err := json.Unmarshal(altJSON, &p2); err != nil {
		t.Fatalf("unmarshal altJSON: %v", err)
	}
	if p2.Kind != "resume" || p2.FreeBytes != 5000000 || p2.DirtyStoryID != "S-1" {
		t.Errorf("unexpected p2: %+v", p2)
	}
}

func TestIsStoryID(t *testing.T) {
	valid := []string{
		"STORY-GOV-3655",
		"story-gov-3655",
		"gov-3655",
		"STORY-CORE-123",
		"feat-999",
	}
	for _, s := range valid {
		if !isStoryID(s) {
			t.Errorf("expected %q to be recognized as valid story ID", s)
		}
	}

	invalid := []string{
		"",
		"   ",
		"resume complete current ase flow",
		"Resume Complete Current ASE Flow",
		"STORY-RESUME-INPLACE",
		"story-resume-inplace",
		"main",
		"random text without story",
	}
	for _, s := range invalid {
		if isStoryID(s) {
			t.Errorf("expected %q to be rejected as invalid story ID", s)
		}
	}
}
