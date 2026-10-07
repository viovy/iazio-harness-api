// Package control is the harness control-plane state machine.
// It does not store conversation transcripts and it does not call a model.
package control

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// DefaultMinFreeBytes is the default free-space floor on the worktree mount (5 GiB).
const DefaultMinFreeBytes uint64 = 5 << 30

// MinFreeBytes is the fallback constant kept for backward compatibility (10 GiB).
const MinFreeBytes uint64 = 10 << 30

// RequiredMinFreeBytes returns the active threshold in bytes, configurable via IAZIO_PREFLIGHT_MIN_FREE_BYTES.
func RequiredMinFreeBytes() uint64 {
	if v := os.Getenv("IAZIO_PREFLIGHT_MIN_FREE_BYTES"); v != "" {
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMinFreeBytes
}

const (

	IdeaNew      = "NEW"
	IdeaTriaged  = "TRIAGED"
	IdeaRefining = "REFINING"
	IdeaRefined  = "REFINED"

	QueueOpen     = "OPEN"
	QueueHealing  = "HEALING"
	QueuePaused   = "PAUSED"
	QueueBlocked  = "BLOCKED_AWAITING_CORRELATION"
	QueueCleaning = "CLEANING_HUB"

	LockIdle     = "IDLE"
	LockRunning  = "RUNNING_HARNESS"
	LockCooling  = "COOLING_OFF"
	LockCleaning = "CLEANING_HUB"

	KindOrdinary     = "ordinary"
	KindRefinement   = "story_refinement"
	KindIntervention = "intervention"

	ReasonDirty       = "HALTED_DIRTY"
	ReasonDetached    = "HALTED_DETACHED"
	ReasonUntracked   = "HALTED_UNTRACKED"
	ReasonDisk        = "HALTED_DISK"
	ReasonNoDocsHub   = "HALTED_NO_DOCS_HUB"
	ReasonGitAuth     = "HALTED_GIT_AUTH"
	ReasonCorrTimeout = "CORRELATION_TIMEOUT"
)

// ErrBadShareURL is returned when an import URL is not a Gemini share link.
var ErrBadShareURL = errors.New("share url must be https://gemini.google.com/share/<id>")

// ErrExtractorBusy is returned when a Chrome extraction is already running.
var ErrExtractorBusy = errors.New("extractor busy")

// ErrIdeaState is returned when a refinement schedule is not legal.
var ErrIdeaState = errors.New("refinement is accepted only from NEW or TRIAGED")

// Preflight is the checkout report the supervisor posts before spawn.
type Preflight struct {
	Kind          string
	FreeBytes     uint64
	WorkPorcelain string
	HubPorcelain  string
	HeadAttached  bool
	Branch        string
	DefaultBranch string
	HasUpstream   bool
	DocsHubOK     bool
	GitAuthOK     bool
	GitWorkTree   bool
}

// Halt is the pre-launch decision.
type Halt struct {
	// Reason is empty when the IDE CLI may start.
	Reason string
	// PauseQueue is true only for dirty, detached, and untracked ordinary halts.
	PauseQueue bool
	// Heal is always false for a pre-launch halt.
	Heal bool
}

// FinishInput is the post-cooling report.
type FinishInput struct {
	Kind            string
	ASEComplete     bool
	WorkPorcelain   string
	HubPorcelain    string
	HubAhead        int
	HealingAttempts int
	StoryDraftOK    bool
	HubPushOK       bool
}

// FinishDecision is what the API applies to the repo queue.
type FinishDecision struct {
	Queue           string
	Reason          string
	HealingAttempts int
	LeaseResume     bool
	Decrement       bool
}

// ValidateShareURL accepts only https://gemini.google.com/share/<id>.
func ValidateShareURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.Host != "gemini.google.com" {
		return ErrBadShareURL
	}
	if !strings.HasPrefix(u.Path, "/share/") || strings.Contains(u.Path, "..") {
		return ErrBadShareURL
	}
	id := strings.TrimPrefix(u.Path, "/share/")
	if id == "" || strings.Contains(id, "/") {
		return ErrBadShareURL
	}
	return nil
}

// CanScheduleRefinement reports whether an idea may enter REFINING.
func CanScheduleRefinement(status string) bool {
	return status == IdeaNew || status == IdeaTriaged
}

// DecidePreflight applies the spawn gate. A disk, docs-hub, or git-auth halt
// leaves the queue unpaused. Ordinary porcelain, detached HEAD, and untracked
// branch halts pause the queue and do not lease a resume.
func DecidePreflight(p Preflight) Halt {
	if p.FreeBytes < RequiredMinFreeBytes() {
		return Halt{Reason: ReasonDisk}
	}
	if !p.DocsHubOK {
		return Halt{Reason: ReasonNoDocsHub}
	}
	if !p.GitAuthOK {
		return Halt{Reason: ReasonGitAuth}
	}
	if p.Kind == KindIntervention {
		if !p.GitWorkTree {
			return Halt{Reason: ReasonNoDocsHub, PauseQueue: false}
		}
		return Halt{}
	}
	hubDirty := strings.TrimSpace(p.HubPorcelain) != ""
	workDirty := strings.TrimSpace(p.WorkPorcelain) != ""
	if p.Kind == KindRefinement {
		if hubDirty {
			return Halt{Reason: ReasonDirty, PauseQueue: true}
		}
		return Halt{}
	}
	if hubDirty || workDirty {
		return Halt{Reason: ReasonDirty, PauseQueue: true}
	}
	if !p.HeadAttached {
		return Halt{Reason: ReasonDetached, PauseQueue: true}
	}
	if p.Branch != p.DefaultBranch && !p.HasUpstream {
		return Halt{Reason: ReasonUntracked, PauseQueue: true}
	}
	return Halt{}
}

// DecideFinish applies the one-resume budget. An ahead hub or a dirty tree
// pauses and does not consume the budget. A clean incomplete finish leases
// one resume when healing_attempts is 0.
func DecideFinish(in FinishInput) FinishDecision {
	dirty := strings.TrimSpace(in.WorkPorcelain) != "" || strings.TrimSpace(in.HubPorcelain) != ""
	if in.Kind == KindRefinement {
		dirty = strings.TrimSpace(in.HubPorcelain) != ""
		if !in.HubPushOK || dirty || in.HubAhead > 0 || !in.StoryDraftOK {
			return FinishDecision{Queue: QueuePaused, Reason: ReasonDirty, HealingAttempts: in.HealingAttempts}
		}
		return FinishDecision{Queue: QueueOpen, HealingAttempts: 0, Decrement: false}
	}
	if dirty {
		return FinishDecision{Queue: QueuePaused, Reason: ReasonDirty, HealingAttempts: in.HealingAttempts}
	}
	if in.HubAhead > 0 {
		return FinishDecision{Queue: QueuePaused, Reason: ReasonDirty, HealingAttempts: in.HealingAttempts}
	}
	if in.ASEComplete {
		return FinishDecision{Queue: QueueOpen, HealingAttempts: 0, Decrement: true}
	}
	if in.HealingAttempts == 0 {
		return FinishDecision{Queue: QueueHealing, HealingAttempts: 1, LeaseResume: true}
	}
	return FinishDecision{Queue: QueuePaused, HealingAttempts: in.HealingAttempts}
}
