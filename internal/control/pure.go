// Package control is the harness control-plane state machine.
// It does not store conversation transcripts and it does not call a model.
package control

import (
	"encoding/json"
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

	LockIdle      = "IDLE"
	LockRunning   = "RUNNING_HARNESS"
	LockCooling   = "COOLING_OFF"
	LockCleaning  = "CLEANING_HUB"
	LockAutopilot = "AUTOPILOT_LOCAL"
	LockASE       = "RUNNING_ASE"

	KindOrdinary     = "ordinary"
	KindRefinement   = "story_refinement"
	KindIntervention = "intervention"
	KindResume       = "resume"

	ReasonDirty       = "HALTED_DIRTY"
	ReasonDetached    = "HALTED_DETACHED"
	ReasonUntracked   = "HALTED_UNTRACKED"
	ReasonDisk        = "HALTED_DISK"
	ReasonNoDocsHub   = "HALTED_NO_DOCS_HUB"
	ReasonGitAuth     = "HALTED_GIT_AUTH"
	ReasonCorrTimeout = "CORRELATION_TIMEOUT"
)

// AutoResumePolicy defines bounded auto-resume and circuit breaker rules.
type AutoResumePolicy struct {
	Enabled                bool `json:"enabled"`
	MaxAutoResumes         int  `json:"max_auto_resumes"`
	BackoffInitialSec      int  `json:"backoff_initial_sec"`
	BackoffMaxSec          int  `json:"backoff_max_sec"`
	RequireForwardProgress bool `json:"require_forward_progress"`
}

// DefaultAutoResumePolicy returns default values for autonomous jobs.
func DefaultAutoResumePolicy() AutoResumePolicy {
	return AutoResumePolicy{
		Enabled:                true,
		MaxAutoResumes:         5,
		BackoffInitialSec:      10,
		BackoffMaxSec:          120,
		RequireForwardProgress: true,
	}
}

// ResumeOptions specifies parameters when resuming a stalled or abandoned execution.
type ResumeOptions struct {
	ConversationID        string         `json:"conversation_id,omitempty"`
	InitialConversationID string         `json:"initial_conversation_id,omitempty"`
	ActiveConversationID  string         `json:"active_conversation_id,omitempty"`
	ParentJobID           string         `json:"parent_job_id,omitempty"`
	RootJobID             string         `json:"root_job_id,omitempty"`
	ResumeMode            string         `json:"resume_mode,omitempty"`
	ResumeFallbackReason  string         `json:"resume_fallback_reason,omitempty"`
	InterruptedASEState   map[string]any `json:"interrupted_ase_state,omitempty"`
	AllowDirty            bool           `json:"allow_dirty,omitempty"`
	Force                 bool           `json:"force,omitempty"`
	StoryID               string         `json:"story_id,omitempty"`
	JobID                 string         `json:"job_id,omitempty"`
	PromptID              string         `json:"prompt_id,omitempty"`
	PromptTitle           string         `json:"prompt_title,omitempty"`
	HostID                string         `json:"host_id,omitempty"`
	WorktreePath          string         `json:"worktree_path,omitempty"`
	Engine                string         `json:"engine,omitempty"`
	Prompt                string         `json:"prompt,omitempty"`
	CustomPrompt          string         `json:"custom_prompt,omitempty"`
}

// ExitOptions provides additional diagnostic and error context when a job finishes.
type ExitOptions struct {
	ExitCode       int    `json:"exit_code"`
	Reason         string `json:"reason,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	DiagnosticCode string `json:"diagnostic_code,omitempty"`
	Engine         string `json:"engine,omitempty"`
	Model          string `json:"model,omitempty"`
	Remediation    string `json:"remediation,omitempty"`
	ASEComplete    bool   `json:"ase_complete,omitempty"`
}

// ErrBadShareURL is returned when an import URL is not a Gemini share link.
var ErrBadShareURL = errors.New("share url must be https://gemini.google.com/share/<id>")

// ErrExtractorBusy is returned when a Chrome extraction is already running.
var ErrExtractorBusy = errors.New("extractor busy")

// ErrIdeaState is returned when a refinement schedule is not legal.
var ErrIdeaState = errors.New("refinement is accepted only from NEW or TRIAGED")

// Preflight is the checkout report the supervisor posts before spawn.
type Preflight struct {
	Kind                   string `json:"kind"`
	FreeBytes              uint64 `json:"free_bytes"`
	WorkPorcelain          string `json:"work_porcelain"`
	HubPorcelain           string `json:"hub_porcelain"`
	HeadAttached           bool   `json:"head_attached"`
	Branch                 string `json:"branch"`
	DefaultBranch          string `json:"default_branch"`
	HasUpstream            bool   `json:"has_upstream"`
	DocsHubOK              bool   `json:"docs_hub_ok"`
	GitAuthOK              bool   `json:"git_auth_ok"`
	GitWorkTree            bool   `json:"git_work_tree"`
	DirtyStoryID           string `json:"dirty_story_id,omitempty"`
	DirtyReviewFile        string `json:"dirty_review_file,omitempty"`
	DetectedConversationID string `json:"detected_conversation_id,omitempty"`
	DetectedVerdict        string `json:"detected_verdict,omitempty"`
}

func (p *Preflight) UnmarshalJSON(data []byte) error {
	type rawPreflight Preflight
	var raw struct {
		rawPreflight
		AltKind                   string `json:"Kind"`
		AltFreeBytes              uint64 `json:"FreeBytes"`
		AltWorkPorcelain          string `json:"WorkPorcelain"`
		AltHubPorcelain           string `json:"HubPorcelain"`
		AltHeadAttached           bool   `json:"HeadAttached"`
		AltBranch                 string `json:"Branch"`
		AltDefaultBranch          string `json:"DefaultBranch"`
		AltHasUpstream            bool   `json:"HasUpstream"`
		AltDocsHubOK              bool   `json:"DocsHubOK"`
		AltGitAuthOK              bool   `json:"GitAuthOK"`
		AltGitWorkTree            bool   `json:"GitWorkTree"`
		AltDirtyStoryID           string `json:"DirtyStoryID"`
		AltDirtyReviewFile        string `json:"DirtyReviewFile"`
		AltDetectedConversationID string `json:"DetectedConversationID"`
		AltDetectedVerdict        string `json:"DetectedVerdict"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*p = Preflight(raw.rawPreflight)
	if p.Kind == "" && raw.AltKind != "" {
		p.Kind = raw.AltKind
	}
	if p.FreeBytes == 0 && raw.AltFreeBytes != 0 {
		p.FreeBytes = raw.AltFreeBytes
	}
	if p.WorkPorcelain == "" && raw.AltWorkPorcelain != "" {
		p.WorkPorcelain = raw.AltWorkPorcelain
	}
	if p.HubPorcelain == "" && raw.AltHubPorcelain != "" {
		p.HubPorcelain = raw.AltHubPorcelain
	}
	if !p.HeadAttached && raw.AltHeadAttached {
		p.HeadAttached = raw.AltHeadAttached
	}
	if p.Branch == "" && raw.AltBranch != "" {
		p.Branch = raw.AltBranch
	}
	if p.DefaultBranch == "" && raw.AltDefaultBranch != "" {
		p.DefaultBranch = raw.AltDefaultBranch
	}
	if !p.HasUpstream && raw.AltHasUpstream {
		p.HasUpstream = raw.AltHasUpstream
	}
	if !p.DocsHubOK && raw.AltDocsHubOK {
		p.DocsHubOK = raw.AltDocsHubOK
	}
	if !p.GitAuthOK && raw.AltGitAuthOK {
		p.GitAuthOK = raw.AltGitAuthOK
	}
	if !p.GitWorkTree && raw.AltGitWorkTree {
		p.GitWorkTree = raw.AltGitWorkTree
	}
	if p.DirtyStoryID == "" && raw.AltDirtyStoryID != "" {
		p.DirtyStoryID = raw.AltDirtyStoryID
	}
	if p.DirtyReviewFile == "" && raw.AltDirtyReviewFile != "" {
		p.DirtyReviewFile = raw.AltDirtyReviewFile
	}
	if p.DetectedConversationID == "" && raw.AltDetectedConversationID != "" {
		p.DetectedConversationID = raw.AltDetectedConversationID
	}
	if p.DetectedVerdict == "" && raw.AltDetectedVerdict != "" {
		p.DetectedVerdict = raw.AltDetectedVerdict
	}
	return nil
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
	if p.Kind == KindResume {
		// Resumption is explicitly intended to recover an in-flight prompt execution
		// that left uncommitted work, modified submodules, or local review reports in the worktree.
		// It permits dirty porcelain and untracked branches, skipping ReasonDirty and ReasonUntracked.
		if !p.GitWorkTree {
			return Halt{Reason: ReasonNoDocsHub, PauseQueue: false}
		}
		if !p.HeadAttached {
			return Halt{Reason: ReasonDetached, PauseQueue: true}
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
