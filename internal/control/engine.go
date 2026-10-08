package control

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// RingCap is the live chunk ring per job.
	RingCap = 500
	// DefaultMaxExec is the schedule duration when unset.
	DefaultMaxExec = 60 * time.Minute
	// LeaseInterval is the heartbeat the agent must renew.
	LeaseInterval = 30 * time.Second
	// ReapFreeze keeps a cancel from looking like a missed heartbeat.
	ReapFreeze = 30 * time.Second
	// CorrelationWindow is how long a missing transcript waits.
	CorrelationWindow = 5 * time.Minute
	// EphemeralGrace archives an offline one-shot host.
	EphemeralGrace = 10 * time.Minute
)

var (
	// ErrNotLeaseHolder rejects a story draft from the wrong caller.
	ErrNotLeaseHolder = errors.New("caller does not hold the lease")
	// ErrCancelClosed rejects cancel after the exit code is posted.
	ErrCancelClosed = errors.New("cancel rejected after exit")
	// ErrPromptRunning refuses body edits and deletes on a running revision.
	ErrPromptRunning = errors.New("prompt revision is running")
	// ErrQueueBlocked refuses ordinary and intervention leases.
	ErrQueueBlocked = errors.New("queue is not leasable")
	// ErrCloneBusy is the global leaf lease.
	ErrCloneBusy = errors.New("clone url already leased")
	// ErrHubBusy is the fleet-wide refinement lease.
	ErrHubBusy = errors.New("docs hub refinement already leased")
)

// Idea is an imported share-link row.
type Idea struct {
	ID             string
	Title          string
	ShareURL       string
	Transcript     string
	Status         string
	PreviousStatus string
	StoryID        string
}

// Prompt is an operator-authored instruction. Placeholders stay unfilled.
type Prompt struct {
	ID           string
	Title        string
	Body         string
	Engine       string
	StoryID      string
	SourceIdeaID string
	Status       string
	Revision     int
}

// Schedule is one Execute click.
type Schedule struct {
	ID                   string
	PromptID             string
	Revision             int
	HostID               string
	WorktreePath         string
	CloneURL             string
	Kind                 string
	IterationsTotal      int
	IterationsRemaining  int
	IterationsCompleted  int
	MaxExecutionDuration time.Duration
	EnvKeys              []string
	EnvVars              map[string]string
	Status               string
	PromptTitle          string
	Engine               string
	Priority             int
	ResumeConversationID string
	LeaseRetries         int
}

// JobDetail carries the execution payload for iazio-harness.
type JobDetail struct {
	ID                          string            `json:"id"`
	ScheduleID                  string            `json:"schedule_id"`
	Kind                        string            `json:"kind"`
	Status                      string            `json:"status"`
	Engine                      string            `json:"engine"`
	Prompt                      string            `json:"prompt"`
	StoryID                     string            `json:"story_id"`
	SourceIdeaID                string            `json:"source_idea_id"`
	Transcript                  string            `json:"transcript,omitempty"`
	WorktreePath                string            `json:"worktree_path"`
	DocsHubPath                 string            `json:"docs_hub_path"`
	EnvVars                     map[string]string `json:"env_vars,omitempty"`
	MaxExecutionDurationSeconds int               `json:"max_execution_duration_seconds"`
	ResumeConversationID        string            `json:"resume_conversation_id,omitempty"`
	ConversationIDs             []string          `json:"conversation_ids,omitempty"`
}

// Job is one leased execution.
type Job struct {
	ID                   string
	ScheduleID           string
	Kind                 string
	Status               string
	LeaseHolder          string
	LeaseExpiry          time.Time
	FrozenUntil          time.Time
	ExitPosted           bool
	Correlation          string
	HealingUsed          bool
	StartedAt            time.Time
	ResumeConversationID string
	ConversationIDs      []string
}

// Repo is one registered checkout.
type Repo struct {
	HostID          string
	WorktreePath    string
	DocsHubPath     string
	CloneURL        string
	DefaultBranch   string
	Queue           string
	Lock            string
	Reason          string
	HealingAttempts     int
	Porcelain           string
	DiscardPending      bool
	DistributionProfile string
}

// Tool is one binary from the latest heartbeat.
type Tool struct {
	Name    string
	Path    string
	Version string
	Status  string
}

// HistoryRow is one finished execution on a repo page.
type HistoryRow struct {
	JobID           string
	ScheduleID      string
	PromptTitle     string
	Engine          string
	Status          string
	Clean           bool
	ASEComplete     bool
	ConversationIDs []string
	Reason          string
}

// HistoryItem is one execution record reported across repositories.
type HistoryItem struct {
	HostID          string
	WorktreePath    string
	JobID           string
	ScheduleID      string
	PromptTitle     string
	Engine          string
	Status          string
	Clean           bool
	ASEComplete     bool
	ConversationIDs []string
	Reason          string
}

// LogChunk is one stripped output event.
type LogChunk struct {
	Seq          int
	Type         string
	Stream       string
	Text         string
	Head         string
	Tail         string
	DroppedBytes int
	SilentForMs  int
}

// RepoRequest is a checkout the browser asked the agent to register.
type RepoRequest struct {
	HostID   string
	Mode     string
	Path     string
	CloneURL string
}

// Host is a registered supervisor.
type Host struct {
	ID                  string
	Name                string
	Kind                string
	Presence            string
	LastSeen            time.Time
	FetchFailed         bool
	DistributionProfile string
	Tools               []Tool
}

// Engine is the in-process control plane.
type Engine struct {
	mu         sync.Mutex
	now        func() time.Time
	seq        int
	extracting bool
	ideas      map[string]*Idea
	prompts    map[string]*Prompt
	schedules  map[string]*Schedule
	jobs       map[string]*Job
	repos      map[string]*Repo
	hosts      map[string]*Host
	profiles   map[string]*DistributionProfile
	cloneLease map[string]string
	hubLease   string
	runningRev map[string]bool
	history    map[string][]HistoryRow
	requests   []RepoRequest
	logs         map[string][]LogChunk
	subs         map[string][]chan LogChunk
	OnRepoChange func(Repo)
	OnHistoryChange func(host, path string, row HistoryRow)
	OnHistoryDelete func(host, path string, jobID string)
}

func (e *Engine) notifyRepoLocked(r *Repo) {
	if r == nil || e.OnRepoChange == nil {
		return
	}
	cp := *r
	go e.OnRepoChange(cp)
}

func (e *Engine) notifyHistoryLocked(host, path string, row HistoryRow) {
	if e.OnHistoryChange == nil {
		return
	}
	cp := row
	go e.OnHistoryChange(host, path, cp)
}

func (e *Engine) notifyHistoryDeleteLocked(host, path, jobID string) {
	if e.OnHistoryDelete == nil || jobID == "" {
		return
	}
	go e.OnHistoryDelete(host, path, jobID)
}

// SetHistory populates history for a repo key on startup.
func (e *Engine) SetHistory(key string, rows []HistoryRow) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.history[key] = append([]HistoryRow(nil), rows...)
}

// NewEngine returns an empty control plane.
func NewEngine(now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}
	return &Engine{
		now:        now,
		ideas:      map[string]*Idea{},
		prompts:    map[string]*Prompt{},
		schedules:  map[string]*Schedule{},
		jobs:       map[string]*Job{},
		repos:      map[string]*Repo{},
		hosts:      map[string]*Host{},
		profiles:   map[string]*DistributionProfile{},
		cloneLease: map[string]string{},
		runningRev: map[string]bool{},
		history:    map[string][]HistoryRow{},
		logs:       map[string][]LogChunk{},
		subs:       map[string][]chan LogChunk{},
	}
}

func (e *Engine) next(prefix string) string {
	e.seq++
	return prefix + itoa(e.seq)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func repoKey(host, path string) string { return host + "\x00" + path }

// BeginExtract admits one Chrome worker. A second call returns ErrExtractorBusy.
func (e *Engine) BeginExtract() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.extracting {
		return ErrExtractorBusy
	}
	e.extracting = true
	return nil
}

// EndExtract releases the Chrome worker.
func (e *Engine) EndExtract() {
	e.mu.Lock()
	e.extracting = false
	e.mu.Unlock()
}

// ImportIdea stores a share URL and transcript as NEW.
func (e *Engine) ImportIdea(shareURL, transcript, title string) (Idea, error) {
	if err := ValidateShareURL(shareURL); err != nil {
		return Idea{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	id := e.next("idea-")
	idea := &Idea{ID: id, Title: title, ShareURL: shareURL, Transcript: transcript, Status: IdeaNew}
	e.ideas[id] = idea
	return *idea, nil
}

// Triage moves NEW to TRIAGED.
func (e *Engine) Triage(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	idea, ok := e.ideas[id]
	if !ok || idea.Status != IdeaNew {
		return ErrIdeaState
	}
	idea.Status = IdeaTriaged
	return nil
}

// ScheduleRefinement accepts NEW or TRIAGED and sets REFINING.
// A second schedule for REFINING or REFINED is rejected.
func (e *Engine) ScheduleRefinement(ideaID, hostID, hubPath string) (Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	idea, ok := e.ideas[ideaID]
	if !ok || !CanScheduleRefinement(idea.Status) {
		return Job{}, ErrIdeaState
	}
	if e.hubLease != "" {
		return Job{}, ErrHubBusy
	}
	idea.PreviousStatus = idea.Status
	idea.Status = IdeaRefining
	e.hubLease = ideaID
	job := e.newJobLocked(hostID, hubPath, "", KindRefinement)
	return *job, nil
}

func (e *Engine) newJobLocked(host, path, clone, kind string) *Job {
	id := e.next("job-")
	sid := e.next("sch-")
	e.schedules[sid] = &Schedule{
		ID: sid, HostID: host, WorktreePath: path, CloneURL: clone, Kind: kind,
		IterationsTotal: 1, IterationsRemaining: 1,
		MaxExecutionDuration: DefaultMaxExec, Status: "RUNNING",
	}
	job := &Job{
		ID: id, ScheduleID: sid, Kind: kind, Status: "RUNNING",
		LeaseHolder: host, LeaseExpiry: e.now().Add(LeaseInterval),
		StartedAt: e.now(),
	}
	e.jobs[id] = job
	if clone != "" {
		e.cloneLease[clone] = id
	}
	return job
}

// FailRefinement restores the idea status from before REFINING and drops the hub lease.
func (e *Engine) FailRefinement(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok || job.Kind != KindRefinement {
		return ErrIdeaState
	}
	for _, idea := range e.ideas {
		if idea.Status == IdeaRefining {
			idea.Status = idea.PreviousStatus
			if idea.Status == "" {
				idea.Status = IdeaNew
			}
		}
	}
	e.hubLease = ""
	job.Status = "FAILED"
	job.ExitPosted = true
	return nil
}

// StoryDraft stores the draft and marks the idea REFINED when the lease holder posts it.
func (e *Engine) StoryDraft(jobID, holder, storyID, body string) (Prompt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok || job.LeaseHolder != holder {
		return Prompt{}, ErrNotLeaseHolder
	}
	var idea *Idea
	for _, it := range e.ideas {
		if it.Status == IdeaRefining {
			idea = it
			break
		}
	}
	if idea == nil {
		return Prompt{}, ErrIdeaState
	}
	idea.Status = IdeaRefined
	idea.StoryID = storyID
	e.hubLease = ""
	job.Status = "RUN_FINISHED"
	job.ExitPosted = true
	pid := e.next("prompt-")
	p := &Prompt{
		ID: pid, Title: "story " + storyID, Body: body, Engine: "agent",
		StoryID: storyID, SourceIdeaID: idea.ID, Status: "DRAFT", Revision: 1,
	}
	e.prompts[pid] = p
	return *p, nil
}

// UpsertRepo registers a checkout.
func (e *Engine) UpsertRepo(r Repo) error {
	if r.HostID == "" || r.WorktreePath == "" {
		return errors.New("host and worktree_path are required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if r.Queue == "" {
		r.Queue = QueueOpen
	}
	if r.Lock == "" {
		r.Lock = LockIdle
	}
	if r.DefaultBranch == "" {
		r.DefaultBranch = "main"
	}
	if r.DistributionProfile == "" {
		r.DistributionProfile = "generic"
	}
	cp := r
	e.repos[repoKey(r.HostID, r.WorktreePath)] = &cp
	return nil
}

// RegisterHost records kind, presence, and distribution profile.
func (e *Engine) RegisterHost(id, kind string, profile ...string) Host {
	if kind == "" {
		kind = "permanent"
	}
	p := "generic"
	if len(profile) > 0 && strings.TrimSpace(profile[0]) != "" {
		p = strings.TrimSpace(profile[0])
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	h := &Host{ID: id, Name: id, Kind: kind, Presence: "ONLINE", LastSeen: e.now(), DistributionProfile: p}
	e.hosts[id] = h
	return *h
}

// LeaseOrdinary assigns one job per clone URL. Refinement does not take that lease.
func (e *Engine) LeaseOrdinary(host, path, clone string) (Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return Job{}, ErrQueueBlocked
	}
	if repo.Queue != QueueOpen && repo.Queue != QueueHealing {
		return Job{}, ErrQueueBlocked
	}
	if repo.Lock == LockRunning || repo.Lock == LockCooling || repo.Lock == LockCleaning {
		return Job{}, ErrQueueBlocked
	}
	if holder, ok := e.cloneLease[clone]; ok && holder != "" {
		return Job{}, ErrCloneBusy
	}
	repo.Lock = LockRunning
	repo.Queue = QueueOpen
	job := e.newJobLocked(host, path, clone, KindOrdinary)
	return *job, nil
}

// LeaseIntervention is the only lease accepted on a PAUSED repo.
func (e *Engine) LeaseIntervention(host, path, clone string) (Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil || repo.Queue != QueuePaused || repo.Lock != LockIdle {
		return Job{}, ErrQueueBlocked
	}
	repo.Lock = LockRunning
	job := e.newJobLocked(host, path, clone, KindIntervention)
	return *job, nil
}

// ApplyPreflight records a halt. Disk and git-auth do not pause the queue.
func (e *Engine) ApplyPreflight(host, path string, p Preflight) Halt {
	h := DecidePreflight(p)
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return h
	}
	if strings.TrimSpace(p.WorkPorcelain) != "" {
		repo.Porcelain = p.WorkPorcelain
	} else if strings.TrimSpace(p.HubPorcelain) != "" {
		repo.Porcelain = p.HubPorcelain
	}
	if h.Reason == "" {
		if repo.Reason == ReasonDisk {
			repo.Reason = ""
		}
		if repo.Queue == QueuePaused && repo.Reason == ReasonDirty {
			repo.Queue = QueueOpen
			repo.Reason = ""
			repo.Porcelain = ""
		}
		hasActive := false
		for _, other := range e.jobs {
			if !other.ExitPosted && (other.Status == "RUNNING" || other.Status == "CANCEL_REQUESTED") {
				if otherSch := e.schedules[other.ScheduleID]; otherSch != nil {
					if otherSch.HostID == host && otherSch.WorktreePath == path {
						hasActive = true
						break
					}
				}
			}
		}
		if hasActive {
			repo.Lock = LockRunning
		} else {
			repo.Lock = LockIdle
		}
		e.notifyRepoLocked(repo)
		return h
	}
	repo.Lock = LockIdle
	if h.PauseQueue {
		repo.Queue = QueuePaused
		repo.Reason = h.Reason
	} else {
		repo.Reason = h.Reason
	}
	e.notifyRepoLocked(repo)
	return h
}

// ResumeRepo unpauses a paused repository queue and clears any halt reason.
func (e *Engine) ResumeRepo(host, path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return ErrNotFound
	}
	repo.Queue = QueueOpen
	repo.Reason = ""
	repo.Porcelain = ""
	repo.DiscardPending = false
	e.notifyRepoLocked(repo)
	return nil
}

// PostExit marks the child gone. Cancel after this is rejected.
func (e *Engine) PostExit(jobID string, code int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return ErrCancelClosed
	}
	job.ExitPosted = true
	var changedRepo *Repo
	if job.Status == "CANCEL_REQUESTED" {
		job.Status = "CANCELLED"
		if sch, ok := e.schedules[job.ScheduleID]; ok {
			sch.Status = "CANCELLED"
			sch.IterationsRemaining = 0
			if sch.CloneURL != "" {
				delete(e.cloneLease, sch.CloneURL)
			}
			if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
				if repo.Lock == LockRunning {
					repo.Lock = LockIdle
				}
				changedRepo = repo
			}
		}
	} else if job.Status == "RUNNING" {
		if code != 0 {
			job.Status = "FAILED"
			if sch, ok := e.schedules[job.ScheduleID]; ok {
				if sch.IterationsRemaining <= 0 {
					sch.Status = "FAILED"
				} else {
					sch.Status = "QUEUED"
				}
				if sch.CloneURL != "" {
					delete(e.cloneLease, sch.CloneURL)
				}
				if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
					if repo.Lock == LockRunning {
						repo.Lock = LockIdle
					}
					if repo.Queue == QueueOpen {
						repo.Queue = QueuePaused
					}
					repo.Reason = "NON_ZERO_EXIT"
					changedRepo = repo
				}
				e.updateHistoryStatusLocked(sch.HostID, sch.WorktreePath, jobID, "FAILED")
			}
		} else {
			job.Status = "RUN_FINISHED"
			if sch, ok := e.schedules[job.ScheduleID]; ok {
				if sch.IterationsRemaining <= 0 || (sch.IterationsTotal > 0 && sch.IterationsCompleted >= sch.IterationsTotal) {
					sch.Status = "FINISHED"
					sch.IterationsRemaining = 0
				} else {
					sch.Status = "QUEUED"
				}
				if sch.CloneURL != "" {
					delete(e.cloneLease, sch.CloneURL)
				}
				if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
					if repo.Lock == LockRunning {
						repo.Lock = LockIdle
					}
					changedRepo = repo
				}
				e.updateHistoryStatusLocked(sch.HostID, sch.WorktreePath, jobID, "RUN_FINISHED")
			}
		}
	}
	if changedRepo != nil {
		e.notifyRepoLocked(changedRepo)
	}
	return nil
}

// DeclineJob rejects an assignment when the agent cannot execute it (e.g. busy worktree or spawn failure).
// It resets the schedule to QUEUED, refunds the decremented iteration, and marks the job DECLINED.
func (e *Engine) DeclineJob(jobID, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	if job.ExitPosted {
		return ErrCancelClosed
	}
	job.Status = "DECLINED"
	job.ExitPosted = true
	var changedRepo *Repo
	if sch, ok := e.schedules[job.ScheduleID]; ok {
		sch.IterationsRemaining++
		if sch.IterationsCompleted > 0 {
			sch.IterationsCompleted--
		}
		sch.Status = "QUEUED"
		if sch.CloneURL != "" {
			delete(e.cloneLease, sch.CloneURL)
		}
		if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
			hasActive := false
			for _, other := range e.jobs {
				if other.ID != jobID && !other.ExitPosted && (other.Status == "RUNNING" || other.Status == "CANCEL_REQUESTED") {
					if otherSch := e.schedules[other.ScheduleID]; otherSch != nil {
						if otherSch.HostID == repo.HostID && otherSch.WorktreePath == repo.WorktreePath {
							hasActive = true
							break
						}
					}
				}
			}
			if !hasActive && repo.Lock == LockRunning {
				repo.Lock = LockIdle
			}
			changedRepo = repo
		}
		key := repoKey(sch.HostID, sch.WorktreePath)
		rows := e.history[key]
		var deduplicated []HistoryRow
		var droppedJobIDs []string
		for _, r := range rows {
			if r.ScheduleID == sch.ID && r.Status == "DECLINED" && r.JobID != jobID {
				droppedJobIDs = append(droppedJobIDs, r.JobID)
				continue
			}
			deduplicated = append(deduplicated, r)
		}
		e.history[key] = deduplicated
		e.updateHistoryDeclineLocked(sch.HostID, sch.WorktreePath, jobID, reason)
		for _, dropID := range droppedJobIDs {
			e.notifyHistoryDeleteLocked(sch.HostID, sch.WorktreePath, dropID)
		}
	}
	if changedRepo != nil {
		e.notifyRepoLocked(changedRepo)
	}
	return nil
}

// Cancel requests abort only while the child has not posted an exit.
func (e *Engine) Cancel(jobID, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok || job.ExitPosted {
		return ErrCancelClosed
	}
	job.Status = "CANCEL_REQUESTED"
	job.FrozenUntil = e.now().Add(ReapFreeze)
	if reason == "" {
		reason = "cancelled"
	}
	if sch, ok := e.schedules[job.ScheduleID]; ok {
		e.updateHistoryStatusLocked(sch.HostID, sch.WorktreePath, jobID, "CANCEL_REQUESTED")
	}
	_ = reason
	return nil
}

// ApplyFinish runs the finish rule and releases the clone lease.
func (e *Engine) ApplyFinish(host, path string, jobID string, in FinishInput) FinishDecision {
	d := DecideFinish(in)
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo != nil {
		repo.Queue = d.Queue
		repo.Reason = d.Reason
		repo.HealingAttempts = d.HealingAttempts
		repo.Lock = LockIdle
		e.notifyRepoLocked(repo)
	}
	if job, ok := e.jobs[jobID]; ok {
		job.ExitPosted = true
		if job.Status != "CANCELLED" {
			job.Status = "RUN_FINISHED"
		}
		if sch, ok := e.schedules[job.ScheduleID]; ok {
			if sch.IterationsRemaining <= 0 || (sch.IterationsTotal > 0 && sch.IterationsCompleted >= sch.IterationsTotal) {
				sch.Status = "FINISHED"
				sch.IterationsRemaining = 0
			} else {
				sch.Status = "QUEUED"
			}
			if sch.CloneURL != "" {
				delete(e.cloneLease, sch.CloneURL)
			}
			clean := strings.TrimSpace(in.WorkPorcelain) == "" && strings.TrimSpace(in.HubPorcelain) == "" && in.HubAhead == 0
			e.updateHistoryFinishLocked(host, path, jobID, clean, in.ASEComplete, job.Status)
		}
	}
	return d
}

// NoteCorrelation keeps the lock in COOLING_OFF until the window ends.
func (e *Engine) NoteCorrelation(host, path string, matched bool, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return
	}
	if matched {
		repo.Lock = LockIdle
		repo.Queue = QueueOpen
		return
	}
	if elapsed < CorrelationWindow {
		repo.Lock = LockCooling
		repo.Queue = QueueOpen
		repo.Reason = "PENDING_CORRELATION"
		return
	}
	repo.Lock = LockIdle
	repo.Queue = QueueBlocked
	repo.Reason = ReasonCorrTimeout
}

// ForcePause ends correlation wait and allows a later intervention.
func (e *Engine) ForcePause(host, path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return
	}
	repo.Lock = LockIdle
	repo.Queue = QueuePaused
	repo.Reason = ReasonCorrTimeout
	e.notifyRepoLocked(repo)
	for _, job := range e.jobs {
		sch := e.schedules[job.ScheduleID]
		if sch == nil || sch.HostID != host || sch.WorktreePath != path {
			continue
		}
		if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
			job.Status = "CANCELLED"
			job.ExitPosted = true
			if sch.Status == "RUNNING" {
				sch.Status = "PAUSED"
			}
			if sch.CloneURL != "" {
				delete(e.cloneLease, sch.CloneURL)
			}
		}
	}
}

// MarkPromptRunning pins the revision so edits create a new revision instead.
func (e *Engine) MarkPromptRunning(id string, running bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.runningRev[id] = running
}

// UpdatePrompt refuses an in-flight revision and otherwise stores a new revision.
func (e *Engine) UpdatePrompt(id, body string) (Prompt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.prompts[id]
	if !ok {
		return Prompt{}, ErrPromptRunning
	}
	if e.runningRev[id] {
		return Prompt{}, ErrPromptRunning
	}
	p.Body = body
	p.Revision++
	return *p, nil
}

// DeletePrompt is refused while the revision is running.
func (e *Engine) DeletePrompt(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runningRev[id] {
		return ErrPromptRunning
	}
	delete(e.prompts, id)
	return nil
}

func (e *Engine) ensureSeqLocked(id string) {
	idx := strings.LastIndex(id, "-")
	if idx >= 0 && idx < len(id)-1 {
		if n, err := strconv.Atoi(id[idx+1:]); err == nil && n > e.seq {
			e.seq = n
		}
	}
}

// PutIdea inserts or updates an idea in memory, typically when hydrating from persistent store.
func (e *Engine) PutIdea(it Idea) Idea {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureSeqLocked(it.ID)
	cp := it
	e.ideas[it.ID] = &cp
	return cp
}

// SetHost stores or updates a host with its full state in memory.
func (e *Engine) SetHost(h Host) Host {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := h
	if cp.DistributionProfile == "" {
		cp.DistributionProfile = "generic"
	}
	e.hosts[h.ID] = &cp
	return cp
}

// PutPrompt inserts a prompt for tests and the HTTP layer.
func (e *Engine) PutPrompt(p Prompt) Prompt {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.ID == "" {
		p.ID = e.next("prompt-")
	} else {
		e.ensureSeqLocked(p.ID)
	}
	if p.Revision == 0 {
		p.Revision = 1
	}
	if p.Status == "" {
		p.Status = "DRAFT"
	}
	cp := p
	e.prompts[p.ID] = &cp
	return cp
}

// ArchiveEphemeral removes an offline ephemeral host after the grace period.
// The id is not reused. Leased jobs become FAILED host_vanished and do not heal.
func (e *Engine) ArchiveEphemeral(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok || h.Kind != "ephemeral" {
		return false
	}
	if h.Presence != "OFFLINE" || e.now().Sub(h.LastSeen) < EphemeralGrace {
		return false
	}
	for _, job := range e.jobs {
		if job.LeaseHolder == id && !job.ExitPosted {
			job.Status = "FAILED"
			job.ExitPosted = true
		}
	}
	delete(e.hosts, id)
	return true
}

// MissHeartbeat marks presence OFFLINE. Permanent hosts are not deleted.
func (e *Engine) MissHeartbeat(id string, missed int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok {
		return
	}
	if missed >= 2 {
		h.Presence = "OFFLINE"
		h.LastSeen = e.now().Add(-EphemeralGrace)
	}
}

// GetRepo returns a copy of the repo row.
func (e *Engine) GetRepo(host, path string) (Repo, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.repos[repoKey(host, path)]
	if !ok {
		return Repo{}, false
	}
	return *r, true
}

func (e *Engine) reapExpiredLocked() {
	now := e.now()
	for _, job := range e.jobs {
		if job.ExitPosted && job.Status != "RUNNING" && job.Status != "CANCEL_REQUESTED" {
			continue
		}
		sch := e.schedules[job.ScheduleID]
		if job.Status == "CANCEL_REQUESTED" && !job.FrozenUntil.IsZero() && now.After(job.FrozenUntil) {
			job.Status = "CANCELLED"
			job.ExitPosted = true
			if sch != nil {
				sch.Status = "CANCELLED"
				sch.IterationsRemaining = 0
				if sch.CloneURL != "" {
					delete(e.cloneLease, sch.CloneURL)
				}
				if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
					if repo.Lock == LockRunning {
						repo.Lock = LockIdle
					}
					if repo.Queue == QueueOpen {
						repo.Queue = QueuePaused
					}
					repo.Reason = "CANCELLED"
					e.notifyRepoLocked(repo)
				}
			}
			continue
		}
		if job.Status == "RUNNING" {
			maxDur := DefaultMaxExec
			if sch != nil && sch.MaxExecutionDuration > 0 {
				maxDur = sch.MaxExecutionDuration
			}
			started := job.StartedAt
			if started.IsZero() {
				started = job.LeaseExpiry.Add(-LeaseInterval)
			}
			timedOut := !started.IsZero() && now.Sub(started) > maxDur
			hostOffline := false
			if sch != nil {
				if h, ok := e.hosts[sch.HostID]; ok && h.Presence == "OFFLINE" && now.Sub(h.LastSeen) > EphemeralGrace {
					hostOffline = true
				}
			}
			leaseExpired := !job.LeaseExpiry.IsZero() && now.After(job.LeaseExpiry)
			if timedOut || hostOffline || leaseExpired {
				job.Status = "FAILED"
				job.ExitPosted = true
				reason := "EXECUTION_TIMEOUT"
				if hostOffline {
					reason = "HOST_OFFLINE"
				} else if leaseExpired && !timedOut {
					reason = "LEASE_EXPIRED"
				}
				if sch != nil {
					if leaseExpired && !timedOut && sch.LeaseRetries < 2 {
						sch.LeaseRetries++
						sch.IterationsRemaining++
						if sch.IterationsCompleted > 0 {
							sch.IterationsCompleted--
						}
						sch.Status = "QUEUED"
					} else if sch.IterationsRemaining > 0 {
						sch.Status = "QUEUED"
					} else {
						sch.Status = "FAILED"
					}
					if sch.CloneURL != "" && sch.IterationsRemaining <= 0 {
						delete(e.cloneLease, sch.CloneURL)
					}
					if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
						if repo.Lock == LockRunning {
							repo.Lock = LockIdle
						}
						if timedOut || hostOffline {
							if repo.Queue == QueueOpen {
								repo.Queue = QueuePaused
							}
						}
						repo.Reason = reason
						e.notifyRepoLocked(repo)
					}
					e.updateHistoryStatusLocked(sch.HostID, sch.WorktreePath, job.ID, reason)
				}
			}
		}
	}
	for _, sch := range e.schedules {
		if sch.Status == "RUNNING" || sch.Status == "COMPLETED" {
			hasActive := false
			for _, job := range e.jobs {
				if job.ScheduleID == sch.ID && !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
					hasActive = true
					break
				}
			}
			if !hasActive {
				if sch.IterationsRemaining > 0 && (sch.IterationsTotal <= 0 || sch.IterationsCompleted < sch.IterationsTotal) {
					sch.Status = "QUEUED"
				} else {
					sch.Status = "FINISHED"
					sch.IterationsRemaining = 0
				}
				if sch.CloneURL != "" && sch.IterationsRemaining <= 0 {
					delete(e.cloneLease, sch.CloneURL)
				}
			}
		}
	}
	for _, repo := range e.repos {
		if repo.Lock == LockRunning {
			hasActive := false
			for _, job := range e.jobs {
				if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
					sch := e.schedules[job.ScheduleID]
					if sch != nil && sch.HostID == repo.HostID && sch.WorktreePath == repo.WorktreePath {
						hasActive = true
						break
					}
				}
			}
			if !hasActive {
				repo.Lock = LockIdle
				e.notifyRepoLocked(repo)
			}
		}
	}
}

// PollHost leases the next queued schedule for the host.
// The bool is false when nothing is waiting.
func (e *Engine) PollHost(host string) (Job, Schedule, string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reapExpiredLocked()
	var candidates []*Schedule
	for _, sch := range e.schedules {
		if sch.HostID == host && sch.Status == "QUEUED" {
			candidates = append(candidates, sch)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		idxI := strings.LastIndex(candidates[i].ID, "-")
		idxJ := strings.LastIndex(candidates[j].ID, "-")
		if idxI >= 0 && idxJ >= 0 {
			nI, errI := strconv.Atoi(candidates[i].ID[idxI+1:])
			nJ, errJ := strconv.Atoi(candidates[j].ID[idxJ+1:])
			if errI == nil && errJ == nil {
				return nI < nJ
			}
		}
		return candidates[i].ID < candidates[j].ID
	})
	for _, sch := range candidates {
		repo := e.repos[repoKey(host, sch.WorktreePath)]
		if repo != nil {
			if repo.Lock == LockRunning || repo.Lock == LockCooling || repo.Lock == LockCleaning {
				continue
			}
			if repo.Queue != QueueOpen && repo.Queue != QueueHealing {
				if !(repo.Queue == QueuePaused && sch.Kind == KindIntervention) {
					continue
				}
			}
			if repo.Reason != "" {
				if repo.Reason == ReasonDisk {
					continue
				}
				if sch.Kind != KindIntervention {
					continue
				}
			}
		}
		if sch.CloneURL != "" {
			if holder, ok := e.cloneLease[sch.CloneURL]; ok && holder != "" {
				continue
			}
		}
		if sch.IterationsRemaining > 0 {
			sch.IterationsRemaining--
		}
		sch.IterationsCompleted++

		sch.Status = "RUNNING"
		id := e.next("job-")
		job := &Job{
			ID: id, ScheduleID: sch.ID, Kind: sch.Kind, Status: "RUNNING",
			LeaseHolder: host, LeaseExpiry: e.now().Add(LeaseInterval),
			StartedAt:            e.now(),
			ResumeConversationID: sch.ResumeConversationID,
		}
		if sch.ResumeConversationID != "" {
			job.ConversationIDs = []string{sch.ResumeConversationID}
		}
		e.jobs[id] = job
		if sch.CloneURL != "" {
			e.cloneLease[sch.CloneURL] = id
		}
		docs := ""
		if repo != nil {
			repo.Lock = LockRunning
			docs = repo.DocsHubPath
			e.notifyRepoLocked(repo)
		}
		row := HistoryRow{
			JobID:           id,
			ScheduleID:      sch.ID,
			PromptTitle:     sch.PromptTitle,
			Engine:          sch.Engine,
			Status:          "RUNNING",
			Clean:           false,
			ASEComplete:     false,
			ConversationIDs: []string{},
		}
		if sch.ResumeConversationID != "" {
			row.ConversationIDs = append(row.ConversationIDs, sch.ResumeConversationID)
		}
		e.history[repoKey(host, sch.WorktreePath)] = append(e.history[repoKey(host, sch.WorktreePath)], row)
		e.notifyHistoryLocked(host, sch.WorktreePath, row)
		return *job, *sch, docs, true
	}
	return Job{}, Schedule{}, "", false
}

func (e *Engine) updateHistoryStatusLocked(host, path, jobID, status string) {
	key := repoKey(host, path)
	rows := e.history[key]
	for i := range rows {
		if rows[i].JobID == jobID {
			rows[i].Status = status
			if rows[i].Reason == "" && (status == "LEASE_EXPIRED" || status == "EXECUTION_TIMEOUT" || status == "HOST_OFFLINE" || status == "FAILED") {
				rows[i].Reason = status
			}
			e.notifyHistoryLocked(host, path, rows[i])
			return
		}
	}
}

func (e *Engine) updateHistoryDeclineLocked(host, path, jobID, reason string) {
	key := repoKey(host, path)
	rows := e.history[key]
	for i := range rows {
		if rows[i].JobID == jobID {
			rows[i].Status = "DECLINED"
			rows[i].Reason = reason
			e.notifyHistoryLocked(host, path, rows[i])
			return
		}
	}
}

func (e *Engine) updateHistoryFinishLocked(host, path, jobID string, clean, aseComplete bool, status string) {
	key := repoKey(host, path)
	rows := e.history[key]
	for i := range rows {
		if rows[i].JobID == jobID {
			rows[i].Clean = clean
			rows[i].ASEComplete = aseComplete
			if status != "" {
				rows[i].Status = status
			}
			e.notifyHistoryLocked(host, path, rows[i])
			return
		}
	}
	title, engine, schID := "", "", ""
	if j, ok := e.jobs[jobID]; ok {
		if sch := e.schedules[j.ScheduleID]; sch != nil {
			title = sch.PromptTitle
			engine = sch.Engine
			schID = sch.ID
		}
	}
	row := HistoryRow{
		JobID: jobID, ScheduleID: schID, PromptTitle: title, Engine: engine,
		Status: status, Clean: clean, ASEComplete: aseComplete,
	}
	e.history[key] = append(e.history[key], row)
	e.notifyHistoryLocked(host, path, row)
}

// ChangeSchedulePriority shifts a schedule's priority by delta.
func (e *Engine) ChangeSchedulePriority(scheduleID string, delta int) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sch, ok := e.schedules[scheduleID]
	if !ok {
		return Schedule{}, ErrNotFound
	}
	sch.Priority += delta
	if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
		e.notifyRepoLocked(repo)
	}
	return *sch, nil
}

// SetSchedulePriority sets a schedule's priority directly.
func (e *Engine) SetSchedulePriority(scheduleID string, priority int) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sch, ok := e.schedules[scheduleID]
	if !ok {
		return Schedule{}, ErrNotFound
	}
	sch.Priority = priority
	if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
		e.notifyRepoLocked(repo)
	}
	return *sch, nil
}

// CancelSchedule cancels a schedule by ID and aborts any active running job for it.
func (e *Engine) CancelSchedule(scheduleID string) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sch, ok := e.schedules[scheduleID]
	if !ok {
		return Schedule{}, ErrNotFound
	}
	if sch.Status == "FINISHED" || sch.Status == "CANCELLED" {
		return *sch, nil
	}
	sch.Status = "CANCELLED"
	sch.IterationsRemaining = 0
	if sch.CloneURL != "" {
		delete(e.cloneLease, sch.CloneURL)
	}

	for _, job := range e.jobs {
		if job.ScheduleID == scheduleID && !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
			job.Status = "CANCEL_REQUESTED"
			job.FrozenUntil = e.now().Add(ReapFreeze)
			e.updateHistoryStatusLocked(sch.HostID, sch.WorktreePath, job.ID, "CANCEL_REQUESTED")
		}
	}

	if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
		hasActive := false
		for _, job := range e.jobs {
			if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
				if s := e.schedules[job.ScheduleID]; s != nil && s.HostID == repo.HostID && s.WorktreePath == repo.WorktreePath {
					hasActive = true
					break
				}
			}
		}
		if !hasActive && repo.Lock == LockRunning {
			repo.Lock = LockIdle
		}
		e.notifyRepoLocked(repo)
	}
	return *sch, nil
}

// GetRepoForJob returns the repository associated with the given job ID.
func (e *Engine) GetRepoForJob(jobID string) (Repo, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return Repo{}, false
	}
	sch, ok := e.schedules[job.ScheduleID]
	if !ok || sch == nil {
		return Repo{}, false
	}
	repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]
	if repo == nil {
		return Repo{}, false
	}
	return *repo, true
}

// GetIdea returns a copy of the idea.
func (e *Engine) GetIdea(id string) (Idea, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	it, ok := e.ideas[id]
	if !ok {
		return Idea{}, false
	}
	return *it, true
}

// GetJob returns execution details for the harness child.
func (e *Engine) GetJob(id string) (JobDetail, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reapExpiredLocked()
	job, ok := e.jobs[id]
	if !ok {
		return JobDetail{}, false
	}
	detail := JobDetail{
		ID:                   job.ID,
		ScheduleID:           job.ScheduleID,
		Kind:                 job.Kind,
		Status:               job.Status,
		ResumeConversationID: job.ResumeConversationID,
		ConversationIDs:      append([]string(nil), job.ConversationIDs...),
	}
	sch := e.schedules[job.ScheduleID]
	if sch != nil {
		if detail.ResumeConversationID == "" {
			detail.ResumeConversationID = sch.ResumeConversationID
		}
		detail.Engine = sch.Engine
		detail.WorktreePath = sch.WorktreePath
		detail.EnvVars = sch.EnvVars
		detail.MaxExecutionDurationSeconds = int(sch.MaxExecutionDuration.Seconds())
		if p, ok := e.prompts[sch.PromptID]; ok {
			detail.Prompt = p.Body
			detail.StoryID = p.StoryID
			detail.SourceIdeaID = p.SourceIdeaID
			if detail.Engine == "" {
				detail.Engine = p.Engine
			}
		}
		if repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]; repo != nil {
			detail.DocsHubPath = repo.DocsHubPath
		}
	}
	if detail.Engine == "" {
		detail.Engine = "agent"
	}
	if detail.MaxExecutionDurationSeconds == 0 {
		detail.MaxExecutionDurationSeconds = int(DefaultMaxExec.Seconds())
	}
	if detail.Kind == KindRefinement {
		for _, it := range e.ideas {
			if it.Status == IdeaRefining || (detail.SourceIdeaID != "" && it.ID == detail.SourceIdeaID) {
				detail.Transcript = it.Transcript
				if detail.StoryID == "" {
					detail.StoryID = it.StoryID
				}
				if detail.SourceIdeaID == "" {
					detail.SourceIdeaID = it.ID
				}
				break
			}
		}
		if detail.Prompt == "" {
			detail.Prompt = "Refine idea transcript at {{.SourceTranscriptPath}} into story {{.StoryID}}"
		}
	}
	return detail, true
}

// GetJobLeaseExpiry returns the lease expiry for a job.
func (e *Engine) GetJobLeaseExpiry(id string) (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[id]
	if !ok {
		return time.Time{}, false
	}
	return j.LeaseExpiry, true
}

// AddJobConversation records an early or streamed conversation ID for a job.
func (e *Engine) AddJobConversation(jobID, convID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	convID = strings.TrimSpace(convID)
	if convID == "" {
		return errors.New("empty conversation id")
	}
	found := false
	for _, c := range job.ConversationIDs {
		if c == convID {
			found = true
			break
		}
	}
	if !found {
		job.ConversationIDs = append(job.ConversationIDs, convID)
	}
	sch, okSch := e.schedules[job.ScheduleID]
	if okSch {
		key := repoKey(sch.HostID, sch.WorktreePath)
		rows := e.history[key]
		for i := range rows {
			if rows[i].JobID == jobID {
				has := false
				for _, c := range rows[i].ConversationIDs {
					if c == convID {
						has = true
						break
					}
				}
				if !has {
					rows[i].ConversationIDs = append(rows[i].ConversationIDs, convID)
					e.notifyHistoryLocked(sch.HostID, sch.WorktreePath, rows[i])
				}
				break
			}
		}
	}
	return nil
}

