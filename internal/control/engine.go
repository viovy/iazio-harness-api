package control

import (
	"errors"
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
	MaxExecutionDuration time.Duration
	EnvKeys              []string
	Status               string
	PromptTitle          string
	Engine               string
}

// Job is one leased execution.
type Job struct {
	ID          string
	ScheduleID  string
	Kind        string
	Status      string
	LeaseHolder string
	LeaseExpiry time.Time
	FrozenUntil time.Time
	ExitPosted  bool
	Correlation string
	HealingUsed bool
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
	HealingAttempts int
	Porcelain       string
	DiscardPending  bool
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
	PromptTitle     string
	Engine          string
	Clean           bool
	ASEComplete     bool
	ConversationIDs []string
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
	ID          string
	Name        string
	Kind        string
	Presence    string
	LastSeen    time.Time
	FetchFailed bool
	Tools       []Tool
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
	cloneLease map[string]string
	hubLease   string
	runningRev map[string]bool
	history    map[string][]HistoryRow
	requests   []RepoRequest
	logs       map[string][]LogChunk
	subs       map[string][]chan LogChunk
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

// UpsertRepo registers a checkout. cloneURL is required.
func (e *Engine) UpsertRepo(r Repo) error {
	if r.CloneURL == "" || r.HostID == "" || r.WorktreePath == "" {
		return errors.New("clone_url, host, and worktree_path are required")
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
	cp := r
	e.repos[repoKey(r.HostID, r.WorktreePath)] = &cp
	return nil
}

// RegisterHost records kind and presence.
func (e *Engine) RegisterHost(id, kind string) Host {
	if kind == "" {
		kind = "permanent"
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	h := &Host{ID: id, Name: id, Kind: kind, Presence: "ONLINE", LastSeen: e.now()}
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
		repo.Lock = LockRunning
		return h
	}
	repo.Lock = LockIdle
	if h.PauseQueue {
		repo.Queue = QueuePaused
		repo.Reason = h.Reason
	} else {
		repo.Reason = h.Reason
	}
	return h
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
	if job.Status == "CANCEL_REQUESTED" {
		job.Status = "CANCELLED"
	}
	_ = code
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
	}
	if job, ok := e.jobs[jobID]; ok {
		job.ExitPosted = true
		if job.Status != "CANCELLED" {
			job.Status = "RUN_FINISHED"
		}
		if sch, ok := e.schedules[job.ScheduleID]; ok {
			if d.Decrement && sch.IterationsRemaining > 0 {
				sch.IterationsRemaining--
			}
			if sch.CloneURL != "" {
				delete(e.cloneLease, sch.CloneURL)
			}
			clean := strings.TrimSpace(in.WorkPorcelain) == "" && strings.TrimSpace(in.HubPorcelain) == "" && in.HubAhead == 0
			e.history[repoKey(host, path)] = append(e.history[repoKey(host, path)], HistoryRow{
				JobID: jobID, PromptTitle: sch.PromptTitle, Engine: sch.Engine,
				Clean: clean, ASEComplete: in.ASEComplete,
			})
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

// PutPrompt inserts a prompt for tests and the HTTP layer.
func (e *Engine) PutPrompt(p Prompt) Prompt {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.ID == "" {
		p.ID = e.next("prompt-")
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

// PollHost leases the next queued schedule for the host.
// The bool is false when nothing is waiting.
func (e *Engine) PollHost(host string) (Job, Schedule, string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, sch := range e.schedules {
		if sch.HostID != host || sch.Status != "QUEUED" {
			continue
		}
		sch.Status = "RUNNING"
		id := e.next("job-")
		job := &Job{
			ID: id, ScheduleID: sch.ID, Kind: sch.Kind, Status: "RUNNING",
			LeaseHolder: host, LeaseExpiry: e.now().Add(LeaseInterval),
		}
		e.jobs[id] = job
		docs := ""
		if repo := e.repos[repoKey(host, sch.WorktreePath)]; repo != nil {
			repo.Lock = LockRunning
			docs = repo.DocsHubPath
		}
		return *job, *sch, docs, true
	}
	return Job{}, Schedule{}, "", false
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
