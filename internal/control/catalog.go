package control

import (
	"errors"
	"sort"
	"strings"
)

const logPageSize = 100

var (
	// ErrNotReady rejects Execute on a draft prompt.
	ErrNotReady = errors.New("execute requires a READY prompt")
	// ErrNotFound is a missing host, repo, prompt, or job.
	ErrNotFound = errors.New("not found")
	// ErrDiscardBusy rejects discard unless the lock is idle.
	ErrDiscardBusy = errors.New("discard requires an idle lock")
)

// ListIdeas returns every idea, newest id last.
func (e *Engine) ListIdeas() []Idea {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Idea, 0, len(e.ideas))
	for _, idea := range e.ideas {
		out = append(out, *idea)
	}
	return out
}

// ListPrompts returns every prompt record.
func (e *Engine) ListPrompts() []Prompt {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Prompt, 0, len(e.prompts))
	for _, p := range e.prompts {
		out = append(out, *p)
	}
	return out
}

// Heartbeat stores the tool inventory and marks the host online.
func (e *Engine) Heartbeat(id string, tools []Tool, fetchFailed bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok {
		return ErrNotFound
	}
	h.Presence = "ONLINE"
	h.LastSeen = e.now()
	h.FetchFailed = fetchFailed
	h.Tools = append([]Tool(nil), tools...)
	return nil
}

// HostSummary is the fleet-list row.
type HostSummary struct {
	Host        Host
	ReposPaused int
}

// ListHosts returns active hosts. Archived ids are absent.
func (e *Engine) ListHosts() []HostSummary {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]HostSummary, 0, len(e.hosts))
	for _, h := range e.hosts {
		paused := 0
		for _, repo := range e.repos {
			if repo.HostID == h.ID && repo.Queue == QueuePaused {
				paused++
			}
		}
		out = append(out, HostSummary{Host: *h, ReposPaused: paused})
	}
	return out
}

// HostDetail is one host plus repos and tools.
type HostDetail struct {
	Host  Host
	Repos []Repo
}

// GetHostDetail returns the host and its repos.
func (e *Engine) GetHostDetail(id string) (HostDetail, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok {
		return HostDetail{}, false
	}
	detail := HostDetail{Host: *h}
	for _, repo := range e.repos {
		if repo.HostID == id {
			detail.Repos = append(detail.Repos, *repo)
		}
	}
	return detail, true
}

// RepoDetail is the queue page.
type RepoDetail struct {
	Repo           Repo
	FetchFailed    bool
	DocsHubAllIdle bool
	Running        *Job
	RunningTitle   string
	RunningEngine  string
	Schedules      []Schedule
	History        []HistoryRow
}

// GetRepoDetail returns the queue, lock, schedules, and history.
func (e *Engine) GetRepoDetail(host, path string) (RepoDetail, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reapExpiredLocked()
	repo, ok := e.repos[repoKey(host, path)]
	if !ok {
		return RepoDetail{}, false
	}
	detail := RepoDetail{Repo: *repo, DocsHubAllIdle: true}
	if h, ok := e.hosts[host]; ok {
		detail.FetchFailed = h.FetchFailed
	}
	for _, other := range e.repos {
		if other.DocsHubPath == repo.DocsHubPath && other.DocsHubPath != "" && other.Lock != LockIdle {
			detail.DocsHubAllIdle = false
		}
	}
	if repo.DocsHubPath == "" {
		detail.DocsHubAllIdle = repo.Lock == LockIdle
	}
	var latest *Job
	var latestTitle, latestEngine string
	var latestN int
	for _, job := range e.jobs {
		sch := e.schedules[job.ScheduleID]
		if sch == nil || sch.HostID != host || sch.WorktreePath != path {
			continue
		}
		if n := atoi(strings.TrimPrefix(job.ID, "job-")); n >= latestN {
			cp := *job
			latest = &cp
			latestN = n
			latestTitle = sch.PromptTitle
			latestEngine = sch.Engine
		}
		if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
			cp := *job
			detail.Running = &cp
			detail.RunningTitle = sch.PromptTitle
			detail.RunningEngine = sch.Engine
		}
	}
	for _, sch := range e.schedules {
		if sch.HostID != host || sch.WorktreePath != path {
			continue
		}
		if (sch.Status == "QUEUED" || sch.Status == "RUNNING") && sch.IterationsRemaining > 0 {
			if sch.IterationsTotal <= 0 || sch.IterationsCompleted < sch.IterationsTotal {
				detail.Schedules = append(detail.Schedules, *sch)
			}
		}
	}
	sort.Slice(detail.Schedules, func(i, j int) bool {
		return detail.Schedules[i].ID < detail.Schedules[j].ID
	})
	if detail.Running == nil && latest != nil && (repo.Lock == LockCooling || repo.Queue == QueueBlocked) {
		detail.Running = latest
		detail.RunningTitle = latestTitle
		detail.RunningEngine = latestEngine
		if repo.Lock == LockCooling {
			detail.Running.Status = "PENDING_CORRELATION"
		}
	}
	detail.History = append([]HistoryRow(nil), e.history[repoKey(host, path)]...)
	return detail, true
}

// ExecutePrompt creates a schedule. Environment keys are extracted for display; values are passed to the child process.
func (e *Engine) ExecutePrompt(promptID, host, path string, iterations int, envVars map[string]string) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.prompts[promptID]
	if !ok {
		return Schedule{}, ErrNotFound
	}
	if p.Status != "READY" {
		return Schedule{}, ErrNotReady
	}
	if _, ok := e.hosts[host]; !ok {
		return Schedule{}, ErrNotFound
	}
	if _, ok := e.repos[repoKey(host, path)]; !ok {
		return Schedule{}, ErrNotFound
	}
	if iterations < 1 {
		iterations = 1
	}
	var envKeys []string
	var envCopy map[string]string
	if len(envVars) > 0 {
		envCopy = make(map[string]string, len(envVars))
		for k, v := range envVars {
			envKeys = append(envKeys, k)
			envCopy[k] = v
		}
	} else {
		envKeys = []string{}
	}
	sid := e.next("sch-")
	sch := &Schedule{
		ID: sid, PromptID: promptID, Revision: p.Revision, HostID: host, WorktreePath: path,
		Kind: KindOrdinary, IterationsTotal: iterations, IterationsRemaining: iterations,
		MaxExecutionDuration: DefaultMaxExec, EnvKeys: envKeys, EnvVars: envCopy,
		Status: "QUEUED", PromptTitle: p.Title, Engine: p.Engine,
	}
	e.schedules[sid] = sch
	return *sch, nil
}

// ScheduleIntervention queues a READY prompt on a paused repo.
func (e *Engine) ScheduleIntervention(host, path, promptID string) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil || repo.Queue != QueuePaused || repo.Lock != LockIdle {
		return Schedule{}, ErrQueueBlocked
	}
	p, ok := e.prompts[promptID]
	if !ok || p.Status != "READY" {
		return Schedule{}, ErrNotReady
	}
	sid := e.next("sch-")
	sch := &Schedule{
		ID: sid, PromptID: promptID, Revision: p.Revision, HostID: host, WorktreePath: path,
		Kind: KindIntervention, IterationsTotal: 1, IterationsRemaining: 1,
		MaxExecutionDuration: DefaultMaxExec, Status: "QUEUED",
		PromptTitle: p.Title, Engine: p.Engine, CloneURL: repo.CloneURL,
	}
	e.schedules[sid] = sch
	return *sch, nil
}

// ReplacePrompt updates a prompt that is not running and bumps the revision.
func (e *Engine) ReplacePrompt(id string, next Prompt) (Prompt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.prompts[id]
	if !ok {
		return Prompt{}, ErrNotFound
	}
	if e.runningRev[id] {
		return Prompt{}, ErrPromptRunning
	}
	p.Title = next.Title
	p.Body = next.Body
	p.Engine = next.Engine
	p.Status = next.Status
	p.StoryID = next.StoryID
	p.SourceIdeaID = next.SourceIdeaID
	p.Revision++
	return *p, nil
}

// UnregisterRepo removes a repo only when the queue is open and the lock is idle.
func (e *Engine) UnregisterRepo(host, path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return ErrNotFound
	}
	if repo.Queue != QueueOpen || repo.Lock != LockIdle {
		return ErrQueueBlocked
	}
	delete(e.repos, repoKey(host, path))
	return nil
}

// RequestCheckout records a path the agent must register and registers the repository into the catalog.
func (e *Engine) RequestCheckout(req RepoRequest) error {
	if req.HostID == "" || req.Path == "" {
		return ErrNotFound
	}
	if req.Mode == "empty-folder" && req.CloneURL == "" {
		return errors.New("clone_url is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.hosts[req.HostID]; !ok {
		return ErrNotFound
	}
	e.requests = append(e.requests, req)
	key := repoKey(req.HostID, req.Path)
	if _, exists := e.repos[key]; !exists {
		e.repos[key] = &Repo{
			HostID:        req.HostID,
			WorktreePath:  req.Path,
			CloneURL:      req.CloneURL,
			DefaultBranch: "main",
			Queue:         QueueOpen,
			Lock:          LockIdle,
		}
	}
	return nil
}

// CheckoutRequests returns pending registration requests for a host.
func (e *Engine) CheckoutRequests(host string) []RepoRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []RepoRequest
	for _, req := range e.requests {
		if req.HostID == host {
			out = append(out, req)
		}
	}
	return out
}

// RequestDiscard accepts discard only while the repo lock is idle.
func (e *Engine) RequestDiscard(host, path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		return ErrNotFound
	}
	if repo.Lock != LockIdle {
		return ErrDiscardBusy
	}
	repo.DiscardPending = true
	return nil
}

// RequestDiscardHub moves every idle repo that shares the hub to CLEANING_HUB.
func (e *Engine) RequestDiscardHub(host, hubPath string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if hubPath == "" {
		seen := map[string]struct{}{}
		for _, repo := range e.repos {
			if repo.HostID == host && repo.DocsHubPath != "" {
				seen[repo.DocsHubPath] = struct{}{}
				hubPath = repo.DocsHubPath
			}
		}
		if len(seen) != 1 {
			return errors.New("docs_hub_path is required")
		}
	}
	var matched []*Repo
	for _, repo := range e.repos {
		if repo.HostID == host && repo.DocsHubPath == hubPath {
			if repo.Lock != LockIdle {
				return ErrDiscardBusy
			}
			matched = append(matched, repo)
		}
	}
	if len(matched) == 0 {
		return ErrNotFound
	}
	for _, repo := range matched {
		repo.Lock = LockCleaning
		repo.Queue = QueueCleaning
	}
	return nil
}

// ForcePauseJob ends correlation for the job's repo.
func (e *Engine) ForcePauseJob(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	sch := e.schedules[job.ScheduleID]
	if sch == nil {
		return ErrNotFound
	}
	repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]
	if repo == nil {
		return ErrNotFound
	}
	repo.Lock = LockIdle
	repo.Queue = QueuePaused
	repo.Reason = ReasonCorrTimeout
	job.ExitPosted = true
	if job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED" {
		job.Status = "FAILED"
	}
	if sch.Status == "RUNNING" {
		sch.Status = "PAUSED"
	}
	if sch.CloneURL != "" {
		delete(e.cloneLease, sch.CloneURL)
	}
	return nil
}

// RetryCorrelation puts the job's repo back into the cooling window.
func (e *Engine) RetryCorrelation(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return ErrNotFound
	}
	sch := e.schedules[job.ScheduleID]
	if sch == nil {
		return ErrNotFound
	}
	repo := e.repos[repoKey(sch.HostID, sch.WorktreePath)]
	if repo == nil {
		return ErrNotFound
	}
	repo.Lock = LockCooling
	repo.Queue = QueueOpen
	repo.Reason = "PENDING_CORRELATION"
	job.Correlation = "PENDING_CORRELATION"
	return nil
}

// AppendLog stores a chunk and wakes stream subscribers. The ring keeps 500.
func (e *Engine) AppendLog(jobID string, chunk LogChunk) {
	e.mu.Lock()
	defer e.mu.Unlock()
	chunk.Seq = len(e.logs[jobID]) + 1
	if chunk.Type == "" {
		chunk.Type = "OUTPUT_CHUNK"
	}
	e.logs[jobID] = append(e.logs[jobID], chunk)
	if len(e.logs[jobID]) > RingCap {
		e.logs[jobID] = e.logs[jobID][len(e.logs[jobID])-RingCap:]
	}
	if job, ok := e.jobs[jobID]; ok {
		job.LeaseExpiry = e.now().Add(LeaseInterval * 2)
	}
	for _, ch := range e.subs[jobID] {
		select {
		case ch <- chunk:
		default:
		}
	}
}

// LogPage is one stored-log response.
type LogPage struct {
	Chunks     []LogChunk
	NextCursor string
}

// JobLogs reads the stored blob. from/to select a gap; otherwise page and cursor page forward.
func (e *Engine) JobLogs(jobID string, page int, cursor string, fromSeq, toSeq int) LogPage {
	e.mu.Lock()
	defer e.mu.Unlock()
	all := e.logs[jobID]
	if fromSeq > 0 || toSeq > 0 {
		var picked []LogChunk
		for _, c := range all {
			if fromSeq > 0 && c.Seq < fromSeq {
				continue
			}
			if toSeq > 0 && c.Seq > toSeq {
				continue
			}
			picked = append(picked, c)
		}
		return LogPage{Chunks: picked}
	}
	start := 0
	if cursor != "" {
		n := atoi(cursor)
		for i, c := range all {
			if c.Seq > n {
				start = i
				break
			}
			start = i + 1
		}
	} else if page > 1 {
		start = (page - 1) * logPageSize
	}
	if start > len(all) {
		start = len(all)
	}
	end := start + logPageSize
	if end > len(all) {
		end = len(all)
	}
	pageChunks := append([]LogChunk(nil), all[start:end]...)
	next := ""
	if end < len(all) && len(pageChunks) > 0 {
		next = itoa(pageChunks[len(pageChunks)-1].Seq)
	}
	return LogPage{Chunks: pageChunks, NextCursor: next}
}

// SubscribeLogs returns a channel of new chunks. Cancel removes it.
func (e *Engine) SubscribeLogs(jobID string) (<-chan LogChunk, func()) {
	ch := make(chan LogChunk, 8)
	e.mu.Lock()
	e.subs[jobID] = append(e.subs[jobID], ch)
	e.mu.Unlock()
	cancel := func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		list := e.subs[jobID]
		for i, c := range list {
			if c == ch {
				e.subs[jobID] = append(list[:i], list[i+1:]...)
				close(ch)
				return
			}
		}
	}
	return ch, cancel
}

// TailLogs returns the last n chunks, or chunks with seq greater than afterSeq.
// gapFrom and gapTo are set when afterSeq is older than the ring.
func (e *Engine) TailLogs(jobID string, tail, afterSeq int) (chunks []LogChunk, gapFrom, gapTo int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	all := e.logs[jobID]
	if tail <= 0 {
		tail = 100
	}
	if tail > RingCap {
		tail = RingCap
	}
	if afterSeq > 0 {
		if len(all) > 0 && afterSeq < all[0].Seq-1 {
			gapFrom = afterSeq + 1
			gapTo = all[0].Seq - 1
		}
		for _, c := range all {
			if c.Seq > afterSeq {
				chunks = append(chunks, c)
			}
		}
		return chunks, gapFrom, gapTo
	}
	if len(all) > tail {
		all = all[len(all)-tail:]
	}
	return append([]LogChunk(nil), all...), 0, 0
}

// JobStatus returns the job status, or empty when missing.
func (e *Engine) JobStatus(jobID string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	job, ok := e.jobs[jobID]
	if !ok {
		return "", false
	}
	return job.Status, true
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}
