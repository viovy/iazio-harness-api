package control

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	logPageSize    = 100
	DefaultHeadCap = 25
)

func isAttemptHeader(c LogChunk) bool {
	if c.Type == "TURN_AUTO_CONTINUED" {
		return true
	}
	text := c.Text
	if text == "" {
		return false
	}
	return strings.Contains(text, "[iazio] Starting Attempt") ||
		strings.Contains(text, "[ASE-SUPERVISOR] Starting execution") ||
		strings.Contains(text, "[ASE-SUPERVISOR] Initializing execution") ||
		strings.Contains(text, "Initiating in-job continuation turn") ||
		strings.Contains(text, "Autonomously resuming execution")
}

func trimLogsPreservingHead(logs []LogChunk, capLimit int) []LogChunk {
	if len(logs) <= capLimit {
		return logs
	}
	headLimit := DefaultHeadCap
	if headLimit > capLimit/4 {
		headLimit = capLimit / 4
	}
	if headLimit > len(logs) {
		headLimit = len(logs)
	}

	head := logs[:headLimit]
	seenSeq := make(map[int]bool, capLimit)
	for _, c := range head {
		seenSeq[c.Seq] = true
	}

	tailAllowance := capLimit - headLimit
	if tailAllowance < 0 {
		tailAllowance = 0
	}
	tailStart := len(logs) - tailAllowance
	if tailStart < headLimit {
		tailStart = headLimit
	}

	var attemptHeaders []LogChunk
	for i := headLimit; i < tailStart; i++ {
		c := logs[i]
		if isAttemptHeader(c) {
			attemptHeaders = append(attemptHeaders, c)
			seenSeq[c.Seq] = true
		}
	}

	availableForTail := capLimit - len(head) - len(attemptHeaders)
	if availableForTail < 0 {
		availableForTail = 50
	}
	actualTailStart := len(logs) - availableForTail
	if actualTailStart < headLimit {
		actualTailStart = headLimit
	}

	var tail []LogChunk
	for i := actualTailStart; i < len(logs); i++ {
		c := logs[i]
		if !seenSeq[c.Seq] {
			tail = append(tail, c)
		}
	}

	result := make([]LogChunk, 0, len(head)+len(attemptHeaders)+len(tail))
	result = append(result, head...)
	result = append(result, attemptHeaders...)
	result = append(result, tail...)
	return result
}

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

// ComparePromptsAlphabetically compares two prompts case-insensitively by
// trimmed title (code-point/ASCII A-Z, not locale collation), placing missing
// or whitespace-only titles after non-empty titles and breaking ties
// deterministically by raw trimmed title, lowercase ID, and raw ID.
func ComparePromptsAlphabetically(a, b Prompt) int {
	ta := strings.TrimSpace(a.Title)
	tb := strings.TrimSpace(b.Title)
	aEmpty := ta == ""
	bEmpty := tb == ""
	if aEmpty != bEmpty {
		if aEmpty {
			return 1
		}
		return -1
	}
	if !aEmpty {
		if cmp := strings.Compare(strings.ToLower(ta), strings.ToLower(tb)); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(ta, tb); cmp != 0 {
			return cmp
		}
	}
	if cmp := strings.Compare(strings.ToLower(a.ID), strings.ToLower(b.ID)); cmp != 0 {
		return cmp
	}
	return strings.Compare(a.ID, b.ID)
}

// SortPromptsAlphabetically sorts prompts in place using ComparePromptsAlphabetically.
func SortPromptsAlphabetically(prompts []Prompt) {
	sort.Slice(prompts, func(i, j int) bool {
		return ComparePromptsAlphabetically(prompts[i], prompts[j]) < 0
	})
}

// ListPrompts returns every prompt record sorted alphabetically (case-insensitive A-Z)
// by title with deterministic tie-breaking.
func (e *Engine) ListPrompts() []Prompt {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Prompt, 0, len(e.prompts))
	for _, p := range e.prompts {
		out = append(out, *p)
	}
	SortPromptsAlphabetically(out)
	return out
}

// GetPrompt returns a prompt by ID.
func (e *Engine) GetPrompt(id string) (Prompt, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.prompts[id]
	if !ok {
		return Prompt{}, false
	}
	return *p, true
}

// QueueToolUpdate registers an update command for tools on the given host.
func (e *Engine) QueueToolUpdate(hostID string, tools []string, force bool, targetVersion ...string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[hostID]
	if !ok {
		return ErrNotFound
	}
	var tv string
	if len(targetVersion) > 0 {
		tv = targetVersion[0]
	}
	h.PendingToolUpdate = &PendingToolUpdate{
		Tools:         tools,
		Force:         force,
		TargetVersion: tv,
	}
	return nil
}

// ConsumePendingToolUpdate retrieves and clears the pending tool update for a host.
func (e *Engine) ConsumePendingToolUpdate(hostID string) *PendingToolUpdate {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[hostID]
	if !ok || h.PendingToolUpdate == nil {
		return nil
	}
	res := h.PendingToolUpdate
	h.PendingToolUpdate = nil
	return res
}

// DeleteTool removes a tool from the host's tool list.
func (e *Engine) DeleteTool(hostID, toolName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[hostID]
	if !ok {
		return ErrNotFound
	}
	idx := -1
	for i, t := range h.Tools {
		if strings.EqualFold(t.Name, toolName) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ErrNotFound
	}
	h.Tools = append(h.Tools[:idx], h.Tools[idx+1:]...)
	return nil
}

// QueueCommand registers a command to be delivered to the host on its next check/heartbeat.
func (e *Engine) QueueCommand(hostID string, cmd CommandEnvelope) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[hostID]
	if !ok {
		return ErrNotFound
	}
	h.PendingCommands = append(h.PendingCommands, cmd)
	return nil
}

// ConsumePendingCommands retrieves and clears all pending commands for a host.
func (e *Engine) ConsumePendingCommands(hostID string) []CommandEnvelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[hostID]
	if !ok || len(h.PendingCommands) == 0 {
		return nil
	}
	cmds := h.PendingCommands
	h.PendingCommands = nil
	return cmds
}

// SetHostDevBinaries records the list of discovered dev and shadow binaries for a host.
func (e *Engine) SetHostDevBinaries(hostID string, bins []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if h, ok := e.hosts[hostID]; ok && h != nil {
		h.DiscoveredDevBinaries = append([]string(nil), bins...)
	}
}

// RegisterPingWaiter registers a channel to receive a live ping acknowledgment.
func (e *Engine) RegisterPingWaiter(pingID string) chan PingAck {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pingWaiters == nil {
		e.pingWaiters = make(map[string]chan PingAck)
	}
	ch := make(chan PingAck, 1)
	e.pingWaiters[pingID] = ch
	return ch
}

// DeregisterPingWaiter unregisters a ping acknowledgment channel.
func (e *Engine) DeregisterPingWaiter(pingID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pingWaiters, pingID)
}

// NotifyPingAck notifies any registered waiter of a received live ping acknowledgment.
func (e *Engine) NotifyPingAck(pingID, version string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ch, ok := e.pingWaiters[pingID]; ok {
		select {
		case ch <- PingAck{PingID: pingID, AgentVersion: version, ReceivedAt: time.Now()}:
		default:
		}
	}
}

// HeartbeatDetailed stores the tool inventory, updates presence, and reconciles active jobs.
func (e *Engine) HeartbeatDetailed(id, presence string, tools []Tool, fetchFailed bool, activeJobs ...ActiveJob) error {
	return e.HeartbeatWithCapabilities(id, presence, tools, fetchFailed, nil, "", "", activeJobs...)
}

// HeartbeatWithCapabilities updates presence, capabilities, version info, tools, and reconciles active jobs.
func (e *Engine) HeartbeatWithCapabilities(id, presence string, tools []Tool, fetchFailed bool, supportedCaps []string, version, commitSHA string, activeJobs ...ActiveJob) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok {
		return ErrNotFound
	}
	if strings.TrimSpace(presence) != "" {
		h.Presence = strings.TrimSpace(presence)
	} else {
		h.Presence = "ONLINE"
	}
	h.LastSeen = e.now()
	h.FetchFailed = fetchFailed
	h.Tools = append([]Tool(nil), tools...)
	h.ActiveJobs = append([]ActiveJob(nil), activeJobs...)
	h.HarnessActive = len(activeJobs) > 0
	if len(supportedCaps) > 0 {
		h.SupportedCapabilities = append([]string(nil), supportedCaps...)
	} else if len(h.SupportedCapabilities) == 0 {
		h.SupportedCapabilities = []string{
			"user_service_watchdog_v2",
			"deterministic_resume_4tuple_v1",
			"agy_conversation_pinning_v1",
			"workspace_recovery_modes_v1",
			"background_task_watchdog_v1",
		}
	}
	if version != "" {
		h.Version = version
	}
	if commitSHA != "" {
		h.CommitSHA = commitSHA
	}

	activeMap := make(map[string]bool)
	for _, aj := range activeJobs {
		if aj.JobID != "" {
			activeMap[aj.JobID] = true
			wt := strings.TrimSpace(aj.WorktreePath)
			if wt != "" && wt != "." && wt != "root" {
				for _, r := range e.repos {
					if r != nil && r.HostID == id && (r.WorktreePath == wt || r.EffectiveAlias() == wt) {
						if r.RunningJobID == "" || r.RunningJobID == aj.JobID {
							r.Lock = LockRunning
							r.RunningJobID = aj.JobID
							r.LockHolder = aj.JobID
						}
					}
				}
			}
		}
	}
	for _, r := range e.repos {
		if r != nil && r.HostID == id && r.Lock == LockRunning && r.RunningJobID != "" {
			h.HarnessActive = true
		}
	}

	for _, j := range e.jobs {
		if j.LeaseHolder == id && j.Status == "RUNNING" {
			if len(activeJobs) > 0 {
				if activeMap[j.ID] {
					extendLeaseExpiry(j, e.now(), ActiveLeaseDuration)
				}
			} else {
				extendLeaseExpiry(j, e.now(), ActiveLeaseDuration)
			}
		}
	}
	e.reconcileQueuesLocked()
	return nil
}

// Heartbeat stores the tool inventory, marks the host online, and reconciles active jobs.
func (e *Engine) Heartbeat(id string, tools []Tool, fetchFailed bool, activeJobs ...ActiveJob) error {
	return e.HeartbeatDetailed(id, "ONLINE", tools, fetchFailed, activeJobs...)
}

// SetHostEagerUpdate updates the eager update telemetry reported by the agent.
func (e *Engine) SetHostEagerUpdate(id string, enabled bool, lastUpdate *time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[id]
	if !ok {
		return
	}
	h.EagerUpdateEnabled = enabled
	if lastUpdate != nil && !lastUpdate.IsZero() {
		t := *lastUpdate
		h.LastEagerUpdateAt = &t
	}
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
	now := e.now()
	out := make([]HostSummary, 0, len(e.hosts))
	for _, h := range e.hosts {
		hostCopy := *h
		if !hostCopy.LastSeen.IsZero() && now.Sub(hostCopy.LastSeen) > LeaseInterval*2 {
			hostCopy.Presence = "OFFLINE"
		}
		paused := 0
		for _, repo := range e.repos {
			if repo.HostID == h.ID && repo.Queue == QueuePaused {
				paused++
			}
		}
		out = append(out, HostSummary{Host: hostCopy, ReposPaused: paused})
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
	e.reapExpiredLocked()
	h, ok := e.hosts[id]
	if !ok {
		return HostDetail{}, false
	}
	hostCopy := *h
	if !hostCopy.LastSeen.IsZero() && e.now().Sub(hostCopy.LastSeen) > LeaseInterval*2 {
		hostCopy.Presence = "OFFLINE"
	}
	detail := HostDetail{Host: hostCopy}
	for _, repo := range e.repos {
		if repo.HostID == id {
			detail.Repos = append(detail.Repos, *repo)
		}
	}
	return detail, true
}

// UpdateEagerReadiness updates or stores the eager update readiness report for a host.
func (e *Engine) UpdateEagerReadiness(hostID string, report EagerUpdateReadinessReport) {
	e.mu.Lock()
	defer e.mu.Unlock()

	h, hasHost := e.hosts[hostID]
	if !hasHost || h == nil {
		return
	}
	reportCopy := report
	if reportCopy.LastEvaluatedAt.IsZero() {
		reportCopy.LastEvaluatedAt = e.now().UTC()
	}
	h.EagerReadiness = &reportCopy
}

// GetEagerReadiness retrieves the eager update readiness report for a host.
func (e *Engine) GetEagerReadiness(hostID string) (EagerUpdateReadinessReport, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	h, hasHost := e.hosts[hostID]
	if !hasHost || h == nil || h.EagerReadiness == nil {
		return EagerUpdateReadinessReport{}, false
	}
	return *h.EagerReadiness, true
}

// EvaluateEagerUpdate requests the host to evaluate eager tool updates immediately.
func (e *Engine) EvaluateEagerUpdate(hostID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	h, ok := e.hosts[hostID]
	if !ok || h == nil {
		return ErrNotFound
	}
	cmdID := fmt.Sprintf("eval-eager-%d", e.now().UnixNano())
	h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
		ID:        cmdID,
		Type:      "EVALUATE_EAGER_UPDATE",
		CreatedAt: e.now().UTC().Format(time.RFC3339),
	})
	return nil
}

// ForceEagerUpdate requests the host to force tool updates immediately.
func (e *Engine) ForceEagerUpdate(hostID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	h, ok := e.hosts[hostID]
	if !ok || h == nil {
		return ErrNotFound
	}
	cmdID := fmt.Sprintf("force-tool-%d", e.now().UnixNano())
	h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
		ID:        cmdID,
		Type:      "FORCE_TOOL_UPDATE",
		CreatedAt: e.now().UTC().Format(time.RFC3339),
	})
	return nil
}

// RepoDetail is the queue page.
type RepoDetail struct {
	Repo                 Repo
	FetchFailed          bool
	DocsHubAllIdle       bool
	Running              *Job
	RunningTitle         string
	RunningEngine        string
	RunningSilentSeconds int
	RunningIsStalled     bool
	RunningStartedAt     time.Time
	Schedules            []Schedule
	GlobalSchedules      []Schedule
	History              []HistoryRow
}

// GetRepoDetail returns the queue, lock, schedules, and history.
func (e *Engine) GetRepoDetail(host, path string) (RepoDetail, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reapExpiredLocked()
	repo, ok := e.repos[repoKey(host, path)]
	if !ok {
		for _, r := range e.repos {
			if r.HostID == host && (r.EffectiveAlias() == path || r.WorktreePath == path) {
				repo = r
				path = r.WorktreePath
				ok = true
				break
			}
		}
	}
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
			detail.RunningStartedAt = job.StartedAt
			lastAct := job.StartedAt
			if !job.LastOutputAt.IsZero() && job.LastOutputAt.After(lastAct) {
				lastAct = job.LastOutputAt
			}
			detail.RunningSilentSeconds = int(e.now().Sub(lastAct).Seconds())
			if job.Status == "RUNNING" && detail.RunningSilentSeconds > 120 {
				detail.RunningIsStalled = true
			}
		}
	}
	for _, sch := range e.schedules {
		if (sch.Status == "QUEUED" || sch.Status == "RUNNING") && sch.IterationsRemaining > 0 {
			if sch.IterationsTotal <= 0 || sch.IterationsCompleted < sch.IterationsTotal {
				if sch.HostID == host && sch.WorktreePath == path && !sch.IsGlobal {
					detail.Schedules = append(detail.Schedules, *sch)
				} else if sch.IsGlobal || sch.HostID == "global" || sch.HostID == "" {
					detail.GlobalSchedules = append(detail.GlobalSchedules, *sch)
				}
			}
		}
	}
	sortSchedules := func(s []Schedule) {
		sort.Slice(s, func(i, j int) bool {
			if s[i].Priority != s[j].Priority {
				return s[i].Priority > s[j].Priority
			}
			idxI := strings.LastIndex(s[i].ID, "-")
			idxJ := strings.LastIndex(s[j].ID, "-")
			if idxI >= 0 && idxJ >= 0 {
				nI, errI := strconv.Atoi(s[i].ID[idxI+1:])
				nJ, errJ := strconv.Atoi(s[j].ID[idxJ+1:])
				if errI == nil && errJ == nil {
					return nI < nJ
				}
			}
			return s[i].ID < s[j].ID
		})
	}
	sortSchedules(detail.Schedules)
	sortSchedules(detail.GlobalSchedules)
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
	return e.ExecutePromptWithResume(promptID, host, path, iterations, envVars, "")
}

// ExecutePromptWithResume creates a schedule with an optional conversation ID to resume.
func (e *Engine) ExecutePromptWithResume(promptID, host, path string, iterations int, envVars map[string]string, resumeConvID string) (Schedule, error) {
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
	repo, ok := e.repos[repoKey(host, path)]
	if !ok {
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
		MaxExecutionDuration: 0, EnvKeys: envKeys, EnvVars: envCopy,
		Status: "QUEUED", PromptTitle: p.Title, Engine: p.Engine, CloneURL: repo.CloneURL,
		ResumeConversationID: strings.TrimSpace(resumeConvID),
		StoryID:              p.StoryID,
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
		MaxExecutionDuration: 0, Status: "QUEUED",
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

// RequestDiscard accepts discard. If force is false, it requires the repo lock to be idle.
// If force is true, it terminates active in-progress executions, resets the lock to idle,
// clears porcelain/dirty states, and dispatches FORCE_CLEAN_GIT and KILL_BACKGROUND_TASKS commands.
func (e *Engine) RequestDiscard(host, path string, force ...bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	isForce := len(force) > 0 && force[0]

	repo := e.repos[repoKey(host, path)]
	if repo == nil {
		for _, candidate := range e.repos {
			if candidate.HostID == host && (candidate.EffectiveAlias() == path || candidate.WorktreePath == path) {
				repo = candidate
				break
			}
		}
	}
	if repo == nil {
		return ErrNotFound
	}
	if repo.Lock != LockIdle {
		if !isForce {
			return ErrDiscardBusy
		}
		// In-progress job termination & lock recovery
		for _, job := range e.jobs {
			if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
				if s := e.schedules[job.ScheduleID]; s != nil && s.HostID == repo.HostID && s.WorktreePath == repo.WorktreePath {
					job.Status = "CANCELLED"
					job.ExitPosted = true
					job.ExitCode = 137
					e.updateHistoryStatusLocked(s.HostID, s.WorktreePath, job.ID, "CANCELLED")
				}
			}
		}
		if repo.RunningJobID != "" {
			if j, ok := e.jobs[repo.RunningJobID]; ok {
				j.Status = "CANCELLED"
				j.ExitPosted = true
				j.ExitCode = 137
			}
		}
		repo.Lock = LockIdle
		repo.RunningJobID = ""
		repo.Queue = QueueOpen
		repo.Reason = ""

		if h, ok := e.hosts[host]; ok && h != nil {
			h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
				ID:        fmt.Sprintf("kill-tasks-%d", e.now().UnixNano()),
				Type:      "KILL_BACKGROUND_TASKS",
				Payload:   repo.WorktreePath,
				CreatedAt: e.now().UTC().Format(time.RFC3339),
			})
		}
	}

	repo.Porcelain = ""
	repo.DirtyStoryID = ""
	repo.DirtyReviewFile = ""
	repo.DetectedConversationID = ""
	repo.DetectedVerdict = ""
	repo.DiscardPending = true

	if h, ok := e.hosts[host]; ok && h != nil {
		h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
			ID:        fmt.Sprintf("clean-git-%d", e.now().UnixNano()),
			Type:      "FORCE_CLEAN_GIT",
			Payload:   repo.WorktreePath,
			CreatedAt: e.now().UTC().Format(time.RFC3339),
		})
	}

	e.notifyRepoLocked(repo)
	return nil
}

// RequestDiscardAll requests discard for all repositories registered on a host.
// If force is true, it terminates active in-progress jobs and forces cleanups across all repos.
func (e *Engine) RequestDiscardAll(host string, force bool) ([]*Repo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var matched []*Repo
	for _, repo := range e.repos {
		if repo.HostID == host {
			matched = append(matched, repo)
		}
	}
	if len(matched) == 0 {
		if _, ok := e.hosts[host]; !ok {
			return nil, ErrNotFound
		}
		return nil, nil
	}

	if !force {
		for _, repo := range matched {
			if repo.Lock != LockIdle {
				return nil, ErrDiscardBusy
			}
		}
	}

	for _, repo := range matched {
		if repo.Lock != LockIdle && force {
			for _, job := range e.jobs {
				if !job.ExitPosted && (job.Status == "RUNNING" || job.Status == "CANCEL_REQUESTED") {
					if s := e.schedules[job.ScheduleID]; s != nil && s.HostID == repo.HostID && s.WorktreePath == repo.WorktreePath {
						job.Status = "CANCELLED"
						job.ExitPosted = true
						job.ExitCode = 137
						e.updateHistoryStatusLocked(s.HostID, s.WorktreePath, job.ID, "CANCELLED")
					}
				}
			}
			if repo.RunningJobID != "" {
				if j, ok := e.jobs[repo.RunningJobID]; ok {
					j.Status = "CANCELLED"
					j.ExitPosted = true
					j.ExitCode = 137
				}
			}
			repo.Lock = LockIdle
			repo.RunningJobID = ""
			repo.Queue = QueueOpen
			repo.Reason = ""
		}

		repo.Porcelain = ""
		repo.DirtyStoryID = ""
		repo.DirtyReviewFile = ""
		repo.DetectedConversationID = ""
		repo.DetectedVerdict = ""
		repo.DiscardPending = true

		if h, ok := e.hosts[host]; ok && h != nil {
			if force {
				h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
					ID:        fmt.Sprintf("kill-tasks-%d", e.now().UnixNano()),
					Type:      "KILL_BACKGROUND_TASKS",
					Payload:   repo.WorktreePath,
					CreatedAt: e.now().UTC().Format(time.RFC3339),
				})
			}
			h.PendingCommands = append(h.PendingCommands, CommandEnvelope{
				ID:        fmt.Sprintf("clean-git-%d", e.now().UnixNano()),
				Type:      "FORCE_CLEAN_GIT",
				Payload:   repo.WorktreePath,
				CreatedAt: e.now().UTC().Format(time.RFC3339),
			})
		}
		e.notifyRepoLocked(repo)
	}

	return matched, nil
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
	e.notifyRepoLocked(repo)
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
	if chunk.Type != "OUTPUT_TICK" {
		e.logs[jobID] = append(e.logs[jobID], chunk)
		if len(e.logs[jobID]) > RingCap {
			e.logs[jobID] = trimLogsPreservingHead(e.logs[jobID], RingCap)
		}
	}
	if job, ok := e.jobs[jobID]; ok {
		extendLeaseExpiry(job, e.now(), ActiveLeaseDuration)
		job.LastChunkAt = e.now()
		if chunk.Type != "OUTPUT_TICK" {
			job.LastOutputAt = e.now()
		} else if chunk.SilentForMs > 0 {
			job.LastOutputAt = e.now().Add(-time.Duration(chunk.SilentForMs) * time.Millisecond)
		}
		if chunk.HarnessPID > 0 {
			job.HarnessPID = chunk.HarnessPID
		}
		if chunk.ToolPID > 0 {
			job.ToolPID = chunk.ToolPID
		}
		if chunk.ElapsedSeconds > 0 {
			job.ElapsedSeconds = chunk.ElapsedSeconds
		}
		if chunk.ActiveChildTasks != "" {
			job.ActiveChildTasks = chunk.ActiveChildTasks
		}
		if chunk.Type == "TURN_AUTO_CONTINUED" {
			if chunk.ContinuationTurn > 0 {
				job.ExecutionAttempt = chunk.ContinuationTurn + 1
				job.AutoResumeCount = chunk.ContinuationTurn
				job.ContinuationTurn = chunk.ContinuationTurn
			}
			if chunk.ConversationID != "" && job.ActiveConversationID != chunk.ConversationID {
				job.ActiveConversationID = chunk.ConversationID
			}
		}
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

func (e *Engine) getOrSynthesizeLogsLocked(jobID string) []LogChunk {
	all := e.logs[jobID]
	if len(all) > 0 {
		return all
	}
	job, ok := e.jobs[jobID]
	var (
		convID, worktree, engine, storyID, reason, promptText, status, hostID string
		harnessPID, toolPID, exitCode, resumeCount                            int
		elapsedSec                                                            int64
		asePhase                                                              string
		aseComplete, resumable                                                bool
		dirtyFilesCount                                                       int
		ts                                                                    time.Time
		kind, parentJobID, rootJobID                                          string
	)
	engine = "agy"

	if ok {
		convID = job.ActiveConversationID
		if convID == "" {
			convID = job.ResumeConversationID
		}
		if convID == "" {
			convID = job.InitialConversationID
		}
		if convID == "" && len(job.ConversationIDs) > 0 {
			convID = job.ConversationIDs[0]
		}
		sch, _ := e.schedules[job.ScheduleID]
		if sch != nil {
			worktree = sch.WorktreePath
			if sch.Engine != "" {
				engine = sch.Engine
			}
			storyID = sch.StoryID
		}
		if storyID == "" && job.InterruptedASEState != nil {
			if s, ok := job.InterruptedASEState["story_id"].(string); ok {
				storyID = s
			}
		}
		reason = job.Reason
		if reason == "" {
			reason = job.TerminationReasonCode
		}
		if reason == "" {
			reason = "UNKNOWN"
		}
		ts = job.StartedAt
		if ts.IsZero() {
			ts = e.now()
		}
		status = job.Status
		exitCode = job.ExitCode
		if sch != nil {
			promptText = sch.PromptTitle
		}
		hostID = job.LeaseHolder
		if hostID == "" {
			hostID = job.TargetHost
		}
		kind = job.Kind
		parentJobID = job.ParentJobID
		rootJobID = job.RootJobID
		harnessPID = job.HarnessPID
		toolPID = job.ToolPID
		elapsedSec = job.ElapsedSeconds
		resumeCount = job.ResumeCount
		aseComplete = job.ASEComplete
		dirtyFilesCount = len(job.WorkspaceDirtyFiles)
		resumable = job.Resumable || convID != "" || storyID != ""
		if job.InterruptedASEState != nil {
			if p, ok := job.InterruptedASEState["phase"].(string); ok && p != "" {
				asePhase = p
			}
		}
	} else {
		var found bool
		for key, rows := range e.history {
			for _, row := range rows {
				if row.JobID == jobID {
					found = true
					convID = row.ActiveConversationID
					if convID == "" {
						convID = row.InitialConversationID
					}
					if convID == "" && len(row.ConversationIDs) > 0 {
						convID = row.ConversationIDs[0]
					}
					if idx := strings.Index(key, "\x00"); idx >= 0 {
						hostID, worktree = key[:idx], key[idx+1:]
					}
					if row.Engine != "" {
						engine = row.Engine
					}
					status = row.Status
					reason = row.Reason
					if reason == "" {
						reason = row.TerminationReasonCode
					}
					if reason == "" {
						reason = "COMPLETED"
					}
					exitCode = row.ExitCode
					promptText = row.PromptTitle
					ts = row.CreatedAt
					aseComplete = row.ASEComplete
					dirtyFilesCount = len(row.WorkspaceDirtyFiles)
					harnessPID = row.HarnessPID
					toolPID = row.ToolPID
					parentJobID = row.ParentJobID
					rootJobID = row.RootJobID
					resumeCount = row.AutoResumeCount
					resumable = row.Resumable || convID != ""
					if row.InterruptedASEState != nil {
						if s, ok := row.InterruptedASEState["story_id"].(string); ok {
							storyID = s
						}
						if p, ok := row.InterruptedASEState["phase"].(string); ok {
							asePhase = p
						}
					}
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return nil
		}
	}

	tsStr := ts.UTC().Format(time.RFC3339)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Live Console Telemetry for Job %s\n", tsStr, jobID))
	sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Status: %s | Reason: %s | Exit Code: %d\n", tsStr, status, reason, exitCode))
	if promptText != "" {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Executed Prompt: %s\n", tsStr, promptText))
	}
	if kind == "resume" || parentJobID != "" {
		actionType := "Manual Resume"
		if kind == "auto-resume" || strings.Contains(reason, "auto_resume") || strings.Contains(reason, "watchdog") || strings.Contains(reason, "interrupted") || strings.Contains(reason, "timeout") {
			actionType = "Automatic Resume"
		}
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Action Mode: %s (Kind: %s, Parent Job: %s, Root Job: %s)\n", tsStr, actionType, kind, parentJobID, rootJobID))
	}
	if harnessPID > 0 || toolPID > 0 {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Process Telemetry: HarnessPID=%d, ToolPID=%d, ElapsedSeconds=%d, ResumeCount=%d\n", tsStr, harnessPID, toolPID, elapsedSec, resumeCount))
	}
	if worktree != "" {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Worktree: %s | Engine: %s | Host: %s\n", tsStr, worktree, engine, hostID))
	}
	if convID != "" {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Conversation resolved: %s --conversation=%s (source: control-plane record)\n", tsStr, engine, convID))
	} else {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Conversation: none captured before process termination\n", tsStr))
	}
	if storyID != "" {
		if asePhase == "" {
			asePhase = "IN_PROGRESS"
		}
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] ASE State: story=%s, phase=%s, complete=%v\n", tsStr, storyID, asePhase, aseComplete))
	} else {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] ASE State: N/A (ordinary prompt execution)\n", tsStr))
	}
	if dirtyFilesCount > 0 {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Fleet Git State: %d dirty file(s) recorded\n", tsStr, dirtyFilesCount))
	} else {
		sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Fleet Git State: clean\n", tsStr))
	}
	sb.WriteString(fmt.Sprintf("[%s] [ASE-SUPERVISOR] Resumable: %v | Use 'Resume (%s --conversation)' to resume execution in-place.\n", tsStr, resumable, engine))

	return []LogChunk{
		{
			Seq:    1,
			Stream: "stdout",
			Text:   sb.String(),
		},
	}
}

// JobLogs reads the stored blob. from/to select a gap; otherwise page and cursor page forward.
func (e *Engine) JobLogs(jobID string, page int, cursor string, fromSeq, toSeq int) LogPage {
	e.mu.Lock()
	defer e.mu.Unlock()
	all := e.getOrSynthesizeLogsLocked(jobID)
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
	all := e.getOrSynthesizeLogsLocked(jobID)
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
		return trimLogsPreservingHead(all, tail), 0, 0
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

// ListSchedules returns matching schedules across the engine.
func (e *Engine) ListSchedules(hostID, repoPath, status string) []Schedule {
	e.mu.Lock()
	defer e.mu.Unlock()
	cleanPath := ""
	if repoPath != "" {
		cleanPath = filepath.Clean(repoPath)
	}
	var out []Schedule
	for _, sch := range e.schedules {
		if hostID != "" && sch.HostID != hostID {
			continue
		}
		if cleanPath != "" && filepath.Clean(sch.WorktreePath) != cleanPath {
			continue
		}
		if status != "" && !strings.EqualFold(sch.Status, status) {
			continue
		}
		sCopy := *sch
		sCopy.AssociatedJobs = e.GetAssociatedJobsForScheduleLocked(sch.ID)
		out = append(out, sCopy)
	}

	// Track all known in-memory schedule IDs so we never generate phantom duplicates
	existingScheds := make(map[string]bool)
	for id := range e.schedules {
		existingScheds[id] = true
	}

	// Group historical rows by ScheduleID to deterministically pick the latest row
	type histSchedCand struct {
		row  HistoryRow
		host string
		path string
	}
	historicalMap := make(map[string]histSchedCand)

	for key, rows := range e.history {
		parts := strings.SplitN(key, "\x00", 2)
		hID := ""
		rPath := ""
		if len(parts) == 2 {
			hID = parts[0]
			rPath = parts[1]
		}
		if hostID != "" && hID != hostID {
			continue
		}
		if cleanPath != "" && filepath.Clean(rPath) != cleanPath {
			continue
		}
		for _, r := range rows {
			if r.ScheduleID == "" || existingScheds[r.ScheduleID] {
				continue
			}
			cand, exists := historicalMap[r.ScheduleID]
			if !exists || r.CreatedAt.After(cand.row.CreatedAt) || (r.CreatedAt.Equal(cand.row.CreatedAt) && r.JobID > cand.row.JobID) {
				historicalMap[r.ScheduleID] = histSchedCand{
					row:  r,
					host: hID,
					path: rPath,
				}
			}
		}
	}

	matchesScheduleStatus := func(rowStatus, filterStatus string) bool {
		if filterStatus == "" {
			return true
		}
		if strings.EqualFold(rowStatus, filterStatus) {
			return true
		}
		fLower := strings.ToLower(filterStatus)
		rLower := strings.ToLower(rowStatus)
		if fLower == "failed" && (rLower == "execution_timeout" || rLower == "error") {
			return true
		}
		if fLower == "completed" && rLower == "success" {
			return true
		}
		return false
	}

	for schID, cand := range historicalMap {
		r := cand.row
		if !matchesScheduleStatus(r.Status, status) {
			continue
		}
		sCopy := Schedule{
			ID:           schID,
			HostID:       cand.host,
			WorktreePath: cand.path,
			PromptTitle:  r.PromptTitle,
			Engine:       r.Engine,
			Status:       r.Status,
			ParentJobID:  r.ParentJobID,
			RootJobID:    r.RootJobID,
		}
		sCopy.AssociatedJobs = e.GetAssociatedJobsForScheduleLocked(schID)
		out = append(out, sCopy)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		idxI := strings.LastIndex(out[i].ID, "-")
		idxJ := strings.LastIndex(out[j].ID, "-")
		if idxI >= 0 && idxJ >= 0 {
			nI, errI := strconv.Atoi(out[i].ID[idxI+1:])
			nJ, errJ := strconv.Atoi(out[j].ID[idxJ+1:])
			if errI == nil && errJ == nil {
				return nI < nJ
			}
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ListHistory returns execution history across repositories.
func (e *Engine) ListHistory(hostID, repoPath, status string, limit int) []HistoryItem {
	e.mu.Lock()
	defer e.mu.Unlock()
	cleanPath := ""
	if repoPath != "" {
		cleanPath = filepath.Clean(repoPath)
	}
	var out []HistoryItem
	for key, rows := range e.history {
		parts := strings.SplitN(key, "\x00", 2)
		hID := ""
		rPath := ""
		if len(parts) == 2 {
			hID = parts[0]
			rPath = parts[1]
		}
		if hostID != "" && hID != hostID {
			continue
		}
		if cleanPath != "" && filepath.Clean(rPath) != cleanPath {
			continue
		}
		for _, row := range rows {
			if status != "" && !strings.EqualFold(row.Status, status) {
				continue
			}
			out = append(out, HistoryItem{
				HostID:                    hID,
				WorktreePath:              rPath,
				JobID:                     row.JobID,
				ScheduleID:                row.ScheduleID,
				PromptTitle:               row.PromptTitle,
				Engine:                    row.Engine,
				Status:                    row.Status,
				Clean:                     row.Clean,
				ASEComplete:               row.ASEComplete,
				ConversationIDs:           row.ConversationIDs,
				Reason:                    row.Reason,
				ErrorMessage:              row.ErrorMessage,
				DiagnosticCode:            row.DiagnosticCode,
				Model:                     row.Model,
				Remediation:               row.Remediation,
				TraceID:                   row.TraceID,
				CreatedAt:                 row.CreatedAt,
				ParentJobID:               row.ParentJobID,
				RootJobID:                 row.RootJobID,
				ExecutionAttempt:          row.ExecutionAttempt,
				ExecutionChain:            row.ExecutionChain,
				InitialConversationID:     row.InitialConversationID,
				ActiveConversationID:      row.ActiveConversationID,
				ResumeMode:                row.ResumeMode,
				ResumeFallbackReason:      row.ResumeFallbackReason,
				SameConversationConfirmed: row.SameConversationConfirmed,
				AttachConfirmation:        row.AttachConfirmation,
				InterruptedASEState:       row.InterruptedASEState,
				WorkspaceStateManifest:    row.WorkspaceStateManifest,
				ReapResult:                row.ReapResult,
				RehydrationStatus:         row.RehydrationStatus,
				ConversationLineage:       row.ConversationLineage,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		idxI := strings.LastIndex(out[i].JobID, "-")
		idxJ := strings.LastIndex(out[j].JobID, "-")
		if idxI >= 0 && idxJ >= 0 {
			nI, errI := strconv.Atoi(out[i].JobID[idxI+1:])
			nJ, errJ := strconv.Atoi(out[j].JobID[idxJ+1:])
			if errI == nil && errJ == nil {
				return nI > nJ
			}
		}
		return out[i].JobID > out[j].JobID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// RepoCounts returns the number of idle, running, and paused repositories across the fleet.
func (e *Engine) RepoCounts() (idle, running, paused int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reapExpiredLocked()
	for _, r := range e.repos {
		switch r.Lock {
		case LockRunning:
			running++
		case LockIdle:
			idle++
		default:
			paused++
		}
	}
	return
}

// ActiveJobsCount returns the number of jobs currently in-flight.
func (e *Engine) ActiveJobsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	count := 0
	for _, j := range e.jobs {
		if !j.ExitPosted && (j.Status == "RUNNING" || j.Status == "CANCEL_REQUESTED") {
			count++
		}
	}
	return count
}
