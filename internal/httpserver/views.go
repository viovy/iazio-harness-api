package httpserver

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func ideaView(idea control.Idea) map[string]any {
	return map[string]any{
		"id":        idea.ID,
		"title":     idea.Title,
		"status":    idea.Status,
		"share_url": idea.ShareURL,
		"story_id":  idea.StoryID,
	}
}

func promptView(p control.Prompt) map[string]any {
	return map[string]any{
		"id":             p.ID,
		"title":          p.Title,
		"body":           p.Body,
		"engine":         p.Engine,
		"status":         p.Status,
		"story_id":       p.StoryID,
		"source_idea_id": p.SourceIdeaID,
		"revision":       p.Revision,
	}
}

func scheduleView(s control.Schedule) map[string]any {
	keys := s.EnvKeys
	if keys == nil {
		keys = []string{}
	}
	return map[string]any{
		"id":                     s.ID,
		"prompt_id":              s.PromptID,
		"prompt_title":           s.PromptTitle,
		"host_id":                s.HostID,
		"repo_path":              s.WorktreePath,
		"iterations":             s.IterationsRemaining,
		"iterations_total":       s.IterationsTotal,
		"iterations_remaining":   s.IterationsRemaining,
		"iterations_completed":   s.IterationsCompleted,
		"env_keys":               keys,
		"kind":                   s.Kind,
		"priority":               s.Priority,
		"status":                 s.Status,
		"engine":                 s.Engine,
		"resume_conversation_id": s.ResumeConversationID,
		"story_id":               s.StoryID,
		"is_global":              s.IsGlobal,
	}
}

func hostView(h control.Host, paused int) map[string]any {
	name := h.Name
	if name == "" {
		name = h.ID
	}
	var beat any
	if !h.LastSeen.IsZero() {
		beat = h.LastSeen.UTC().Format(time.RFC3339)
	}
	tools := make([]map[string]any, 0, len(h.Tools))
	for _, tool := range h.Tools {
		tools = append(tools, toolView(tool))
	}
	prof := h.DistributionProfile
	if prof == "" {
		prof = "generic"
	}
	return map[string]any{
		"id":                   h.ID,
		"name":                 name,
		"kind":                 h.Kind,
		"presence":             h.Presence,
		"profile":              prof,
		"distribution_profile": prof,
		"last_heartbeat":       beat,
		"repos_paused":         paused,
		"fetch_failed":         h.FetchFailed,
		"tools":                tools,
	}
}

func repoSummary(r control.Repo) map[string]any {
	var expiresAt any
	if !r.LockExpiresAt.IsZero() {
		expiresAt = r.LockExpiresAt.UTC().Format(time.RFC3339)
	}
	runningID := r.RunningJobID
	if r.Lock == control.LockIdle {
		runningID = ""
	}
	return map[string]any{
		"path":                            r.WorktreePath,
		"worktree_path":                   r.WorktreePath,
		"repo_alias":                      r.EffectiveAlias(),
		"alias":                           r.EffectiveAlias(),
		"queue":                           r.Queue,
		"lock":                            r.Lock,
		"lock_holder":                     r.LockHolder,
		"lock_expires_at":                 expiresAt,
		"reason":                          r.Reason,
		"discard_pending":                 r.DiscardPending,
		"docs_hub_path":                   r.DocsHubPath,
		"clone_url":                       r.CloneURL,
		"distribution_profile":            r.DistributionProfile,
		"running_job_id":                  runningID,
		"dirty_story_id":                  r.DirtyStoryID,
		"current_branch":                  r.CurrentBranch,
		"global_queue_allocation_enabled": r.GlobalQueueAllocationEnabled,
	}
}

func toolView(t control.Tool) map[string]any {
	return map[string]any{
		"name":              t.Name,
		"path":              t.Path,
		"version":           t.Version,
		"status":            t.Status,
		"service_installed": t.ServiceInstalled,
		"service_type":      t.ServiceType,
		"service_status":    t.ServiceStatus,
		"capabilities":      t.Capabilities,
	}
}

func chunkView(c control.LogChunk) map[string]any {
	return map[string]any{
		"seq":             c.Seq,
		"type":            c.Type,
		"stream":          c.Stream,
		"text":            c.Text,
		"head":            c.Head,
		"tail":            c.Tail,
		"dropped_bytes":   c.DroppedBytes,
		"silent_for_ms":   c.SilentForMs,
		"harness_pid":     c.HarnessPID,
		"tool_pid":        c.ToolPID,
		"elapsed_seconds": c.ElapsedSeconds,
	}
}

func repoDetailView(d control.RepoDetail) map[string]any {
	var running any
	if d.Running != nil {
		cids := d.Running.ConversationIDs
		if cids == nil {
			cids = []string{}
		}
		var startedAt any
		if !d.RunningStartedAt.IsZero() {
			startedAt = d.RunningStartedAt.UTC().Format(time.RFC3339)
		}
		hostID := d.Running.HostID
		if hostID == "" {
			hostID = d.Running.TargetHost
		}
		if hostID == "" {
			hostID = d.Repo.HostID
		}
		targetHost := d.Running.TargetHost
		if targetHost == "" {
			targetHost = hostID
		}
		running = map[string]any{
			"id":                 d.Running.ID,
			"status":             d.Running.Status,
			"prompt_title":       d.RunningTitle,
			"engine":             d.RunningEngine,
			"conversation_ids":   cids,
			"silent_for_seconds": d.RunningSilentSeconds,
			"is_stalled":         d.RunningIsStalled,
			"started_at":         startedAt,
			"host_id":            hostID,
			"target_host":        targetHost,
			"harness_pid":        d.Running.HarnessPID,
			"tool_pid":           d.Running.ToolPID,
			"elapsed_seconds":    d.Running.ElapsedSeconds,
			"resume_count":       d.Running.ResumeCount,
		}
	}
	schedules := make([]map[string]any, 0, len(d.Schedules))
	for _, s := range d.Schedules {
		schedules = append(schedules, scheduleView(s))
	}
	globalSchedules := make([]map[string]any, 0, len(d.GlobalSchedules))
	for _, s := range d.GlobalSchedules {
		globalSchedules = append(globalSchedules, scheduleView(s))
	}
	history := make([]map[string]any, 0, len(d.History))
	for _, h := range d.History {
		ids := h.ConversationIDs
		if ids == nil {
			ids = []string{}
		}
		history = append(history, map[string]any{
			"job_id":           h.JobID,
			"schedule_id":      h.ScheduleID,
			"prompt_title":     h.PromptTitle,
			"engine":           h.Engine,
			"status":           h.Status,
			"clean":            h.Clean,
			"ase_complete":     h.ASEComplete,
			"conversation_ids": ids,
			"reason":           h.Reason,
			"error_message":    h.ErrorMessage,
			"diagnostic_code":  h.DiagnosticCode,
			"model":            h.Model,
			"remediation":      h.Remediation,
			"trace_id":         h.TraceID,
			"created_at":       h.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	var lockExpiresAt any
	if !d.Repo.LockExpiresAt.IsZero() {
		lockExpiresAt = d.Repo.LockExpiresAt.UTC().Format(time.RFC3339)
	}

	return map[string]any{
		"host_id":                         d.Repo.HostID,
		"path":                            d.Repo.WorktreePath,
		"worktree_path":                   d.Repo.WorktreePath,
		"repo_alias":                      d.Repo.EffectiveAlias(),
		"alias":                           d.Repo.EffectiveAlias(),
		"queue":                           d.Repo.Queue,
		"lock":                            d.Repo.Lock,
		"lock_holder":                     d.Repo.LockHolder,
		"lock_expires_at":                 lockExpiresAt,
		"reason":                          d.Repo.Reason,
		"fetch_failed":                    d.FetchFailed,
		"porcelain":                       d.Repo.Porcelain,
		"docs_hub_path":                   d.Repo.DocsHubPath,
		"docs_hub_all_idle":               d.DocsHubAllIdle,
		"distribution_profile":            d.Repo.DistributionProfile,
		"current_branch":                  d.Repo.CurrentBranch,
		"dirty_story_id":                  d.Repo.DirtyStoryID,
		"dirty_review_file":               d.Repo.DirtyReviewFile,
		"detected_conversation_id":        d.Repo.DetectedConversationID,
		"detected_verdict":                d.Repo.DetectedVerdict,
		"running_job":                     running,
		"schedules":                       schedules,
		"global_schedules":                globalSchedules,
		"history":                         history,
		"global_queue_allocation_enabled": d.Repo.GlobalQueueAllocationEnabled,
		"submodules_clean":                d.Repo.SubmodulesClean,
		"dirty_submodules":                d.Repo.DirtySubmodules,
		"ase_state":                       d.Repo.ASEState,
		"active_tool":                     d.Repo.ActiveTool,
		"tool_liveness":                   d.Repo.ToolLiveness,
		"execution_provenance":            d.Repo.ExecutionProvenance,
	}
}

func jobDetailView(j control.JobDetail) map[string]any {
	cids := j.ConversationIDs
	if cids == nil {
		cids = []string{}
	}
	targetHost := j.TargetHost
	if targetHost == "" {
		targetHost = j.HostID
	}
	hostID := j.HostID
	if hostID == "" {
		hostID = j.TargetHost
	}
	return map[string]any{
		"id":                             j.ID,
		"schedule_id":                    j.ScheduleID,
		"kind":                           j.Kind,
		"status":                         j.Status,
		"engine":                         j.Engine,
		"prompt":                         j.Prompt,
		"story_id":                       j.StoryID,
		"source_idea_id":                 j.SourceIdeaID,
		"transcript":                     j.Transcript,
		"worktree_path":                  j.WorktreePath,
		"docs_hub_path":                  j.DocsHubPath,
		"env_vars":                       j.EnvVars,
		"max_execution_duration_seconds": j.MaxExecutionDurationSeconds,
		"resume_conversation_id":         j.ResumeConversationID,
		"conversation_ids":               cids,
		"silent_for_seconds":             j.SilentForSeconds,
		"is_stalled":                     j.IsStalled,
		"started_at":                     j.StartedAt,
		"reason":                         j.Reason,
		"host_id":                        hostID,
		"target_host":                    targetHost,
		"harness_pid":                    j.HarnessPID,
		"tool_pid":                       j.ToolPID,
		"elapsed_seconds":                j.ElapsedSeconds,
		"resume_count":                   j.ResumeCount,
		"trace_id":                       j.TraceID,
	}
}

func historyJobDetailView(r control.HistoryRow, host, path string) map[string]any {
	cids := r.ConversationIDs
	if cids == nil {
		cids = []string{}
	}
	return map[string]any{
		"id":                             r.JobID,
		"schedule_id":                    r.ScheduleID,
		"kind":                           "story_execution",
		"status":                         r.Status,
		"engine":                         r.Engine,
		"prompt":                         r.PromptTitle,
		"story_id":                       r.PromptTitle,
		"source_idea_id":                 "",
		"transcript":                     "",
		"worktree_path":                  path,
		"docs_hub_path":                  "",
		"env_vars":                       map[string]string{},
		"max_execution_duration_seconds": 0,
		"resume_conversation_id":         "",
		"conversation_ids":               cids,
		"silent_for_seconds":             0,
		"is_stalled":                     false,
		"started_at":                     r.CreatedAt,
		"reason":                         r.Reason,
		"clean":                          r.Clean,
		"ase_complete":                   r.ASEComplete,
		"error_message":                  r.ErrorMessage,
		"diagnostic_code":                r.DiagnosticCode,
		"model":                          r.Model,
		"remediation":                    r.Remediation,
		"trace_id":                       r.TraceID,
		"host_id":                        host,
		"target_host":                    host,
	}
}

func historyItemView(h control.HistoryItem) map[string]any {
	ids := h.ConversationIDs
	if ids == nil {
		ids = []string{}
	}
	return map[string]any{
		"host_id":          h.HostID,
		"repo_path":        h.WorktreePath,
		"job_id":           h.JobID,
		"schedule_id":      h.ScheduleID,
		"prompt_title":     h.PromptTitle,
		"engine":           h.Engine,
		"status":           h.Status,
		"clean":            h.Clean,
		"ase_complete":     h.ASEComplete,
		"conversation_ids": ids,
		"reason":           h.Reason,
		"error_message":    h.ErrorMessage,
		"diagnostic_code":  h.DiagnosticCode,
		"model":            h.Model,
		"remediation":      h.Remediation,
		"trace_id":         h.TraceID,
		"created_at":       h.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func parseDispatchPayload(payload []byte) (title, prompt, storyID string) {
	if len(payload) == 0 {
		return "", "", ""
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return "", "", ""
	}
	if t, ok := m["title"].(string); ok {
		title = t
	}
	if p, ok := m["prompt"].(string); ok {
		prompt = p
	}
	if s, ok := m["story_id"].(string); ok {
		storyID = s
	}
	return title, prompt, storyID
}

func dispatchScheduleView(j *dispatch.Job) map[string]any {
	if j == nil {
		return map[string]any{}
	}
	title, _, storyID := parseDispatchPayload(j.Payload)
	if title == "" {
		title = fmt.Sprintf("Dispatch Job %s (%s)", j.ID, j.Type)
	}
	engine := j.TargetAgent
	if engine == "" {
		engine = "auto"
	}
	hostID := j.ClaimedHost
	if hostID == "" {
		hostID = j.HostID
	}
	if hostID == "" {
		hostID = j.TargetHost
	}
	targetHost := j.TargetHost
	if targetHost == "" || targetHost == "auto" {
		targetHost = hostID
	}
	status := strings.ToUpper(j.Status)
	if status == "PENDING" {
		status = "QUEUED"
	} else if status == "CLAIMED" {
		status = "PENDING_EXECUTION"
	}

	return map[string]any{
		"id":                     j.ID,
		"prompt_id":              "",
		"prompt_title":           title,
		"host_id":                hostID,
		"target_host":            targetHost,
		"repo_path":              j.Repo,
		"iterations":             1,
		"iterations_total":       1,
		"iterations_remaining":   1,
		"iterations_completed":   0,
		"env_keys":               []string{},
		"kind":                   j.Type,
		"priority":               5,
		"status":                 status,
		"engine":                 engine,
		"resume_conversation_id": "",
		"story_id":               storyID,
		"is_global":              true,
		"trace_id":               j.TraceID,
		"parent_id":              j.ParentID,
	}
}

func dispatchHistoryView(j *dispatch.Job) map[string]any {
	if j == nil {
		return map[string]any{}
	}
	title, _, storyID := parseDispatchPayload(j.Payload)
	if title == "" {
		title = fmt.Sprintf("Dispatch Job %s (%s)", j.ID, j.Type)
	}
	engine := j.TargetAgent
	if engine == "" {
		engine = "auto"
	}
	hostID := j.ClaimedHost
	if hostID == "" {
		hostID = j.HostID
	}
	if hostID == "" {
		hostID = j.TargetHost
	}
	targetHost := j.TargetHost
	if targetHost == "" || targetHost == "auto" {
		targetHost = hostID
	}
	status := strings.ToUpper(j.Status)
	clean := status == "COMPLETED"

	createdAtStr := ""
	if !j.CreatedAt.IsZero() {
		createdAtStr = j.CreatedAt.UTC().Format(time.RFC3339)
	}

	return map[string]any{
		"host_id":          hostID,
		"target_host":      targetHost,
		"repo_path":        j.Repo,
		"job_id":           j.ID,
		"schedule_id":      "",
		"prompt_title":     title,
		"engine":           engine,
		"status":           status,
		"clean":            clean,
		"ase_complete":     clean,
		"conversation_ids": []string{},
		"reason":           j.Error,
		"error_message":    j.Error,
		"diagnostic_code":  "",
		"model":            "",
		"remediation":      "",
		"created_at":       createdAtStr,
		"story_id":         storyID,
		"trace_id":         j.TraceID,
		"parent_id":        j.ParentID,
	}
}

func dispatchJobDetailView(j *dispatch.Job) map[string]any {
	if j == nil {
		return map[string]any{}
	}
	title, promptText, storyID := parseDispatchPayload(j.Payload)
	if title == "" {
		title = fmt.Sprintf("Dispatch Job %s (%s)", j.ID, j.Type)
	}
	engine := j.TargetAgent
	if engine == "" {
		engine = "auto"
	}
	hostID := j.ClaimedHost
	if hostID == "" {
		hostID = j.HostID
	}
	if hostID == "" {
		hostID = j.TargetHost
	}
	targetHost := j.TargetHost
	if targetHost == "" || targetHost == "auto" {
		targetHost = hostID
	}
	res := map[string]any{
		"id":                             j.ID,
		"schedule_id":                    "",
		"kind":                           j.Type,
		"status":                         strings.ToUpper(j.Status),
		"engine":                         engine,
		"target_agent":                   j.TargetAgent,
		"target_host":                    targetHost,
		"host_id":                        hostID,
		"prompt":                         promptText,
		"prompt_title":                   title,
		"story_id":                       storyID,
		"worktree_path":                  j.Repo,
		"docs_hub_path":                  "",
		"env_vars":                       map[string]string{},
		"max_execution_duration_seconds": 0,
		"resume_conversation_id":         "",
		"conversation_ids":               []string{},
		"silent_for_seconds":             0,
		"is_stalled":                     false,
		"reason":                         j.Error,
		"error":                          j.Error,
		"trace_id":                       j.TraceID,
		"parent_id":                      j.ParentID,
	}
	if !j.CreatedAt.IsZero() {
		res["started_at"] = j.CreatedAt.UTC().Format(time.RFC3339)
		res["created_at"] = j.CreatedAt.UTC().Format(time.RFC3339)
	}
	if j.ClaimedAt != nil {
		res["claimed_at"] = j.ClaimedAt.UTC().Format(time.RFC3339)
	}
	if j.CompletedAt != nil {
		res["completed_at"] = j.CompletedAt.UTC().Format(time.RFC3339)
		if !j.CreatedAt.IsZero() {
			res["duration_ms"] = j.CompletedAt.Sub(j.CreatedAt).Milliseconds()
		}
	}
	return res
}
