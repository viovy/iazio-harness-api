package httpserver

import (
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
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
		"id":           s.ID,
		"prompt_id":    s.PromptID,
		"prompt_title": s.PromptTitle,
		"host_id":      s.HostID,
		"repo_path":    s.WorktreePath,
		"iterations":   s.IterationsTotal,
		"env_keys":     keys,
		"kind":         s.Kind,
		"status":       s.Status,
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
	return map[string]any{
		"id":             h.ID,
		"name":           name,
		"kind":           h.Kind,
		"presence":       h.Presence,
		"last_heartbeat": beat,
		"repos_paused":   paused,
		"fetch_failed":   h.FetchFailed,
	}
}

func repoSummary(r control.Repo) map[string]any {
	return map[string]any{
		"path":          r.WorktreePath,
		"worktree_path": r.WorktreePath,
		"queue":         r.Queue,
		"lock":          r.Lock,
		"docs_hub_path": r.DocsHubPath,
		"clone_url":     r.CloneURL,
	}
}

func toolView(t control.Tool) map[string]any {
	return map[string]any{
		"name":    t.Name,
		"path":    t.Path,
		"version": t.Version,
		"status":  t.Status,
	}
}

func chunkView(c control.LogChunk) map[string]any {
	return map[string]any{
		"seq":           c.Seq,
		"type":          c.Type,
		"stream":        c.Stream,
		"text":          c.Text,
		"head":          c.Head,
		"tail":          c.Tail,
		"dropped_bytes": c.DroppedBytes,
		"silent_for_ms": c.SilentForMs,
	}
}

func repoDetailView(d control.RepoDetail) map[string]any {
	var running any
	if d.Running != nil {
		running = map[string]any{
			"id":           d.Running.ID,
			"status":       d.Running.Status,
			"prompt_title": d.RunningTitle,
			"engine":       d.RunningEngine,
		}
	}
	schedules := make([]map[string]any, 0, len(d.Schedules))
	for _, s := range d.Schedules {
		schedules = append(schedules, scheduleView(s))
	}
	history := make([]map[string]any, 0, len(d.History))
	for _, h := range d.History {
		ids := h.ConversationIDs
		if ids == nil {
			ids = []string{}
		}
		history = append(history, map[string]any{
			"job_id":           h.JobID,
			"prompt_title":     h.PromptTitle,
			"engine":           h.Engine,
			"clean":            h.Clean,
			"ase_complete":     h.ASEComplete,
			"conversation_ids": ids,
		})
	}
	return map[string]any{
		"host_id":           d.Repo.HostID,
		"path":              d.Repo.WorktreePath,
		"queue":             d.Repo.Queue,
		"lock":              d.Repo.Lock,
		"reason":            d.Repo.Reason,
		"fetch_failed":      d.FetchFailed,
		"porcelain":         d.Repo.Porcelain,
		"docs_hub_path":     d.Repo.DocsHubPath,
		"docs_hub_all_idle": d.DocsHubAllIdle,
		"running_job":       running,
		"schedules":         schedules,
		"history":           history,
	}
}
