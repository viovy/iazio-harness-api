package dispatch

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
)

var (
	provenanceStoryIDRegex    = regexp.MustCompile(`(?i)\b(STORY-[A-Z]+-[0-9]{3,6})\b`)
	provenanceReviewPathRegex = regexp.MustCompile(`(?i)(?:^|[\s"'` + "`" + `(])(?:\S*/)?docs/reviews/\S+`)
)

// ExtractStoryIDFromPrompt extracts a canonical STORY-<DOMAIN>-<NNNN> identifier from text if present,
// ignoring STORY-* substrings that only appear inside docs/reviews/... artifact paths.
func ExtractStoryIDFromPrompt(text string) string {
	sanitized := provenanceReviewPathRegex.ReplaceAllString(text, " ")
	if m := provenanceStoryIDRegex.FindStringSubmatch(sanitized); len(m) > 1 {
		return strings.ToUpper(strings.TrimSpace(m[1]))
	}
	return ""
}

// ConversationJobProvenance binds a conversation ID (including child Phase 4 review
// sessions) to its parent ASE orchestrator job, story, harness, model, host, and repository.
type ConversationJobProvenance struct {
	ConversationID       string   `json:"conversation_id"`
	JobID                string   `json:"job_id"`
	StoryID              string   `json:"story_id,omitempty"`
	TargetAgent          string   `json:"target_agent,omitempty"`
	Harness              string   `json:"harness,omitempty"`
	Model                string   `json:"model,omitempty"`
	ModelAuthor          string   `json:"model_author,omitempty"`
	HostID               string   `json:"host_id,omitempty"`
	Repo                 string   `json:"repo,omitempty"`
	WorktreePath         string   `json:"worktree_path,omitempty"`
	ParentJobID          string   `json:"parent_job_id,omitempty"`
	RootJobID            string   `json:"root_job_id,omitempty"`
	IsChildReviewSession bool     `json:"is_child_review_session"`
	Status               string   `json:"status,omitempty"`
	ConversationIDs      []string `json:"conversation_ids,omitempty"`
}

// NormalizeHarnessName normalizes an agent/engine/tool identifier into a canonical
// harness value ("agy", "opencode", "cursor", "claude", "autopilot", "codex").
func NormalizeHarnessName(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" || s == "auto" || s == "global" || s == "default" {
		return ""
	}
	switch s {
	case "agy", "antigravity", "antigravity-cli", "gemini", "gemini-cli":
		return "agy"
	case "opencode", "open-code", "opencode-cli":
		return "opencode"
	case "cursor", "cursor-agent", "cursor-cli", "agent":
		return "cursor"
	case "claude", "claude-code", "claude-cli", "anthropic":
		return "claude"
	case "autopilot", "autopilot-cli", "ase":
		return "autopilot"
	case "codex", "codex-cli", "openai":
		return "codex"
	}
	switch {
	case strings.Contains(s, "antigravity") || strings.HasPrefix(s, "agy"):
		return "agy"
	case strings.Contains(s, "opencode") || strings.Contains(s, "open-code"):
		return "opencode"
	case strings.Contains(s, "cursor"):
		return "cursor"
	case strings.Contains(s, "claude"):
		return "claude"
	case strings.Contains(s, "autopilot"):
		return "autopilot"
	case strings.Contains(s, "codex"):
		return "codex"
	default:
		return s
	}
}

// DefaultModelForHarness returns the sensible default LLM model for a canonical harness.
func DefaultModelForHarness(harness string) string {
	switch NormalizeHarnessName(harness) {
	case "agy":
		return "gemini-2.5-pro"
	case "claude":
		return "claude-sonnet-4-20250514"
	case "codex":
		return "gpt-5-codex"
	case "cursor":
		return "claude-3-7-sonnet"
	case "opencode":
		return "claude-sonnet-4-20250514"
	case "autopilot":
		return "gemini-2.5-pro"
	default:
		return "gemini-2.5-pro"
	}
}

// InferModelAuthor derives the canonical model author/provider ("google", "anthropic",
// "openai", "deepseek", "qwen", "xai", etc.) from a model identifier and fallback harness.
func InferModelAuthor(model, harness string) string {
	m := strings.ToLower(strings.TrimSpace(model))
	if m != "" {
		switch {
		case strings.HasPrefix(m, "google/") || strings.HasPrefix(m, "vertex/") ||
			strings.Contains(m, "gemini") || strings.Contains(m, "gemma") || strings.Contains(m, "palm"):
			return "google"
		case strings.HasPrefix(m, "anthropic/") ||
			strings.Contains(m, "claude") || strings.Contains(m, "sonnet") ||
			strings.Contains(m, "opus") || strings.Contains(m, "haiku"):
			return "anthropic"
		case strings.HasPrefix(m, "openai/") ||
			strings.Contains(m, "gpt") || strings.Contains(m, "codex") || strings.Contains(m, "chatgpt") ||
			strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3") || strings.HasPrefix(m, "o4") ||
			strings.Contains(m, "/o1") || strings.Contains(m, "/o3") || strings.Contains(m, "/o4"):
			return "openai"
		case strings.HasPrefix(m, "deepseek/") || strings.Contains(m, "deepseek"):
			return "deepseek"
		case strings.HasPrefix(m, "qwen/") || strings.HasPrefix(m, "alibaba/") || strings.Contains(m, "qwen"):
			return "qwen"
		case strings.HasPrefix(m, "xai/") || strings.Contains(m, "grok"):
			return "xai"
		case strings.HasPrefix(m, "meta/") || strings.Contains(m, "llama"):
			return "meta"
		case strings.HasPrefix(m, "mistral/") || strings.Contains(m, "mistral") ||
			strings.Contains(m, "mixtral") || strings.Contains(m, "codestral"):
			return "mistral"
		}
		if idx := strings.Index(m, "/"); idx > 0 {
			prov := strings.TrimSpace(m[:idx])
			if prov != "" {
				return prov
			}
		}
	}

	switch NormalizeHarnessName(harness) {
	case "claude", "cursor", "opencode":
		return "anthropic"
	case "codex":
		return "openai"
	case "agy", "autopilot":
		return "google"
	default:
		return "google"
	}
}

func parseJSONMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func firstNonEmptyMapString(maps []map[string]any, keys ...string) string {
	for _, m := range maps {
		if m == nil {
			continue
		}
		for _, k := range keys {
			if v, ok := m[k].(string); ok {
				if trimmed := strings.TrimSpace(v); trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func isBareZipkinSpanID(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// JobMatchesConversationID reports whether job j matches convID via first-class fields
// or JSON keys in Payload / Result. It also returns whether the match is on a primary
// conversation field and whether the JSON match specifically indicates a child review conversation.
func JobMatchesConversationID(j *Job, convID string) (matched bool, isPrimary bool, isExplicitChildConv bool) {
	cid := strings.TrimSpace(convID)
	if j == nil || cid == "" {
		return false, false, false
	}

	for _, cand := range []string{
		j.ActiveConversationID,
		j.ConversationID,
		j.InitialConversationID,
		j.ResumeConversationID,
	} {
		if strings.TrimSpace(cand) == cid {
			return true, true, false
		}
	}

	for _, item := range j.ConversationIDs {
		if strings.TrimSpace(item) == cid {
			matched = true
		}
	}

	for _, blob := range []json.RawMessage{j.Result, j.Payload} {
		m := parseJSONMap(blob)
		if m == nil {
			continue
		}
		for _, k := range []string{"active_conversation_id", "conversation_id", "initial_conversation_id", "resume_conversation_id"} {
			if v, ok := m[k].(string); ok && strings.TrimSpace(v) == cid {
				return true, true, false
			}
		}
		for _, k := range []string{"child_conversation_id", "review_conversation_id"} {
			if v, ok := m[k].(string); ok && strings.TrimSpace(v) == cid {
				return true, true, true
			}
		}
		if arr, ok := m["conversation_ids"].([]any); ok {
			for _, item := range arr {
				if s, ok := item.(string); ok && strings.TrimSpace(s) == cid {
					matched = true
				}
			}
		}
		for _, k := range []string{"child_conversation_ids", "review_conversation_ids"} {
			if arr, ok := m[k].([]any); ok {
				for _, item := range arr {
					if s, ok := item.(string); ok && strings.TrimSpace(s) == cid {
						matched = true
						isExplicitChildConv = true
					}
				}
			}
		}
		if arr, ok := m["conversation_lineage"].([]any); ok {
			for _, item := range arr {
				if lm, ok := item.(map[string]any); ok {
					if v, ok := lm["conversation_id"].(string); ok && strings.TrimSpace(v) == cid {
						matched = true
						role, _ := lm["role"].(string)
						phase, _ := lm["ase_phase"].(string)
						reason, _ := lm["reason"].(string)
						if isLineageEntryChildReview(role, phase, reason, cid, j.InitialConversationID) {
							isExplicitChildConv = true
						}
					}
				}
			}
		}
	}

	return matched, false, isExplicitChildConv
}

func isLineageEntryChildReview(role, phase, reason, convID, initialConvID string) bool {
	rLower := strings.ToLower(strings.TrimSpace(role))
	pLower := strings.ToLower(strings.TrimSpace(phase))
	rsnLower := strings.ToLower(strings.TrimSpace(reason))

	if strings.Contains(rLower, "review") || strings.Contains(rLower, "child") || strings.Contains(rLower, "subagent") {
		return true
	}
	if strings.Contains(pLower, "review") || strings.Contains(pLower, "phase_4") || strings.Contains(pLower, "phase4") || strings.Contains(pLower, "phase 4") {
		return true
	}
	if strings.Contains(rsnLower, "review-scheme") || strings.Contains(rsnLower, "adversarial-reviewer") || strings.Contains(rsnLower, "docs/reviews/") || strings.Contains(rsnLower, "review") {
		return true
	}
	isSecondary := initialConvID != "" && strings.TrimSpace(convID) != "" && strings.TrimSpace(convID) != strings.TrimSpace(initialConvID)
	if isSecondary && rLower != "" && rLower != "initial_active" && rLower != "initial_resumed" && rLower != "root_initial" {
		if strings.Contains(rLower, "secondary") || strings.Contains(rLower, "unpinned") || pLower == "4" {
			return true
		}
	}
	return false
}

// TextIndicatesReviewSession checks whether prompt, action, role, or path text contains
// canonical Phase 4 review session markers.
func TextIndicatesReviewSession(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "" {
		return false
	}
	for _, marker := range []string{
		"review-scheme",
		"review_scheme",
		"adversarial-reviewer",
		"adversarial_reviewer",
		"docs/reviews/",
		"review_iteration",
		"review-iteration",
		"child_review",
		"child-review",
		"phase_4_review",
		"phase4_review",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func resolveJobParentID(j *Job, maps []map[string]any, jobExists func(string) bool) string {
	if j == nil {
		return ""
	}
	if pid := firstNonEmptyMapString(maps, "parent_job_id", "parent_id"); pid != "" && pid != j.ID {
		return pid
	}
	if dep := strings.TrimSpace(j.DependsOnJobID); dep != "" && dep != j.ID {
		return dep
	}
	for _, dep := range j.DependsOnJobIDs {
		if trimmed := strings.TrimSpace(dep); trimmed != "" && trimmed != j.ID {
			return trimmed
		}
	}
	if dep := firstNonEmptyMapString(maps, "depends_on_job_id"); dep != "" && dep != j.ID {
		return dep
	}
	if pid := strings.TrimSpace(j.ParentID); pid != "" && pid != j.ID {
		// Distinguish a real parent job ID from an unreferenced 16-hex-char Zipkin span ID
		if isBareZipkinSpanID(pid) {
			if jobExists != nil && jobExists(pid) {
				return pid
			}
			return ""
		}
		return pid
	}
	return ""
}

func extractJobModel(maps []map[string]any, harness string) string {
	model := firstNonEmptyMapString(maps, "model", "model_id", "llm_model", "review_model", "agent_model", "default_model")
	if model == "" {
		for _, m := range maps {
			if m == nil {
				continue
			}
			if envMap, ok := m["env"].(map[string]any); ok && envMap != nil {
				for _, k := range []string{"MODEL", "OPENCODE_MODEL", "ANTHROPIC_MODEL", "OPENAI_MODEL", "GEMINI_MODEL", "model"} {
					if v, ok := envMap[k].(string); ok && strings.TrimSpace(v) != "" {
						model = strings.TrimSpace(v)
						break
					}
				}
			}
			if model != "" {
				break
			}
		}
	}
	if model == "" {
		model = DefaultModelForHarness(harness)
	}
	return model
}

func detectJobChildReviewSession(j *Job, maps []map[string]any, parentJobID, convID string, isExplicitChildConv bool) bool {
	if isExplicitChildConv {
		return true
	}
	for _, m := range maps {
		if m == nil {
			continue
		}
		for _, bk := range []string{"is_child_review_session", "child_review", "is_review_session"} {
			if b, ok := m[bk].(bool); ok && b {
				return true
			}
			if s, ok := m[bk].(string); ok {
				sl := strings.ToLower(strings.TrimSpace(s))
				if sl == "true" || sl == "1" || sl == "yes" {
					return true
				}
			}
		}
		if revIter, exists := m["review_iteration"]; exists && revIter != nil {
			switch v := revIter.(type) {
			case float64:
				if v > 0 {
					return true
				}
			case int:
				if v > 0 {
					return true
				}
			case string:
				if strings.TrimSpace(v) != "" && strings.TrimSpace(v) != "0" {
					return true
				}
			}
		}
	}

	roleOrPhase := firstNonEmptyMapString(maps, "role", "agent_role", "subagent_role", "ase_phase", "phase", "review_scheme")
	if roleOrPhase != "" {
		rpLower := strings.ToLower(roleOrPhase)
		if TextIndicatesReviewSession(rpLower) ||
			strings.Contains(rpLower, "review") ||
			strings.Contains(rpLower, "phase_4") ||
			strings.Contains(rpLower, "phase4") ||
			strings.Contains(rpLower, "phase 4") {
			return true
		}
	}

	candidates := []string{
		j.NormalizedPrompt,
		j.EffectiveAction,
		j.SourceConversationTitle,
		j.Type,
		firstNonEmptyMapString(maps, "prompt", "normalized_prompt"),
		firstNonEmptyMapString(maps, "action", "effective_action", "command", "mode", "kind", "title"),
	}
	for _, c := range candidates {
		if TextIndicatesReviewSession(c) {
			return true
		}
		if parentJobID != "" && strings.Contains(strings.ToLower(c), "review") {
			return true
		}
	}

	if j.InitialConversationID != "" && convID != "" && convID != j.InitialConversationID {
		for _, c := range candidates {
			if strings.Contains(strings.ToLower(c), "review") {
				return true
			}
		}
	}

	return false
}

func collectJobConversationIDs(j *Job, maps []map[string]any, matchedConvID string) []string {
	var out []string
	seen := make(map[string]bool)
	add := func(id string) {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" || seen[trimmed] {
			return
		}
		if isSyntheticTestConversationID(trimmed) && trimmed != strings.TrimSpace(matchedConvID) {
			return
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}

	add(j.InitialConversationID)
	add(j.ResumeConversationID)
	add(j.ConversationID)
	add(j.ActiveConversationID)
	for _, cid := range j.ConversationIDs {
		add(cid)
	}
	for _, m := range maps {
		if m == nil {
			continue
		}
		for _, k := range []string{"initial_conversation_id", "resume_conversation_id", "conversation_id", "active_conversation_id", "child_conversation_id", "review_conversation_id"} {
			if v, ok := m[k].(string); ok {
				add(v)
			}
		}
		for _, k := range []string{"conversation_ids", "child_conversation_ids", "review_conversation_ids"} {
			if arr, ok := m[k].([]any); ok {
				for _, item := range arr {
					if s, ok := item.(string); ok {
						add(s)
					}
				}
			}
		}
	}
	add(matchedConvID)
	return out
}

// BuildJobProvenance constructs a ConversationJobProvenance record for a dispatch.Job,
// walking parent links via lookupJob when available to resolve RootJobID and inherited metadata.
func BuildJobProvenance(j *Job, convID string, isExplicitChildConv bool, lookupJob func(jobID string) *Job) ConversationJobProvenance {
	if j == nil {
		return ConversationJobProvenance{}
	}
	cid := strings.TrimSpace(convID)
	if cid == "" {
		cid = ResolveJobConversationID(j)
	}

	resultMap := parseJSONMap(j.Result)
	payloadMap := parseJSONMap(j.Payload)
	maps := []map[string]any{resultMap, payloadMap}

	jobExists := func(id string) bool {
		if lookupJob == nil {
			return false
		}
		return lookupJob(id) != nil
	}

	parentJobID := resolveJobParentID(j, maps, jobExists)
	explicitRootJobID := firstNonEmptyMapString(maps, "root_job_id")

	storyID := strings.TrimSpace(j.StoryID)
	if storyID == "" {
		storyID = firstNonEmptyMapString(maps, "story_id")
	}

	var inheritedStoryID, inheritedRepo, inheritedWorktree, inheritedHost string
	topAncestorID := ""
	if parentJobID != "" {
		visited := map[string]bool{j.ID: true}
		currID := parentJobID
		for currID != "" && !visited[currID] && len(visited) < 32 {
			visited[currID] = true
			topAncestorID = currID
			if lookupJob == nil {
				break
			}
			pJob := lookupJob(currID)
			if pJob == nil {
				break
			}
			pMaps := []map[string]any{parseJSONMap(pJob.Result), parseJSONMap(pJob.Payload)}
			if inheritedStoryID == "" {
				pStory := strings.TrimSpace(pJob.StoryID)
				if pStory == "" {
					pStory = firstNonEmptyMapString(pMaps, "story_id")
				}
				if pStory == "" {
					pStory = ExtractStoryIDFromPrompt(pJob.NormalizedPrompt)
				}
				if pStory != "" {
					inheritedStoryID = pStory
				}
			}
			if inheritedRepo == "" && strings.TrimSpace(pJob.Repo) != "" {
				inheritedRepo = strings.TrimSpace(pJob.Repo)
			}
			if inheritedWorktree == "" && strings.TrimSpace(pJob.WorktreePath) != "" {
				inheritedWorktree = strings.TrimSpace(pJob.WorktreePath)
			}
			if inheritedHost == "" {
				for _, hc := range []string{pJob.ClaimedHost, pJob.HostID, pJob.TargetHost} {
					hc = strings.TrimSpace(hc)
					if hc != "" && !strings.EqualFold(hc, "auto") && !strings.EqualFold(hc, "global") {
						inheritedHost = hc
						break
					}
				}
			}
			if explicitRootJobID == "" {
				if rID := firstNonEmptyMapString(pMaps, "root_job_id"); rID != "" {
					explicitRootJobID = rID
				}
			}
			currID = resolveJobParentID(pJob, pMaps, jobExists)
		}
	}

	rootJobID := explicitRootJobID
	if topAncestorID != "" && (rootJobID == "" || rootJobID == parentJobID) {
		rootJobID = topAncestorID
	}
	if rootJobID == "" {
		if parentJobID != "" {
			rootJobID = parentJobID
		} else {
			rootJobID = j.ID
		}
	}

	if storyID == "" && inheritedStoryID != "" {
		storyID = inheritedStoryID
	}
	if storyID == "" && rootJobID != "" && rootJobID != j.ID && lookupJob != nil {
		if rJob := lookupJob(rootJobID); rJob != nil && strings.TrimSpace(rJob.StoryID) != "" {
			storyID = strings.TrimSpace(rJob.StoryID)
		}
	}
	if storyID == "" {
		storyID = ExtractStoryIDFromPrompt(j.NormalizedPrompt)
	}
	if storyID == "" {
		storyID = ExtractStoryIDFromPrompt(firstNonEmptyMapString(maps, "prompt", "normalized_prompt", "title"))
	}

	rawAgent := strings.TrimSpace(j.TargetAgent)
	if rawAgent == "" || strings.EqualFold(rawAgent, "auto") {
		if fromMap := firstNonEmptyMapString(maps, "harness", "target_agent", "engine", "tool", "agent", "review_tool", "reviewer_tool"); fromMap != "" {
			rawAgent = fromMap
		} else if j.AgentAffinity != nil && strings.TrimSpace(j.AgentAffinity.AgentName) != "" {
			rawAgent = strings.TrimSpace(j.AgentAffinity.AgentName)
		}
	}
	harness := NormalizeHarnessName(rawAgent)
	if harness == "" {
		harness = NormalizeHarnessName(firstNonEmptyMapString(maps, "harness", "target_agent", "engine", "tool", "agent", "review_tool", "reviewer_tool"))
	}
	if harness == "" {
		harness = "agy"
	}
	targetAgent := strings.TrimSpace(j.TargetAgent)
	if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
		targetAgent = rawAgent
	}
	if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
		targetAgent = harness
	}

	model := extractJobModel(maps, harness)
	modelAuthor := InferModelAuthor(model, harness)

	var hostID string
	for _, hc := range []string{
		j.ClaimedHost,
		j.HostID,
		j.TargetHost,
		firstNonEmptyMapString(maps, "host_id", "claimed_host", "target_host", "host"),
	} {
		hc = strings.TrimSpace(hc)
		if hc != "" && !strings.EqualFold(hc, "auto") && !strings.EqualFold(hc, "global") {
			hostID = hc
			break
		}
	}
	if hostID == "" && j.AgentAffinity != nil && strings.TrimSpace(j.AgentAffinity.HostID) != "" {
		hostID = strings.TrimSpace(j.AgentAffinity.HostID)
	}
	if hostID == "" {
		hostID = inheritedHost
	}

	worktreePath := strings.TrimSpace(j.WorktreePath)
	if worktreePath == "" {
		worktreePath = firstNonEmptyMapString(maps, "worktree_path", "workspace")
	}
	if worktreePath == "" {
		worktreePath = inheritedWorktree
	}

	rawRepo := strings.TrimSpace(j.Repo)
	if rawRepo == "" || rawRepo == "." {
		if sub := firstNonEmptyMapString(maps, "submodule", "target_submodule", "repo"); sub != "" && sub != "." {
			rawRepo = sub
		}
	}
	if rawRepo == "" && inheritedRepo != "" {
		rawRepo = inheritedRepo
	}
	var repo string
	if rawRepo != "" {
		repo = NormalizeRepoName(rawRepo)
	} else if worktreePath != "" {
		repo = NormalizeRepoName(worktreePath)
	}

	isChildReview := detectJobChildReviewSession(j, maps, parentJobID, cid, isExplicitChildConv)
	convIDs := collectJobConversationIDs(j, maps, cid)

	return ConversationJobProvenance{
		ConversationID:       cid,
		JobID:                j.ID,
		StoryID:              storyID,
		TargetAgent:          targetAgent,
		Harness:              harness,
		Model:                model,
		ModelAuthor:          modelAuthor,
		HostID:               hostID,
		Repo:                 repo,
		WorktreePath:         worktreePath,
		ParentJobID:          parentJobID,
		RootJobID:            rootJobID,
		IsChildReviewSession: isChildReview,
		Status:               strings.TrimSpace(j.Status),
		ConversationIDs:      convIDs,
	}
}

// FindProvenanceByConversationID searches all jobs in InMemoryQueue for convID
// and resolves the complete ConversationJobProvenance.
func (q *InMemoryQueue) FindProvenanceByConversationID(convID string) (ConversationJobProvenance, bool) {
	cid := strings.TrimSpace(convID)
	if q == nil || cid == "" {
		return ConversationJobProvenance{}, false
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var bestJob *Job
	var bestPrimary bool
	var bestExplicitChild bool
	var bestHasParent bool

	lookupLocked := func(jobID string) *Job {
		return q.jobs[strings.TrimSpace(jobID)]
	}

	for _, j := range q.jobs {
		if j == nil {
			continue
		}
		matched, isPrimary, isExplicitChild := JobMatchesConversationID(j, cid)
		if !matched {
			continue
		}
		maps := []map[string]any{parseJSONMap(j.Result), parseJSONMap(j.Payload)}
		hasParent := resolveJobParentID(j, maps, func(id string) bool { return q.jobs[id] != nil }) != ""
		if bestJob == nil {
			bestJob = j
			bestPrimary = isPrimary
			bestExplicitChild = isExplicitChild
			bestHasParent = hasParent
			continue
		}
		// Prefer primary conversation match over secondary list match; on tie, prefer child job or newer job.
		if isPrimary && !bestPrimary {
			bestJob = j
			bestPrimary = isPrimary
			bestExplicitChild = isExplicitChild
			bestHasParent = hasParent
		} else if isPrimary == bestPrimary {
			if hasParent && !bestHasParent {
				bestJob = j
				bestPrimary = isPrimary
				bestExplicitChild = isExplicitChild
				bestHasParent = hasParent
			} else if hasParent == bestHasParent && j.CreatedAt.After(bestJob.CreatedAt) {
				bestJob = j
				bestPrimary = isPrimary
				bestExplicitChild = isExplicitChild
				bestHasParent = hasParent
			}
		}
	}

	if bestJob == nil {
		return ConversationJobProvenance{}, false
	}

	cp := cloneJobSnapshot(bestJob)
	return BuildJobProvenance(cp, cid, bestExplicitChild, func(jobID string) *Job {
		if target := lookupLocked(jobID); target != nil {
			return cloneJobSnapshot(target)
		}
		return nil
	}), true
}

// ResolveQueueProvenanceByConversationID resolves ConversationJobProvenance from any Queue implementation.
func ResolveQueueProvenanceByConversationID(ctx context.Context, q Queue, convID string) (ConversationJobProvenance, bool) {
	cid := strings.TrimSpace(convID)
	if q == nil || cid == "" {
		return ConversationJobProvenance{}, false
	}
	if finder, ok := q.(interface {
		FindProvenanceByConversationID(convID string) (ConversationJobProvenance, bool)
	}); ok {
		return finder.FindProvenanceByConversationID(cid)
	}

	jobs, err := q.List(ctx, JobFilter{Limit: 1000})
	if err != nil {
		return ConversationJobProvenance{}, false
	}
	var bestJob *Job
	var bestPrimary bool
	var bestExplicitChild bool
	for _, j := range jobs {
		if j == nil {
			continue
		}
		matched, isPrimary, isExplicitChild := JobMatchesConversationID(j, cid)
		if !matched {
			continue
		}
		if bestJob == nil || (isPrimary && !bestPrimary) || (isPrimary == bestPrimary && j.CreatedAt.After(bestJob.CreatedAt)) {
			bestJob = j
			bestPrimary = isPrimary
			bestExplicitChild = isExplicitChild
		}
	}
	if bestJob == nil {
		return ConversationJobProvenance{}, false
	}
	return BuildJobProvenance(bestJob, cid, bestExplicitChild, func(jobID string) *Job {
		if target, ok := q.Get(jobID); ok {
			return target
		}
		return nil
	}), true
}
