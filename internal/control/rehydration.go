package control

import (
	"fmt"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

// ToolActionSummary captures a structured action/outcome from the prior conversation.
type ToolActionSummary struct {
	Step    int    `json:"step"`
	Tool    string `json:"tool"`
	Action  string `json:"action"`
	Summary string `json:"summary"`
	Status  string `json:"status"`
}

// RehydrationBundle contains the synthesized context necessary to bootstrap a fallback conversation.
type RehydrationBundle struct {
	ConversationID         string              `json:"conversation_id"`
	JobID                  string              `json:"job_id,omitempty"`
	StoryID                string              `json:"story_id,omitempty"`
	StorySpecification     string              `json:"story_specification,omitempty"`
	AcceptanceCriteria     []string            `json:"acceptance_criteria,omitempty"`
	CompletedPhases        []string            `json:"completed_phases,omitempty"`
	InterruptedPhase       string              `json:"interrupted_phase,omitempty"`
	InterruptedStep        string              `json:"interrupted_step,omitempty"`
	RemainingChecklist     []string            `json:"remaining_checklist,omitempty"`
	LastToolActions        []ToolActionSummary         `json:"last_tool_actions,omitempty"`
	WorkspaceStateManifest map[string]any              `json:"workspace_state_manifest,omitempty"`
	RunComparison          *dispatch.RunComparisonDiff `json:"run_comparison,omitempty"`
	Phase4ReviewContext    string                      `json:"phase4_review_context,omitempty"`
	BootstrapPrompt        string                      `json:"bootstrap_prompt"`
	GeneratedAt            time.Time                   `json:"generated_at"`
}

// GetRehydrationBundle synthesizes the recovery state bundle for conversationID
// from active jobs, historical executions, spooled log chunks, and ASE state.
func (e *Engine) GetRehydrationBundle(convID string) (RehydrationBundle, error) {
	convID = strings.TrimSpace(convID)
	if convID == "" {
		return RehydrationBundle{}, fmt.Errorf("conversation_id is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	var matchedJobID string
	var scheduleID string
	var interruptedState map[string]any
	var workspaceManifest map[string]any
	var logs []LogChunk

	// 1. Search active jobs
	for id, j := range e.jobs {
		if j.InitialConversationID == convID || j.ActiveConversationID == convID || j.ResumeConversationID == convID {
			matchedJobID = id
			scheduleID = j.ScheduleID
			interruptedState = j.InterruptedASEState
			workspaceManifest = j.WorkspaceStateManifest
			logs = e.logs[id]
			break
		}
		for _, c := range j.ConversationIDs {
			if c == convID {
				matchedJobID = id
				scheduleID = j.ScheduleID
				interruptedState = j.InterruptedASEState
				workspaceManifest = j.WorkspaceStateManifest
				logs = e.logs[id]
				break
			}
		}
		if matchedJobID != "" {
			break
		}
	}

	// 2. Search history if not active
	if matchedJobID == "" {
		for _, repoHist := range e.history {
			for _, row := range repoHist {
				if row.InitialConversationID == convID || row.ActiveConversationID == convID {
					matchedJobID = row.JobID
					scheduleID = row.ScheduleID
					interruptedState = row.InterruptedASEState
					workspaceManifest = row.WorkspaceStateManifest
					logs = e.logs[row.JobID]
					break
				}
				for _, c := range row.ConversationIDs {
					if c == convID {
						matchedJobID = row.JobID
						scheduleID = row.ScheduleID
						interruptedState = row.InterruptedASEState
						workspaceManifest = row.WorkspaceStateManifest
						logs = e.logs[row.JobID]
						break
					}
				}
				if matchedJobID != "" {
					break
				}
			}
			if matchedJobID != "" {
				break
			}
		}
	}

	// If no prior job was bound to this conversation, return minimal valid bundle
	if matchedJobID == "" {
		return RehydrationBundle{
			ConversationID: convID,
			BootstrapPrompt: fmt.Sprintf("[CANONICAL TRANSCRIPT REHYDRATION BOOTSTRAP]\nPrior Conversation: %s\nResume execution from current repository state.\n",
				convID),
			GeneratedAt: e.now().UTC(),
		}, nil
	}

	var storyID string
	var prompt string
	if scheduleID != "" {
		if sch, ok := e.schedules[scheduleID]; ok {
			storyID = sch.StoryID
			prompt = sch.CustomPrompt
			if prompt == "" && sch.PromptID != "" {
				if pr, ok := e.prompts[sch.PromptID]; ok {
					prompt = pr.Body
				}
			}
		}
	}

	// 3. Extract Acceptance Criteria from prompt
	acceptanceCriteria := make([]string, 0)
	if prompt != "" {
		lines := strings.Split(prompt, "\n")
		for _, l := range lines {
			trimmed := strings.TrimSpace(l)
			if strings.HasPrefix(trimmed, "- [ ]") || strings.HasPrefix(trimmed, "- [x]") || strings.HasPrefix(trimmed, "* [ ]") {
				acceptanceCriteria = append(acceptanceCriteria, trimmed)
			}
		}
	}

	// 4. Extract Completed Phases and Interrupted State
	completedPhases := make([]string, 0)
	interruptedPhase := "UNKNOWN"
	interruptedStep := "0"
	remainingChecklist := make([]string, 0)

	if interruptedState != nil {
		if p, ok := interruptedState["phase"].(string); ok && p != "" {
			interruptedPhase = p
		}
		if s := fmt.Sprintf("%v", interruptedState["step"]); s != "" && s != "<nil>" {
			interruptedStep = s
		}
		if rawCP, ok := interruptedState["completed_phases"].([]string); ok {
			completedPhases = append(completedPhases, rawCP...)
		} else if rawAny, ok := interruptedState["completed_phases"].([]any); ok {
			for _, cp := range rawAny {
				completedPhases = append(completedPhases, fmt.Sprintf("%v", cp))
			}
		} else {
			// Inferred baseline completed phases based on interrupted phase
			switch strings.ToUpper(interruptedPhase) {
			case "IMPLEMENTATION", "PHASE_2_TDD", "TDD":
				completedPhases = append(completedPhases, "PHASE_0_STORY_BIND", "PHASE_1_ARCHITECTURE_DISCOVERY")
			case "VERIFICATION", "PHASE_3_GATES":
				completedPhases = append(completedPhases, "PHASE_0_STORY_BIND", "PHASE_1_ARCHITECTURE_DISCOVERY", "PHASE_2_TDD_IMPLEMENTATION")
			case "REVIEW", "PHASE_4_REVIEW_SCHEME":
				completedPhases = append(completedPhases, "PHASE_0_STORY_BIND", "PHASE_1_ARCHITECTURE_DISCOVERY", "PHASE_2_TDD_IMPLEMENTATION", "PHASE_3_VALIDATE_LOCAL")
			case "GIT_FLOW", "PHASE_5_MERGE":
				completedPhases = append(completedPhases, "PHASE_0_STORY_BIND", "PHASE_1_ARCHITECTURE_DISCOVERY", "PHASE_2_TDD_IMPLEMENTATION", "PHASE_3_VALIDATE_LOCAL", "PHASE_4_REVIEW_SCHEME")
			}
		}

		if rawUnmet, ok := interruptedState["unmet_gates"].([]string); ok {
			remainingChecklist = append(remainingChecklist, rawUnmet...)
		} else if rawAny, ok := interruptedState["unmet_gates"].([]any); ok {
			for _, ug := range rawAny {
				remainingChecklist = append(remainingChecklist, fmt.Sprintf("%v", ug))
			}
		}
	}

	if len(remainingChecklist) == 0 && len(acceptanceCriteria) > 0 {
		remainingChecklist = append(remainingChecklist, acceptanceCriteria...)
	}

	// 5. Extract Last 15 Tool Actions from logs
	toolActions := make([]ToolActionSummary, 0, 15)
	if len(logs) > 0 {
		stepIdx := 1
		for i := len(logs) - 1; i >= 0 && len(toolActions) < 15; i-- {
			chunk := logs[i]
			text := strings.TrimSpace(chunk.Text)
			if text == "" {
				continue
			}
			lower := strings.ToLower(text)
			toolName := "system"
			action := "execute"
			if strings.Contains(lower, "git") {
				toolName = "git"
			} else if strings.Contains(lower, "go test") || strings.Contains(lower, "test") {
				toolName = "test-runner"
			} else if strings.Contains(lower, "make") {
				toolName = "make"
			} else if strings.Contains(lower, "curl") || strings.Contains(lower, "http") {
				toolName = "network"
			} else if strings.Contains(lower, "file") || strings.Contains(lower, "edit") {
				toolName = "editor"
			}

			summary := text
			if len(summary) > 200 {
				summary = summary[:200] + "..."
			}

			status := "SUCCESS"
			if strings.Contains(lower, "error") || strings.Contains(lower, "fail") || strings.Contains(lower, "fatal") {
				status = "FAILED"
			}

			toolActions = append([]ToolActionSummary{{
				Step:    stepIdx,
				Tool:    toolName,
				Action:  action,
				Summary: summary,
				Status:  status,
			}}, toolActions...)
			stepIdx++
		}
	}

	// 6. Build Deterministic Bootstrap Prompt
	var sb strings.Builder
	sb.WriteString("================================================================================\n")
	sb.WriteString("[CANONICAL TRANSCRIPT REHYDRATION BOOTSTRAP]\n")
	sb.WriteString(fmt.Sprintf("Prior Conversation: %s (Job: %s, Story: %s)\n", convID, matchedJobID, storyID))
	sb.WriteString(fmt.Sprintf("Interrupted ASE Phase: %s | Interrupted Step: %s\n\n", interruptedPhase, interruptedStep))

	if len(completedPhases) > 0 {
		sb.WriteString("COMPLETED ASE PHASES:\n")
		for _, cp := range completedPhases {
			sb.WriteString(fmt.Sprintf("  - %s\n", cp))
		}
		sb.WriteString("\n")
	}

	if len(remainingChecklist) > 0 {
		sb.WriteString("REMAINING DEFINITION OF DONE CHECKLIST / UNMET GATES:\n")
		for _, item := range remainingChecklist {
			sb.WriteString(fmt.Sprintf("  - %s\n", item))
		}
		sb.WriteString("\n")
	}

	if len(toolActions) > 0 {
		sb.WriteString(fmt.Sprintf("LAST %d STRUCTURED TOOL ACTIONS & OUTCOMES:\n", len(toolActions)))
		for idx, ta := range toolActions {
			sb.WriteString(fmt.Sprintf("  %d. [%s:%s] %s -> %s\n", idx+1, ta.Tool, ta.Status, ta.Summary, ta.Status))
		}
		sb.WriteString("\n")
	}

	if workspaceManifest != nil && len(workspaceManifest) > 0 {
		sb.WriteString("WORKSPACE STATE MANIFEST:\n")
		if b, ok := workspaceManifest["branch"].(string); ok && b != "" {
			sb.WriteString(fmt.Sprintf("  - Branch: %s\n", b))
		}
		if h, ok := workspaceManifest["head_sha"].(string); ok && h != "" {
			sb.WriteString(fmt.Sprintf("  - HEAD SHA: %s\n", h))
		}
		if pf, ok := workspaceManifest["preserved_files_count"].(int); ok {
			sb.WriteString(fmt.Sprintf("  - Preserved Uncommitted Files: %d\n", pf))
		}
		if mf, ok := workspaceManifest["modified_files"].([]string); ok && len(mf) > 0 {
			sb.WriteString(fmt.Sprintf("  - Modified Files: %s\n", strings.Join(mf, ", ")))
		}
		sb.WriteString("\n")
	}

	if prompt != "" {
		sb.WriteString("ORIGINAL SPECIFICATION:\n")
		sb.WriteString(prompt)
		sb.WriteString("\n\n")
	}

	var runComparison *dispatch.RunComparisonDiff
	if matchedJobID != "" {
		if cj, ok := e.jobs[matchedJobID]; ok && cj != nil {
			synthJob := &dispatch.Job{
				ID:                   cj.ID,
				StoryID:              storyID,
				Status:               strings.ToLower(cj.Status),
				Attempts:             cj.ExecutionAttempt,
				FailureCategory:      cj.FailureCategory,
				FailureStage:         cj.FailureStage,
				FailureReasonSummary: cj.FailureReasonSummary,
				Error:                cj.Reason,
				AttemptHistory:       cj.AttemptHistory,
				TraceID:              cj.TraceID,
				ZipkinTraceID:        cj.ZipkinTraceID,
			}
			runComparison = dispatch.CompareJobWithBaseline(synthJob, nil, nil, nil)
		} else {
			for _, repoHist := range e.history {
				for _, row := range repoHist {
					if row.JobID == matchedJobID {
						synthJob := &dispatch.Job{
							ID:                   row.JobID,
							StoryID:              storyID,
							Status:               strings.ToLower(row.Status),
							Attempts:             row.ExecutionAttempt,
							FailureCategory:      row.FailureCategory,
							FailureStage:         row.FailureStage,
							FailureReasonSummary: row.FailureReasonSummary,
							Error:                row.Reason,
							AttemptHistory:       row.AttemptHistory,
							TraceID:              row.TraceID,
							ZipkinTraceID:        row.ZipkinTraceID,
						}
						runComparison = dispatch.CompareJobWithBaseline(synthJob, nil, nil, nil)
						break
					}
				}
				if runComparison != nil {
					break
				}
			}
		}
	}
	if runComparison != nil && runComparison.Phase4ReviewContext != "" {
		sb.WriteString(runComparison.Phase4ReviewContext)
		sb.WriteString("\n\n")
	}

	sb.WriteString("CONTINUATION DIRECTIVE:\n")
	sb.WriteString(fmt.Sprintf("You are resuming execution on a fallback conversation after session loss on %s.\n", convID))
	sb.WriteString(fmt.Sprintf("Do NOT re-execute already completed phases. Continue directly from Phase: %s (Step: %s).\n", interruptedPhase, interruptedStep))
	sb.WriteString("================================================================================\n")

	var phase4ReviewContext string
	if runComparison != nil {
		phase4ReviewContext = runComparison.Phase4ReviewContext
	}

	return RehydrationBundle{
		ConversationID:         convID,
		JobID:                  matchedJobID,
		StoryID:                storyID,
		StorySpecification:     prompt,
		AcceptanceCriteria:     acceptanceCriteria,
		CompletedPhases:        completedPhases,
		InterruptedPhase:       interruptedPhase,
		InterruptedStep:        interruptedStep,
		RemainingChecklist:     remainingChecklist,
		LastToolActions:        toolActions,
		WorkspaceStateManifest: workspaceManifest,
		RunComparison:          runComparison,
		Phase4ReviewContext:    phase4ReviewContext,
		BootstrapPrompt:        sb.String(),
		GeneratedAt:            e.now().UTC(),
	}, nil
}

func isLineageNodeReviewSession(node ConversationLineage, convID, initialConvID string) bool {
	rLower := strings.ToLower(strings.TrimSpace(node.Role))
	pLower := strings.ToLower(strings.TrimSpace(node.ASEPhase))
	rsnLower := strings.ToLower(strings.TrimSpace(node.Reason))
	cliLower := strings.ToLower(strings.TrimSpace(node.CLIArgs))

	if strings.Contains(rLower, "review") || strings.Contains(rLower, "child") || strings.Contains(rLower, "subagent") {
		return true
	}
	if strings.Contains(pLower, "review") || strings.Contains(pLower, "phase_4") || strings.Contains(pLower, "phase4") || strings.Contains(pLower, "phase 4") {
		return true
	}
	for _, s := range []string{rsnLower, cliLower} {
		if dispatch.TextIndicatesReviewSession(s) || strings.Contains(s, "review") {
			return true
		}
	}
	isSecondary := strings.TrimSpace(initialConvID) != "" && strings.TrimSpace(convID) != "" && strings.TrimSpace(convID) != strings.TrimSpace(initialConvID)
	if isSecondary && rLower != "" && rLower != "initial_active" && rLower != "initial_resumed" && rLower != "root_initial" {
		if strings.Contains(rLower, "secondary") || strings.Contains(rLower, "unpinned") || pLower == "4" {
			return true
		}
	}
	return false
}

func dedupeConversationList(initial, active string, list []string, lineage []ConversationLineage, matchedConvID string) []string {
	var out []string
	seen := make(map[string]bool)
	add := func(s string) {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" || seen[trimmed] {
			return
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	add(initial)
	add(active)
	for _, c := range list {
		add(c)
	}
	for _, lin := range lineage {
		add(lin.ConversationID)
	}
	add(matchedConvID)
	return out
}

func (e *Engine) findJobRecordLocked(jobID string) (storyID, parentJobID, rootJobID, hostID, worktreePath, engine, model, promptText string, found bool) {
	id := strings.TrimSpace(jobID)
	if id == "" {
		return "", "", "", "", "", "", "", "", false
	}
	if j, ok := e.jobs[id]; ok && j != nil {
		storyID = strings.TrimSpace(j.StoryID)
		parentJobID = strings.TrimSpace(j.ParentJobID)
		rootJobID = strings.TrimSpace(j.RootJobID)
		hostID = strings.TrimSpace(j.HostID)
		if hostID == "" {
			hostID = strings.TrimSpace(j.TargetHost)
		}
		promptText = strings.TrimSpace(j.NormalizedPrompt)
		if sch := e.schedules[j.ScheduleID]; sch != nil {
			if storyID == "" {
				storyID = strings.TrimSpace(sch.StoryID)
			}
			if parentJobID == "" {
				parentJobID = strings.TrimSpace(sch.ParentJobID)
				if parentJobID == "" {
					parentJobID = strings.TrimSpace(sch.DependsOnJobID)
				}
			}
			if rootJobID == "" {
				rootJobID = strings.TrimSpace(sch.RootJobID)
			}
			if hostID == "" {
				hostID = strings.TrimSpace(sch.HostID)
			}
			worktreePath = strings.TrimSpace(sch.WorktreePath)
			engine = strings.TrimSpace(sch.Engine)
			if promptText == "" {
				promptText = strings.TrimSpace(sch.CustomPrompt)
				if promptText == "" {
					promptText = strings.TrimSpace(sch.PromptTitle)
				}
			}
			if sch.EnvVars != nil {
				for _, k := range []string{"MODEL", "OPENCODE_MODEL", "ANTHROPIC_MODEL", "OPENAI_MODEL", "GEMINI_MODEL"} {
					if v := strings.TrimSpace(sch.EnvVars[k]); v != "" {
						model = v
						break
					}
				}
			}
		}
		if storyID == "" {
			storyID = extractStoryIDFromString(promptText)
		}
		return storyID, parentJobID, rootJobID, hostID, worktreePath, engine, model, promptText, true
	}

	for key, rows := range e.history {
		h, p := "", ""
		if idx := strings.Index(key, "\x00"); idx >= 0 {
			h, p = key[:idx], key[idx+1:]
		}
		for _, r := range rows {
			if r.JobID == id {
				sID := strings.TrimSpace(r.StoryID)
				if sID == "" {
					sID = extractStoryIDFromString(r.PromptTitle)
				}
				return sID, strings.TrimSpace(r.ParentJobID), strings.TrimSpace(r.RootJobID), h, p, strings.TrimSpace(r.Engine), strings.TrimSpace(r.Model), strings.TrimSpace(r.PromptTitle), true
			}
		}
	}

	if sch, ok := e.schedules[id]; ok && sch != nil {
		sID := strings.TrimSpace(sch.StoryID)
		if sID == "" {
			sID = extractStoryIDFromString(sch.PromptTitle)
		}
		pID := strings.TrimSpace(sch.ParentJobID)
		if pID == "" {
			pID = strings.TrimSpace(sch.DependsOnJobID)
		}
		return sID, pID, strings.TrimSpace(sch.RootJobID), strings.TrimSpace(sch.HostID), strings.TrimSpace(sch.WorktreePath), strings.TrimSpace(sch.Engine), "", strings.TrimSpace(sch.CustomPrompt), true
	}

	return "", "", "", "", "", "", "", "", false
}

// FindJobMetadataByID returns job lineage and story metadata from Engine active jobs, history, or schedules.
func (e *Engine) FindJobMetadataByID(jobID string) (storyID, parentJobID, rootJobID, hostID, repo, worktreePath string, found bool) {
	if e == nil || strings.TrimSpace(jobID) == "" {
		return "", "", "", "", "", "", false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	storyID, parentJobID, rootJobID, hostID, worktreePath, _, _, _, found = e.findJobRecordLocked(jobID)
	if !found {
		return "", "", "", "", "", "", false
	}
	storyID, parentJobID, rootJobID, hostID, worktreePath = e.walkAncestorsLocked(jobID, storyID, parentJobID, rootJobID, hostID, worktreePath)
	if worktreePath != "" {
		repo = dispatch.NormalizeRepoName(worktreePath)
	}
	return storyID, parentJobID, rootJobID, hostID, repo, worktreePath, true
}

func (e *Engine) walkAncestorsLocked(jobID, storyID, parentJobID, rootJobID, hostID, worktreePath string) (string, string, string, string, string) {
	topAncestorID := ""
	if parentJobID != "" {
		visited := map[string]bool{jobID: true}
		currID := parentJobID
		for currID != "" && !visited[currID] && len(visited) < 32 {
			visited[currID] = true
			topAncestorID = currID
			pStory, nextParent, pRoot, pHost, pWT, _, _, _, ok := e.findJobRecordLocked(currID)
			if !ok {
				break
			}
			if storyID == "" && pStory != "" {
				storyID = pStory
			}
			if hostID == "" && pHost != "" {
				hostID = pHost
			}
			if worktreePath == "" && pWT != "" {
				worktreePath = pWT
			}
			if rootJobID == "" && pRoot != "" {
				rootJobID = pRoot
			}
			currID = nextParent
		}
	}
	if topAncestorID != "" && (rootJobID == "" || rootJobID == parentJobID) {
		rootJobID = topAncestorID
	}
	if rootJobID == "" {
		if parentJobID != "" {
			rootJobID = parentJobID
		} else {
			rootJobID = jobID
		}
	}
	if storyID == "" && rootJobID != "" && rootJobID != jobID {
		if rStory, _, _, _, _, _, _, _, ok := e.findJobRecordLocked(rootJobID); ok && rStory != "" {
			storyID = rStory
		}
	}
	return storyID, parentJobID, rootJobID, hostID, worktreePath
}

// FindProvenanceByConversationID inspects Engine active jobs, history rows, schedules,
// and repo runtime state to resolve ConversationJobProvenance for convID.
func (e *Engine) FindProvenanceByConversationID(convID string) (dispatch.ConversationJobProvenance, bool) {
	cid := strings.TrimSpace(convID)
	if e == nil || cid == "" {
		return dispatch.ConversationJobProvenance{}, false
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Search history rows (preferring primary or explicit lineage matches, newest first)
	var matchedRow *HistoryRow
	var matchedHost, matchedPath string
	var matchedPrimary, matchedLineageReview bool

	for key, rows := range e.history {
		h, p := "", ""
		if idx := strings.Index(key, "\x00"); idx >= 0 {
			h, p = key[:idx], key[idx+1:]
		}
		for i := range rows {
			r := &rows[i]
			isPrimary := strings.TrimSpace(r.ActiveConversationID) == cid || strings.TrimSpace(r.InitialConversationID) == cid
			foundInRow := isPrimary
			if !foundInRow {
				for _, c := range r.ConversationIDs {
					if strings.TrimSpace(c) == cid {
						foundInRow = true
						break
					}
				}
			}
			lineageReview := false
			for _, lin := range r.ConversationLineage {
				if strings.TrimSpace(lin.ConversationID) == cid {
					foundInRow = true
					if isLineageNodeReviewSession(lin, cid, r.InitialConversationID) {
						lineageReview = true
					}
				}
			}
			if !foundInRow {
				continue
			}
			if matchedRow == nil || (isPrimary && !matchedPrimary) || (isPrimary == matchedPrimary && r.CreatedAt.After(matchedRow.CreatedAt)) {
				matchedRow = r
				matchedHost = h
				matchedPath = p
				matchedPrimary = isPrimary
				matchedLineageReview = lineageReview
			}
		}
	}

	if matchedRow != nil {
		parentJobID := strings.TrimSpace(matchedRow.ParentJobID)
		if parentJobID == "" && len(matchedRow.ExecutionChain) > 1 {
			for i, chID := range matchedRow.ExecutionChain {
				if chID == matchedRow.JobID && i > 0 {
					parentJobID = strings.TrimSpace(matchedRow.ExecutionChain[i-1])
					break
				}
			}
		}
		rootJobID := strings.TrimSpace(matchedRow.RootJobID)
		storyID := strings.TrimSpace(matchedRow.StoryID)
		if storyID == "" {
			storyID = extractStoryIDFromString(matchedRow.PromptTitle)
		}
		promptText := matchedRow.PromptTitle
		if sch := e.schedules[matchedRow.ScheduleID]; sch != nil {
			if storyID == "" && sch.StoryID != "" {
				storyID = strings.TrimSpace(sch.StoryID)
			}
			if parentJobID == "" {
				parentJobID = strings.TrimSpace(sch.ParentJobID)
				if parentJobID == "" {
					parentJobID = strings.TrimSpace(sch.DependsOnJobID)
				}
			}
			if rootJobID == "" && sch.RootJobID != "" {
				rootJobID = strings.TrimSpace(sch.RootJobID)
			}
			if sch.CustomPrompt != "" {
				promptText = sch.CustomPrompt
			}
		}

		storyID, parentJobID, rootJobID, matchedHost, matchedPath = e.walkAncestorsLocked(
			matchedRow.JobID, storyID, parentJobID, rootJobID, matchedHost, matchedPath,
		)

		isChildReview := matchedLineageReview ||
			dispatch.TextIndicatesReviewSession(matchedRow.PromptTitle) ||
			dispatch.TextIndicatesReviewSession(promptText) ||
			dispatch.TextIndicatesReviewSession(matchedRow.Reason) ||
			(parentJobID != "" && (strings.Contains(strings.ToLower(matchedRow.PromptTitle), "review") ||
				strings.Contains(strings.ToLower(promptText), "review") ||
				strings.Contains(strings.ToLower(matchedRow.Reason), "review")))

		if matchedLineageReview && parentJobID == "" && strings.TrimSpace(matchedRow.InitialConversationID) != "" && cid != strings.TrimSpace(matchedRow.InitialConversationID) {
			parentJobID = matchedRow.JobID
		}

		rawAgent := strings.TrimSpace(matchedRow.Engine)
		harness := dispatch.NormalizeHarnessName(rawAgent)
		if harness == "" {
			harness = "agy"
		}
		targetAgent := rawAgent
		if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
			targetAgent = harness
		}
		model := strings.TrimSpace(matchedRow.Model)
		if model == "" {
			model = dispatch.DefaultModelForHarness(harness)
		}
		modelAuthor := dispatch.InferModelAuthor(model, harness)

		var repo string
		if matchedPath != "" {
			repo = dispatch.NormalizeRepoName(matchedPath)
		}

		convIDs := dedupeConversationList(
			matchedRow.InitialConversationID,
			matchedRow.ActiveConversationID,
			matchedRow.ConversationIDs,
			matchedRow.ConversationLineage,
			cid,
		)

		return dispatch.ConversationJobProvenance{
			ConversationID:       cid,
			JobID:                matchedRow.JobID,
			StoryID:              storyID,
			TargetAgent:          targetAgent,
			Harness:              harness,
			Model:                model,
			ModelAuthor:          modelAuthor,
			HostID:               matchedHost,
			Repo:                 repo,
			WorktreePath:         matchedPath,
			ParentJobID:          parentJobID,
			RootJobID:            rootJobID,
			IsChildReviewSession: isChildReview,
			Status:               strings.ToLower(strings.TrimSpace(matchedRow.Status)),
			ConversationIDs:      convIDs,
		}, true
	}

	// 2. Search active control.Job entries
	for id, j := range e.jobs {
		if j == nil {
			continue
		}
		foundInJob := strings.TrimSpace(j.ActiveConversationID) == cid ||
			strings.TrimSpace(j.InitialConversationID) == cid ||
			strings.TrimSpace(j.ResumeConversationID) == cid
		if !foundInJob {
			for _, c := range j.ConversationIDs {
				if strings.TrimSpace(c) == cid {
					foundInJob = true
					break
				}
			}
		}
		lineageReview := false
		for _, lin := range j.ConversationLineage {
			if strings.TrimSpace(lin.ConversationID) == cid {
				foundInJob = true
				if isLineageNodeReviewSession(lin, cid, j.InitialConversationID) {
					lineageReview = true
				}
			}
		}
		if !foundInJob {
			continue
		}

		storyID, parentJobID, rootJobID, hostID, worktreePath, engine, model, promptText, _ := e.findJobRecordLocked(id)
		storyID, parentJobID, rootJobID, hostID, worktreePath = e.walkAncestorsLocked(id, storyID, parentJobID, rootJobID, hostID, worktreePath)

		isChildReview := lineageReview ||
			dispatch.TextIndicatesReviewSession(j.NormalizedPrompt) ||
			dispatch.TextIndicatesReviewSession(j.EffectiveAction) ||
			dispatch.TextIndicatesReviewSession(promptText) ||
			dispatch.TextIndicatesReviewSession(j.Reason) ||
			(parentJobID != "" && (strings.Contains(strings.ToLower(j.NormalizedPrompt), "review") ||
				strings.Contains(strings.ToLower(promptText), "review")))

		if lineageReview && parentJobID == "" && strings.TrimSpace(j.InitialConversationID) != "" && cid != strings.TrimSpace(j.InitialConversationID) {
			parentJobID = j.ID
		}

		harness := dispatch.NormalizeHarnessName(engine)
		if harness == "" {
			harness = "agy"
		}
		targetAgent := engine
		if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
			targetAgent = harness
		}
		if model == "" {
			model = dispatch.DefaultModelForHarness(harness)
		}
		modelAuthor := dispatch.InferModelAuthor(model, harness)

		var repo string
		if worktreePath != "" {
			repo = dispatch.NormalizeRepoName(worktreePath)
		}

		convIDs := dedupeConversationList(j.InitialConversationID, j.ActiveConversationID, j.ConversationIDs, j.ConversationLineage, cid)
		return dispatch.ConversationJobProvenance{
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
			Status:               strings.ToLower(strings.TrimSpace(j.Status)),
			ConversationIDs:      convIDs,
		}, true
	}

	// 3. Search schedules
	for _, sch := range e.schedules {
		if sch == nil {
			continue
		}
		if strings.TrimSpace(sch.ActiveConversationID) != cid &&
			strings.TrimSpace(sch.InitialConversationID) != cid &&
			strings.TrimSpace(sch.ResumeConversationID) != cid {
			continue
		}
		jobID := sch.ID
		if len(sch.AssociatedJobs) > 0 && strings.TrimSpace(sch.AssociatedJobs[len(sch.AssociatedJobs)-1]) != "" {
			jobID = strings.TrimSpace(sch.AssociatedJobs[len(sch.AssociatedJobs)-1])
		} else if strings.HasPrefix(sch.ID, "sch-") {
			jobID = "job-" + strings.TrimPrefix(sch.ID, "sch-")
		}
		storyID := strings.TrimSpace(sch.StoryID)
		if storyID == "" {
			storyID = extractStoryIDFromString(sch.PromptTitle)
		}
		parentJobID := strings.TrimSpace(sch.ParentJobID)
		if parentJobID == "" {
			parentJobID = strings.TrimSpace(sch.DependsOnJobID)
		}
		rootJobID := strings.TrimSpace(sch.RootJobID)
		hostID := strings.TrimSpace(sch.HostID)
		worktreePath := strings.TrimSpace(sch.WorktreePath)
		storyID, parentJobID, rootJobID, hostID, worktreePath = e.walkAncestorsLocked(jobID, storyID, parentJobID, rootJobID, hostID, worktreePath)

		harness := dispatch.NormalizeHarnessName(sch.Engine)
		if harness == "" {
			harness = "agy"
		}
		targetAgent := strings.TrimSpace(sch.Engine)
		if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
			targetAgent = harness
		}
		model := dispatch.DefaultModelForHarness(harness)
		modelAuthor := dispatch.InferModelAuthor(model, harness)
		repo := strings.TrimSpace(sch.TargetSubmodule)
		if repo == "" && worktreePath != "" {
			repo = dispatch.NormalizeRepoName(worktreePath)
		} else if repo != "" {
			repo = dispatch.NormalizeRepoName(repo)
		}
		isChildReview := dispatch.TextIndicatesReviewSession(sch.CustomPrompt) ||
			dispatch.TextIndicatesReviewSession(sch.PromptTitle) ||
			(parentJobID != "" && (strings.Contains(strings.ToLower(sch.CustomPrompt), "review") || strings.Contains(strings.ToLower(sch.PromptTitle), "review")))

		convIDs := dedupeConversationList(sch.InitialConversationID, sch.ActiveConversationID, []string{sch.ResumeConversationID}, nil, cid)
		return dispatch.ConversationJobProvenance{
			ConversationID:       cid,
			JobID:                jobID,
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
			Status:               strings.ToLower(strings.TrimSpace(sch.Status)),
			ConversationIDs:      convIDs,
		}, true
	}

	// 4. Search registered repos runtime state
	for _, r := range e.repos {
		if r == nil || strings.TrimSpace(r.DetectedConversationID) != cid {
			continue
		}
		jobID := strings.TrimSpace(r.RunningJobID)
		if jobID == "" {
			jobID = "repo-" + r.HostID + "-" + r.EffectiveAlias()
		}
		storyID := strings.TrimSpace(r.DirtyStoryID)
		if storyID == "" {
			storyID, _ = extractStoryIDFromPorcelain(r.Porcelain)
		}
		var parentJobID, rootJobID string
		hostID := r.HostID
		worktreePath := r.WorktreePath
		storyID, parentJobID, rootJobID, hostID, worktreePath = e.walkAncestorsLocked(jobID, storyID, parentJobID, rootJobID, hostID, worktreePath)

		harness := dispatch.NormalizeHarnessName(r.ActiveTool)
		if harness == "" {
			harness = "agy"
		}
		targetAgent := strings.TrimSpace(r.ActiveTool)
		if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
			targetAgent = harness
		}
		model := dispatch.DefaultModelForHarness(harness)
		modelAuthor := dispatch.InferModelAuthor(model, harness)
		repo := dispatch.NormalizeRepoName(r.EffectiveAlias())
		isChildReview := strings.TrimSpace(r.DirtyReviewFile) != "" || strings.Contains(strings.ToLower(r.ASEState), "review")

		return dispatch.ConversationJobProvenance{
			ConversationID:       cid,
			JobID:                jobID,
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
			Status:               "running",
			ConversationIDs:      []string{cid},
		}, true
	}

	// 5. Search host repo runtime statuses
	for _, h := range e.hosts {
		if h == nil {
			continue
		}
		for _, st := range h.RepoStatuses {
			if strings.TrimSpace(st.ActiveConversationID) != cid {
				continue
			}
			repo := dispatch.NormalizeRepoName(st.RepoPath)
			if st.Alias != "" {
				repo = dispatch.NormalizeRepoName(st.Alias)
			}
			jobID := "repo-" + h.ID + "-" + repo
			harness := dispatch.NormalizeHarnessName(st.ActiveTool)
			if harness == "" {
				harness = "agy"
			}
			targetAgent := strings.TrimSpace(st.ActiveTool)
			if targetAgent == "" || strings.EqualFold(targetAgent, "auto") {
				targetAgent = harness
			}
			model := dispatch.DefaultModelForHarness(harness)
			modelAuthor := dispatch.InferModelAuthor(model, harness)
			isChildReview := strings.Contains(strings.ToLower(st.ASEState), "review")

			return dispatch.ConversationJobProvenance{
				ConversationID:       cid,
				JobID:                jobID,
				StoryID:              strings.TrimSpace(st.ActiveStoryID),
				TargetAgent:          targetAgent,
				Harness:              harness,
				Model:                model,
				ModelAuthor:          modelAuthor,
				HostID:               h.ID,
				Repo:                 repo,
				WorktreePath:         st.RepoPath,
				RootJobID:            jobID,
				IsChildReviewSession: isChildReview,
				Status:               "running",
				ConversationIDs:      []string{cid},
			}, true
		}
	}

	return dispatch.ConversationJobProvenance{}, false
}

