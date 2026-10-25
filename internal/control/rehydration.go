package control

import (
	"fmt"
	"strings"
	"time"
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
	LastToolActions        []ToolActionSummary `json:"last_tool_actions,omitempty"`
	WorkspaceStateManifest map[string]any      `json:"workspace_state_manifest,omitempty"`
	BootstrapPrompt        string              `json:"bootstrap_prompt"`
	GeneratedAt            time.Time           `json:"generated_at"`
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

	sb.WriteString("CONTINUATION DIRECTIVE:\n")
	sb.WriteString(fmt.Sprintf("You are resuming execution on a fallback conversation after session loss on %s.\n", convID))
	sb.WriteString(fmt.Sprintf("Do NOT re-execute already completed phases. Continue directly from Phase: %s (Step: %s).\n", interruptedPhase, interruptedStep))
	sb.WriteString("================================================================================\n")

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
		BootstrapPrompt:        sb.String(),
		GeneratedAt:            e.now().UTC(),
	}, nil
}
