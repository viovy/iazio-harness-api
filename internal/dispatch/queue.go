// Package dispatch provides in-memory and distributed job queue mechanisms
// for dispatching prompt execution and story scaffolding jobs to host agents.
package dispatch

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

var executeStoryPrefixRegex = regexp.MustCompile(`(?i)^execute\s+story\s+(?:STORY-[A-Za-z0-9_-]+|[A-Z]{2,16}-[0-9]+)(?:\s*[:\-]\s*|\s+|$)`)

// MaxExecutiveSummaryChars is the maximum length of result.summary on completed/failed jobs (AC3: <= 500 chars).
const MaxExecutiveSummaryChars = 500

var agyStepOutputLineRegex = regexp.MustCompile(`^(?:\[[^\]]+\]\s*)?\[agy step #\d+\]\s*output:\s*(.+)$`)

func extractCraftOrTerminalHeadline(sources ...string) string {
	var headline string
	for _, src := range sources {
		lines := strings.Split(src, "\n")
		for i, rawLine := range lines {
			trimmed := strings.TrimSpace(rawLine)
			if m := agyStepOutputLineRegex.FindStringSubmatch(trimmed); len(m) > 1 {
				trimmed = strings.TrimSpace(m[1])
			}
			if trimmed == "" || strings.Contains(trimmed, "[agy step #") || strings.Contains(trimmed, "[ASE-SUPERVISOR]") {
				continue
			}
			lower := strings.ToLower(trimmed)
			if strings.HasPrefix(lower, "## craft closeout") || strings.HasPrefix(lower, "# craft closeout") {
				detail := ""
				for j := i + 1; j < len(lines) && j <= i+12; j++ {
					cand := strings.TrimSpace(lines[j])
					if m := agyStepOutputLineRegex.FindStringSubmatch(cand); len(m) > 1 {
						cand = strings.TrimSpace(m[1])
					}
					if cand == "" || strings.HasPrefix(cand, "#") || strings.Contains(cand, "[agy step #") || strings.Contains(cand, "[ASE-SUPERVISOR]") {
						continue
					}
					detail = cand
					break
				}
				if detail != "" {
					headline = trimmed + " — " + detail
				} else {
					headline = trimmed
				}
			} else if strings.Contains(lower, "autonomous story execution is complete") || strings.Contains(lower, "craft closeout") {
				headline = trimmed
			}
		}
	}
	if len(headline) > 280 {
		headline = strings.TrimSpace(headline[:277]) + "..."
	}
	return headline
}

// FormatConciseExecutiveSummary strips [agy step #...] trace lines and [ASE-SUPERVISOR] log lines
// and bounds the resulting executive summary to <= maxChars (default 500 chars), prioritizing
// CRAFT closeout headlines or terminal status lines rather than duplicating output.
func FormatConciseExecutiveSummary(summary, output string, maxChars int) string {
	if maxChars <= 0 {
		maxChars = MaxExecutiveSummaryChars
	}
	filterClean := func(s string) string {
		if strings.TrimSpace(s) == "" {
			return ""
		}
		var kept []string
		for _, line := range strings.Split(s, "\n") {
			trimmed := strings.TrimRight(line, "\r")
			if strings.TrimSpace(trimmed) == "" {
				continue
			}
			if strings.Contains(trimmed, "[agy step #") || strings.Contains(trimmed, "[ASE-SUPERVISOR]") {
				continue
			}
			kept = append(kept, trimmed)
		}
		return strings.TrimSpace(strings.Join(kept, "\n"))
	}

	var lastStepOutput string
	for _, src := range []string{output, summary} {
		for _, line := range strings.Split(src, "\n") {
			trimmed := strings.TrimSpace(line)
			if m := agyStepOutputLineRegex.FindStringSubmatch(trimmed); len(m) > 1 {
				cand := strings.TrimSpace(m[1])
				if cand != "" && !strings.Contains(cand, "[agy step #") && !strings.Contains(cand, "[ASE-SUPERVISOR]") {
					lastStepOutput = cand
				}
			}
		}
	}
	craftHeadline := extractCraftOrTerminalHeadline(output, summary)
	if craftHeadline != "" && lastStepOutput == "" {
		lastStepOutput = craftHeadline
	}
	if len(lastStepOutput) > 320 {
		lastStepOutput = strings.TrimSpace(lastStepOutput[:317]) + "..."
	}

	cleaned := filterClean(summary)
	if cleaned == "" && output != "" {
		cleaned = filterClean(output)
	}
	if craftHeadline != "" && !strings.Contains(cleaned, craftHeadline) {
		if cleaned == "" {
			cleaned = craftHeadline
		} else {
			cleaned = craftHeadline + "\n" + cleaned
		}
	}
	if lastStepOutput != "" && !strings.Contains(cleaned, lastStepOutput) {
		if cleaned == "" {
			cleaned = lastStepOutput
		} else {
			cleaned = cleaned + "\n" + lastStepOutput
		}
	}
	cleaned = strings.TrimSpace(cleaned)
	if len(cleaned) > maxChars {
		if lastStepOutput != "" && len(lastStepOutput) < maxChars-10 && !strings.Contains(cleaned[:maxChars-3], lastStepOutput) {
			headBudget := maxChars - len(lastStepOutput) - 5
			if headBudget > 0 {
				cleaned = strings.TrimSpace(cleaned[:headBudget]) + "\n...\n" + lastStepOutput
			} else {
				cleaned = lastStepOutput
			}
		}
		if len(cleaned) > maxChars {
			cleaned = strings.TrimSpace(cleaned[:maxChars-3]) + "..."
		}
	}
	return cleaned
}

// SanitizeJobResultSummary ensures result.summary contains only a concise executive summary
// (<=1,000 chars, stripped of [agy step #...] and [ASE-SUPERVISOR] log lines) while preserving
// full execution output in result.output.
func SanitizeJobResultSummary(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return raw
	}
	rawSummary, hasSummary := m["summary"].(string)
	rawOutput, _ := m["output"].(string)
	modified := false

	if strings.TrimSpace(rawOutput) == "" && strings.TrimSpace(rawSummary) != "" &&
		(strings.Contains(rawSummary, "[agy step #") || strings.Contains(rawSummary, "[ASE-SUPERVISOR]") || len(rawSummary) > MaxExecutiveSummaryChars) {
		m["output"] = rawSummary
		rawOutput = rawSummary
		modified = true
	}
	if hasSummary || strings.TrimSpace(rawOutput) != "" {
		cleanSummary := FormatConciseExecutiveSummary(rawSummary, rawOutput, MaxExecutiveSummaryChars)
		if cleanSummary != rawSummary || !hasSummary {
			m["summary"] = cleanSummary
			modified = true
		}
	}
	if rawFail, ok := m["failure_reason_summary"].(string); ok && strings.TrimSpace(rawFail) != "" {
		cleanFail := FormatConciseExecutiveSummary(rawFail, "", MaxExecutiveSummaryChars)
		if cleanFail != rawFail {
			m["failure_reason_summary"] = cleanFail
			modified = true
		}
	}
	if !modified {
		return raw
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return encoded
}

// StripJobResultOutputForQueueList sanitizes result.summary and strips the heavy result.output
// field for compact queue listing endpoints.
func StripJobResultOutputForQueueList(raw json.RawMessage) json.RawMessage {
	sanitized := SanitizeJobResultSummary(raw)
	if len(bytes.TrimSpace(sanitized)) == 0 {
		return sanitized
	}
	var m map[string]any
	if err := json.Unmarshal(sanitized, &m); err != nil || m == nil {
		return sanitized
	}
	if _, hasOutput := m["output"]; !hasOutput {
		return sanitized
	}
	delete(m, "output")
	encoded, err := json.Marshal(m)
	if err != nil {
		return sanitized
	}
	return encoded
}

// isSyntheticTestConversationID returns true for legacy unit-test fixture UUIDs
// ("11111111-2222-3333-4444-555555555555" and "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
// as a defense-in-depth filter when a real conversation UUID is also present on a job.
func isSyntheticTestConversationID(cid string) bool {
	switch strings.ToLower(strings.TrimSpace(cid)) {
	case "11111111-2222-3333-4444-555555555555",
		"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee":
		return true
	default:
		return false
	}
}

func isUnitTestDummyStoryID(storyID string) bool {
	switch strings.ToUpper(strings.TrimSpace(storyID)) {
	case "STORY-EXPLICIT-111", "STORY-DETECTED-222", "STORY-HOOK-333":
		return true
	default:
		return false
	}
}

// Standard job types.
const (
	JobTypeStoryCreate   = "story.create"
	JobTypeStoryExecute  = "story.execute"
	JobTypePromptExecute = "prompt.execute"
)

// Standard job statuses.
const (
	JobStatusPending             = "pending"
	JobStatusBlockedByDependency = "blocked_by_dependency"
	JobStatusLeased              = "leased"
	JobStatusClaimed             = "claimed"
	JobStatusRunning             = "running"
	JobStatusCompleted           = "completed"
	JobStatusFailed              = "failed"
	JobStatusCanceled            = "canceled"
	JobStatusRequeued            = "requeued"
)

// Canonical Failure Taxonomy
const (
	FailureCategoryNone                   = "NONE"
	FailureCategoryDispatchClaimTimeout   = "DISPATCH_CLAIM_TIMEOUT"
	FailureCategoryToolQuotaOrAuth        = "TOOL_QUOTA_OR_AUTH"
	FailureCategoryToolCrash              = "TOOL_CRASH"
	FailureCategoryWorkspacePrepError     = "WORKSPACE_PREP_ERROR"
	FailureCategoryBackgroundDrainTimeout = "BACKGROUND_DRAIN_TIMEOUT"
	FailureCategoryASEGateIncomplete      = "ASE_GATE_INCOMPLETE"
	FailureCategoryCircuitBreakerTripped  = "CIRCUIT_BREAKER_TRIPPED"
	FailureCategoryStoryNotFound          = "STORY_NOT_FOUND"
	FailureCategoryDependencyFailed       = "DEPENDENCY_FAILED"
	FailureCategoryPolicyHookBlocked      = "POLICY_HOOK_BLOCKED"
	// Infra / operator-stop categories (STORY-GOV-5275): jobs may stop ONLY for these.
	FailureCategoryInfraDown              = "INFRA_DOWN"
	FailureCategoryUpstreamSCMUnavailable = "UPSTREAM_SCM_UNAVAILABLE"
	FailureCategoryUpstreamCIUnavailable  = "UPSTREAM_CI_UNAVAILABLE"
	FailureCategoryDownstreamDBUnavailable = "DOWNSTREAM_DATABASE_UNAVAILABLE"
	FailureCategoryDownstreamIngestUnavailable = "DOWNSTREAM_INGEST_UNAVAILABLE"

	FailureStageIngress         = "INGRESS"
	FailureStageQueueDispatch   = "QUEUE_DISPATCH"
	FailureStageWorkspacePrep   = "WORKSPACE_PREP"
	FailureStageToolExecution   = "TOOL_EXECUTION"
	FailureStageBackgroundDrain = "BACKGROUND_DRAIN"
	FailureStageCompletionGate  = "COMPLETION_GATE"
	FailureStageStoryValidation = "STORY_VALIDATION"

	FailureDirectionNone           = "NONE"
	FailureDirectionUpstream       = "UPSTREAM"
	FailureDirectionDownstream     = "DOWNSTREAM"
	FailureDirectionLocalExecution = "LOCAL_EXECUTION"
)

const (
	DefaultLeaseAckDeadline = 20 * time.Second
	// DefaultWebBaseURL is the canonical public base URL for iazio-web deep links.
	DefaultWebBaseURL = "https://tian.go.ro/iazio-web"
)

// ResolveWebBaseURL returns the configured iazio-web base URL (from IAZIO_WEB_BASE_URL
// or PUBLIC_WEB_URL), defaulting to https://tian.go.ro/iazio-web and never example.com.
func ResolveWebBaseURL() string {
	for _, envKey := range []string{"IAZIO_WEB_BASE_URL", "PUBLIC_WEB_URL"} {
		if val := strings.TrimSpace(os.Getenv(envKey)); val != "" && !strings.Contains(strings.ToLower(val), "example.com") {
			return strings.TrimRight(val, "/")
		}
	}
	return DefaultWebBaseURL
}

// BuildConversationURL computes the persistent iazio-web deep link for a conversation ID.
func BuildConversationURL(webBaseURL, conversationID string) string {
	cid := strings.TrimSpace(conversationID)
	if cid == "" {
		return ""
	}
	base := strings.TrimSpace(webBaseURL)
	if base == "" || strings.Contains(strings.ToLower(base), "example.com") {
		base = ResolveWebBaseURL()
	}
	return strings.TrimRight(base, "/") + "/conversation/" + cid
}

// BuildLiveURL computes the canonical iazio-web deep link for a job ID (/jobs/<job_id>).
func BuildLiveURL(webBaseURL, jobID string) string {
	id := strings.TrimSpace(jobID)
	if id == "" {
		return ""
	}
	base := strings.TrimSpace(webBaseURL)
	if base == "" || strings.Contains(strings.ToLower(base), "example.com") {
		base = ResolveWebBaseURL()
	}
	return strings.TrimRight(base, "/") + "/jobs/" + id
}

// NormalizeLiveURL rewrites legacy /agents/.../live?job=<id> or /hosts/.../live?job=<id>
// or example.com URLs into the canonical <web_base_url>/jobs/<job_id> URL.
func NormalizeLiveURL(rawURL, jobID string) string {
	raw := strings.TrimSpace(rawURL)
	id := strings.TrimSpace(jobID)
	if id == "" && strings.Contains(raw, "job=") {
		if parsed, err := url.Parse(raw); err == nil {
			id = strings.TrimSpace(parsed.Query().Get("job"))
		}
		if id == "" {
			if idx := strings.Index(raw, "job="); idx >= 0 {
				rest := raw[idx+len("job="):]
				if amp := strings.IndexByte(rest, '&'); amp >= 0 {
					rest = rest[:amp]
				}
				if hash := strings.IndexByte(rest, '#'); hash >= 0 {
					rest = rest[:hash]
				}
				id = strings.TrimSpace(rest)
			}
		}
	}
	if id != "" {
		lowerRaw := strings.ToLower(raw)
		if raw == "" ||
			strings.Contains(lowerRaw, "example.com") ||
			strings.Contains(raw, "/agents/") ||
			strings.Contains(raw, "/hosts/") ||
			strings.Contains(raw, "/live?job=") ||
			!strings.Contains(raw, "/jobs/") {
			return BuildLiveURL("", id)
		}
	}
	for _, prefix := range []string{"https://example.com/iazio-web", "http://example.com/iazio-web"} {
		if strings.HasPrefix(strings.ToLower(raw), prefix) {
			raw = strings.TrimRight(ResolveWebBaseURL(), "/") + raw[len(prefix):]
			break
		}
	}
	return raw
}

// NormalizeRepoName normalizes root worktree indicators (".", "root", empty, or meta-repo worktree paths)
// to "meta-repo" and extracts the base repository name from filesystem worktree paths.
func NormalizeRepoName(repo string) string {
	trimmed := strings.TrimSpace(repo)
	lower := strings.ToLower(trimmed)
	if trimmed == "" || trimmed == "." || trimmed == "/" || trimmed == `\` || lower == "root" || lower == "metarepo" {
		return "meta-repo"
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, `\`) {
		slashed := strings.TrimRight(filepath.ToSlash(trimmed), "/")
		if slashed == "" || slashed == "." {
			return "meta-repo"
		}
		base := filepath.Base(slashed)
		baseLower := strings.ToLower(base)
		if base == "" || base == "." || base == "/" || baseLower == "root" || baseLower == "metarepo" {
			return "meta-repo"
		}
		return base
	}
	return trimmed
}

// IsKnownMetaRepoSubmodule returns true when name refers to a submodule belonging to the meta-repo superproject.
func IsKnownMetaRepoSubmodule(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" || lower == "." || lower == "root" || lower == "metarepo" || strings.HasPrefix(lower, "meta-repo") {
		return false
	}
	switch {
	case lower == "iazio" || strings.HasPrefix(lower, "iazio-"),
		strings.HasPrefix(lower, "autopilot"),
		strings.HasPrefix(lower, "vigo"),
		strings.HasPrefix(lower, "eye_habit") || lower == "eye-habit",
		strings.HasPrefix(lower, "k8s"),
		strings.HasPrefix(lower, "up_dividends"),
		strings.HasPrefix(lower, "viovy-core"),
		strings.HasPrefix(lower, "central-telemetry"),
		lower == "home-infra",
		lower == "vi-ansible-collection",
		lower == "build-container-images",
		lower == "vi-scripts",
		lower == "auth-playground",
		lower == "helm-charts",
		lower == "isetup",
		lower == "flutter-start-template",
		lower == "thewebcafe",
		lower == "local_agent":
		return true
	default:
		return false
	}
}

// IsHostLevelNonWorktreeJob returns true when a dispatch job operates on host-level binaries/tools
// (such as "agent.tools.update" or repo "fleet") and does not require or lock a git worktree slot.
func IsHostLevelNonWorktreeJob(jobType, repo string) bool {
	cleanType := strings.ToLower(strings.TrimSpace(jobType))
	if cleanType == "agent.tools.update" {
		return true
	}
	cleanRepo := strings.ToLower(strings.TrimSpace(repo))
	return cleanRepo == "fleet" || cleanRepo == "tools"
}

// IsControlPlaneScheduleJobID returns true when id matches the control-plane engine
// schedule job identifier format ("job-<digits>", e.g. "job-1408", "job-1424", "job-1570"),
// as distinct from dispatch queue job IDs ("job_<16hex>").
func IsControlPlaneScheduleJobID(id string) bool {
	clean := strings.TrimSpace(id)
	if !strings.HasPrefix(clean, "job-") || len(clean) <= 4 {
		return false
	}
	for i := 4; i < len(clean); i++ {
		if clean[i] < '0' || clean[i] > '9' {
			return false
		}
	}
	return true
}

// IsPhantomEmptyQueueJob returns true when a queue entry is an unpopulated phantom
// artifact (empty Type, empty NormalizedPrompt, empty Payload, and no StoryID) that
// was canceled/reconciled as phantom or synthetic (`phantom_empty_job`,
// `phantom_or_superseded_remediation_*`, `terminal_engine_job`, `synthetic_test_job`).
func IsPhantomEmptyQueueJob(j *Job) bool {
	if j == nil {
		return false
	}
	if strings.TrimSpace(j.Type) != "" || strings.TrimSpace(j.NormalizedPrompt) != "" || len(j.Payload) > 0 || strings.TrimSpace(j.StoryID) != "" {
		return false
	}
	id := strings.TrimSpace(j.ID)
	if id == "job-cp-fallback" || strings.HasPrefix(id, "job-reap-") || strings.HasPrefix(id, "job-missing-") {
		return true
	}
	errLower := strings.ToLower(strings.TrimSpace(j.Error))
	if errLower == "phantom_empty_job" ||
		strings.Contains(errLower, "phantom_or_superseded_remediation") ||
		errLower == "terminal_engine_job" ||
		errLower == "synthetic_test_job" {
		return true
	}
	return false
}

// NormalizeToSuperprojectSlotFamily normalizes submodule names (e.g. iazio-mcp, iazio-agent)
// and meta-repo worktree paths (e.g. /home/romeo/work/meta-repo-02) to their superproject worktree slot family ("meta-repo").
func NormalizeToSuperprojectSlotFamily(repo string) string {
	norm := strings.ToLower(strings.TrimSpace(NormalizeRepoName(repo)))
	if norm == "" || norm == "." || norm == "root" || norm == "metarepo" || strings.HasPrefix(norm, "meta-repo") {
		return "meta-repo"
	}
	if IsKnownMetaRepoSubmodule(norm) {
		return "meta-repo"
	}
	trimmed := strings.ToLower(filepath.ToSlash(strings.TrimSpace(repo)))
	if strings.Contains(trimmed, "/meta-repo") {
		return "meta-repo"
	}
	return norm
}

// ExtractTargetSubmodule returns the normalized target submodule name (e.g., "iazio-mcp", "iazio-agent")
// if the job targets a specific submodule, or "" if the job targets the generic superproject root.
func ExtractTargetSubmodule(repo string, payload json.RawMessage) string {
	norm := strings.ToLower(strings.TrimSpace(NormalizeRepoName(repo)))
	if IsKnownMetaRepoSubmodule(norm) {
		return norm
	}
	if len(payload) > 0 {
		var pm map[string]any
		if json.Unmarshal(payload, &pm) == nil && pm != nil {
			for _, key := range []string{"target_submodule", "submodule", "repo"} {
				if v, ok := pm[key].(string); ok {
					cand := strings.ToLower(strings.TrimSpace(NormalizeRepoName(v)))
					if IsKnownMetaRepoSubmodule(cand) {
						return cand
					}
				}
			}
		}
	}
	return ""
}

// AllowConcurrentSubmoduleMutation inspects job payload for explicit opt-in to concurrent mutations on the same submodule.
func AllowConcurrentSubmoduleMutation(payload json.RawMessage) bool {
	if len(payload) == 0 {
		return false
	}
	var pm map[string]any
	if json.Unmarshal(payload, &pm) != nil || pm == nil {
		return false
	}
	if b, ok := pm["allow_concurrent_submodule_mutation"].(bool); ok && b {
		return true
	}
	if s, ok := pm["allow_concurrent_submodule_mutation"].(string); ok {
		lower := strings.ToLower(strings.TrimSpace(s))
		if lower == "true" || lower == "1" || lower == "yes" {
			return true
		}
	}
	if envMap, ok := pm["env"].(map[string]any); ok && envMap != nil {
		for _, k := range []string{"ALLOW_CONCURRENT_SUBMODULE_MUTATION", "allow_concurrent_submodule_mutation"} {
			if b, ok := envMap[k].(bool); ok && b {
				return true
			}
			if s, ok := envMap[k].(string); ok {
				lower := strings.ToLower(strings.TrimSpace(s))
				if lower == "true" || lower == "1" || lower == "yes" {
					return true
				}
			}
		}
	}
	return false
}

// ResolveJobConversationID resolves the canonical conversation ID for a job from its
// first-class fields, Result JSON, or Payload JSON.
func ResolveJobConversationID(j *Job) string {
	if j == nil {
		return ""
	}
	for _, cand := range []string{
		j.ActiveConversationID,
		j.ConversationID,
		j.ResumeConversationID,
		j.InitialConversationID,
	} {
		if trimmed := strings.TrimSpace(cand); trimmed != "" {
			return trimmed
		}
	}
	for _, raw := range j.ConversationIDs {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			return trimmed
		}
	}
	for _, blob := range []json.RawMessage{j.Result, j.Payload} {
		if len(blob) == 0 {
			continue
		}
		var m map[string]any
		if json.Unmarshal(blob, &m) == nil && m != nil {
			for _, key := range []string{"active_conversation_id", "conversation_id", "resume_conversation_id", "initial_conversation_id"} {
				if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
			if arr, ok := m["conversation_ids"].([]any); ok {
				for _, item := range arr {
					if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
						return strings.TrimSpace(s)
					}
				}
			}
		}
	}
	return ""
}

// PopulateJobConversationFields synchronizes ConversationID, ActiveConversationID,
// ConversationIDs, and ConversationURL on a Job record.
func PopulateJobConversationFields(j *Job) {
	if j == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(j.Repo), "root") {
		j.Repo = "meta-repo"
	}
	for _, blob := range []json.RawMessage{j.Result, j.Payload} {
		if len(blob) == 0 {
			continue
		}
		var m map[string]any
		if json.Unmarshal(blob, &m) == nil && m != nil {
			if arr, ok := m["conversation_ids"].([]any); ok {
				for _, item := range arr {
					if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
						addConversationIDLocked(j, strings.TrimSpace(s))
					}
				}
			}
			for _, key := range []string{"active_conversation_id", "conversation_id"} {
				if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
					cid := strings.TrimSpace(v)
					if j.ActiveConversationID == "" {
						j.ActiveConversationID = cid
					}
					addConversationIDLocked(j, cid)
				}
			}
		}
	}
	filtered := make([]string, 0, len(j.ConversationIDs))
	for _, cid := range j.ConversationIDs {
		trimmed := strings.TrimSpace(cid)
		if trimmed != "" && !isSyntheticTestConversationID(trimmed) {
			filtered = append(filtered, trimmed)
		}
	}
	var primaryReal string
	if len(filtered) > 0 {
		primaryReal = filtered[len(filtered)-1]
	} else {
		for _, cand := range []string{
			j.ActiveConversationID,
			j.ConversationID,
			j.ResumeConversationID,
			j.InitialConversationID,
		} {
			if trimmed := strings.TrimSpace(cand); trimmed != "" && !isSyntheticTestConversationID(trimmed) {
				primaryReal = trimmed
				break
			}
		}
	}
	if primaryReal != "" {
		j.ConversationIDs = filtered
		if isSyntheticTestConversationID(j.ActiveConversationID) {
			j.ActiveConversationID = primaryReal
		}
		if isSyntheticTestConversationID(j.ConversationID) {
			j.ConversationID = primaryReal
		}
		if isSyntheticTestConversationID(j.InitialConversationID) {
			if len(filtered) > 0 {
				j.InitialConversationID = filtered[0]
			} else {
				j.InitialConversationID = primaryReal
			}
		}
		if isSyntheticTestConversationID(j.ResumeConversationID) {
			j.ResumeConversationID = primaryReal
		}
	}
	resolved := ResolveJobConversationID(j)
	if resolved != "" {
		if j.ConversationID == "" {
			j.ConversationID = resolved
		}
		if j.ActiveConversationID == "" {
			j.ActiveConversationID = resolved
		}
		addConversationIDLocked(j, resolved)
		j.ConversationID = j.ActiveConversationID
		j.ConversationURL = BuildConversationURL("", j.ConversationID)
	}
}

// AttemptFailure models one failed dispatch or execution attempt.
type AttemptFailure struct {
	AttemptNumber    int    `json:"attempt_number"`
	Host             string `json:"host"`
	Workspace        string `json:"workspace"`
	Tool             string `json:"tool"`
	FailureCategory  string `json:"failure_category"`
	FailureStage     string `json:"failure_stage,omitempty"`
	FailureDirection string `json:"failure_direction,omitempty"`
	FailureService   string `json:"failure_service,omitempty"`
	Reason           string `json:"reason,omitempty"`
	ExitCode         int    `json:"exit_code,omitempty"`
	StderrTail       string `json:"stderr_tail,omitempty"`
	ZipkinSpanID     string `json:"zipkin_span_id,omitempty"`
	Timestamp        string `json:"timestamp,omitempty"`
	StartedAt        string `json:"started_at,omitempty"`
	EndedAt          string `json:"ended_at,omitempty"`
	TriggerReason    string `json:"trigger_reason,omitempty"`
	OutcomeSummary   string `json:"outcome_summary,omitempty"`
	LogSeqOffset     int    `json:"log_seq_offset,omitempty"`
	LogByteOffset    int64  `json:"log_byte_offset,omitempty"`
}

const (
	// DefaultClaimTimeout is the duration a claimed job can remain active before lease expiration.
	DefaultClaimTimeout = 30 * time.Minute
	// DefaultHeartbeatTimeout is the maximum duration between runner heartbeats before considering a runner stalled (90s, allowing >=6 retry margins).
	DefaultHeartbeatTimeout = 90 * time.Second
	// DefaultJobTimeout is the maximum total duration of a job before execution timeout (0 = unbounded).
	DefaultJobTimeout = 0
	// DefaultMaxAttempts is the maximum number of times a stalled job may be reclaimed.
	DefaultMaxAttempts = 3
)

// EligibleHost describes an agent/workspace candidate evaluated for job dispatch.
type EligibleHost struct {
	Host      string `json:"host"`
	Workspace string `json:"workspace"`
	Tool      string `json:"tool"`
	Status    string `json:"status,omitempty"`
}

// DispatchDiagnostics contains real-time dispatch eligibility and blockage diagnostics for a queue job.
type DispatchDiagnostics struct {
	QueueState               string            `json:"queue_state"` // queued, dispatching, running, completed, failed
	EligibleHosts            []EligibleHost    `json:"eligible_hosts"`
	LastDispatchEvaluationAt string            `json:"last_dispatch_evaluation_at,omitempty"`
	BlockedReasonsByHost     map[string]string `json:"blocked_reasons_by_host,omitempty"`
}

var (
	// ErrJobNotFound indicates the requested job ID was not found.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobAlreadyCompleted indicates the job has already reached a terminal state.
	ErrJobAlreadyCompleted = errors.New("job already completed")
	// ErrTimeout indicates an await operation timed out before job completion.
	ErrTimeout = errors.New("dispatch await timeout")
	// ErrLeaseFenced indicates a superseded worker attempted to mutate job state.
	ErrLeaseFenced = errors.New("lease fenced: stale fencing token presented")
)

// AgentAffinity defines binding constraints for workers claiming jobs.
type AgentAffinity struct {
	HostID               string   `json:"host_id,omitempty"`
	AgentName            string   `json:"agent_name,omitempty"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
}

// Job represents an asynchronous task dispatched to an iazio-agent daemon.
type Job struct {
	ID                      string               `json:"id"`
	Type                    string               `json:"type"`
	Repo                    string               `json:"repo"`
	WorktreePath            string               `json:"worktree_path,omitempty"`
	TargetHost              string               `json:"target_host,omitempty"`
	ClaimedHost             string               `json:"claimed_host,omitempty"`
	HostID                  string               `json:"host_id,omitempty"`
	TargetAgent             string               `json:"target_agent,omitempty"`
	Model                   string               `json:"model,omitempty"`
	FallbackModels          []string             `json:"fallback_models,omitempty"`
	StoryID                 string               `json:"story_id,omitempty"`
	StoryTitle              string               `json:"story_title,omitempty"`
	StoryPath               string               `json:"story_path,omitempty"`
	StoryAllocationStatus   string               `json:"story_allocation_status,omitempty"`
	AllocationSource        string               `json:"allocation_source,omitempty"`
	PriorityRankSelected    int                  `json:"priority_rank_selected,omitempty"`
	SourceConversationTitle string               `json:"source_conversation_title,omitempty"`
	NormalizedPrompt        string               `json:"normalized_prompt,omitempty"`
	EffectiveAction         string               `json:"effective_action,omitempty"`
	AgentAffinity           *AgentAffinity       `json:"agent_affinity,omitempty"`
	Payload                 json.RawMessage      `json:"payload"`
	Status                  string               `json:"status"`
	Result                  json.RawMessage      `json:"result,omitempty"`
	Error                   string               `json:"error,omitempty"`
	TraceID                 string               `json:"trace_id,omitempty"`
	ParentID                string               `json:"parent_id,omitempty"`
	SearchID                string               `json:"search_id,omitempty"`
	RelevanceEmitted        bool                 `json:"relevance_emitted"`
	PromotedMemories        []string             `json:"promoted_memories,omitempty"`
	FencingToken            int64                `json:"fencing_token"`
	Attempts                int                  `json:"attempts"`
	CreatedAt               time.Time            `json:"created_at"`
	ClaimedAt               *time.Time           `json:"claimed_at,omitempty"`
	AckedAt                 *time.Time           `json:"acked_at,omitempty"`
	HeartbeatAt             *time.Time           `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt          *time.Time           `json:"lease_expires_at,omitempty"`
	CompletedAt             *time.Time           `json:"completed_at,omitempty"`
	DispatchDiagnostics     *DispatchDiagnostics `json:"dispatch_diagnostics,omitempty"`
	LeaseToken              string               `json:"lease_token,omitempty"`
	LeaseAckDeadlineAt      *time.Time           `json:"lease_ack_deadline_at,omitempty"`
	LeaseOwner              string               `json:"lease_owner,omitempty"`
	LeaseVersion            int64                `json:"lease_version,omitempty"`
	FailureCategory         string               `json:"failure_category,omitempty"`
	FailureStage            string               `json:"failure_stage,omitempty"`
	FailureDirection        string               `json:"failure_direction,omitempty"`
	FailureService          string               `json:"failure_service,omitempty"`
	FailureReasonSummary    string               `json:"failure_reason_summary,omitempty"`
	Retriable               bool                 `json:"retriable"`
	FinalTurnCount          int                  `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256   string               `json:"final_transcript_sha256,omitempty"`
	AttemptHistory          []AttemptFailure     `json:"attempt_history,omitempty"`
	ZipkinTraceID           string               `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL          string               `json:"zipkin_trace_url,omitempty"`
	Traceparent             string               `json:"traceparent,omitempty"`
	InfraRequeueCount       int                  `json:"infra_requeue_count,omitempty"`
	AgentPID                int                  `json:"agent_pid,omitempty"`
	HarnessPID              int                  `json:"harness_pid,omitempty"`
	ToolPID                 int                  `json:"tool_pid,omitempty"`
	ElapsedSeconds          int64                `json:"elapsed_seconds,omitempty"`
	ResumeConversationID    string               `json:"resume_conversation_id,omitempty"`
	InitialConversationID   string               `json:"initial_conversation_id,omitempty"`
	ConversationID          string               `json:"conversation_id,omitempty"`
	ActiveConversationID    string               `json:"active_conversation_id,omitempty"`
	ConversationIDs         []string             `json:"conversation_ids,omitempty"`
	ConversationURL         string               `json:"conversation_url,omitempty"`
	DependsOnJobID          string               `json:"depends_on_job_id,omitempty"`
	DependsOnJobIDs         []string             `json:"depends_on_job_ids,omitempty"`
	CallbackURL             string               `json:"callback_url,omitempty"`

	// doneCh signals when the job has been completed or failed.
	doneCh chan struct{}
}

// JobFilter specifies filtering criteria for listing jobs.
type JobFilter struct {
	Repo   string `json:"repo,omitempty"`
	Status string `json:"status,omitempty"`
	Type   string `json:"type,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// Queue defines the contract for enqueuing, claiming, completing, and awaiting agent jobs.
type Queue interface {
	Enqueue(ctx context.Context, job *Job) (*Job, error)
	Claim(ctx context.Context, hostID string, jobTypes []string) (*Job, error)
	ClaimWithWorktrees(ctx context.Context, hostID string, jobTypes []string, idleWorktrees []string) (*Job, error)
	Decline(ctx context.Context, jobID string, reason string) (*Job, error)
	Heartbeat(ctx context.Context, jobID string, fencingToken int64) (*Job, error)
	HeartbeatWithStory(ctx context.Context, jobID string, fencingToken int64, storyID string) (*Job, error)
	Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error
	CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error
	Get(jobID string) (*Job, bool)
	List(ctx context.Context, filter JobFilter) ([]*Job, error)
	Cancel(ctx context.Context, jobID string, reason string) (*Job, error)
	Retry(ctx context.Context, jobID string) (*Job, error)
	BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error)
	BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*Job, error)
	Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error)
	GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error)
	ReapExpired(now time.Time) []string
	StartReaper(ctx context.Context, interval time.Duration)
	ReconcileGlobalQueueSlots(now time.Time, staleGrace time.Duration) []string
	SetDispatchDiagnostics(jobID string, diag *DispatchDiagnostics) error
	ForceDispatch(ctx context.Context, jobID string) (*Job, error)
	AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) (*Job, error)
	Failover(ctx context.Context, jobID string, req AttemptFailure) (*Job, error)
	RetryRetriable(ctx context.Context) ([]*Job, error)
	SetFinalHarvest(jobID string, turnCount int, sha string)
	ExtendLeasesForHost(ctx context.Context, hostID string, activeJobIDs []string) ([]string, error)
}

// JobAttributionResponse models the attribution of a job outcome to retrieval context.
type JobAttributionResponse struct {
	JobID            string     `json:"job_id"`
	TraceID          string     `json:"trace_id,omitempty"`
	SearchID         string     `json:"search_id,omitempty"`
	Status           string     `json:"status"`
	RelevanceEmitted bool       `json:"relevance_emitted"`
	PromotedMemories []string   `json:"promoted_memories"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
}

// QueueStore defines the database persistence interface for queue jobs and distributed leases.
type QueueStore interface {
	SaveQueueJob(ctx context.Context, job *Job) error
	LoadQueueJobs(ctx context.Context) ([]*Job, error)
	UpdateQueueJobLease(ctx context.Context, jobID string, leaseOwner string, leaseVersion int64, heartbeatAt, expiresAt time.Time) error
	CompleteQueueJob(ctx context.Context, jobID string, status string, result json.RawMessage, errStr string, completedAt time.Time) error
}

// InMemoryQueue is a thread-safe in-memory implementation of Queue with optional database backing.
type InMemoryQueue struct {
	mu                     sync.Mutex
	jobs                   map[string]*Job
	activeJobs             sync.Map
	pendingSlotReleases    []string
	fencingSeq             int64
	attributionURL         string
	httpClient             *http.Client
	claimTimeout           time.Duration
	jobTimeout             time.Duration
	startupTime            time.Time
	startupGrace           time.Duration
	lastReapAt             time.Time
	lastReapJobCount       int
	store                  QueueStore
	nowFunc                func() time.Time
	terminalHooks          []func(job *Job, promoted []*Job)
	hostEligibilityChecker func(hostID string) (bool, string)
	slotAllocator          func(hostID, jobID, targetRepo, targetSubmodule, targetAgent string, idleWorktrees []string) (string, bool)
	slotReleaser           func(jobID string)
	externalDepChecker     func(depID string) (exists, satisfied, failed bool, reason string)
}

func isJobStatusActive(status string) bool {
	st := strings.ToLower(strings.TrimSpace(status))
	return st == JobStatusRunning || st == JobStatusClaimed || st == JobStatusLeased
}

func (q *InMemoryQueue) syncJobActiveStateLocked(j *Job) {
	q.lastReapAt = time.Time{}
	if j == nil || strings.TrimSpace(j.ID) == "" {
		return
	}
	if isJobStatusActive(j.Status) {
		q.activeJobs.Store(j.ID, true)
	} else {
		q.activeJobs.Delete(j.ID)
	}
}

func (q *InMemoryQueue) rebuildActiveJobsLocked() {
	q.lastReapAt = time.Time{}
	activeSet := make(map[string]bool)
	for id, j := range q.jobs {
		if j != nil && isJobStatusActive(j.Status) {
			activeSet[id] = true
			q.activeJobs.Store(id, true)
		}
	}
	q.activeJobs.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok && !activeSet[id] {
			q.activeJobs.Delete(id)
		}
		return true
	})
}

// IsJobActive reports whether jobID is currently in an active execution state (leased, claimed, or running)
// using a lock-free snapshot so callers holding external mutexes (such as control.Engine) never contend on q.mu.
func (q *InMemoryQueue) IsJobActive(jobID string) bool {
	cleanID := strings.TrimSpace(jobID)
	if cleanID == "" {
		return false
	}
	_, ok := q.activeJobs.Load(cleanID)
	return ok
}

func (q *InMemoryQueue) flushPendingSlotReleases() {
	q.mu.Lock()
	if len(q.pendingSlotReleases) == 0 {
		q.mu.Unlock()
		return
	}
	ids := append([]string(nil), q.pendingSlotReleases...)
	q.pendingSlotReleases = nil
	releaser := q.slotReleaser
	q.mu.Unlock()

	if releaser != nil {
		for _, id := range ids {
			releaser(id)
		}
	}
}

// SetHostEligibilityChecker sets an optional callback that gates Claim on host readiness (e.g., Eager Update Pre-Claim Barrier).
func (q *InMemoryQueue) SetHostEligibilityChecker(fn func(hostID string) (bool, string)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.hostEligibilityChecker = fn
}

// SetSlotAllocator sets an optional callback that evaluates and atomically reserves a clean idle worktree slot on a host at claim time.
func (q *InMemoryQueue) SetSlotAllocator(fn func(hostID, jobID, targetRepo, targetSubmodule, targetAgent string, idleWorktrees []string) (string, bool)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.slotAllocator = fn
}

// SetSlotReleaser sets an optional callback invoked when a leased/claimed job is declined, reaped, or reset to pending.
func (q *InMemoryQueue) SetSlotReleaser(fn func(jobID string)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.slotReleaser = fn
}

// SetExternalDepChecker sets an optional callback to check dependency completion status in the control plane engine.
func (q *InMemoryQueue) SetExternalDepChecker(fn func(depID string) (exists, satisfied, failed bool, reason string)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.externalDepChecker = fn
}

// AddTerminalHook registers a callback invoked whenever a job transitions to a terminal state (completed, failed, canceled).
func (q *InMemoryQueue) AddTerminalHook(fn func(job *Job, promoted []*Job)) {
	if fn == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.terminalHooks = append(q.terminalHooks, fn)
}

// SetTerminalHook sets the primary terminal transition callback on the queue.
func (q *InMemoryQueue) SetTerminalHook(fn func(job *Job, promoted []*Job)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if fn == nil {
		q.terminalHooks = nil
		return
	}
	q.terminalHooks = []func(job *Job, promoted []*Job){fn}
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := *t
	return &v
}

func cloneJobSnapshot(j *Job) *Job {
	if j == nil {
		return nil
	}
	cp := *j
	cp.ClaimedAt = cloneTimePtr(j.ClaimedAt)
	cp.AckedAt = cloneTimePtr(j.AckedAt)
	cp.HeartbeatAt = cloneTimePtr(j.HeartbeatAt)
	cp.LeaseExpiresAt = cloneTimePtr(j.LeaseExpiresAt)
	cp.CompletedAt = cloneTimePtr(j.CompletedAt)
	cp.LeaseAckDeadlineAt = cloneTimePtr(j.LeaseAckDeadlineAt)
	if j.AgentAffinity != nil {
		aff := *j.AgentAffinity
		if len(j.AgentAffinity.RequiredCapabilities) > 0 {
			aff.RequiredCapabilities = append([]string(nil), j.AgentAffinity.RequiredCapabilities...)
		}
		cp.AgentAffinity = &aff
	}
	if j.DispatchDiagnostics != nil {
		diag := *j.DispatchDiagnostics
		if len(j.DispatchDiagnostics.EligibleHosts) > 0 {
			diag.EligibleHosts = append([]EligibleHost(nil), j.DispatchDiagnostics.EligibleHosts...)
		}
		if len(j.DispatchDiagnostics.BlockedReasonsByHost) > 0 {
			diag.BlockedReasonsByHost = make(map[string]string, len(j.DispatchDiagnostics.BlockedReasonsByHost))
			for k, v := range j.DispatchDiagnostics.BlockedReasonsByHost {
				diag.BlockedReasonsByHost[k] = v
			}
		}
		cp.DispatchDiagnostics = &diag
	}
	if len(j.AttemptHistory) > 0 {
		cp.AttemptHistory = append([]AttemptFailure(nil), j.AttemptHistory...)
	}
	if len(j.DependsOnJobIDs) > 0 {
		cp.DependsOnJobIDs = append([]string(nil), j.DependsOnJobIDs...)
	}
	if len(j.PromotedMemories) > 0 {
		cp.PromotedMemories = append([]string(nil), j.PromotedMemories...)
	}
	if len(j.ConversationIDs) > 0 {
		cp.ConversationIDs = append([]string(nil), j.ConversationIDs...)
	}
	if len(j.FallbackModels) > 0 {
		cp.FallbackModels = append([]string(nil), j.FallbackModels...)
	}
	if len(j.Payload) > 0 {
		cp.Payload = append(json.RawMessage(nil), j.Payload...)
	}
	if len(j.Result) > 0 {
		cp.Result = append(json.RawMessage(nil), j.Result...)
	}
	return &cp
}

func (q *InMemoryQueue) fireTerminalHooks(job *Job, promoted []*Job) {
	if job == nil {
		return
	}
	q.mu.Lock()
	hooks := make([]func(job *Job, promoted []*Job), len(q.terminalHooks))
	copy(hooks, q.terminalHooks)
	q.mu.Unlock()
	for _, fn := range hooks {
		if fn != nil {
			fn(job, promoted)
		}
	}
}

func normalizeJobDependencies(job *Job) {
	if job == nil {
		return
	}
	if len(job.Payload) > 0 &&
		((job.DependsOnJobID == "" && len(job.DependsOnJobIDs) == 0 && bytes.Contains(job.Payload, []byte("depends_on_job"))) ||
			(job.CallbackURL == "" && bytes.Contains(job.Payload, []byte("callback_url")))) {
		var pm map[string]any
		if json.Unmarshal(job.Payload, &pm) == nil && pm != nil {
			if job.DependsOnJobID == "" {
				if v, ok := pm["depends_on_job_id"].(string); ok && strings.TrimSpace(v) != "" {
					job.DependsOnJobID = strings.TrimSpace(v)
				}
			}
			if len(job.DependsOnJobIDs) == 0 {
				if arr, ok := pm["depends_on_job_ids"].([]any); ok {
					for _, raw := range arr {
						if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
							job.DependsOnJobIDs = append(job.DependsOnJobIDs, strings.TrimSpace(s))
						}
					}
				}
			}
			if job.CallbackURL == "" {
				if v, ok := pm["callback_url"].(string); ok && strings.TrimSpace(v) != "" {
					job.CallbackURL = strings.TrimSpace(v)
				}
			}
		}
	}

	job.DependsOnJobID = strings.TrimSpace(job.DependsOnJobID)
	job.CallbackURL = strings.TrimSpace(job.CallbackURL)

	seen := make(map[string]bool)
	var normalized []string
	if job.DependsOnJobID != "" && job.DependsOnJobID != job.ID {
		seen[job.DependsOnJobID] = true
		normalized = append(normalized, job.DependsOnJobID)
	}
	for _, raw := range job.DependsOnJobIDs {
		d := strings.TrimSpace(raw)
		if d != "" && d != job.ID && !seen[d] {
			seen[d] = true
			normalized = append(normalized, d)
		}
	}
	if len(normalized) > 0 {
		job.DependsOnJobIDs = normalized
		if job.DependsOnJobID == "" {
			job.DependsOnJobID = normalized[0]
		}
	} else {
		job.DependsOnJobID = ""
		job.DependsOnJobIDs = nil
	}
}

func (q *InMemoryQueue) hasActiveReplacementChildLocked(parentID string) bool {
	return q.hasActiveReplacementDescendantLocked(parentID)
}

// isActiveReplacementStatus is retained as a thin status-only helper for callers/tests;
// authoritative parking semantics live in isActiveReplacementChild (STORY-IAZIO-0202).
func isActiveReplacementStatus(status string) bool {
	return isActiveReplacementChild(&Job{Status: status})
}

// isActiveReplacementChild reports whether child still supersedes parent for claim/reconcile.
// Active = pending/leased/claimed/running/blocked/requeued/failed. Only completed/canceled
// children release the parent for superseded-completion or orphan promote (STORY-IAZIO-0202).
func isActiveReplacementChild(child *Job) bool {
	if child == nil {
		return false
	}
	switch strings.TrimSpace(child.Status) {
	case JobStatusPending, JobStatusLeased, JobStatusClaimed, JobStatusRunning,
		JobStatusBlockedByDependency, JobStatusRequeued, JobStatusFailed:
		return true
	default:
		return false
	}
}

func (q *InMemoryQueue) buildChildrenByParentLocked() map[string][]*Job {
	children := make(map[string][]*Job)
	for _, cand := range q.jobs {
		if cand == nil {
			continue
		}
		if pid := strings.TrimSpace(cand.ParentID); pid != "" {
			children[pid] = append(children[pid], cand)
		}
	}
	return children
}

// hasActiveReplacementDescendantLocked reports whether parentID has any active descendant,
// walking through completed/canceled intermediate nodes (review N2 / STORY-IAZIO-0202).
func (q *InMemoryQueue) hasActiveReplacementDescendantLocked(parentID string) bool {
	cleanID := strings.TrimSpace(parentID)
	if cleanID == "" || q.jobs[cleanID] == nil {
		return false
	}
	return q.hasActiveReplacementDescendantWithIndexLocked(cleanID, q.buildChildrenByParentLocked())
}

func (q *InMemoryQueue) hasActiveReplacementDescendantWithIndexLocked(parentID string, children map[string][]*Job) bool {
	cleanID := strings.TrimSpace(parentID)
	if cleanID == "" || q.jobs[cleanID] == nil {
		return false
	}
	if len(children[cleanID]) == 0 {
		return false
	}
	visited := map[string]bool{cleanID: true}
	queue := []string{cleanID}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for _, cand := range children[curr] {
			if cand == nil || visited[cand.ID] {
				continue
			}
			visited[cand.ID] = true
			if isActiveReplacementChild(cand) {
				return true
			}
			// Traverse completed/canceled so active grandchildren still park the root.
			if cand.Status == JobStatusCompleted || cand.Status == JobStatusCanceled {
				queue = append(queue, cand.ID)
			}
		}
	}
	return false
}

func isSuccessfulCompletedChild(job *Job) bool {
	if job == nil || job.Status != JobStatusCompleted {
		return false
	}
	if ok, failed := isJobTerminalCompleted(job); !ok || failed != nil {
		return false
	}
	return true
}

func (q *InMemoryQueue) latestSuccessfulCompletedChildLocked(parentID string) *Job {
	cleanID := strings.TrimSpace(parentID)
	if cleanID == "" {
		return nil
	}
	return q.latestSuccessfulCompletedChildWithIndexLocked(cleanID, q.buildChildrenByParentLocked())
}

func (q *InMemoryQueue) latestSuccessfulCompletedChildWithIndexLocked(parentID string, children map[string][]*Job) *Job {
	cleanID := strings.TrimSpace(parentID)
	if cleanID == "" {
		return nil
	}
	var latest *Job
	for _, cand := range children[cleanID] {
		if cand == nil || !isSuccessfulCompletedChild(cand) {
			continue
		}
		if latest == nil || cand.CreatedAt.After(latest.CreatedAt) {
			latest = cand
		}
	}
	return latest
}

// completeSupersededParentLocked marks a requeued/pending parent completed when its
// replacement child already finished successfully (STORY-IAZIO-0202 RC-6).
func completeSupersededParentLocked(job *Job, child *Job, now time.Time) {
	if job == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	// Snapshot failure telemetry BEFORE clearing fields (review F2 / STORY-IAZIO-0202).
	priorResult := strings.TrimSpace(string(job.Result))
	priorError := strings.TrimSpace(job.Error)
	priorFailCat := strings.TrimSpace(job.FailureCategory)
	priorFailStage := strings.TrimSpace(job.FailureStage)
	priorFailSummary := strings.TrimSpace(job.FailureReasonSummary)
	priorHost := strings.TrimSpace(job.HostID)
	if priorHost == "" {
		priorHost = strings.TrimSpace(job.ClaimedHost)
	}
	priorTool := strings.TrimSpace(job.TargetAgent)
	priorWorkspace := strings.TrimSpace(job.Repo)
	priorLooksFailed := (priorResult != "" &&
		(strings.Contains(priorResult, "test_failure_count") ||
			strings.Contains(priorResult, "failed_tests") ||
			strings.Contains(priorResult, `"has_error":true`))) ||
		(priorError != "" && !strings.Contains(strings.ToLower(priorError), "phantom_or_superseded")) ||
		(priorFailCat != "" && priorFailCat != FailureCategoryNone) ||
		priorFailSummary != ""
	if priorLooksFailed {
		stderrTail := priorResult
		if stderrTail == "" {
			stderrTail = firstNonEmptyTrimmed(priorError, priorFailSummary, priorFailCat)
		}
		job.AttemptHistory = append(job.AttemptHistory, AttemptFailure{
			AttemptNumber:   len(job.AttemptHistory) + 1,
			Host:            priorHost,
			Workspace:       priorWorkspace,
			Tool:            priorTool,
			FailureCategory: priorFailCat,
			FailureStage:    priorFailStage,
			Reason:          "pre_supersede_failure",
			StderrTail:      stderrTail,
			Timestamp:       now.UTC().Format(time.RFC3339),
			TriggerReason:   "superseded_by_completed_replacement_child",
			OutcomeSummary:  firstNonEmptyTrimmed(priorError, priorFailSummary, "pre_supersede_failure"),
		})
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	job.FailureCategory = ""
	job.FailureStage = ""
	job.FailureDirection = ""
	job.FailureService = ""
	job.FailureReasonSummary = ""
	job.Retriable = false
	job.ClaimedHost = ""
	job.HostID = ""
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseAckDeadlineAt = nil
	if job.CompletedAt == nil {
		completedAt := now
		job.CompletedAt = &completedAt
	}
	if child != nil && len(child.Result) > 0 {
		job.Result = append(json.RawMessage(nil), child.Result...)
	} else if len(job.Result) == 0 || !strings.Contains(string(job.Result), "completed") {
		job.Result = json.RawMessage(`{"status":"completed_via_eager_readiness","superseded_by_completed_replacement_child":true}`)
	}
	if job.DispatchDiagnostics != nil {
		job.DispatchDiagnostics.QueueState = "completed"
	} else {
		job.DispatchDiagnostics = &DispatchDiagnostics{QueueState: "completed"}
	}
	if job.doneCh != nil {
		select {
		case <-job.doneCh:
		default:
			close(job.doneCh)
		}
	}
}

func (q *InMemoryQueue) latestReplacementChildLocked(parentID string) *Job {
	return q.latestReplacementChildWithIndexLocked(parentID, q.buildChildrenByParentLocked())
}

func (q *InMemoryQueue) latestReplacementChildWithIndexLocked(parentID string, children map[string][]*Job) *Job {
	cleanID := strings.TrimSpace(parentID)
	if cleanID == "" || q.jobs[cleanID] == nil {
		return nil
	}
	visited := map[string]bool{cleanID: true}
	currID := cleanID
	var latest *Job
	for {
		var nextActive *Job
		var nextBridge *Job // completed/canceled bridge toward an active descendant
		for _, cand := range children[currID] {
			if cand == nil || cand.ID == currID || visited[cand.ID] {
				continue
			}
			if isActiveReplacementChild(cand) {
				if nextActive == nil || cand.CreatedAt.After(nextActive.CreatedAt) {
					nextActive = cand
				}
				continue
			}
			// Walk through completed/canceled bridges so active grandchildren park the root (N2).
			if cand.Status == JobStatusCompleted || cand.Status == JobStatusCanceled {
				if nextBridge == nil || cand.CreatedAt.After(nextBridge.CreatedAt) {
					nextBridge = cand
				}
			}
		}
		next := nextActive
		if next == nil {
			next = nextBridge
		}
		if next == nil {
			break
		}
		visited[next.ID] = true
		if isActiveReplacementChild(next) {
			latest = next
		}
		currID = next.ID
	}
	return latest
}

func isJobTerminalCompleted(dep *Job) (satisfied bool, failedJob *Job) {
	if dep == nil || dep.Status != JobStatusCompleted {
		return false, nil
	}
	if len(dep.Result) > 0 {
		var rm map[string]any
		if json.Unmarshal(dep.Result, &rm) == nil && rm != nil {
			if rawEC, hasEC := rm["exit_code"]; hasEC {
				switch ec := rawEC.(type) {
				case float64:
					if int(ec) != 0 {
						return false, &Job{ID: dep.ID, Status: JobStatusFailed, Error: fmt.Sprintf("exit_code=%d", int(ec))}
					}
				case int:
					if ec != 0 {
						return false, &Job{ID: dep.ID, Status: JobStatusFailed, Error: fmt.Sprintf("exit_code=%d", ec)}
					}
				}
			}
			if rawASE, hasASE := rm["ase_complete"]; hasASE {
				if aseBool, ok := rawASE.(bool); ok && !aseBool {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

func (q *InMemoryQueue) buildLatestCompletedByStoryLocked() map[string]*Job {
	bestByStory := make(map[string]*Job)
	for _, cand := range q.jobs {
		if cand == nil {
			continue
		}
		storyID := strings.ToLower(strings.TrimSpace(cand.StoryID))
		if storyID == "" || storyID == "pending-allocation" || storyID == "pending_harness_creation" {
			continue
		}
		if ok, _ := isJobTerminalCompleted(cand); !ok {
			continue
		}
		if prev := bestByStory[storyID]; prev == nil || cand.CreatedAt.After(prev.CreatedAt) {
			bestByStory[storyID] = cand
		}
	}
	return bestByStory
}

func (q *InMemoryQueue) resolveEffectiveDependencyJobLocked(depID string) (*Job, bool) {
	return q.resolveEffectiveDependencyJobWithIndexLocked(depID, nil, nil)
}

func (q *InMemoryQueue) resolveEffectiveDependencyJobWithIndexLocked(depID string, children map[string][]*Job, bestByStory map[string]*Job) (*Job, bool) {
	dep, exists := q.jobs[depID]
	if !exists || dep == nil {
		return nil, false
	}
	if dep.Status == JobStatusCompleted {
		return dep, true
	}
	if children == nil {
		children = q.buildChildrenByParentLocked()
	}
	if rep := q.latestReplacementChildWithIndexLocked(dep.ID, children); rep != nil {
		return rep, true
	}
	storyID := strings.ToLower(strings.TrimSpace(dep.StoryID))
	if storyID != "" && storyID != "pending-allocation" && storyID != "pending_harness_creation" {
		if bestByStory == nil {
			bestByStory = q.buildLatestCompletedByStoryLocked()
		}
		if bestCompleted := bestByStory[storyID]; bestCompleted != nil && bestCompleted.ID != dep.ID {
			return bestCompleted, true
		}
	}
	return dep, true
}

func (q *InMemoryQueue) areDependenciesSatisfiedLocked(job *Job) (satisfied bool, failedDep *Job) {
	return q.areDependenciesSatisfiedWithIndexLocked(job, nil, nil)
}

func (q *InMemoryQueue) areDependenciesSatisfiedWithIndexLocked(job *Job, children map[string][]*Job, bestByStory map[string]*Job) (satisfied bool, failedDep *Job) {
	if job == nil {
		return true, nil
	}
	normalizeJobDependencies(job)
	if len(job.DependsOnJobIDs) == 0 {
		return true, nil
	}
	for i, depID := range job.DependsOnJobIDs {
		dep, exists := q.resolveEffectiveDependencyJobWithIndexLocked(depID, children, bestByStory)
		if !exists || dep == nil {
			if q.externalDepChecker != nil {
				extExists, extSatisfied, extFailed, extReason := q.externalDepChecker(depID)
				if extExists {
					if extFailed {
						return false, &Job{ID: depID, Status: JobStatusFailed, Error: extReason}
					}
					if !extSatisfied {
						return false, nil
					}
					continue
				}
			}
			return false, nil
		}
		if dep.ID != depID {
			job.DependsOnJobIDs[i] = dep.ID
			if job.DependsOnJobID == depID {
				job.DependsOnJobID = dep.ID
			}
		}
		if dep.Status == JobStatusFailed || dep.Status == JobStatusCanceled {
			return false, dep
		}
		if dep.Status != JobStatusCompleted {
			return false, nil
		}
		if ok, failed := isJobTerminalCompleted(dep); !ok {
			return false, failed
		}
	}
	return true, nil
}

func (q *InMemoryQueue) resolveDependentJobsLocked(ctx context.Context, now time.Time) (promoted []*Job, cascadedFailed []*Job, doneChs []chan struct{}) {
	if now.IsZero() {
		now = q.now()
	}
	var depChildren []*Job
	dependentsByDepID := make(map[string][]*Job)
	for _, j := range q.jobs {
		if j == nil {
			continue
		}
		normalizeJobDependencies(j)
		if len(j.DependsOnJobIDs) == 0 {
			continue
		}
		depChildren = append(depChildren, j)
		for _, depID := range j.DependsOnJobIDs {
			dependentsByDepID[depID] = append(dependentsByDepID[depID], j)
		}
	}
	if len(depChildren) == 0 {
		return nil, nil, nil
	}
	children := q.buildChildrenByParentLocked()
	bestByStory := q.buildLatestCompletedByStoryLocked()
	for {
		changed := false
		for _, child := range depChildren {
			if child == nil {
				continue
			}
			if child.Status == JobStatusRequeued && q.hasActiveReplacementDescendantWithIndexLocked(child.ID, children) {
				if rep := q.latestReplacementChildWithIndexLocked(child.ID, children); rep != nil {
					for _, grandChild := range dependentsByDepID[child.ID] {
						if grandChild == nil || grandChild.ID == child.ID || grandChild.ID == rep.ID {
							continue
						}
						reb := false
						if grandChild.DependsOnJobID == child.ID {
							grandChild.DependsOnJobID = rep.ID
							reb = true
						}
						for idx, dID := range grandChild.DependsOnJobIDs {
							if dID == child.ID {
								grandChild.DependsOnJobIDs[idx] = rep.ID
								reb = true
							}
						}
						if reb {
							dependentsByDepID[rep.ID] = append(dependentsByDepID[rep.ID], grandChild)
							if q.store != nil {
								_ = q.store.SaveQueueJob(ctx, grandChild)
							}
						}
					}
				}
				if child.FailureCategory == FailureCategoryDependencyFailed || strings.HasPrefix(strings.TrimSpace(child.Error), "DEPENDENCY_FAILED") {
					child.Error = ""
					child.FailureCategory = ""
					child.FailureStage = ""
					child.FailureDirection = ""
					child.FailureService = ""
					child.FailureReasonSummary = ""
					child.Retriable = false
					if q.store != nil {
						_ = q.store.SaveQueueJob(ctx, child)
					}
				}
				continue
			}
			isDepFailedState := child.FailureCategory == FailureCategoryDependencyFailed || strings.HasPrefix(strings.TrimSpace(child.Error), "DEPENDENCY_FAILED")
			if child.Status != JobStatusBlockedByDependency && child.Status != JobStatusPending && child.Status != JobStatusRequeued && !(child.Status == JobStatusFailed && isDepFailedState) {
				continue
			}
			satisfied, failedDep := q.areDependenciesSatisfiedWithIndexLocked(child, children, bestByStory)
			if failedDep != nil {
				reason := strings.TrimSpace(failedDep.Error)
				var depErr string
				if reason != "" {
					depErr = fmt.Sprintf("DEPENDENCY_FAILED: upstream job %s %s (%s)", failedDep.ID, failedDep.Status, reason)
				} else {
					depErr = fmt.Sprintf("DEPENDENCY_FAILED: upstream job %s %s", failedDep.ID, failedDep.Status)
				}
				if child.Status != JobStatusBlockedByDependency || child.FailureCategory != FailureCategoryDependencyFailed || child.Error != depErr || child.CompletedAt != nil {
					child.Status = JobStatusBlockedByDependency
					child.Error = depErr
					child.FailureCategory = FailureCategoryDependencyFailed
					child.FailureStage = FailureStageQueueDispatch
					child.FailureDirection = FailureDirectionUpstream
					child.FailureService = "iazio-harness-api"
					child.FailureReasonSummary = child.Error
					child.Retriable = true
					child.CompletedAt = nil
					child.doneCh = make(chan struct{})
					if child.DispatchDiagnostics != nil {
						child.DispatchDiagnostics.QueueState = JobStatusBlockedByDependency
					} else {
						child.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
					}
					if q.store != nil {
						_ = q.store.SaveQueueJob(ctx, child)
					}
					changed = true
				}
			} else if satisfied && (child.Status == JobStatusBlockedByDependency || child.Status == JobStatusRequeued || (child.Status == JobStatusFailed && isDepFailedState) || (child.Status == JobStatusPending && (child.Error != "" || child.FailureCategory != ""))) {
				child.Status = JobStatusPending
				child.Error = ""
				child.FailureCategory = ""
				child.FailureStage = ""
				child.FailureDirection = ""
				child.FailureService = ""
				child.FailureReasonSummary = ""
				child.Retriable = false
				child.CompletedAt = nil
				child.ClaimedHost = ""
				child.HostID = ""
				child.LeaseOwner = ""
				child.LeaseToken = ""
				child.ClaimedAt = nil
				child.HeartbeatAt = nil
				child.LeaseExpiresAt = nil
				child.LeaseAckDeadlineAt = nil
				child.doneCh = make(chan struct{})
				if child.DispatchDiagnostics != nil {
					child.DispatchDiagnostics.QueueState = "queued"
				} else {
					child.DispatchDiagnostics = &DispatchDiagnostics{QueueState: "queued"}
				}
				if q.store != nil {
					_ = q.store.SaveQueueJob(ctx, child)
				}
				promoted = append(promoted, cloneJobSnapshot(child))
				changed = true
			} else if !satisfied && (child.Status == JobStatusPending || child.Status == JobStatusRequeued || (child.Status == JobStatusFailed && isDepFailedState) || (child.Status == JobStatusBlockedByDependency && (child.FailureCategory == FailureCategoryDependencyFailed || child.Error != ""))) {
				child.Status = JobStatusBlockedByDependency
				child.Error = ""
				child.FailureCategory = ""
				child.FailureStage = ""
				child.FailureDirection = ""
				child.FailureService = ""
				child.FailureReasonSummary = ""
				child.CompletedAt = nil
				child.doneCh = make(chan struct{})
				if child.DispatchDiagnostics != nil {
					child.DispatchDiagnostics.QueueState = JobStatusBlockedByDependency
				} else {
					child.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
				}
				if q.store != nil {
					_ = q.store.SaveQueueJob(ctx, child)
				}
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	return promoted, cascadedFailed, doneChs
}

// SetStore binds a durable database backing store to the queue and re-hydrates in-flight jobs.
func (q *InMemoryQueue) SetStore(s QueueStore) error {
	q.mu.Lock()
	q.store = s
	q.lastReapAt = time.Time{}
	q.mu.Unlock()

	if s != nil {
		persisted, err := s.LoadQueueJobs(context.Background())
		if err != nil {
			return err
		}
		q.mu.Lock()
		defer q.mu.Unlock()
		now := q.now()
		for _, j := range persisted {
			if j == nil || IsPhantomEmptyQueueJob(j) {
				if j != nil {
					if deleter, ok := s.(interface {
						DeleteQueueJob(ctx context.Context, jobID string) error
					}); ok {
						_ = deleter.DeleteQueueJob(context.Background(), j.ID)
					}
				}
				continue
			}
			normalizeJobDependencies(j)
			if len(j.Result) > 0 {
				sanitized := SanitizeJobResultSummary(j.Result)
				if !bytes.Equal(sanitized, j.Result) {
					j.Result = sanitized
					_ = s.SaveQueueJob(context.Background(), j)
				}
			}
			PopulateJobConversationFields(j)
			if j.FencingToken > q.fencingSeq {
				q.fencingSeq = j.FencingToken
			}
			if j.LeaseVersion > q.fencingSeq {
				q.fencingSeq = j.LeaseVersion
			}
			if j.Status == JobStatusCompleted && strings.TrimSpace(j.Error) == "" {
				j.FailureCategory = ""
				j.FailureStage = ""
				j.FailureDirection = ""
				j.FailureService = ""
				j.FailureReasonSummary = ""
				j.Retriable = false
				if j.DispatchDiagnostics != nil {
					j.DispatchDiagnostics.QueueState = "completed"
				}
			} else if j.Status == JobStatusFailed && j.FailureCategory == FailureCategoryDependencyFailed && len(j.DependsOnJobIDs) > 0 {
				// STORY-GOV-5256: Restore any cascade-failed dependent jobs from terminal failed history back into active blocked_by_dependency queue state.
				j.Status = JobStatusBlockedByDependency
				j.CompletedAt = nil
				j.Retriable = true
				if j.DispatchDiagnostics != nil {
					j.DispatchDiagnostics.QueueState = JobStatusBlockedByDependency
				} else {
					j.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
				}
				_ = s.SaveQueueJob(context.Background(), j)
			}
			if q.startupGrace > 0 && (j.Status == JobStatusRunning || j.Status == JobStatusClaimed || j.Status == JobStatusLeased) {
				minExpiry := now.Add(q.startupGrace)
				if j.LeaseExpiresAt == nil || j.LeaseExpiresAt.Before(minExpiry) {
					j.LeaseExpiresAt = &minExpiry
				}
			}
			if existing, ok := q.jobs[j.ID]; !ok || existing.CreatedAt.Before(j.CreatedAt) {
				if j.doneCh == nil {
					j.doneCh = make(chan struct{})
					if j.Status == JobStatusCompleted || j.Status == JobStatusFailed || j.Status == JobStatusCanceled {
						close(j.doneCh)
					}
				}
				q.jobs[j.ID] = j
			}
		}
		_, _, depDoneChs := q.resolveDependentJobsLocked(context.Background(), now)
		q.reconcileQueueIntegrityLocked(now)
		q.rebuildActiveJobsLocked()
		for _, ch := range depDoneChs {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}
	return nil
}

// SetAttributionConfig sets the URL and optional client used for relevance attribution dispatches.
func (q *InMemoryQueue) SetAttributionConfig(apiURL string, client *http.Client) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.attributionURL = apiURL
	q.httpClient = client
}

// SetStartupGrace sets the post-startup reconciliation grace window.
func (q *InMemoryQueue) SetStartupGrace(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastReapAt = time.Time{}
	q.startupGrace = d
	q.startupTime = q.now()
	if d > 0 {
		minExpiry := q.startupTime.Add(d)
		for _, j := range q.jobs {
			if j != nil && (j.Status == JobStatusRunning || j.Status == JobStatusClaimed || j.Status == JobStatusLeased) {
				if j.LeaseExpiresAt == nil || j.LeaseExpiresAt.Before(minExpiry) {
					exp := minExpiry
					j.LeaseExpiresAt = &exp
				}
			}
		}
	}
}

// NewInMemoryQueue constructs a new thread-safe InMemoryQueue.
func NewInMemoryQueue() *InMemoryQueue {
	now := time.Now().UTC()
	return &InMemoryQueue{
		jobs:         make(map[string]*Job),
		claimTimeout: DefaultClaimTimeout,
		jobTimeout:   DefaultJobTimeout,
		startupTime:  now,
		nowFunc:      func() time.Time { return time.Now().UTC() },
	}
}

// SetTimeouts overrides default lease and execution timeouts for this queue.
func (q *InMemoryQueue) SetTimeouts(claimTimeout, jobTimeout time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastReapAt = time.Time{}
	if claimTimeout > 0 {
		q.claimTimeout = claimTimeout
	}
	if jobTimeout > 0 {
		q.jobTimeout = jobTimeout
	}
}

// SetNowFunc overrides the clock for testing deterministic lease expiration.
func (q *InMemoryQueue) SetNowFunc(fn func() time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastReapAt = time.Time{}
	if fn != nil {
		q.nowFunc = fn
	}
}

func (q *InMemoryQueue) now() time.Time {
	if q.nowFunc != nil {
		return q.nowFunc()
	}
	return time.Now().UTC()
}

func (q *InMemoryQueue) reapExpiredLocked(now time.Time) []string {
	if now.IsZero() {
		now = q.now()
	}
	if q.startupGrace > 0 && !q.startupTime.IsZero() && now.Sub(q.startupTime) < q.startupGrace {
		return nil
	}
	var expiredIDs []string
	var doneChannels []chan struct{}

	for _, j := range q.jobs {
		if j.Status != JobStatusPending && j.Status != JobStatusClaimed && j.Status != JobStatusLeased && j.Status != JobStatusRunning {
			continue
		}

		// 1. Max execution timeout: from CreatedAt (0 = unbounded, stories immune; skip already-remediated EXECUTION_TIMEOUT pending jobs to stop ping-pong)
		isStory := j.StoryID != "" || j.Type == "story" || j.Type == "refinement"
		if !isStory && q.jobTimeout > 0 && !j.CreatedAt.IsZero() && now.Sub(j.CreatedAt) > q.jobTimeout &&
			(j.Status != JobStatusPending || !strings.HasPrefix(strings.TrimSpace(j.Error), "EXECUTION_TIMEOUT:")) {
			j.Status = JobStatusFailed
			j.Error = "EXECUTION_TIMEOUT: job exceeded max execution duration"
			j.FailureCategory = FailureCategoryBackgroundDrainTimeout
			j.FailureStage = FailureStageBackgroundDrain
			j.FailureReasonSummary = j.Error
			j.Retriable = j.Attempts < DefaultMaxAttempts
			completedAt := now
			j.CompletedAt = &completedAt
			expiredIDs = append(expiredIDs, j.ID)
			if j.doneCh != nil {
				doneChannels = append(doneChannels, j.doneCh)
			}
			continue
		}

		// 1b. Unacknowledged lease expiration (Two-Phase Lease Handshake):
		if j.Status == JobStatusLeased {
			if j.LeaseAckDeadlineAt != nil && !j.LeaseAckDeadlineAt.IsZero() && now.After(*j.LeaseAckDeadlineAt) {
				// Revert to pending without consuming story retry budget when reaped within handshake window
				j.Status = JobStatusPending
				if j.Attempts > 0 && (j.LeaseExpiresAt == nil || !now.After(*j.LeaseExpiresAt)) {
					j.Attempts--
				}
				j.InfraRequeueCount++
				j.LeaseOwner = ""
				j.LeaseToken = ""
				j.LeaseAckDeadlineAt = nil
				j.ClaimedHost = ""
				j.HostID = ""
				j.WorktreePath = ""
				j.ClaimedAt = nil
				j.HeartbeatAt = nil
				j.LeaseExpiresAt = nil
				j.DispatchDiagnostics = &DispatchDiagnostics{
					QueueState: "queued",
				}
				if q.slotReleaser != nil {
					q.pendingSlotReleases = append(q.pendingSlotReleases, j.ID)
				}
				expiredIDs = append(expiredIDs, j.ID)
				continue
			}
		}

		// 2. Claim lease & heartbeat timeout:
		if j.Status == JobStatusClaimed || j.Status == JobStatusRunning {
			hadProgressAfterClaim := (j.HeartbeatAt != nil && j.ClaimedAt != nil && j.HeartbeatAt.After(*j.ClaimedAt)) ||
				j.ToolPID > 0 || j.ActiveConversationID != "" || j.StoryID != ""
			hasActiveHeartbeatOrTool := (j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.IsZero() && !now.After(*j.LeaseExpiresAt)) &&
				hadProgressAfterClaim
			if q.claimTimeout > 0 && j.ClaimedAt != nil && !j.ClaimedAt.IsZero() && now.Sub(*j.ClaimedAt) > q.claimTimeout && !hasActiveHeartbeatOrTool && !hadProgressAfterClaim {
				j.Status = JobStatusFailed
				j.Error = "LEASE_EXPIRED: execution lease expired without completion"
				j.FailureCategory = FailureCategoryBackgroundDrainTimeout
				j.FailureStage = FailureStageBackgroundDrain
				j.FailureReasonSummary = j.Error
				j.Retriable = j.Attempts < DefaultMaxAttempts
				completedAt := now
				j.CompletedAt = &completedAt
				expiredIDs = append(expiredIDs, j.ID)
				if j.doneCh != nil {
					doneChannels = append(doneChannels, j.doneCh)
				}
				continue
			}

			if j.LeaseExpiresAt != nil && !j.LeaseExpiresAt.IsZero() && now.After(*j.LeaseExpiresAt) {
				attemptNum := j.Attempts
				if attemptNum <= 0 {
					attemptNum = len(j.AttemptHistory) + 1
				}
				attemptRecord := AttemptFailure{
					AttemptNumber:   attemptNum,
					Host:            j.HostID,
					Workspace:       j.Repo,
					Tool:            j.TargetAgent,
					FailureCategory: FailureCategoryBackgroundDrainTimeout,
					FailureStage:    FailureStageBackgroundDrain,
					Reason:          "LEASE_EXPIRED",
					ExitCode:        0,
					StderrTail:      "Job execution lease expired before completion",
					Timestamp:       now.UTC().Format(time.RFC3339),
					TriggerReason:   "lease_expired",
					OutcomeSummary:  "Execution lease expired without heartbeat",
				}
				j.AttemptHistory = append(j.AttemptHistory, attemptRecord)

				consecutiveLeaseExpired := false
				if len(j.AttemptHistory) >= 2 {
					prev := j.AttemptHistory[len(j.AttemptHistory)-2]
					if prev.Reason == "LEASE_EXPIRED" {
						consecutiveLeaseExpired = true
					}
				}

				// STORY-GOV-5275: lease/heartbeat expiry is remediable — never permanently abandon
				// unless a true terminal-stop category applies. Keep the SAME job ID pending.
				if j.ResumeConversationID == "" && j.ActiveConversationID != "" {
					j.ResumeConversationID = j.ActiveConversationID
				}
				if strings.TrimSpace(j.StoryID) != "" && strings.TrimSpace(j.HostID) != "" &&
					(strings.TrimSpace(j.ResumeConversationID) != "" || strings.TrimSpace(j.ActiveConversationID) != "") {
					j.TargetHost = j.HostID
				}
				leaseErr := "LEASE_EXPIRED: execution lease expired without completion"
				if consecutiveLeaseExpired {
					leaseErr = "LEASE_EXPIRED: consecutive attempts expired lease without progress"
				} else if j.Attempts >= DefaultMaxAttempts {
					leaseErr = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, retrying with same job id"
				}
				if IsTerminalStopFailure(j.FailureCategory, leaseErr) {
					j.Status = JobStatusFailed
					j.Retriable = false
					j.Error = leaseErr
					j.FailureCategory = FailureCategoryBackgroundDrainTimeout
					j.FailureStage = FailureStageBackgroundDrain
					j.FailureReasonSummary = j.Error
					completedAt := now
					j.CompletedAt = &completedAt
					expiredIDs = append(expiredIDs, j.ID)
					if j.doneCh != nil {
						doneChannels = append(doneChannels, j.doneCh)
					}
				} else {
					resetJobPendingInPlaceLocked(j, leaseErr)
					if q.slotReleaser != nil {
						q.pendingSlotReleases = append(q.pendingSlotReleases, j.ID)
					}
					expiredIDs = append(expiredIDs, j.ID)
				}
				continue
			}
		}
	}

	promoted, _, depDoneChs := q.resolveDependentJobsLocked(context.Background(), now)
	doneChannels = append(doneChannels, depDoneChs...)
	if len(expiredIDs) > 0 || len(promoted) > 0 {
		q.rebuildActiveJobsLocked()
	}
	var reconciledSet map[string]bool
	if reconciledIDs := q.reconcileQueueIntegrityLocked(now); len(reconciledIDs) > 0 {
		reconciledSet = make(map[string]bool, len(reconciledIDs))
		for _, id := range reconciledIDs {
			reconciledSet[id] = true
		}
		expiredIDs = append(expiredIDs, reconciledIDs...)
		_, _, postDoneChs := q.resolveDependentJobsLocked(context.Background(), now)
		doneChannels = append(doneChannels, postDoneChs...)
		q.rebuildActiveJobsLocked()
	}

	for _, ch := range doneChannels {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	if q.store != nil && len(expiredIDs) > 0 {
		savedSet := make(map[string]bool, len(expiredIDs))
		for _, id := range expiredIDs {
			if reconciledSet[id] || savedSet[id] {
				continue
			}
			savedSet[id] = true
			if j, ok := q.jobs[id]; ok && j != nil {
				sctx, cancel := storeContext(context.Background())
				_ = q.store.SaveQueueJob(sctx, j)
				cancel()
			}
		}
	}

	q.lastReapAt = now
	q.lastReapJobCount = len(q.jobs)
	return expiredIDs
}

// resetJobPendingInPlaceLocked returns a job to claimable pending state without changing its ID
// (STORY-GOV-5275). Prevents abandoned original job URLs when remediating failures.
func resetJobPendingInPlaceLocked(job *Job, lastError string) {
	if job == nil {
		return
	}
	job.InfraRequeueCount++
	if job.ResumeConversationID == "" {
		if job.ActiveConversationID != "" {
			job.ResumeConversationID = job.ActiveConversationID
		} else if job.ConversationID != "" {
			job.ResumeConversationID = job.ConversationID
		}
	}
	if strings.EqualFold(strings.TrimSpace(job.AllocationSource), "global_queue_tool_priority") &&
		strings.TrimSpace(job.ResumeConversationID) == "" &&
		strings.TrimSpace(job.ActiveConversationID) == "" {
		job.TargetHost = ""
	}
	job.Status = JobStatusPending
	if strings.TrimSpace(lastError) != "" {
		job.Error = lastError
	} else {
		job.Error = ""
	}
	job.FailureCategory = ""
	job.FailureStage = ""
	job.FailureDirection = ""
	job.FailureService = ""
	job.FailureReasonSummary = ""
	job.Retriable = true
	job.CompletedAt = nil
	job.ClaimedHost = ""
	job.HostID = ""
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseAckDeadlineAt = nil
	job.FencingToken = 0
	job.LeaseVersion = 0
	job.doneCh = make(chan struct{})
	if job.DispatchDiagnostics != nil {
		job.DispatchDiagnostics.QueueState = "queued"
	} else {
		job.DispatchDiagnostics = &DispatchDiagnostics{QueueState: "queued"}
	}
}

func applyASECloseoutRemediationLocked(job *Job, reason string) {
	if job == nil {
		return
	}
	if strings.TrimSpace(job.TargetHost) == "" && strings.TrimSpace(job.HostID) != "" {
		job.TargetHost = job.HostID
	} else if strings.TrimSpace(job.TargetHost) == "" && strings.TrimSpace(job.ClaimedHost) != "" {
		job.TargetHost = job.ClaimedHost
	}
	closeoutDirective := fmt.Sprintf(
		"Resume and complete /autonomous-story-execution closeout (%s): run git-flow land / autopilot-git-flow land --story %s --status-done --record-reviews, ensure feature branch is merged and worktree is clean on main.",
		strings.TrimSpace(reason), strings.TrimSpace(job.StoryID),
	)
	if !strings.Contains(job.NormalizedPrompt, "autopilot-git-flow land") {
		if strings.TrimSpace(job.NormalizedPrompt) == "" {
			job.NormalizedPrompt = closeoutDirective
		} else {
			job.NormalizedPrompt = job.NormalizedPrompt + "\n\n" + closeoutDirective
		}
	}
	if len(job.Payload) > 0 {
		var pm map[string]any
		if json.Unmarshal(job.Payload, &pm) == nil && pm != nil {
			if job.ResumeConversationID != "" {
				pm["resume_conversation_id"] = job.ResumeConversationID
				pm["conversation_id"] = job.ResumeConversationID
			}
			if job.TargetHost != "" {
				pm["target_host"] = job.TargetHost
			}
			if job.WorktreePath != "" {
				pm["worktree_path"] = job.WorktreePath
			}
			pm["ase_closeout_remediation"] = true
			if updated, err := json.Marshal(pm); err == nil {
				job.Payload = updated
			}
		}
	}
	resetJobPendingInPlaceLocked(job, "")
}

func (q *InMemoryQueue) reconcileQueueIntegrityLocked(now time.Time) []string {
	if now.IsZero() {
		now = q.now()
	}
	children := q.buildChildrenByParentLocked()
	var bestByStory map[string]*Job
	// STORY-IAZIO-0202: ACTIVE descendants (incl. through completed bridges) park a parent.
	hasActiveReplacementChild := make(map[string]bool)
	for id, j := range q.jobs {
		if j == nil {
			continue
		}
		if q.hasActiveReplacementDescendantWithIndexLocked(id, children) {
			hasActiveReplacementChild[id] = true
		}
	}

	var reconciled []string
	for _, j := range q.jobs {
		if j == nil {
			continue
		}
		mutated := false
		if j.Status != JobStatusCompleted && strings.Contains(string(j.Result), "completed_via_eager_readiness") {
			j.Status = JobStatusCompleted
			j.Error = ""
			j.FailureCategory = ""
			j.FailureStage = ""
			j.FailureDirection = ""
			j.FailureService = ""
			j.FailureReasonSummary = ""
			j.Retriable = false
			if j.CompletedAt == nil {
				completedAt := now
				j.CompletedAt = &completedAt
			}
			if j.DispatchDiagnostics != nil {
				j.DispatchDiagnostics.QueueState = "completed"
			} else {
				j.DispatchDiagnostics = &DispatchDiagnostics{QueueState: "completed"}
			}
			if j.doneCh != nil {
				select {
				case <-j.doneCh:
				default:
					close(j.doneCh)
				}
			}
			if q.store != nil && j.CompletedAt != nil {
				sctx, cancel := storeContext(context.Background())
				_ = q.store.CompleteQueueJob(sctx, j.ID, JobStatusCompleted, j.Result, "", *j.CompletedAt)
				cancel()
			}
			mutated = true
		} else if j.Status == JobStatusPending && hasActiveReplacementChild[j.ID] {
			j.Status = JobStatusRequeued
			j.HostID = ""
			j.ClaimedHost = ""
			j.WorktreePath = ""
			j.LeaseOwner = ""
			j.LeaseToken = ""
			j.ClaimedAt = nil
			j.HeartbeatAt = nil
			j.LeaseExpiresAt = nil
			j.LeaseAckDeadlineAt = nil
			mutated = true
		} else if j.Status == JobStatusRequeued && !hasActiveReplacementChild[j.ID] {
			if completedChild := q.latestSuccessfulCompletedChildWithIndexLocked(j.ID, children); completedChild != nil {
				// STORY-IAZIO-0202 RC-6: replacement already succeeded — complete parent, do not re-dequeue.
				completeSupersededParentLocked(j, completedChild, now)
				if q.store != nil && j.CompletedAt != nil {
					sctx, cancel := storeContext(context.Background())
					_ = q.store.CompleteQueueJob(sctx, j.ID, JobStatusCompleted, j.Result, "", *j.CompletedAt)
					cancel()
				}
				mutated = true
			} else {
				// STORY-GOV-5275: orphan requeued jobs (no active replacement child) must become claimable again.
				if len(j.DependsOnJobIDs) > 0 {
					if bestByStory == nil {
						bestByStory = q.buildLatestCompletedByStoryLocked()
					}
					satisfied, failedDep := q.areDependenciesSatisfiedWithIndexLocked(j, children, bestByStory)
					if satisfied {
						resetJobPendingInPlaceLocked(j, "")
						mutated = true
					} else {
						j.Status = JobStatusBlockedByDependency
						j.CompletedAt = nil
						if failedDep == nil {
							j.Error = ""
							j.FailureCategory = ""
							j.FailureStage = ""
							j.FailureDirection = ""
							j.FailureService = ""
							j.FailureReasonSummary = ""
						}
						j.doneCh = make(chan struct{})
						if j.DispatchDiagnostics != nil {
							j.DispatchDiagnostics.QueueState = JobStatusBlockedByDependency
						} else {
							j.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
						}
						mutated = true
					}
				} else {
					resetJobPendingInPlaceLocked(j, "")
					mutated = true
				}
			}
		} else if j.Status == JobStatusFailed && !hasActiveReplacementChild[j.ID] && !IsTerminalStopFailure(j.FailureCategory, j.Error+" "+j.FailureReasonSummary) {
			// Remediable failed jobs without a replacement child: promote in-place to pending.
			resetJobPendingInPlaceLocked(j, j.Error)
			mutated = true
		} else if j.Status == JobStatusFailed && !hasActiveReplacementChild[j.ID] && j.InfraRequeueCount == 0 &&
			(strings.TrimSpace(j.ActiveConversationID) != "" || strings.TrimSpace(j.ResumeConversationID) != "" || strings.TrimSpace(j.ConversationID) != "") &&
			(j.FailureCategory == FailureCategoryASEGateIncomplete || strings.HasPrefix(strings.TrimSpace(j.Error), "LEASE_EXPIRED")) {
			applyASECloseoutRemediationLocked(j, j.Error)
			mutated = true
		} else if j.Status == JobStatusPending {
			if completedChild := q.latestSuccessfulCompletedChildWithIndexLocked(j.ID, children); completedChild != nil &&
				(strings.Contains(strings.ToLower(j.Error), "phantom_or_superseded") ||
					strings.Contains(string(j.Result), "phantom_or_superseded")) {
				completeSupersededParentLocked(j, completedChild, now)
				if q.store != nil && j.CompletedAt != nil {
					sctx, cancel := storeContext(context.Background())
					_ = q.store.CompleteQueueJob(sctx, j.ID, JobStatusCompleted, j.Result, "", *j.CompletedAt)
					cancel()
				}
				mutated = true
			} else {
				if j.HostID != "" || j.ClaimedHost != "" || j.LeaseOwner != "" || j.LeaseToken != "" {
					j.HostID = ""
					j.ClaimedHost = ""
					j.LeaseOwner = ""
					j.LeaseToken = ""
					mutated = true
				}
				if strings.EqualFold(strings.TrimSpace(j.AllocationSource), "global_queue_tool_priority") &&
					(strings.TrimSpace(j.ParentID) != "" || j.Attempts > 0 || j.InfraRequeueCount > 0) &&
					strings.TrimSpace(j.ResumeConversationID) == "" &&
					strings.TrimSpace(j.ActiveConversationID) == "" &&
					strings.TrimSpace(j.TargetHost) != "" {
					j.TargetHost = ""
					if len(j.Payload) > 0 {
						var pm map[string]any
						if json.Unmarshal(j.Payload, &pm) == nil && pm != nil {
							if _, hasTH := pm["target_host"]; hasTH {
								pm["target_host"] = ""
								if updated, err := json.Marshal(pm); err == nil {
									j.Payload = updated
								}
							}
						}
					}
					mutated = true
				}
			}
		}
		if mutated {
			q.syncJobActiveStateLocked(j)
			reconciled = append(reconciled, j.ID)
			if q.store != nil {
				sctx, cancel := storeContext(context.Background())
				_ = q.store.SaveQueueJob(sctx, j)
				cancel()
			}
		}
	}
	return reconciled
}

// ReapExpired runs an expiration sweep across all pending and claimed jobs.
func (q *InMemoryQueue) ReapExpired(now time.Time) []string {
	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reapExpiredLocked(now)
}

// StartReaper runs background expiration sweeps periodically until context is cancelled.
// During the initial startup grace window (default 5m), the reaper does not mark jobs as lease_expired,
// allowing re-connecting agents to re-hydrate and extend active leases across API restarts/redeployments.
func (q *InMemoryQueue) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	startedAt := q.now()
	grace := q.startupGrace
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if grace > 0 && now.Sub(startedAt) < grace {
					continue
				}
				q.ReapExpired(now.UTC())
			}
		}
	}()
}

// Enqueue adds a new job to the queue. If ID is empty, a random ID is generated.
func (q *InMemoryQueue) Enqueue(ctx context.Context, job *Job) (*Job, error) {
	if job == nil {
		return nil, errors.New("job must not be nil")
	}

	q.mu.Lock()

	if job.ID == "" {
		job.ID = generateJobID()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = q.now()
	}
	job.doneCh = make(chan struct{})

	if strings.EqualFold(strings.TrimSpace(job.TargetHost), "global") || strings.EqualFold(strings.TrimSpace(job.TargetHost), "auto") {
		job.TargetHost = ""
	}
	if job.AgentAffinity != nil && (strings.EqualFold(strings.TrimSpace(job.AgentAffinity.HostID), "global") || strings.EqualFold(strings.TrimSpace(job.AgentAffinity.HostID), "auto")) {
		job.AgentAffinity.HostID = ""
	}

	normalizeJobDependencies(job)
	PopulateJobConversationFields(job)
	var failedOnEnqueue *Job
	if len(job.DependsOnJobIDs) > 0 {
		satisfied, failedDep := q.areDependenciesSatisfiedLocked(job)
		if failedDep != nil {
			job.Status = JobStatusBlockedByDependency
			reason := strings.TrimSpace(failedDep.Error)
			if reason != "" {
				job.Error = fmt.Sprintf("DEPENDENCY_FAILED: upstream job %s %s (%s)", failedDep.ID, failedDep.Status, reason)
			} else {
				job.Error = fmt.Sprintf("DEPENDENCY_FAILED: upstream job %s %s", failedDep.ID, failedDep.Status)
			}
			job.FailureCategory = FailureCategoryDependencyFailed
			job.FailureStage = FailureStageQueueDispatch
			job.FailureDirection = FailureDirectionUpstream
			job.FailureService = "iazio-harness-api"
			job.FailureReasonSummary = job.Error
			job.Retriable = true
			job.CompletedAt = nil
			job.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
		} else if !satisfied {
			job.Status = JobStatusBlockedByDependency
			job.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
		} else {
			job.Status = JobStatusPending
		}
	} else if job.Status == "" {
		job.Status = JobStatusPending
	}

	// Enforce Unassigned Global Queue Residency: while PENDING or BLOCKED_BY_DEPENDENCY,
	// global queue jobs must remain with HostID == "", ClaimedHost == "", and WorktreePath == "".
	if job.Status == JobStatusPending || job.Status == JobStatusBlockedByDependency {
		job.HostID = ""
		job.ClaimedHost = ""
		if strings.TrimSpace(job.TargetHost) == "" {
			job.WorktreePath = ""
			if len(job.Payload) > 0 {
				var pm map[string]any
				if json.Unmarshal(job.Payload, &pm) == nil && pm != nil {
					if origRepo, ok := pm["repo"].(string); !ok || strings.TrimSpace(origRepo) == "" {
						pm["repo"] = job.Repo
					}
					if sub := ExtractTargetSubmodule(job.Repo, job.Payload); sub != "" {
						pm["target_submodule"] = sub
					}
					pm["worktree_path"] = ""
					pm["host_id"] = ""
					if th, ok := pm["target_host"].(string); ok && (strings.TrimSpace(th) != "" || strings.EqualFold(strings.TrimSpace(th), "global")) {
						pm["target_host"] = ""
					}
					if updated, err := json.Marshal(pm); err == nil {
						job.Payload = updated
					}
				}
			}
		}
	}

	// Store copy
	storedJob := cloneJobSnapshot(job)
	storedJob.doneCh = job.doneCh
	q.jobs[job.ID] = storedJob
	q.syncJobActiveStateLocked(storedJob)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, storedJob)
	}
	jobSnapshot := cloneJobSnapshot(storedJob)
	q.mu.Unlock()

	if failedOnEnqueue != nil {
		q.fireTerminalHooks(failedOnEnqueue, nil)
	}

	var spanOpts []telemetry.SpanOption
	spanOpts = append(spanOpts, telemetry.WithTag("job.id", jobSnapshot.ID))
	if jobSnapshot.TraceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(jobSnapshot.TraceID, ""))
	}
	_, span := telemetry.StartSpan(ctx, "global_queue.enqueue", spanOpts...)
	if span != nil {
		span.End()
	}

	return jobSnapshot, nil
}

// Claim finds the oldest pending job matching hostID and jobTypes, marking it claimed.
func (q *InMemoryQueue) Claim(ctx context.Context, hostID string, jobTypes []string) (*Job, error) {
	return q.ClaimWithWorktrees(ctx, hostID, jobTypes, nil)
}

func isHostLevelToolsUpdateJob(j *Job) bool {
	if j == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(j.Type), "agent.tools.update") ||
		strings.EqualFold(strings.TrimSpace(j.Repo), "fleet")
}

// ClaimWithWorktrees finds the oldest eligible pending job matching hostID, jobTypes, and available idleWorktrees,
// enforcing the Eager Update Pre-Claim Barrier, dependency gating, submodule slot family normalization,
// concurrent submodule mutation guard, and atomic worktree slot reservation.
func (q *InMemoryQueue) ClaimWithWorktrees(ctx context.Context, hostID string, jobTypes []string, idleWorktrees []string) (*Job, error) {
	hostEligibleForRepoJobs := true
	incompleteASEBlockedReason := ""
	if hostID != "" {
		q.mu.Lock()
		checker := q.hostEligibilityChecker
		q.mu.Unlock()
		if checker != nil {
			if eligible, reason := checker(hostID); !eligible {
				upperReason := strings.ToUpper(strings.TrimSpace(reason))
				if strings.Contains(upperReason, "DRIFT_DETECTED") {
					hostEligibleForRepoJobs = false
				} else if strings.Contains(upperReason, "INCOMPLETE ASE STORY") {
					hostEligibleForRepoJobs = false
					incompleteASEBlockedReason = upperReason
				} else {
					return nil, nil
				}
			}
		}
	}

	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	q.reapExpiredLocked(now)
	_, _, depDoneChs := q.resolveDependentJobsLocked(ctx, now)
	for _, ch := range depDoneChs {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	typeFilter := make(map[string]bool)
	for _, t := range jobTypes {
		typeFilter[t] = true
	}

	// Track active submodule mutations across leased/claimed/running jobs so we do not
	// concurrently mutate the exact same target submodule across hosts unless explicitly allowed.
	activeSubmodules := make(map[string]bool)
	for _, active := range q.jobs {
		if active == nil {
			continue
		}
		if active.Status == JobStatusLeased || active.Status == JobStatusClaimed || active.Status == JobStatusRunning {
			if sub := ExtractTargetSubmodule(active.Repo, active.Payload); sub != "" {
				activeSubmodules[sub] = true
			}
		}
	}

	// Sort pending candidates by CreatedAt ascending so FIFO order is deterministic.
	var candidates []*Job
	for _, j := range q.jobs {
		if j == nil || j.Status != JobStatusPending {
			continue
		}
		candidates = append(candidates, j)
	}
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].CreatedAt.Equal(candidates[k].CreatedAt) {
			return candidates[i].ID < candidates[k].ID
		}
		return candidates[i].CreatedAt.Before(candidates[k].CreatedAt)
	})

	var oldest *Job
	var boundWorktree string
	for _, j := range candidates {
		isToolsUpdate := isHostLevelToolsUpdateJob(j)
		isIncompleteStoryOwner := incompleteASEBlockedReason != "" &&
			strings.TrimSpace(j.StoryID) != "" &&
			strings.Contains(incompleteASEBlockedReason, strings.ToUpper(strings.TrimSpace(j.StoryID)))
		if !hostEligibleForRepoJobs && !isToolsUpdate && !isIncompleteStoryOwner {
			continue
		}
		if j.DependsOnJobID != "" || len(j.DependsOnJobIDs) > 0 {
			satisfied, failedDep := q.areDependenciesSatisfiedLocked(j)
			if failedDep != nil || !satisfied {
				if failedDep == nil && !satisfied {
					j.Status = JobStatusBlockedByDependency
				}
				continue
			}
		}
		if len(typeFilter) > 0 && !typeFilter[j.Type] {
			continue
		}
		if j.TargetHost != "" && hostID != "" && j.TargetHost != hostID {
			continue
		}
		if j.AgentAffinity != nil && j.AgentAffinity.HostID != "" && hostID != "" && j.AgentAffinity.HostID != hostID {
			continue
		}

		matchedWT := ""
		if !isToolsUpdate && !IsHostLevelNonWorktreeJob(j.Type, j.Repo) {
			submod := ExtractTargetSubmodule(j.Repo, j.Payload)
			if submod != "" && activeSubmodules[submod] && !AllowConcurrentSubmoduleMutation(j.Payload) {
				continue
			}

			slotSubmod := submod
			if isIncompleteStoryOwner && slotSubmod == "" {
				slotSubmod = "story:" + strings.TrimSpace(j.StoryID)
			}
			slotFamily := NormalizeToSuperprojectSlotFamily(j.Repo)
			if q.slotAllocator != nil && hostID != "" {
				wt, ok := q.slotAllocator(hostID, j.ID, slotFamily, slotSubmod, j.TargetAgent, idleWorktrees)
				if !ok {
					continue
				}
				matchedWT = wt
			} else if len(idleWorktrees) > 0 {
				found := false
				for _, wt := range idleWorktrees {
					trimmedWT := strings.TrimSpace(wt)
					if trimmedWT == "" {
						continue
					}
					if NormalizeToSuperprojectSlotFamily(trimmedWT) == slotFamily {
						matchedWT = trimmedWT
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
		}

		oldest = j
		boundWorktree = matchedWT
		break
	}

	if oldest == nil {
		return nil, nil
	}

	q.fencingSeq++
	oldest.FencingToken = q.fencingSeq
	oldest.Attempts++
	oldest.Status = JobStatusLeased
	oldest.LeaseToken = fmt.Sprintf("lt-%s-%d", oldest.ID, now.UnixNano())
	ackDeadline := now.Add(DefaultLeaseAckDeadline)
	oldest.LeaseAckDeadlineAt = &ackDeadline
	if boundWorktree != "" {
		oldest.WorktreePath = boundWorktree
	}
	if oldest.ResumeConversationID == "" {
		if oldest.InitialConversationID != "" {
			oldest.ResumeConversationID = oldest.InitialConversationID
		} else if oldest.ActiveConversationID != "" {
			oldest.ResumeConversationID = oldest.ActiveConversationID
		} else if len(oldest.ConversationIDs) > 0 {
			oldest.ResumeConversationID = oldest.ConversationIDs[0]
		}
	}
	if len(oldest.Payload) > 0 {
		var payloadMap map[string]any
		if err := json.Unmarshal(oldest.Payload, &payloadMap); err == nil && payloadMap != nil {
			changedPayload := false
			if oldest.ResumeConversationID != "" {
				payloadMap["resume_conversation_id"] = oldest.ResumeConversationID
				payloadMap["conversation_id"] = oldest.ResumeConversationID
				changedPayload = true
			}
			if boundWorktree != "" {
				payloadMap["worktree_path"] = boundWorktree
				changedPayload = true
			}
			if hostID != "" {
				payloadMap["host_id"] = hostID
				changedPayload = true
			}
			if changedPayload {
				if updated, err := json.Marshal(payloadMap); err == nil {
					oldest.Payload = updated
				}
			}
		}
	}
	if oldest.TraceID != "" && oldest.Traceparent == "" {
		oldest.Traceparent = fmt.Sprintf("00-%s-%016x-01", oldest.TraceID, now.UnixNano())
	}
	if hostID != "" {
		oldest.ClaimedHost = hostID
		oldest.HostID = hostID
	}
	oldest.ClaimedAt = &now
	oldest.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	oldest.LeaseExpiresAt = &expires
	oldest.LeaseOwner = hostID
	oldest.LeaseVersion = oldest.FencingToken
	PopulateJobConversationFields(oldest)
	q.syncJobActiveStateLocked(oldest)
	if q.store != nil {
		if err := q.store.SaveQueueJob(ctx, oldest); err != nil {
			log.Printf("[dispatch-queue] ERROR: failed to persist claimed job %s: %v", oldest.ID, err)
			return nil, fmt.Errorf("persist claimed job %s: %w", oldest.ID, err)
		}
	}

	var claimSpanOpts []telemetry.SpanOption
	claimSpanOpts = append(claimSpanOpts, telemetry.WithTag("job.id", oldest.ID), telemetry.WithTag("host.id", hostID))
	if oldest.TraceID != "" {
		claimSpanOpts = append(claimSpanOpts, telemetry.WithParentTrace(oldest.TraceID, ""))
	}
	_, claimSpan := telemetry.StartSpan(ctx, "global_queue.lease_and_dispatch", claimSpanOpts...)
	if claimSpan != nil {
		claimSpan.End()
	}

	return cloneJobSnapshot(oldest), nil
}

// Decline resets a leased, claimed, or busy-abandoned job back to PENDING with HostID == "" and WorktreePath == ""
// so any free host can immediately claim it without consuming retry budget.
func (q *InMemoryQueue) Decline(ctx context.Context, jobID string, reason string) (*Job, error) {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists || job == nil {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		snap := cloneJobSnapshot(job)
		q.mu.Unlock()
		return snap, nil
	}

	if job.Attempts > 0 && (job.Status == JobStatusLeased || strings.EqualFold(strings.TrimSpace(reason), "REPO_BUSY") || strings.EqualFold(strings.TrimSpace(reason), "process_disappeared_on_host") || strings.Contains(strings.ToUpper(reason), "DECLINE")) {
		job.Attempts--
	}
	if job.ResumeConversationID == "" && job.ActiveConversationID != "" {
		job.ResumeConversationID = job.ActiveConversationID
	}
	preserveHostForStory := strings.EqualFold(strings.TrimSpace(reason), "process_disappeared_on_host") &&
		strings.TrimSpace(job.StoryID) != "" && strings.TrimSpace(job.HostID) != ""
	prevHost := job.HostID
	job.Status = JobStatusPending
	job.HostID = ""
	job.ClaimedHost = ""
	job.WorktreePath = ""
	if preserveHostForStory {
		job.TargetHost = prevHost
	} else {
		job.TargetHost = ""
		if job.AgentAffinity != nil {
			job.AgentAffinity.HostID = ""
		}
	}
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.ClaimedAt = nil
	job.AckedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseAckDeadlineAt = nil
	if len(job.Payload) > 0 {
		var pm map[string]any
		if json.Unmarshal(job.Payload, &pm) == nil && pm != nil {
			pm["worktree_path"] = ""
			pm["host_id"] = ""
			pm["target_host"] = ""
			if updated, err := json.Marshal(pm); err == nil {
				job.Payload = updated
			}
		}
	}
	if job.DispatchDiagnostics == nil {
		job.DispatchDiagnostics = &DispatchDiagnostics{}
	}
	job.DispatchDiagnostics.QueueState = "queued"
	q.syncJobActiveStateLocked(job)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}
	jobSnapshot := cloneJobSnapshot(job)
	releaser := q.slotReleaser
	q.mu.Unlock()

	if releaser != nil {
		releaser(jobID)
	}
	return jobSnapshot, nil
}

// Heartbeat extends the lease expiration of an in-flight claimed job, validating the worker's fencing token.
func (q *InMemoryQueue) Heartbeat(ctx context.Context, jobID string, fencingToken int64) (*Job, error) {
	return q.HeartbeatWithStory(ctx, jobID, fencingToken, "")
}

// HeartbeatWithStory extends the lease expiration and optionally binds an allocated story ID atomically.
func (q *InMemoryQueue) HeartbeatWithStory(ctx context.Context, jobID string, fencingToken int64, storyID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	reconciledFromOutage := false
	if (job.Status == JobStatusFailed && isReconcilableFailure(job.Error+" "+job.FailureCategory+" "+job.FailureReasonSummary)) ||
		(job.Status == JobStatusPending && fencingToken > 0) {
		job.Status = JobStatusRunning
		if fencingToken > 0 {
			job.FencingToken = fencingToken
			job.LeaseVersion = fencingToken
		}
		job.Error = ""
		job.FailureCategory = ""
		job.FailureStage = ""
		job.FailureDirection = ""
		job.FailureService = ""
		job.FailureReasonSummary = ""
		job.Retriable = false
		job.CompletedAt = nil
		if job.DispatchDiagnostics != nil {
			job.DispatchDiagnostics.QueueState = "running"
		}
		job.doneCh = make(chan struct{})
		reconciledFromOutage = true
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		return nil, ErrLeaseFenced
	}
	if job.Status != JobStatusClaimed && job.Status != JobStatusLeased && job.Status != JobStatusRunning {
		return nil, fmt.Errorf("job %s is not currently claimed (status: %s)", jobID, job.Status)
	}

	now := q.now()
	statusTransitioned := reconciledFromOutage
	if job.Status == JobStatusLeased || job.Status == JobStatusClaimed {
		job.Status = JobStatusRunning
		job.LeaseAckDeadlineAt = nil
		if job.AckedAt == nil {
			job.AckedAt = &now
		}
		statusTransitioned = true
	} else if job.LeaseAckDeadlineAt != nil {
		job.LeaseAckDeadlineAt = nil
		statusTransitioned = true
	}

	storyUpdated := false
	cleanStoryID := strings.TrimSpace(storyID)
	if cleanStoryID != "" && cleanStoryID != "pending-allocation" && cleanStoryID != "pending" {
		job.StoryID = cleanStoryID
		job.StoryAllocationStatus = "allocated"
		storyUpdated = true
		if len(job.Payload) > 0 {
			var payloadMap map[string]any
			if err := json.Unmarshal(job.Payload, &payloadMap); err == nil {
				payloadMap["story_id"] = cleanStoryID
				if updated, err := json.Marshal(payloadMap); err == nil {
					job.Payload = updated
				}
			}
		}
	}

	job.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	job.LeaseExpiresAt = &expires
	PopulateJobConversationFields(job)
	q.syncJobActiveStateLocked(job)
	q.lastReapAt = time.Time{}
	if q.store != nil {
		sctx, cancel := storeContext(ctx)
		defer cancel()
		if err := q.store.UpdateQueueJobLease(sctx, job.ID, job.LeaseOwner, job.FencingToken, now, expires); err != nil {
			if isTransientPersistenceTimeout(err) {
				log.Printf("[dispatch-queue] WARN: transient DB timeout persisting heartbeat lease for %s (retaining in-memory lease extension): %v", jobID, err)
			} else {
				log.Printf("[dispatch-queue] ERROR: failed to persist heartbeat lease for %s: %v", jobID, err)
				return nil, fmt.Errorf("persist heartbeat lease: %w", err)
			}
		}
		if statusTransitioned || storyUpdated {
			_ = q.store.SaveQueueJob(sctx, job)
		}
	}
	return cloneJobSnapshot(job), nil
}

func storeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}

func isTransientPersistenceTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "deadlock detected")
}

// NormalizeStoryTitle strips any duplicate or chained "Execute story <ID>:" prefix from title
// so repeated story bindings across retries or story rebindings remain idempotent.
func NormalizeStoryTitle(storyID, title string) (string, string) {
	cleanID := strings.TrimSpace(storyID)
	cleanTitle := strings.TrimSpace(title)
	if cleanID == "" {
		return cleanTitle, cleanTitle
	}
	prefixColon := "execute story " + strings.ToLower(cleanID) + ":"
	prefixBare := "execute story " + strings.ToLower(cleanID)
	for {
		if loc := executeStoryPrefixRegex.FindStringIndex(cleanTitle); loc != nil && loc[0] == 0 {
			rest := strings.TrimSpace(cleanTitle[loc[1]:])
			rest = strings.TrimLeft(rest, ":- ")
			cleanTitle = strings.TrimSpace(rest)
			continue
		}
		lower := strings.ToLower(cleanTitle)
		if strings.HasPrefix(lower, prefixColon) {
			cleanTitle = strings.TrimSpace(cleanTitle[len(prefixColon):])
			continue
		}
		if strings.HasPrefix(lower, prefixBare) {
			rest := strings.TrimSpace(cleanTitle[len(prefixBare):])
			rest = strings.TrimLeft(rest, ":- ")
			cleanTitle = strings.TrimSpace(rest)
			continue
		}
		break
	}
	if cleanTitle == "" {
		return "", "Execute story " + cleanID
	}
	return cleanTitle, fmt.Sprintf("Execute story %s: %s", cleanID, cleanTitle)
}

// ExtendLeasesForHost refreshes heartbeat timestamps and lease deadlines for all active jobs
// associated with the reporting host, preventing lease expiration timeouts while the host is healthy.
func (q *InMemoryQueue) ExtendLeasesForHost(ctx context.Context, hostID string, activeJobIDs []string) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if hostID == "" {
		return nil, nil
	}

	activeSet := make(map[string]bool, len(activeJobIDs))
	for _, id := range activeJobIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			activeSet[trimmed] = true
		}
	}

	now := q.now()
	var extended []string
	for _, j := range q.jobs {
		if j == nil {
			continue
		}
		// Match jobs allocated to this host (or explicitly reported active by this host after transient reclaim)
		hostMatch := (j.ClaimedHost == hostID || j.HostID == hostID || (j.AgentAffinity != nil && j.AgentAffinity.HostID == hostID) || (activeSet[j.ID] && (j.ClaimedHost == "" || j.ClaimedHost == hostID)))
		if !hostMatch {
			continue
		}

		reconciled := false
		// Auto-reconcile jobs marked failed or reverted to pending due to lease expiration/timeout if reported active by agent
		if activeSet[j.ID] && ((j.Status == JobStatusFailed && isReconcilableFailure(j.Error+" "+j.FailureCategory+" "+j.FailureReasonSummary)) || (j.Status == JobStatusPending && (len(j.AttemptHistory) > 0 || j.InfraRequeueCount > 0 || j.FencingToken > 0))) {
			j.Status = JobStatusRunning
			j.ClaimedHost = hostID
			j.HostID = hostID
			j.Error = ""
			j.FailureCategory = ""
			j.FailureStage = ""
			j.FailureDirection = ""
			j.FailureService = ""
			j.FailureReasonSummary = ""
			j.Retriable = false
			j.CompletedAt = nil
			if j.DispatchDiagnostics != nil {
				j.DispatchDiagnostics.QueueState = "running"
			}
			j.doneCh = make(chan struct{})
			reconciled = true
		}

		// Only extend active jobs (leased, claimed, running)
		if j.Status != JobStatusClaimed && j.Status != JobStatusLeased && j.Status != JobStatusRunning {
			continue
		}

		// Require membership in the reported activeSet once the job is past a brief 30s initial claim grace window.
		// If a host reboots or loses its runner process and sends heartbeats with active_jobs: [] (len(activeSet) == 0),
		// do NOT extend the lease of dead RUNNING jobs forever.
		if !activeSet[j.ID] {
			if len(activeSet) > 0 || j.ClaimedAt == nil || now.Sub(*j.ClaimedAt) >= 30*time.Second {
				continue
			}
		}

		expires := now.Add(DefaultHeartbeatTimeout)
		if q.store != nil {
			sctx, cancel := storeContext(ctx)
			if err := q.store.UpdateQueueJobLease(sctx, j.ID, hostID, j.FencingToken, now, expires); err != nil {
				cancel()
				if isTransientPersistenceTimeout(err) {
					log.Printf("[dispatch-queue] WARN: transient DB timeout persisting lease update for %s (retaining in-memory lease extension): %v", j.ID, err)
				} else {
					log.Printf("[dispatch-queue] ERROR: failed to persist lease update for %s: %v", j.ID, err)
					continue
				}
			} else {
				cancel()
			}
		}

		j.HeartbeatAt = &now
		j.LeaseExpiresAt = &expires
		j.LeaseOwner = hostID
		j.LeaseVersion = j.FencingToken
		if j.Status == JobStatusLeased || j.Status == JobStatusClaimed {
			j.Status = JobStatusRunning
			reconciled = true
		}
		if reconciled && q.store != nil {
			sctx, cancel := storeContext(ctx)
			_ = q.store.SaveQueueJob(sctx, j)
			cancel()
		}
		extended = append(extended, j.ID)
	}

	for jobID := range activeSet {
		if jobID == "job-cp-fallback" || strings.HasPrefix(jobID, "job-reap-") || strings.HasPrefix(jobID, "job-missing-") {
			delete(q.jobs, jobID)
			continue
		}
		if existing, exists := q.jobs[jobID]; exists {
			if existing != nil && existing.Type == "" && existing.NormalizedPrompt == "" && len(existing.Payload) == 0 && !existing.CreatedAt.IsZero() && now.Sub(existing.CreatedAt) > 10*time.Minute {
				existing.Status = JobStatusCanceled
				CompletedAt := now
				existing.CompletedAt = &CompletedAt
				if q.store != nil {
					sctx, cancel := storeContext(ctx)
					_ = q.store.SaveQueueJob(sctx, existing)
					cancel()
				}
			}
			continue
		}
		if _, exists := q.jobs[jobID]; !exists {
			var rehydrated *Job
			if q.store != nil {
				sctx, cancel := storeContext(ctx)
				if loaded, err := q.store.LoadQueueJobs(sctx); err == nil {
					for _, lj := range loaded {
						if lj.ID == jobID {
							rehydrated = lj
							break
						}
					}
				}
				cancel()
			}
			if rehydrated != nil && (rehydrated.Status == JobStatusCompleted || rehydrated.Status == JobStatusCanceled || rehydrated.Status == JobStatusRequeued) {
				continue
			}
			expires := now.Add(DefaultHeartbeatTimeout)
			if rehydrated != nil {
				rehydrated.Status = JobStatusRunning
				rehydrated.ClaimedHost = hostID
				rehydrated.HostID = hostID
				rehydrated.HeartbeatAt = &now
				rehydrated.LeaseExpiresAt = &expires
				rehydrated.LeaseOwner = hostID
				rehydrated.LeaseVersion = rehydrated.FencingToken
				rehydrated.Error = ""
				rehydrated.FailureCategory = ""
				rehydrated.FailureStage = ""
				rehydrated.FailureDirection = ""
				rehydrated.FailureService = ""
				rehydrated.FailureReasonSummary = ""
				rehydrated.Retriable = false
				rehydrated.CompletedAt = nil
				if rehydrated.doneCh == nil {
					rehydrated.doneCh = make(chan struct{})
				}
				q.jobs[jobID] = rehydrated
			} else {
				q.jobs[jobID] = &Job{
					ID:             jobID,
					Status:         JobStatusRunning,
					ClaimedHost:    hostID,
					HostID:         hostID,
					HeartbeatAt:    &now,
					LeaseExpiresAt: &expires,
					LeaseOwner:     hostID,
					CreatedAt:      now,
					doneCh:         make(chan struct{}),
				}
			}
			if q.store != nil {
				sctx, cancel := storeContext(ctx)
				_ = q.store.SaveQueueJob(sctx, q.jobs[jobID])
				if err := q.store.UpdateQueueJobLease(sctx, jobID, hostID, q.jobs[jobID].FencingToken, now, expires); err != nil {
					if isTransientPersistenceTimeout(err) {
						log.Printf("[dispatch-queue] WARN: transient DB timeout persisting rehydrated lease for %s (retaining in-memory lease extension): %v", jobID, err)
					} else {
						log.Printf("[dispatch-queue] ERROR: failed to persist rehydrated lease for %s: %v", jobID, err)
						cancel()
						delete(q.jobs, jobID)
						continue
					}
				}
				cancel()
			}
			extended = append(extended, jobID)
		}
	}

	q.rebuildActiveJobsLocked()
	return extended, nil
}

// Complete updates a job with final result or error and signals any awaiting callers.
func (q *InMemoryQueue) Complete(ctx context.Context, jobID string, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	fencingToken := int64(0)
	if exists && job != nil {
		fencingToken = job.FencingToken
	}
	q.mu.Unlock()
	return q.CompleteWithFencing(ctx, jobID, fencingToken, result, errStr)
}

// extractDispatchFailureTail strips intermediate multi-turn `[agy step #N]` transcript lines
// from an error string so historical step logs (e.g. story YAML `authorized_slugs:`) do not
// poison terminal failure classification.
func extractDispatchFailureTail(errStr string) string {
	trimmed := strings.TrimSpace(errStr)
	if !strings.Contains(trimmed, "[agy step #") || !strings.Contains(trimmed, "\n") {
		return trimmed
	}
	lines := strings.Split(trimmed, "\n")
	var nonStepLines []string
	var lastStepLines []string
	for _, line := range lines {
		l := strings.TrimSpace(line)
		if l == "" {
			continue
		}
		if strings.Contains(l, "[agy step #") {
			lastStepLines = append(lastStepLines, l)
			if len(lastStepLines) > 3 {
				lastStepLines = lastStepLines[len(lastStepLines)-3:]
			}
			continue
		}
		if strings.HasPrefix(l, "iazio-harness prompt run failed:") && strings.Contains(l, "ALLOCATED_STORY_ID:") {
			continue
		}
		nonStepLines = append(nonStepLines, l)
	}
	tail := append(lastStepLines, nonStepLines...)
	if len(tail) == 0 {
		return lines[len(lines)-1]
	}
	return strings.Join(tail, "\n")
}

func hasQuotaOrAuthFailureToken(lower string) bool {
	for _, tok := range []string{
		"429",
		"resource_exhausted",
		"resourceexhausted",
		"quota",
		"rate limit",
		"token limit",
		"unauthorized",
		"unauthenticated",
		"auth_error",
		"authentication failed",
		"auth expired",
		"auth expiration",
		"invalid_api_key",
		"permission_denied",
		"tool_quota_or_auth",
		"preflight_or_early_tool_exit",
	} {
		if strings.Contains(lower, tok) {
			return true
		}
	}
	if strings.Contains(lower, "auth") {
		cleaned := strings.ReplaceAll(lower, "authorized_slugs", "")
		cleaned = strings.ReplaceAll(cleaned, "authorized", "")
		cleaned = strings.ReplaceAll(cleaned, "author", "")
		return strings.Contains(cleaned, "auth")
	}
	return false
}

// IsTerminalStopFailure reports whether a failure category/error must STOP auto-remediation
// (STORY-GOV-5275). Only no-AI-credits / quota-or-auth exhaustion and infrastructure-down
// categories are terminal. Broken/deceptive local implementations must keep remediating.
func IsTerminalStopFailure(category, errStr string) bool {
	cat := strings.ToUpper(strings.TrimSpace(category))
	lower := strings.ToLower(errStr)
	switch cat {
	case FailureCategoryToolQuotaOrAuth:
		return true
	case FailureCategoryInfraDown, FailureCategoryUpstreamSCMUnavailable, FailureCategoryUpstreamCIUnavailable,
		FailureCategoryDownstreamDBUnavailable, FailureCategoryDownstreamIngestUnavailable:
		return true
	}
	if hasQuotaOrAuthFailureToken(strings.ToLower(extractDispatchFailureTail(errStr))) {
		return true
	}
	if strings.Contains(lower, "infrastructure down") || strings.Contains(lower, "infra_down") ||
		strings.Contains(lower, "no ai credit") || strings.Contains(lower, "out of credits") ||
		strings.Contains(lower, "quota exhausted") {
		return true
	}
	return false
}

// ClassifyDispatchFailureWithDirection maps an error string to canonical failure taxonomy including upstream/downstream service direction.
func ClassifyDispatchFailureWithDirection(errStr string) (category, stage, direction, service, summary string, retriable bool) {
	lower := strings.ToLower(extractDispatchFailureTail(errStr))
	switch {
	case strings.Contains(lower, "hard block:") || strings.Contains(lower, "autonomous self-confirmation detected") || strings.Contains(lower, "iazio-pre-tool-hook") || strings.Contains(lower, "policy_hook_blocked"):
		return FailureCategoryPolicyHookBlocked, FailureStageToolExecution, FailureDirectionLocalExecution, "iazio-pre-tool-hook", "Pre-tool safety hook blocked forbidden autonomous tool call", true
	case strings.Contains(lower, "claim") || strings.Contains(lower, "startup_heartbeat") || strings.Contains(lower, "unacknowledged lease"):
		return FailureCategoryDispatchClaimTimeout, FailureStageQueueDispatch, FailureDirectionDownstream, "iazio-agent", "Agent lease claim timed out or lost before initial acknowledgement", true
	case hasQuotaOrAuthFailureToken(lower):
		// Terminal stop: no AI credits / quota / auth exhaustion (STORY-GOV-5275).
		return FailureCategoryToolQuotaOrAuth, FailureStageToolExecution, FailureDirectionUpstream, "antigravity-llm-provider", "Tool startup failed due to quota exhaustion, rate limit, or auth expiration", false
	case strings.Contains(lower, "dirty_worktree_post_exit") || strings.Contains(lower, "post_exit") || strings.Contains(lower, "unmerged_feature_branch") || strings.Contains(lower, "unmerged feature branch") || strings.Contains(lower, "incomplete_ase_flow") || strings.Contains(lower, "unpushed_commits") || strings.Contains(lower, "ase_completion_gate_violation") || strings.Contains(lower, "ase_gate_incomplete"):
		return FailureCategoryASEGateIncomplete, FailureStageCompletionGate, FailureDirectionLocalExecution, "autopilot", "Post-execution gate: child engine exited before completing git-flow landing or left uncommitted worktree changes", true
	case strings.Contains(lower, "api.github.com") || (strings.Contains(lower, "github.com") && (strings.Contains(lower, "502") || strings.Contains(lower, "503") || strings.Contains(lower, "504") || strings.Contains(lower, "connection refused"))):
		return FailureCategoryUpstreamSCMUnavailable, FailureStageCompletionGate, FailureDirectionUpstream, "github-api", "Upstream GitHub SCM API unavailable during delivery", false
	case strings.Contains(lower, "infrastructure down") || strings.Contains(lower, "infra_down") || strings.Contains(lower, "host unreachable") || strings.Contains(lower, "no route to host"):
		return FailureCategoryInfraDown, FailureStageQueueDispatch, FailureDirectionDownstream, "iazio-infra", "Infrastructure down: host or dependent service unreachable", false
	case strings.Contains(lower, "jenkins") || strings.Contains(lower, "sonarqube"):
		return FailureCategoryUpstreamCIUnavailable, FailureStageCompletionGate, FailureDirectionUpstream, "jenkins-ci", "Upstream CI/SonarQube pipeline unavailable during verification", false
	case strings.Contains(lower, "postgres") || strings.Contains(lower, "pgvector") || strings.Contains(lower, "sqlstate"):
		return FailureCategoryDownstreamDBUnavailable, FailureStageQueueDispatch, FailureDirectionDownstream, "postgresql", "Downstream PostgreSQL database unavailable", false
	case strings.Contains(lower, "iazio-harvester-api") || strings.Contains(lower, "iazio-api") || strings.Contains(lower, "iazio-harvester") || strings.Contains(lower, "/v1/ingest"):
		return FailureCategoryDownstreamIngestUnavailable, FailureStageCompletionGate, FailureDirectionDownstream, "iazio-harvester-api", "Downstream ingestion API or harvester unavailable", false
	case strings.Contains(lower, "crash") || strings.Contains(lower, "exit status") || strings.Contains(lower, "sigsegv") || strings.Contains(lower, "binary"):
		return FailureCategoryToolCrash, FailureStageToolExecution, FailureDirectionLocalExecution, "iazio-harness", "AI agent tool process crashed or exited non-zero during early execution", true
	case strings.Contains(lower, "worktree") || strings.Contains(lower, "workspace_prep") || strings.Contains(lower, "lock") || strings.Contains(lower, "porcelain") || strings.Contains(lower, "submodule") || strings.Contains(lower, "dirty"):
		return FailureCategoryWorkspacePrepError, FailureStageWorkspacePrep, FailureDirectionLocalExecution, "iazio-agent", "Workspace preparation conflict: dirty worktree or stale git locks", true
	case strings.Contains(lower, "1h0m0s") || strings.Contains(lower, "background task") || strings.Contains(lower, "drain") || strings.Contains(lower, "lease_expired") || strings.Contains(lower, "heartbeat_timeout") || strings.Contains(lower, "execution_timeout") || strings.Contains(lower, "lease expired"):
		return FailureCategoryBackgroundDrainTimeout, FailureStageBackgroundDrain, FailureDirectionLocalExecution, "agy-cli", errStr, true
	case strings.Contains(lower, "circuit") || strings.Contains(lower, "breaker") || strings.Contains(lower, "retry_capped"):
		// Remediable: circuit breaker is a control-plane soft gate, not infra-down / credits.
		return FailureCategoryCircuitBreakerTripped, FailureStageQueueDispatch, FailureDirectionDownstream, "iazio-harness-api", errStr, true
	case strings.Contains(lower, "story_not_found") || strings.Contains(lower, "not found in backlog") || strings.Contains(lower, "cannot execute nonexistent story"):
		// Remediable: backlog sync / story allocation may heal this; do not abandon.
		return FailureCategoryStoryNotFound, FailureStageStoryValidation, FailureDirectionLocalExecution, "backlog", "Story not found in backlog or control plane: cannot execute nonexistent story", true
	case strings.Contains(lower, "gate") || strings.Contains(lower, "dod") || strings.Contains(lower, "incomplete") || strings.Contains(lower, "ase_gate"):
		// Remediable ASE/DoD incompleteness — never terminal (STORY-GOV-5275).
		return FailureCategoryASEGateIncomplete, FailureStageCompletionGate, FailureDirectionLocalExecution, "autopilot", "Autonomous Story Execution DoD or review scheme validation incomplete", true
	default:
		return FailureCategoryToolCrash, FailureStageToolExecution, FailureDirectionLocalExecution, "iazio-harness", "Execution error: " + errStr, true
	}
}

// ClassifyDispatchFailure maps an error string to canonical failure taxonomy.
func ClassifyDispatchFailure(errStr string) (category, stage, summary string, retriable bool) {
	cat, stg, _, _, sum, ret := ClassifyDispatchFailureWithDirection(errStr)
	return cat, stg, sum, ret
}

// isReconcilableFailure checks whether a failure reason was caused by lease/heartbeat/reaper timeout or an incomplete ASE landing gate.
func isReconcilableFailure(errStr string) bool {
	lower := strings.ToLower(errStr)
	return strings.Contains(lower, "lease_expired") ||
		strings.Contains(lower, "heartbeat_timeout") ||
		strings.Contains(lower, "execution_timeout") ||
		strings.Contains(lower, "background_drain_timeout") ||
		strings.Contains(lower, "stalled") ||
		strings.Contains(lower, "process_died") ||
		strings.Contains(lower, "lease expired") ||
		strings.Contains(lower, "ase_gate_incomplete") ||
		strings.Contains(lower, "unmerged_feature_branch") ||
		strings.Contains(lower, "unmerged feature branch") ||
		strings.Contains(lower, "dirty_worktree_post_exit") ||
		strings.Contains(lower, "incomplete_ase_flow") ||
		strings.Contains(lower, "unpushed_commits") ||
		strings.Contains(lower, "ase_completion_gate_violation")
}

// CompleteWithFencing validates the fencing token and updates the job with final result or error.
func (q *InMemoryQueue) CompleteWithFencing(ctx context.Context, jobID string, fencingToken int64, result json.RawMessage, errStr string) error {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusCanceled {
		q.mu.Unlock()
		return ErrJobAlreadyCompleted
	}
	reconcilableFailed := job.Status == JobStatusFailed && (errStr == "" || isReconcilableFailure(job.Error+" "+job.FailureCategory+" "+job.FailureReasonSummary))
	if (job.Status == JobStatusPending || reconcilableFailed) && fencingToken > 0 {
		// Active worker finished while job was requeued to pending or reconcilable failed (before any other worker claimed it).
		// Re-adopt fencingToken so Attempt 1 completes cleanly without requiring a redundant Attempt 2 retrigger.
		job.FencingToken = fencingToken
		job.LeaseVersion = fencingToken
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		q.mu.Unlock()
		return ErrLeaseFenced
	}
	if job.Status == JobStatusFailed {
		if !reconcilableFailed && !(errStr == "" && fencingToken > 0 && fencingToken == job.FencingToken) {
			q.mu.Unlock()
			return ErrJobAlreadyCompleted
		}
	}

	now := q.now()
	job.CompletedAt = &now
	// Capture summary/output before sanitize so RC-7 mid-flow markers are not lost.
	var rawResultSummary, rawResultOutput, rawResultStatus string
	if len(result) > 0 {
		var rawMap map[string]any
		if json.Unmarshal(result, &rawMap) == nil && rawMap != nil {
			rawResultSummary, _ = rawMap["summary"].(string)
			rawResultOutput, _ = rawMap["output"].(string)
			rawResultStatus, _ = rawMap["status"].(string)
		}
		result = SanitizeJobResultSummary(result)
	}

	var resMap map[string]any
	if len(result) > 0 {
		_ = json.Unmarshal(result, &resMap)
	}
	if resMap != nil {
		if cid, ok := resMap["active_conversation_id"].(string); ok && strings.TrimSpace(cid) != "" && job.ActiveConversationID == "" {
			job.ActiveConversationID = strings.TrimSpace(cid)
		} else if cid, ok := resMap["conversation_id"].(string); ok && strings.TrimSpace(cid) != "" && job.ActiveConversationID == "" {
			job.ActiveConversationID = strings.TrimSpace(cid)
		}
		if job.ActiveConversationID != "" {
			hasConv := false
			for _, existing := range job.ConversationIDs {
				if existing == job.ActiveConversationID {
					hasConv = true
					break
				}
			}
			if !hasConv {
				job.ConversationIDs = append(job.ConversationIDs, job.ActiveConversationID)
			}
		}
		if hp, ok := resMap["harness_pid"].(float64); ok && int(hp) > 0 && job.HarnessPID == 0 {
			job.HarnessPID = int(hp)
		}
		if ap, ok := resMap["agent_pid"].(float64); ok && int(ap) > 0 && job.AgentPID == 0 {
			job.AgentPID = int(ap)
		}
		if tp, ok := resMap["tool_pid"].(float64); ok && int(tp) > 0 && job.ToolPID == 0 {
			job.ToolPID = int(tp)
		}
		if el, ok := resMap["elapsed_seconds"].(float64); ok && int64(el) > 0 && job.ElapsedSeconds == 0 {
			job.ElapsedSeconds = int64(el)
		}
		if wt, ok := resMap["worktree_path"].(string); ok && strings.TrimSpace(wt) != "" && job.WorktreePath == "" {
			job.WorktreePath = strings.TrimSpace(wt)
		}
		if ch, ok := resMap["host_id"].(string); ok && strings.TrimSpace(ch) != "" {
			if job.ClaimedHost == "" {
				job.ClaimedHost = strings.TrimSpace(ch)
			}
			if job.HostID == "" {
				job.HostID = strings.TrimSpace(ch)
			}
		}
		if sid, ok := resMap["story_id"].(string); ok {
			cleanSID := strings.TrimSpace(sid)
			if cleanSID != "" && cleanSID != "pending-allocation" && cleanSID != "pending" {
				job.StoryID = cleanSID
				job.StoryAllocationStatus = "allocated"
			}
		}
		if stitle, ok := resMap["story_title"].(string); ok && strings.TrimSpace(stitle) != "" {
			cleanStoryTitle, _ := NormalizeStoryTitle(job.StoryID, stitle)
			if cleanStoryTitle != "" {
				job.StoryTitle = cleanStoryTitle
			}
		}
		if spath, ok := resMap["story_path"].(string); ok && strings.TrimSpace(spath) != "" {
			job.StoryPath = strings.TrimSpace(spath)
		}
		if resFailCat, ok := resMap["failure_category"].(string); ok && strings.TrimSpace(resFailCat) != "" && !strings.EqualFold(resFailCat, FailureCategoryNone) {
			job.FailureCategory = strings.TrimSpace(resFailCat)
		}
		if resFailStage, ok := resMap["failure_stage"].(string); ok && strings.TrimSpace(resFailStage) != "" {
			job.FailureStage = strings.TrimSpace(resFailStage)
		}
		if resFailSummary, ok := resMap["failure_reason_summary"].(string); ok && strings.TrimSpace(resFailSummary) != "" {
			job.FailureReasonSummary = FormatConciseExecutiveSummary(strings.TrimSpace(resFailSummary), "", MaxExecutiveSummaryChars)
		}
		if errStr == "" {
			resStatus, _ := resMap["status"].(string)
			resFailCat, _ := resMap["failure_category"].(string)
			resFailStage, _ := resMap["failure_stage"].(string)
			if strings.EqualFold(resStatus, "timed_out") || strings.EqualFold(resStatus, "failed") ||
				(resFailCat != "" && !strings.EqualFold(resFailCat, FailureCategoryNone)) {
				if resFailCat != "" {
					job.FailureCategory = resFailCat
				}
				if resFailStage != "" {
					job.FailureStage = resFailStage
				}
				if job.FailureCategory == FailureCategoryBackgroundDrainTimeout || strings.EqualFold(resStatus, "timed_out") {
					errStr = "BACKGROUND_DRAIN_TIMEOUT: execution interrupted during background drain before completion"
				} else {
					errStr = fmt.Sprintf("execution returned non-completed status %q", resStatus)
				}
			}
		}
	}

	isEagerReadiness := strings.Contains(string(result), "completed_via_eager_readiness")
	if isEagerReadiness {
		errStr = ""
	}

	// STORY-IAZIO-0204 RC-7: reject premature CRAFT / mid-flow "completed" results.
	if errStr == "" && !isEagerReadiness {
		resStatus := rawResultStatus
		if resMap != nil {
			if s, ok := resMap["status"].(string); ok && strings.TrimSpace(s) != "" {
				resStatus = s
			}
		}
		looksCompletedOrSuccess := resStatus == "" ||
			strings.EqualFold(resStatus, "completed") ||
			strings.EqualFold(resStatus, "success")
		if looksCompletedOrSuccess && IsPrematureCraftCloseout(rawResultSummary, rawResultOutput) {
			errStr = PrematureCraftCloseoutMessage
		}
	}

	if errStr != "" {
		cat, stage, dir, svc, summary, retriable := ClassifyDispatchFailureWithDirection(errStr)
		hasResumableASEState := strings.TrimSpace(job.ActiveConversationID) != "" ||
			strings.TrimSpace(job.ResumeConversationID) != "" ||
			strings.TrimSpace(job.ConversationID) != ""
		effectiveCat := cat
		if job.FailureCategory != "" && job.FailureCategory != FailureCategoryNone {
			effectiveCat = job.FailureCategory
		}
		terminalStop := IsTerminalStopFailure(effectiveCat, errStr) || (!retriable && IsTerminalStopFailure(cat, errStr))
		if !terminalStop {
			// Never abandon remediable failures: record attempt and requeue SAME job ID to pending.
			job.Result = result
			attemptNum := job.Attempts
			if attemptNum <= 0 {
				attemptNum = len(job.AttemptHistory) + 1
			}
			job.AttemptHistory = append(job.AttemptHistory, AttemptFailure{
				AttemptNumber:    attemptNum,
				Host:             job.HostID,
				Workspace:        job.Repo,
				Tool:             job.TargetAgent,
				FailureCategory:  cat,
				FailureStage:     stage,
				FailureDirection: dir,
				FailureService:   svc,
				Reason:           cat,
				ExitCode:         1,
				StderrTail:       errStr,
				Timestamp:        now.UTC().Format(time.RFC3339),
				TriggerReason:    "auto_remediate_inplace",
				OutcomeSummary:   summary,
			})
			if (cat == FailureCategoryASEGateIncomplete || job.FailureCategory == FailureCategoryASEGateIncomplete) &&
				job.InfraRequeueCount == 0 && hasResumableASEState {
				applyASECloseoutRemediationLocked(job, errStr)
			} else {
				resetJobPendingInPlaceLocked(job, errStr)
			}
			advanceJobModelOnFailureRetry(job)
			q.syncJobActiveStateLocked(job)
			if q.store != nil {
				sctx, cancel := storeContext(ctx)
				_ = q.store.SaveQueueJob(sctx, job)
				cancel()
			}
			slotRel := q.slotReleaser
			q.mu.Unlock()
			if slotRel != nil {
				slotRel(jobID)
			}
			return nil
		}

		job.Status = JobStatusFailed
		job.Result = result
		job.Error = errStr
		if job.FailureCategory == "" || job.FailureCategory == FailureCategoryNone {
			job.FailureCategory = cat
		}
		if job.FailureStage == "" {
			job.FailureStage = stage
		}
		if job.FailureDirection == "" {
			job.FailureDirection = dir
		}
		if job.FailureService == "" {
			job.FailureService = svc
		}
		if job.FailureReasonSummary == "" {
			job.FailureReasonSummary = summary
		}
		job.Retriable = false
		if job.DispatchDiagnostics != nil {
			job.DispatchDiagnostics.QueueState = "failed"
		}
	} else {
		job.Status = JobStatusCompleted
		job.Result = result
		job.Error = ""
		job.FailureCategory = ""
		job.FailureStage = ""
		job.FailureDirection = ""
		job.FailureService = ""
		job.FailureReasonSummary = ""
		job.Retriable = false
		if job.DispatchDiagnostics != nil {
			job.DispatchDiagnostics.QueueState = "completed"
		}
	}
	q.syncJobActiveStateLocked(job)

	if q.store != nil && job.CompletedAt != nil {
		sctx, cancel := storeContext(ctx)
		if err := q.store.CompleteQueueJob(sctx, job.ID, job.Status, job.Result, job.Error, *job.CompletedAt); err != nil {
			cancel()
			log.Printf("[dispatch-queue] ERROR: failed to persist job completion for %s: %v", job.ID, err)
			q.mu.Unlock()
			return fmt.Errorf("persist job completion: %w", err)
		}
		if strings.Contains(string(job.Result), "completed_via_eager_readiness") {
			_ = q.store.SaveQueueJob(sctx, job)
		}
		cancel()
	}

	searchID := job.SearchID
	traceID := job.TraceID
	var promoted []string
	if job.Status == JobStatusCompleted && len(result) > 0 {
		var resMap map[string]any
		if err := json.Unmarshal(result, &resMap); err == nil {
			if mems, ok := resMap["promoted_memories"].([]any); ok {
				for _, m := range mems {
					if str, ok := m.(string); ok && str != "" {
						promoted = append(promoted, str)
					}
				}
			}
		}
	}
	if len(promoted) == 0 && searchID != "" && job.Status == JobStatusCompleted {
		promoted = []string{"mem-" + searchID}
	}
	job.PromotedMemories = promoted
	if searchID != "" && job.Status == JobStatusCompleted {
		job.RelevanceEmitted = true
	}
	PopulateJobConversationFields(job)
	if q.store != nil {
		sctx, cancel := storeContext(ctx)
		_ = q.store.SaveQueueJob(sctx, job)
		cancel()
	}

	promotedJobs, cascadedFailed, depDoneChs := q.resolveDependentJobsLocked(ctx, now)
	q.rebuildActiveJobsLocked()
	jobSnapshot := cloneJobSnapshot(job)
	done := job.doneCh
	job.doneCh = nil
	q.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	for _, ch := range depDoneChs {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	q.fireTerminalHooks(jobSnapshot, promotedJobs)
	for _, cf := range cascadedFailed {
		q.fireTerminalHooks(cf, nil)
	}

	if (searchID != "" && jobSnapshot.Status == JobStatusCompleted) || len(promoted) > 0 {
		go q.dispatchAttribution(searchID, traceID, promoted)
	}

	return nil
}

// SetFinalHarvest records the final verified turn count and transcript SHA256 onto the job.
func (q *InMemoryQueue) SetFinalHarvest(jobID string, turnCount int, sha string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if job, ok := q.jobs[jobID]; ok {
		updated := false
		if turnCount > 0 {
			job.FinalTurnCount = turnCount
			updated = true
		}
		if sha != "" {
			job.FinalTranscriptSHA256 = sha
			updated = true
		}
		if updated && q.store != nil {
			sctx, cancel := storeContext(context.Background())
			_ = q.store.SaveQueueJob(sctx, job)
			cancel()
		}
	}
}

func (q *InMemoryQueue) dispatchAttribution(searchID, traceID string, promoted []string) {
	q.mu.Lock()
	apiURL := q.attributionURL
	client := q.httpClient
	q.mu.Unlock()

	if apiURL == "" {
		apiURL = os.Getenv("IAZIO_API_URL")
	}
	if apiURL == "" {
		apiURL = "http://127.0.0.1:8080"
	}
	apiURL = strings.TrimRight(apiURL, "/")

	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}

	// 1. Emit positive relevance attribution if searchID present
	if searchID != "" {
		payload, err := json.Marshal(map[string]any{
			"search_id": searchID,
			"outcome":   "positive",
			"trace_id":  traceID,
		})
		if err == nil {
			req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/relevance", bytes.NewReader(payload))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if traceID != "" {
					req.Header.Set("traceparent", fmt.Sprintf("00-%s-0000000000000001-01", traceID))
				}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("[dispatchAttribution] failed to post relevance attribution for search_id %s: %v", searchID, err)
				} else {
					defer resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
						log.Printf("[dispatchAttribution] unexpected status code %d for search_id %s: %s", resp.StatusCode, searchID, string(body))
					}
				}
			}
		}
	}

	// 2. Promote verified memories
	for _, memID := range promoted {
		payload, err := json.Marshal(map[string]any{
			"id":     memID,
			"tier":   "canonical",
			"reason": "verified harness execution completion",
		})
		if err == nil {
			req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/memories/promote", bytes.NewReader(payload))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if traceID != "" {
					req.Header.Set("traceparent", fmt.Sprintf("00-%s-0000000000000001-01", traceID))
				}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("[dispatchAttribution] failed to promote memory %s: %v", memID, err)
				} else {
					defer resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
						log.Printf("[dispatchAttribution] unexpected status code %d promoting memory %s: %s", resp.StatusCode, memID, string(body))
					}
				}
			}
		}
	}
}

// Get retrieves a job by ID, lazily reaping any expired jobs.
func (q *InMemoryQueue) Get(jobID string) (*Job, bool) {
	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reapExpiredLocked(q.now())
	j, ok := q.jobs[jobID]
	if ok && j != nil {
		PopulateJobConversationFields(j)
		return cloneJobSnapshot(j), true
	}
	return nil, false
}

// PurgeJob deletes a job (such as a phantom empty row) from the in-memory queue
// and, if the backing store supports deletion, from durable storage.
func (q *InMemoryQueue) PurgeJob(ctx context.Context, jobID string) bool {
	cleanID := strings.TrimSpace(jobID)
	if cleanID == "" {
		return false
	}
	q.mu.Lock()
	_, existed := q.jobs[cleanID]
	delete(q.jobs, cleanID)
	q.activeJobs.Delete(cleanID)
	store := q.store
	q.mu.Unlock()
	if store != nil {
		if deleter, ok := store.(interface {
			DeleteQueueJob(ctx context.Context, jobID string) error
		}); ok {
			sctx, cancel := storeContext(ctx)
			_ = deleter.DeleteQueueJob(sctx, cleanID)
			cancel()
		}
	}
	return existed
}

func matchesJobStatusFilter(jobStatus, filterStatus string) bool {
	f := strings.ToLower(strings.TrimSpace(filterStatus))
	if f == "" || f == "all" {
		return true
	}
	s := strings.ToLower(strings.TrimSpace(jobStatus))
	switch f {
	case "queued", "pending":
		return s == JobStatusPending || s == "queued"
	case "blocked", "blocked_by_dependency":
		return s == JobStatusBlockedByDependency || s == "blocked"
	case "claimed":
		return s == JobStatusClaimed || s == JobStatusLeased || s == JobStatusRunning
	case "cancelled", "canceled":
		return s == JobStatusCanceled || s == "cancelled"
	default:
		return strings.EqualFold(s, f)
	}
}

// Count returns the total number of jobs matching the filter before limit truncation.
func (q *InMemoryQueue) Count(ctx context.Context, filter JobFilter) (int, error) {
	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapExpiredLocked(q.now())

	count := 0
	for _, j := range q.jobs {
		if IsPhantomEmptyQueueJob(j) {
			continue
		}
		if filter.Repo != "" && j.Repo != filter.Repo {
			continue
		}
		if !matchesJobStatusFilter(j.Status, filter.Status) {
			continue
		}
		if filter.Type != "" && j.Type != filter.Type {
			continue
		}
		count++
	}
	return count, nil
}

// List returns jobs matching the filter, sorted by CreatedAt descending, lazily reaping expired jobs.
func (q *InMemoryQueue) List(ctx context.Context, filter JobFilter) ([]*Job, error) {
	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	if (q.claimTimeout > 0 && q.claimTimeout < time.Second) ||
		(q.jobTimeout > 0 && q.jobTimeout < time.Second) ||
		q.lastReapAt.IsZero() ||
		len(q.jobs) != q.lastReapJobCount ||
		now.Before(q.lastReapAt) ||
		now.Sub(q.lastReapAt) >= time.Second {
		q.reapExpiredLocked(now)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	} else if limit > 1000 {
		limit = 1000
	}

	var matched []*Job
	for _, j := range q.jobs {
		if IsPhantomEmptyQueueJob(j) {
			continue
		}
		if filter.Repo != "" && j.Repo != filter.Repo {
			continue
		}
		if !matchesJobStatusFilter(j.Status, filter.Status) {
			continue
		}
		if filter.Type != "" && j.Type != filter.Type {
			continue
		}
		matched = append(matched, j)
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if len(matched) > limit {
		matched = matched[:limit]
	}

	out := make([]*Job, len(matched))
	for i, j := range matched {
		PopulateJobConversationFields(j)
		out[i] = cloneJobSnapshot(j)
	}
	return out, nil
}

// Cancel terminates a pending or claimed job.
func (q *InMemoryQueue) Cancel(ctx context.Context, jobID string, reason string) (*Job, error) {
	q.mu.Lock()
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		q.mu.Unlock()
		return nil, ErrJobAlreadyCompleted
	}

	now := q.now()
	job.Status = JobStatusCanceled
	if reason == "" {
		reason = "cancelled by user"
	}
	job.Error = reason
	job.CompletedAt = &now
	q.syncJobActiveStateLocked(job)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}
	promotedJobs, cascadedFailed, depDoneChs := q.resolveDependentJobsLocked(ctx, now)
	q.rebuildActiveJobsLocked()
	jobSnapshot := cloneJobSnapshot(job)
	done := job.doneCh
	q.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	for _, ch := range depDoneChs {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}

	q.fireTerminalHooks(jobSnapshot, promotedJobs)
	for _, cf := range cascadedFailed {
		q.fireTerminalHooks(cf, nil)
	}
	return jobSnapshot, nil
}

// Retry re-enqueues a job by creating a fresh pending job with the same parameters.
func (q *InMemoryQueue) Retry(ctx context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	orig, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}

	normalizeJobDependencies(orig)
	isDepBlockedOrFailed := len(orig.DependsOnJobIDs) > 0 &&
		(orig.Status == JobStatusBlockedByDependency ||
			(orig.Status == JobStatusRequeued && !q.hasActiveReplacementChildLocked(orig.ID)) ||
			orig.FailureCategory == FailureCategoryDependencyFailed ||
			strings.HasPrefix(strings.TrimSpace(orig.Error), "DEPENDENCY_FAILED"))
	if isDepBlockedOrFailed {
		satisfied, failedDep := q.areDependenciesSatisfiedLocked(orig)
		if satisfied {
			orig.Status = JobStatusPending
			orig.Error = ""
			orig.FailureCategory = ""
			orig.FailureStage = ""
			orig.FailureDirection = ""
			orig.FailureService = ""
			orig.FailureReasonSummary = ""
			orig.Retriable = false
			orig.CompletedAt = nil
			orig.ClaimedHost = ""
			orig.HostID = ""
			orig.LeaseOwner = ""
			orig.LeaseToken = ""
			orig.ClaimedAt = nil
			orig.HeartbeatAt = nil
			orig.LeaseExpiresAt = nil
			orig.LeaseAckDeadlineAt = nil
			orig.doneCh = make(chan struct{})
			if orig.DispatchDiagnostics != nil {
				orig.DispatchDiagnostics.QueueState = "queued"
			} else {
				orig.DispatchDiagnostics = &DispatchDiagnostics{QueueState: "queued"}
			}
		} else {
			orig.Status = JobStatusBlockedByDependency
			orig.CompletedAt = nil
			if failedDep == nil {
				orig.Error = ""
				orig.FailureCategory = ""
				orig.FailureStage = ""
				orig.FailureDirection = ""
				orig.FailureService = ""
				orig.FailureReasonSummary = ""
			}
			orig.doneCh = make(chan struct{})
			if orig.DispatchDiagnostics != nil {
				orig.DispatchDiagnostics.QueueState = JobStatusBlockedByDependency
			} else {
				orig.DispatchDiagnostics = &DispatchDiagnostics{QueueState: JobStatusBlockedByDependency}
			}
		}
		q.syncJobActiveStateLocked(orig)
		_, _, _ = q.resolveDependentJobsLocked(ctx, q.now())
		q.rebuildActiveJobsLocked()
		if q.store != nil {
			_ = q.store.SaveQueueJob(ctx, orig)
		}
		origSnapshot := cloneJobSnapshot(orig)
		q.mu.Unlock()
		return origSnapshot, nil
	}

	// STORY-GOV-5275: explicit Retry keeps the SAME job ID pending (never orphan as requeued).
	if IsTerminalStopFailure(orig.FailureCategory, orig.Error+" "+orig.FailureReasonSummary) {
		q.mu.Unlock()
		return nil, fmt.Errorf("job %s is terminal-stop (%s): not retriable", orig.ID, orig.FailureCategory)
	}
	wasFailed := orig.Status == JobStatusFailed
	resetJobPendingInPlaceLocked(orig, "")
	if wasFailed {
		advanceJobModelOnFailureRetry(orig)
	}
	q.syncJobActiveStateLocked(orig)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, orig)
	}
	origSnapshot := cloneJobSnapshot(orig)
	q.mu.Unlock()
	return origSnapshot, nil
}

// AckLease completes Phase 2 of the two-phase lease handshake, transitioning the job to running.
func (q *InMemoryQueue) AckLease(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if (job.Status == JobStatusPending || (job.Status == JobStatusFailed && isReconcilableFailure(job.Error+" "+job.FailureCategory+" "+job.FailureReasonSummary))) &&
		(leaseToken != "" || agentPID > 0 || activeConvID != "") {
		job.Status = JobStatusRunning
		if leaseToken != "" && job.LeaseToken == "" {
			job.LeaseToken = leaseToken
		}
		job.Error = ""
		job.FailureCategory = ""
		job.FailureStage = ""
		job.FailureDirection = ""
		job.FailureService = ""
		job.FailureReasonSummary = ""
		job.Retriable = false
		job.CompletedAt = nil
		if job.DispatchDiagnostics != nil {
			job.DispatchDiagnostics.QueueState = "running"
		}
		job.doneCh = make(chan struct{})
	}
	if job.Status != JobStatusLeased && job.Status != JobStatusClaimed && job.Status != JobStatusRunning {
		return nil, fmt.Errorf("job %s is not in leased state (status: %s)", jobID, job.Status)
	}
	if job.LeaseToken != "" && leaseToken != "" && job.LeaseToken != leaseToken {
		return nil, errors.New("invalid or missing lease token")
	} else if job.LeaseToken != "" && leaseToken == "" && agentPID <= 0 && activeConvID == "" {
		return nil, errors.New("invalid or missing lease token")
	}

	now := q.now()
	job.Status = JobStatusRunning
	job.LeaseAckDeadlineAt = nil
	if job.AckedAt == nil {
		job.AckedAt = &now
	}
	if agentPID > 0 {
		job.AgentPID = agentPID
		if job.HarnessPID == 0 {
			job.HarnessPID = agentPID
		}
	}
	if activeConvID != "" {
		job.ActiveConversationID = activeConvID
		job.ConversationID = activeConvID
		addConversationIDLocked(job, activeConvID)
	}
	PopulateJobConversationFields(job)
	job.HeartbeatAt = &now
	expires := now.Add(DefaultHeartbeatTimeout)
	job.LeaseExpiresAt = &expires
	PopulateJobConversationFields(job)
	q.syncJobActiveStateLocked(job)
	q.lastReapAt = time.Time{}
	if q.store != nil {
		sctx, cancel := storeContext(ctx)
		defer cancel()
		if err := q.store.UpdateQueueJobLease(sctx, job.ID, job.LeaseOwner, job.FencingToken, now, expires); err != nil {
			if isTransientPersistenceTimeout(err) {
				log.Printf("[dispatch-queue] WARN: transient DB timeout persisting ack-lease heartbeat for %s (retaining in-memory lease extension): %v", jobID, err)
			} else {
				log.Printf("[dispatch-queue] ERROR: failed to persist heartbeat lease for %s: %v", jobID, err)
				return nil, fmt.Errorf("persist heartbeat lease: %w", err)
			}
		}
		_ = q.store.SaveQueueJob(sctx, job)
	}
	return cloneJobSnapshot(job), nil
}

func addConversationIDLocked(job *Job, convID string) {
	cid := strings.TrimSpace(convID)
	if cid == "" || job == nil {
		return
	}
	if job.InitialConversationID == "" {
		job.InitialConversationID = cid
	}
	if job.ResumeConversationID == "" {
		job.ResumeConversationID = cid
	}
	if job.ConversationID == "" {
		job.ConversationID = cid
	}
	job.ConversationURL = BuildConversationURL("", cid)
	if len(job.Payload) > 0 {
		var payloadMap map[string]any
		if err := json.Unmarshal(job.Payload, &payloadMap); err == nil && payloadMap != nil {
			payloadMap["resume_conversation_id"] = job.ResumeConversationID
			payloadMap["conversation_id"] = cid
			if updated, err := json.Marshal(payloadMap); err == nil {
				job.Payload = updated
			}
		}
	}
	for _, existing := range job.ConversationIDs {
		if existing == cid {
			return
		}
	}
	job.ConversationIDs = append(job.ConversationIDs, cid)
}

// UpdateRuntimeMetadata updates the delegated worktree path, active conversation, PIDs, and elapsed time on a job.
func (q *InMemoryQueue) UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists || job == nil {
		return nil, false
	}
	changed := false
	wt := strings.TrimSpace(worktreePath)
	if wt != "" && wt != "." && wt != "root" && (job.Repo != wt || job.WorktreePath != wt) {
		origSubmodule := job.Repo
		job.Repo = wt
		job.WorktreePath = wt
		changed = true
		if len(job.Payload) > 0 {
			var payloadMap map[string]any
			if err := json.Unmarshal(job.Payload, &payloadMap); err == nil && payloadMap != nil {
				if _, hasSub := payloadMap["submodule"]; !hasSub && origSubmodule != "" {
					payloadMap["submodule"] = origSubmodule
				}
				payloadMap["worktree_path"] = wt
				payloadMap["repo"] = wt
				if updated, err := json.Marshal(payloadMap); err == nil {
					job.Payload = updated
				}
			}
		}
	}
	if cid := strings.TrimSpace(activeConvID); cid != "" {
		if job.ActiveConversationID != cid {
			job.ActiveConversationID = cid
			changed = true
		}
		if job.ConversationID != cid {
			job.ConversationID = cid
			changed = true
		}
		if job.InitialConversationID == "" || job.ResumeConversationID == "" {
			changed = true
		}
		prevLen := len(job.ConversationIDs)
		addConversationIDLocked(job, cid)
		if len(job.ConversationIDs) != prevLen {
			changed = true
		}
	}
	PopulateJobConversationFields(job)
	isFinishedJob := job.Status == JobStatusCompleted || job.Status == JobStatusCanceled
	if !isFinishedJob {
		if harnessPID > 0 {
			if job.AgentPID == 0 && toolPID == 0 {
				job.AgentPID = harnessPID
				changed = true
			}
			if job.HarnessPID == 0 || toolPID > 0 || (harnessPID != job.AgentPID && job.HarnessPID != harnessPID) {
				if job.HarnessPID != harnessPID {
					job.HarnessPID = harnessPID
					changed = true
				}
			}
		}
		if toolPID > 0 && job.ToolPID != toolPID {
			job.ToolPID = toolPID
			changed = true
		}
		if elapsedSec > 0 && job.ElapsedSeconds != elapsedSec {
			job.ElapsedSeconds = elapsedSec
			changed = true
		}
	}
	if (harnessPID > 0 || toolPID > 0 || activeConvID != "") &&
		((job.Status == JobStatusFailed && isReconcilableFailure(job.Error)) ||
			(job.Status == JobStatusPending && (len(job.AttemptHistory) > 0 || job.InfraRequeueCount > 0 || job.FencingToken > 0))) {
		job.Status = JobStatusRunning
		job.Error = ""
		job.FailureCategory = ""
		job.FailureStage = ""
		job.FailureDirection = ""
		job.FailureService = ""
		job.FailureReasonSummary = ""
		job.Retriable = false
		job.CompletedAt = nil
		if job.DispatchDiagnostics != nil {
			job.DispatchDiagnostics.QueueState = "running"
		}
		job.doneCh = make(chan struct{})
		changed = true
	}
	if job.Status == JobStatusRunning || job.Status == JobStatusClaimed || job.Status == JobStatusLeased {
		now := q.now()
		job.HeartbeatAt = &now
		expires := now.Add(DefaultHeartbeatTimeout)
		job.LeaseExpiresAt = &expires
		changed = true
	}
	q.syncJobActiveStateLocked(job)
	if changed && q.store != nil {
		sctx, cancel := storeContext(context.Background())
		_ = q.store.SaveQueueJob(sctx, job)
		cancel()
	}
	return cloneJobSnapshot(job), true
}

// Failover records an attempt failure and resets the job to pending for re-dispatch.
func (q *InMemoryQueue) Failover(ctx context.Context, jobID string, req AttemptFailure) (*Job, error) {
	q.mu.Lock()

	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		return nil, ErrJobNotFound
	}

	if req.AttemptNumber <= 0 {
		req.AttemptNumber = len(job.AttemptHistory) + 1
	}
	if req.Timestamp == "" {
		req.Timestamp = q.now().UTC().Format(time.RFC3339)
	}
	if req.TriggerReason == "" {
		req.TriggerReason = req.Reason
	}
	if req.OutcomeSummary == "" {
		req.OutcomeSummary = fmt.Sprintf("Tool %s on %s failed (exit %d): %s", req.Tool, req.Host, req.ExitCode, req.StderrTail)
	}
	job.AttemptHistory = append(job.AttemptHistory, req)
	job.FailureCategory = req.FailureCategory
	job.FailureStage = FailureStageToolExecution
	job.FailureReasonSummary = req.OutcomeSummary

	// Cap retries if consecutive attempts fail with identical error or identical failure category/exit code
	consecutiveFailures := false
	if len(job.AttemptHistory) >= 2 {
		prev := job.AttemptHistory[len(job.AttemptHistory)-2]
		if (prev.FailureCategory == req.FailureCategory && prev.ExitCode == req.ExitCode && prev.Reason == req.Reason) || len(job.AttemptHistory) >= DefaultMaxAttempts {
			consecutiveFailures = true
		}
	}

	if consecutiveFailures {
		job.Status = JobStatusFailed
		job.Error = fmt.Sprintf("RETRY_CAPPED: consecutive attempts failed with identical error (%s)", req.Reason)
		job.Retriable = false
		completedAt := q.now()
		job.CompletedAt = &completedAt
		q.syncJobActiveStateLocked(job)
		if job.doneCh != nil {
			select {
			case <-job.doneCh:
			default:
				close(job.doneCh)
			}
		}
		if q.store != nil {
			_ = q.store.SaveQueueJob(ctx, job)
		}
		promotedJobs, cascadedFailed, depDoneChs := q.resolveDependentJobsLocked(ctx, completedAt)
		q.rebuildActiveJobsLocked()
		jobSnapshot := cloneJobSnapshot(job)
		q.mu.Unlock()
		for _, ch := range depDoneChs {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
		q.fireTerminalHooks(jobSnapshot, promotedJobs)
		for _, cf := range cascadedFailed {
			q.fireTerminalHooks(cf, nil)
		}
		return jobSnapshot, nil
	}

	failedTool := strings.ToLower(strings.TrimSpace(req.Tool))
	if failedTool == "" {
		failedTool = strings.ToLower(strings.TrimSpace(job.TargetAgent))
	}
	if failedTool != "" && req.FailureCategory != "WORKSPACE_PREP_ERROR" {
		priorityTools := []string{"agy", "opencode", "claude", "cursor"}
		failedSet := make(map[string]bool)
		for _, att := range job.AttemptHistory {
			if t := strings.ToLower(strings.TrimSpace(att.Tool)); t != "" {
				failedSet[t] = true
			}
		}
		failedSet[failedTool] = true
		nextTool := ""
		for _, cand := range priorityTools {
			if !failedSet[cand] {
				nextTool = cand
				break
			}
		}
		if nextTool == "" {
			for _, cand := range priorityTools {
				if cand != failedTool {
					nextTool = cand
					break
				}
			}
		}
		if nextTool != "" {
			job.TargetAgent = nextTool
			job.TargetHost = ""
			if len(job.Payload) > 0 {
				var payloadMap map[string]any
				if err := json.Unmarshal(job.Payload, &payloadMap); err == nil && payloadMap != nil {
					payloadMap["target_agent"] = nextTool
					payloadMap["target_host"] = ""
					if updated, err := json.Marshal(payloadMap); err == nil {
						job.Payload = updated
					}
				}
			}
		}
	}

	job.TargetHost = ""
	job.Status = JobStatusPending
	job.ClaimedHost = ""
	job.HostID = ""
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.LeaseAckDeadlineAt = nil
	job.Retriable = true
	q.syncJobActiveStateLocked(job)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}
	jobSnapshot := cloneJobSnapshot(job)
	q.mu.Unlock()

	return jobSnapshot, nil
}

// RetryRetriable retries all failed jobs that are marked retriable.
func (q *InMemoryQueue) RetryRetriable(ctx context.Context) ([]*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.resolveDependentJobsLocked(ctx, q.now())

	var retried []*Job
	for _, job := range q.jobs {
		if job.Status == JobStatusBlockedByDependency {
			if satisfied, _ := q.areDependenciesSatisfiedLocked(job); satisfied {
				job.Status = JobStatusPending
				job.Error = ""
				job.FailureCategory = ""
				job.FailureStage = ""
				job.FailureDirection = ""
				job.FailureService = ""
				job.FailureReasonSummary = ""
				job.CompletedAt = nil
				q.syncJobActiveStateLocked(job)
				if q.store != nil {
					_ = q.store.SaveQueueJob(ctx, job)
				}
				retried = append(retried, cloneJobSnapshot(job))
			}
			continue
		}
		// Never revive canceled jobs. Only remediable failed jobs, or orphan requeued
		// jobs that have no active replacement child (STORY-GOV-5275 P0).
		orphanRequeued := job.Status == JobStatusRequeued && !q.hasActiveReplacementChildLocked(job.ID)
		remediableFailed := job.Status == JobStatusFailed &&
			(job.Retriable || !IsTerminalStopFailure(job.FailureCategory, job.Error+" "+job.FailureReasonSummary))
		if !remediableFailed && !orphanRequeued {
			continue
		}
		if IsTerminalStopFailure(job.FailureCategory, job.Error+" "+job.FailureReasonSummary) {
			continue
		}
		// STORY-IAZIO-0202: requeued parent whose replacement child already completed must
		// become completed (superseded), not pending — avoids re-dequeue of finished work
		// (job_3ffa5d0be1649f0e / job_985bc2111a0944cd / job_97d33fd156a88144).
		if orphanRequeued {
			if completedChild := q.latestSuccessfulCompletedChildLocked(job.ID); completedChild != nil {
				completeSupersededParentLocked(job, completedChild, q.now())
				q.syncJobActiveStateLocked(job)
				if q.store != nil {
					sctx, cancel := storeContext(ctx)
					if job.CompletedAt != nil {
						_ = q.store.CompleteQueueJob(sctx, job.ID, JobStatusCompleted, job.Result, "", *job.CompletedAt)
					} else {
						_ = q.store.SaveQueueJob(sctx, job)
					}
					cancel()
				}
				retried = append(retried, cloneJobSnapshot(job))
				continue
			}
		}
		if strings.EqualFold(strings.TrimSpace(job.FailureCategory), string(FailureCategoryDependencyFailed)) ||
			strings.Contains(strings.ToUpper(job.Error), "DEPENDENCY_FAILED") {
			if satisfied, _ := q.areDependenciesSatisfiedLocked(job); satisfied {
				resetJobPendingInPlaceLocked(job, "")
			} else {
				job.Status = JobStatusBlockedByDependency
				job.CompletedAt = nil
			}
			q.syncJobActiveStateLocked(job)
			if q.store != nil {
				_ = q.store.SaveQueueJob(ctx, job)
			}
			retried = append(retried, cloneJobSnapshot(job))
			continue
		}

		// Remediable retry keeps the SAME job ID claimable (no orphan requeued parent).
		resetJobPendingInPlaceLocked(job, "")
		if remediableFailed {
			advanceJobModelOnFailureRetry(job)
		}
		if len(job.DependsOnJobIDs) > 0 {
			if satisfied, _ := q.areDependenciesSatisfiedLocked(job); !satisfied {
				job.Status = JobStatusBlockedByDependency
			}
		}
		q.syncJobActiveStateLocked(job)
		if q.store != nil {
			_ = q.store.SaveQueueJob(ctx, job)
		}
		retried = append(retried, cloneJobSnapshot(job))
	}
	q.resolveDependentJobsLocked(ctx, q.now())
	q.rebuildActiveJobsLocked()
	return retried, nil
}

// BindStory binds a newly allocated story ID and metadata to a job.
func (q *InMemoryQueue) BindStory(ctx context.Context, jobID, storyID, storyTitle, storyPath string) (*Job, error) {
	return q.BindStoryWithFencing(ctx, jobID, storyID, storyTitle, storyPath, 0)
}

// BindStoryWithFencing binds an allocated story ID and metadata to a job while enforcing monotonic fencing token CAS.
func (q *InMemoryQueue) BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}

	if (job.Status == JobStatusPending || (job.Status == JobStatusFailed && isReconcilableFailure(job.Error+" "+job.FailureCategory+" "+job.FailureReasonSummary))) && fencingToken > 0 {
		job.Status = JobStatusRunning
		job.FencingToken = fencingToken
		job.LeaseVersion = fencingToken
		job.Error = ""
		job.FailureCategory = ""
		job.FailureStage = ""
		job.FailureDirection = ""
		job.FailureService = ""
		job.FailureReasonSummary = ""
		job.Retriable = false
		job.CompletedAt = nil
		q.syncJobActiveStateLocked(job)
	}
	if job.FencingToken > 0 && fencingToken != job.FencingToken {
		return nil, ErrLeaseFenced
	}

	cleanStoryID := strings.TrimSpace(storyID)
	if cleanStoryID == "" {
		return nil, errors.New("story_id is required")
	}
	if isUnitTestDummyStoryID(cleanStoryID) && strings.TrimSpace(job.StoryID) != "" && !isUnitTestDummyStoryID(job.StoryID) {
		return cloneJobSnapshot(job), nil
	}

	cleanStoryTitle, execTitle := NormalizeStoryTitle(cleanStoryID, storyTitle)

	job.StoryID = cleanStoryID
	if cleanStoryTitle != "" {
		job.StoryTitle = cleanStoryTitle
	}
	if storyPath != "" {
		job.StoryPath = storyPath
	}
	job.StoryAllocationStatus = "allocated"

	if len(job.Payload) > 0 {
		var payloadMap map[string]any
		if err := json.Unmarshal(job.Payload, &payloadMap); err == nil {
			payloadMap["story_id"] = cleanStoryID
			if cleanStoryTitle != "" {
				payloadMap["title"] = execTitle
			} else if existingTitle, ok := payloadMap["title"].(string); ok && strings.TrimSpace(existingTitle) != "" {
				_, normExisting := NormalizeStoryTitle(cleanStoryID, existingTitle)
				payloadMap["title"] = normExisting
			}
			if updated, err := json.Marshal(payloadMap); err == nil {
				job.Payload = updated
			}
		}
	}

	if q.store != nil {
		sctx, cancel := storeContext(ctx)
		_ = q.store.SaveQueueJob(sctx, job)
		cancel()
	}

	return cloneJobSnapshot(job), nil
}

// Await blocks until the specified job completes, the context is cancelled, or the timeout expires.
func (q *InMemoryQueue) Await(ctx context.Context, jobID string, timeout time.Duration) (*Job, error) {
	var waitSpanOpts []telemetry.SpanOption
	waitSpanOpts = append(waitSpanOpts, telemetry.WithTag("job.id", jobID))
	_, waitSpan := telemetry.StartSpan(ctx, "global_queue.wait", waitSpanOpts...)
	if waitSpan != nil {
		defer waitSpan.End()
	}

	q.mu.Lock()
	q.reapExpiredLocked(q.now())
	job, exists := q.jobs[jobID]
	if !exists {
		q.mu.Unlock()
		q.flushPendingSlotReleases()
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed {
		snap := cloneJobSnapshot(job)
		q.mu.Unlock()
		q.flushPendingSlotReleases()
		return snap, nil
	}
	done := job.doneCh
	q.mu.Unlock()
	q.flushPendingSlotReleases()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	case <-done:
		q.mu.Lock()
		defer q.mu.Unlock()
		return cloneJobSnapshot(q.jobs[jobID]), nil
	}
}

// SetDispatchDiagnostics updates the dispatch diagnostics on a specific job.
func (q *InMemoryQueue) SetDispatchDiagnostics(jobID string, diag *DispatchDiagnostics) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	job.DispatchDiagnostics = diag
	return nil
}

// ForceDispatch breaks any hung or stale claim on a job, re-queues it, and marks it ready for immediate allocation.
func (q *InMemoryQueue) ForceDispatch(ctx context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusFailed || job.Status == JobStatusCanceled {
		return nil, ErrJobAlreadyCompleted
	}

	now := q.now()
	job.Status = JobStatusPending
	job.ClaimedHost = ""
	job.HostID = ""
	if strings.EqualFold(strings.TrimSpace(job.AllocationSource), "global_queue_tool_priority") &&
		strings.TrimSpace(job.ResumeConversationID) == "" &&
		strings.TrimSpace(job.ActiveConversationID) == "" {
		job.TargetHost = ""
	}
	job.ClaimedAt = nil
	job.HeartbeatAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseOwner = ""
	job.LeaseToken = ""
	job.LeaseAckDeadlineAt = nil
	job.FencingToken = 0
	job.LeaseVersion = 0
	job.Error = ""
	if job.DispatchDiagnostics == nil {
		job.DispatchDiagnostics = &DispatchDiagnostics{}
	}
	job.DispatchDiagnostics.QueueState = "dispatching"
	job.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)
	q.syncJobActiveStateLocked(job)
	if q.store != nil {
		_ = q.store.SaveQueueJob(ctx, job)
	}

	return cloneJobSnapshot(job), nil
}

// ReconcileGlobalQueueSlots detects and reclaims stale leases (> staleGrace) across all claimed jobs.
func (q *InMemoryQueue) ReconcileGlobalQueueSlots(now time.Time, staleGrace time.Duration) []string {
	defer q.flushPendingSlotReleases()
	q.mu.Lock()
	defer q.mu.Unlock()

	if now.IsZero() {
		now = q.now()
	}
	if staleGrace <= 0 {
		staleGrace = 45 * time.Second
	}

	reclaimedIDs := q.reapExpiredLocked(now)
	for _, j := range q.jobs {
		if j.Status == JobStatusClaimed || j.Status == JobStatusLeased || j.Status == JobStatusRunning {
			stale := false
			if j.HeartbeatAt != nil && now.Sub(*j.HeartbeatAt) > staleGrace {
				stale = true
			}
			if j.LeaseExpiresAt != nil && now.After(*j.LeaseExpiresAt) {
				stale = true
			}
			if stale {
				prevHost := j.ClaimedHost
				if prevHost == "" {
					prevHost = j.HostID
				}
				if j.Attempts < DefaultMaxAttempts {
					j.Status = JobStatusPending
					j.ClaimedHost = ""
					j.HostID = ""
					j.LeaseOwner = ""
					j.LeaseToken = ""
					j.ClaimedAt = nil
					j.HeartbeatAt = nil
					j.LeaseExpiresAt = nil
					j.LeaseAckDeadlineAt = nil
					j.FencingToken = 0
					j.LeaseVersion = 0
					if j.DispatchDiagnostics == nil {
						j.DispatchDiagnostics = &DispatchDiagnostics{}
					}
					j.DispatchDiagnostics.QueueState = "queued"
					j.DispatchDiagnostics.LastDispatchEvaluationAt = now.Format(time.RFC3339)
					if j.DispatchDiagnostics.BlockedReasonsByHost == nil {
						j.DispatchDiagnostics.BlockedReasonsByHost = make(map[string]string)
					}
					j.DispatchDiagnostics.BlockedReasonsByHost["lease"] = fmt.Sprintf("reclaimed from %s after heartbeat timeout > %v", prevHost, staleGrace)
					if q.slotReleaser != nil {
						q.pendingSlotReleases = append(q.pendingSlotReleases, j.ID)
					}
					reclaimedIDs = append(reclaimedIDs, j.ID)
				} else {
					j.Status = JobStatusFailed
					j.Error = "HEARTBEAT_TIMEOUT: runner heartbeat stopped, max retry attempts exceeded"
					j.FailureCategory = FailureCategoryBackgroundDrainTimeout
					j.FailureStage = FailureStageBackgroundDrain
					j.FailureReasonSummary = j.Error
					j.Retriable = j.Attempts < DefaultMaxAttempts
					completedAt := now
					j.CompletedAt = &completedAt
					reclaimedIDs = append(reclaimedIDs, j.ID)
					if j.doneCh != nil {
						select {
						case <-j.doneCh:
						default:
							close(j.doneCh)
						}
					}
				}
				q.syncJobActiveStateLocked(j)
				if q.store != nil {
					sctx, cancel := storeContext(context.Background())
					_ = q.store.SaveQueueJob(sctx, j)
					cancel()
				}
			}
		}
	}

	return reclaimedIDs
}

// GetAttribution returns attribution details linking a completed job to its retrieval context.
func (q *InMemoryQueue) GetAttribution(ctx context.Context, jobID string) (JobAttributionResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return JobAttributionResponse{}, ErrJobNotFound
	}
	promoted := job.PromotedMemories
	if promoted == nil {
		promoted = []string{}
	}
	return JobAttributionResponse{
		JobID:            job.ID,
		TraceID:          job.TraceID,
		SearchID:         job.SearchID,
		Status:           job.Status,
		RelevanceEmitted: job.RelevanceEmitted,
		PromotedMemories: promoted,
		CompletedAt:      job.CompletedAt,
	}, nil
}

func generateJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("job_%s", hex.EncodeToString(b))
}

var defaultModelEscalationChain = []string{
	"gemini-3.8-flash-high",
	"gemini-4-carbon-high",
	"gemini-4-hydrogen-high",
}

// SelectStoryExecutionModel selects the execution model and effort tier for a story or prompt
// based on attempt number, target file count, and repository/submodule scope.
func SelectStoryExecutionModel(repo, submodule, promptText string, targetFilesCount, attempt int) (model, effort string) {
	r := strings.TrimSpace(repo)
	sub := strings.TrimSpace(submodule)
	isUmbrellaOrRoot := isUmbrellaOrRootRepo(r) || isUmbrellaOrRootRepo(sub)
	switch {
	case attempt >= 2:
		return "gemini-4-hydrogen-high", "xhigh"
	case attempt == 1 || targetFilesCount > 5 || isUmbrellaOrRoot:
		return "gemini-4-carbon-high", "high"
	default:
		return "gemini-3.8-flash-high", "high"
	}
}

func isUmbrellaOrRootRepo(name string) bool {
	return name == "." ||
		strings.EqualFold(name, "meta-repo") ||
		strings.EqualFold(name, "autopilot-umbrella") ||
		strings.EqualFold(name, "iazio-infra")
}

// FallbackModelsForModel returns remaining default escalation tiers after the selected model.
func FallbackModelsForModel(model string) []string {
	m := strings.TrimSpace(model)
	for i, tier := range defaultModelEscalationChain {
		if strings.EqualFold(m, tier) {
			if i+1 < len(defaultModelEscalationChain) {
				out := make([]string, len(defaultModelEscalationChain)-(i+1))
				copy(out, defaultModelEscalationChain[i+1:])
				return out
			}
			return nil
		}
	}
	out := make([]string, len(defaultModelEscalationChain))
	copy(out, defaultModelEscalationChain)
	return out
}

// ResolveJobModelSelection resolves the effective model, effort, and fallback ladder for enqueueing a job.
func ResolveJobModelSelection(explicitModel, explicitEffort, repo, submodule, promptText string, targetFilesCount int) (model, effort string, fallbackModels []string) {
	m := strings.TrimSpace(explicitModel)
	if m == "" || strings.EqualFold(m, "auto") {
		model, effort = SelectStoryExecutionModel(repo, submodule, promptText, targetFilesCount, 0)
	} else {
		model = m
		effort = defaultEffortForSelectedModel(m)
	}
	if e := strings.ToLower(strings.TrimSpace(explicitEffort)); e != "" {
		effort = e
	}
	fallbackModels = FallbackModelsForModel(model)
	return model, effort, fallbackModels
}

// DefaultEffortForSelectedModel maps a selected model identifier to its canonical reasoning effort.
func DefaultEffortForSelectedModel(model string) string {
	return defaultEffortForSelectedModel(model)
}

func defaultEffortForSelectedModel(model string) string {
	lower := strings.ToLower(strings.TrimSpace(model))
	switch {
	case lower == "gemini-4-hydrogen-high" || strings.HasSuffix(lower, "-xhigh"):
		return "xhigh"
	case strings.HasSuffix(lower, "-medium"):
		return "medium"
	case strings.HasSuffix(lower, "-low"):
		return "low"
	case strings.HasSuffix(lower, "-max"):
		return "max"
	case lower == "":
		return ""
	default:
		return "high"
	}
}

func advanceJobModelOnFailureRetry(job *Job) bool {
	if job == nil || (len(job.FallbackModels) == 0 && strings.TrimSpace(job.Model) == "") {
		return false
	}
	return AdvanceJobModelFallback(job)
}

// AdvanceJobModelFallback shifts job.Model to the next entry in job.FallbackModels
// (or next tier in the default escalation chain), returning false when exhausted.
func AdvanceJobModelFallback(job *Job) bool {
	if job == nil {
		return false
	}
	cur := strings.TrimSpace(job.Model)
	if job.FallbackModels != nil {
		for i, cand := range job.FallbackModels {
			next := strings.TrimSpace(cand)
			if next != "" && !strings.EqualFold(next, cur) {
				job.Model = next
				rest := make([]string, len(job.FallbackModels)-(i+1))
				copy(rest, job.FallbackModels[i+1:])
				job.FallbackModels = rest
				syncJobPayloadModel(job)
				return true
			}
		}
		job.FallbackModels = []string{}
		return false
	}

	if cur == "" {
		job.Model = defaultModelEscalationChain[0]
		syncJobPayloadModel(job)
		return true
	}
	for i, tier := range defaultModelEscalationChain {
		if strings.EqualFold(cur, tier) {
			if i+1 < len(defaultModelEscalationChain) {
				job.Model = defaultModelEscalationChain[i+1]
				syncJobPayloadModel(job)
				return true
			}
			return false
		}
	}
	return false
}

func syncJobPayloadModel(job *Job) {
	if job == nil || len(job.Payload) == 0 {
		return
	}
	var pm map[string]any
	if err := json.Unmarshal(job.Payload, &pm); err != nil || pm == nil {
		return
	}
	pm["model"] = job.Model
	if eff := defaultEffortForSelectedModel(job.Model); eff != "" {
		pm["effort"] = eff
	}
	if updated, err := json.Marshal(pm); err == nil {
		job.Payload = updated
	}
}
