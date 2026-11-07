package handlers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
)

// HistoryFinder provides lookup of historical job records.
type HistoryFinder interface {
	FindHistoryRow(jobID string) (control.HistoryRow, string, string, bool)
}

// StoryValidator validates story existence before execution dispatch.
type StoryValidator interface {
	StoryExists(storyID, submodule string) bool
}

// NotificationRecord models a terminal job or system event surfaced via /v1/notifications.
type NotificationRecord struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind,omitempty"`
	Category  string         `json:"category"`
	Severity  string         `json:"severity,omitempty"`
	Event     string         `json:"event,omitempty"`
	Title     string         `json:"title"`
	Message   string         `json:"message"`
	Source    string         `json:"source,omitempty"`
	HostID    string         `json:"host_id,omitempty"`
	RepoPath  string         `json:"repo_path,omitempty"`
	JobID     string         `json:"job_id,omitempty"`
	StoryID   string         `json:"story_id,omitempty"`
	Status    string         `json:"status,omitempty"`
	CreatedAt string         `json:"created_at"`
	Timestamp time.Time      `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// TerminalJobCallbackPayload is the canonical outbound payload for webhook and email completion callbacks.
type TerminalJobCallbackPayload struct {
	Event           string   `json:"event"`
	JobID           string   `json:"job_id"`
	StoryID         string   `json:"story_id"`
	StoryTitle      string   `json:"story_title,omitempty"`
	Repo            string   `json:"repo"`
	Status          string   `json:"status"`
	TargetAgent     string   `json:"target_agent,omitempty"`
	TargetHost      string   `json:"target_host,omitempty"`
	ConversationID  string   `json:"conversation_id,omitempty"`
	ConversationURL string   `json:"conversation_url,omitempty"`
	DurationMs      int64    `json:"duration_ms"`
	GitHeadSHA      string   `json:"git_head_sha"`
	Error           string   `json:"error"`
	Summary         string   `json:"summary"`
	DependsOnJobID  string   `json:"depends_on_job_id,omitempty"`
	DependsOnJobIDs []string `json:"depends_on_job_ids,omitempty"`
	PromotedJobIDs  []string `json:"promoted_job_ids,omitempty"`
	CompletedAt     string   `json:"completed_at,omitempty"`
}

const (
	modeStoryCreate           = "story.create"
	modeStoryExecute          = "story.execute"
	modeStoryCreateAndExecute = "story.create_and_execute"
	defaultASECommand         = "/autonomous-story-execution"
)

// JobLifecycleHandler manages querying, listing, canceling, and retrying agent jobs.
type JobLifecycleHandler struct {
	queue                dispatch.Queue
	store                codebase.Store
	scheduler            GlobalScheduler
	historyFinder        HistoryFinder
	storyValidator       StoryValidator
	engine               *control.Engine
	repoRoot             string
	notificationsMu      sync.Mutex
	notifications        []NotificationRecord
	notificationRecorder func(NotificationRecord)
	smtpSendFunc         func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
	webhookHTTPClient    *http.Client
	callbackWG           sync.WaitGroup
}

// NewJobLifecycleHandler constructs a new JobLifecycleHandler.
func NewJobLifecycleHandler(queue dispatch.Queue, store ...codebase.Store) *JobLifecycleHandler {
	var s codebase.Store
	if len(store) > 0 {
		s = store[0]
	}
	h := &JobLifecycleHandler{
		queue:             queue,
		store:             s,
		webhookHTTPClient: &http.Client{Timeout: 5 * time.Second},
	}
	if registrar, ok := queue.(interface {
		SetTerminalHook(fn func(job *dispatch.Job, promoted []*dispatch.Job))
	}); ok {
		registrar.SetTerminalHook(h.HandleTerminalJobTransition)
	}
	return h
}

// SetNotificationRecorder binds an external notification store callback (e.g. Server.RecordNotification).
func (h *JobLifecycleHandler) SetNotificationRecorder(fn func(NotificationRecord)) {
	h.notificationsMu.Lock()
	defer h.notificationsMu.Unlock()
	h.notificationRecorder = fn
}

// SetSMTPSendFunc overrides the SMTP transport function for testing email callback delivery.
func (h *JobLifecycleHandler) SetSMTPSendFunc(fn func(addr string, a smtp.Auth, from string, to []string, msg []byte) error) {
	h.notificationsMu.Lock()
	defer h.notificationsMu.Unlock()
	h.smtpSendFunc = fn
}

// SetWebhookHTTPClient overrides the HTTP client used for outbound webhook delivery.
func (h *JobLifecycleHandler) SetWebhookHTTPClient(client *http.Client) {
	h.notificationsMu.Lock()
	defer h.notificationsMu.Unlock()
	if client != nil {
		h.webhookHTTPClient = client
	}
}

// WaitForCallbacks waits until all in-flight asynchronous webhook/email callbacks complete or timeout expires.
func (h *JobLifecycleHandler) WaitForCallbacks(timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	done := make(chan struct{})
	go func() {
		h.callbackWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ListNotifications returns recorded terminal job notifications in reverse chronological order.
func (h *JobLifecycleHandler) ListNotifications() []NotificationRecord {
	h.notificationsMu.Lock()
	defer h.notificationsMu.Unlock()
	out := make([]NotificationRecord, len(h.notifications))
	copy(out, h.notifications)
	return out
}

// ServeNotifications serves GET /v1/notifications and GET /api/v1/notifications.
func (h *JobLifecycleHandler) ServeNotifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	items := h.ListNotifications()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"notifications": items,
		"total":         len(items),
	})
}

// BuildTerminalJobCallbackPayload constructs the structured terminal callback payload from a completed/failed/canceled job.
func BuildTerminalJobCallbackPayload(job *dispatch.Job, promoted []*dispatch.Job) TerminalJobCallbackPayload {
	if job == nil {
		return TerminalJobCallbackPayload{}
	}
	var durationMs int64
	if job.CompletedAt != nil && !job.CreatedAt.IsZero() {
		durationMs = job.CompletedAt.Sub(job.CreatedAt).Milliseconds()
		if durationMs < 0 {
			durationMs = 0
		}
	}
	if durationMs == 0 && job.ElapsedSeconds > 0 {
		durationMs = job.ElapsedSeconds * 1000
	}

	var gitHeadSHA, summary string
	if len(job.Result) > 0 {
		var resMap map[string]any
		if json.Unmarshal(job.Result, &resMap) == nil && resMap != nil {
			for _, k := range []string{"git_head_sha", "head_sha", "commit_sha", "sha"} {
				if v, ok := resMap[k].(string); ok && strings.TrimSpace(v) != "" {
					gitHeadSHA = strings.TrimSpace(v)
					break
				}
			}
			for _, k := range []string{"summary", "message", "output", "outcome_summary"} {
				if v, ok := resMap[k].(string); ok && strings.TrimSpace(v) != "" {
					summary = strings.TrimSpace(v)
					break
				}
			}
			if durationMs == 0 {
				if d, ok := resMap["duration_ms"].(float64); ok && int64(d) > 0 {
					durationMs = int64(d)
				}
			}
		}
	}
	if summary == "" {
		if job.Status == dispatch.JobStatusCompleted {
			if job.StoryID != "" {
				summary = fmt.Sprintf("Job %s completed story %s in %s", job.ID, job.StoryID, job.Repo)
			} else {
				summary = fmt.Sprintf("Job %s completed successfully in %s", job.ID, job.Repo)
			}
		} else {
			summary = strings.TrimSpace(job.Error)
			if summary == "" {
				summary = fmt.Sprintf("Job %s terminated with status %s", job.ID, job.Status)
			}
		}
	}

	var promotedIDs []string
	for _, p := range promoted {
		if p != nil && p.ID != "" {
			promotedIDs = append(promotedIDs, p.ID)
		}
	}

	completedAtStr := ""
	if job.CompletedAt != nil {
		completedAtStr = job.CompletedAt.UTC().Format(time.RFC3339)
	} else {
		completedAtStr = time.Now().UTC().Format(time.RFC3339)
	}

	effectiveHost := strings.TrimSpace(job.ClaimedHost)
	if effectiveHost == "" {
		effectiveHost = strings.TrimSpace(job.HostID)
	}
	if effectiveHost == "" {
		effectiveHost = strings.TrimSpace(job.TargetHost)
	}

	dispatch.PopulateJobConversationFields(job)
	return TerminalJobCallbackPayload{
		Event:           "job." + strings.ToLower(strings.TrimSpace(job.Status)),
		JobID:           job.ID,
		StoryID:         job.StoryID,
		StoryTitle:      job.StoryTitle,
		Repo:            job.Repo,
		Status:          job.Status,
		TargetAgent:     job.TargetAgent,
		TargetHost:      effectiveHost,
		ConversationID:  job.ConversationID,
		ConversationURL: job.ConversationURL,
		DurationMs:      durationMs,
		GitHeadSHA:      gitHeadSHA,
		Error:           job.Error,
		Summary:         summary,
		DependsOnJobID:  job.DependsOnJobID,
		DependsOnJobIDs: job.DependsOnJobIDs,
		PromotedJobIDs:  promotedIDs,
		CompletedAt:     completedAtStr,
	}
}

// FormatASECallbackEmail formats the deterministic [IAZIO-ASE-CALLBACK] email subject, body, and RFC 822 message.
func FormatASECallbackEmail(from string, to []string, payload TerminalJobCallbackPayload) (subject string, body string, rawMessage []byte) {
	subject = fmt.Sprintf("[IAZIO-ASE-CALLBACK] job=%s story=%s status=%s", payload.JobID, payload.StoryID, payload.Status)
	jsonBytes, _ := json.MarshalIndent(payload, "", "  ")
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("job_id: %s\n", payload.JobID))
	sb.WriteString(fmt.Sprintf("story_id: %s\n", payload.StoryID))
	sb.WriteString(fmt.Sprintf("repo: %s\n", payload.Repo))
	sb.WriteString(fmt.Sprintf("status: %s\n", payload.Status))
	sb.WriteString(fmt.Sprintf("duration_ms: %d\n", payload.DurationMs))
	sb.WriteString(fmt.Sprintf("git_head_sha: %s\n", payload.GitHeadSHA))
	sb.WriteString(fmt.Sprintf("error: %s\n", payload.Error))
	sb.WriteString(fmt.Sprintf("summary: %s\n", payload.Summary))
	if len(payload.PromotedJobIDs) > 0 {
		sb.WriteString(fmt.Sprintf("promoted_job_ids: %s\n", strings.Join(payload.PromotedJobIDs, ",")))
	}
	sb.WriteString("\n")
	sb.Write(jsonBytes)
	sb.WriteString("\n")
	body = sb.String()

	var msg strings.Builder
	if from != "" {
		msg.WriteString(fmt.Sprintf("From: %s\r\n", from))
	}
	if len(to) > 0 {
		msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(to, ", ")))
	}
	msg.WriteString(fmt.Sprintf("Subject: %s\r\n", subject))
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(body)
	return subject, body, []byte(msg.String())
}

// SignCallbackPayload computes the HMAC-SHA256 signature header value ("sha256=<hex>") for a webhook payload.
func SignCallbackPayload(body []byte, secret string) string {
	if secret == "" {
		secret = "iazio-ase-callback-default-secret"
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// HandleTerminalJobTransition executes outbound completion hooks (NotificationRecord, [IAZIO-ASE-CALLBACK] email, and webhook POST).
func (h *JobLifecycleHandler) HandleTerminalJobTransition(job *dispatch.Job, promoted []*dispatch.Job) {
	if h == nil || job == nil {
		return
	}
	payload := BuildTerminalJobCallbackPayload(job, promoted)

	// 1. Emit built-in NotificationRecord (/v1/notifications)
	category := "success"
	severity := "info"
	kind := "job_completed"
	title := fmt.Sprintf("Job %s completed", job.ID)
	if job.StoryID != "" {
		title = fmt.Sprintf("Story %s (%s) %s", job.StoryID, job.ID, job.Status)
	}
	switch job.Status {
	case dispatch.JobStatusFailed:
		category = "error"
		severity = "error"
		kind = "job_failed"
		if job.StoryID == "" {
			title = fmt.Sprintf("Job %s failed", job.ID)
		}
	case dispatch.JobStatusCanceled:
		category = "warning"
		severity = "warning"
		kind = "job_canceled"
		if job.StoryID == "" {
			title = fmt.Sprintf("Job %s canceled", job.ID)
		}
	}

	notif := NotificationRecord{
		ID:        fmt.Sprintf("notif_%s_%d", job.ID, time.Now().UTC().UnixNano()),
		Kind:      kind,
		Category:  category,
		Severity:  severity,
		Event:     payload.Event,
		Title:     title,
		Message:   payload.Summary,
		Source:    "iazio-harness-api",
		HostID:    payload.TargetHost,
		RepoPath:  job.Repo,
		JobID:     job.ID,
		StoryID:   job.StoryID,
		Status:    job.Status,
		CreatedAt: payload.CompletedAt,
		Timestamp: time.Now().UTC(),
		Metadata: map[string]any{
			"story_id":     job.StoryID,
			"repo":         job.Repo,
			"status":       job.Status,
			"duration_ms":  payload.DurationMs,
			"git_head_sha": payload.GitHeadSHA,
		},
	}

	h.notificationsMu.Lock()
	h.notifications = append([]NotificationRecord{notif}, h.notifications...)
	if len(h.notifications) > 200 {
		h.notifications = h.notifications[:200]
	}
	recorder := h.notificationRecorder
	smtpSender := h.smtpSendFunc
	httpClient := h.webhookHTTPClient
	h.notificationsMu.Unlock()

	if recorder != nil {
		recorder(notif)
	}

	// 2. Email Callback Hook ([IAZIO-ASE-CALLBACK])
	smtpHost := strings.TrimSpace(os.Getenv("IAZIO_CALLBACK_SMTP_HOST"))
	smtpPort := strings.TrimSpace(os.Getenv("IAZIO_CALLBACK_SMTP_PORT"))
	if smtpPort == "" {
		smtpPort = "587"
	}
	smtpUser := strings.TrimSpace(os.Getenv("IAZIO_CALLBACK_SMTP_USER"))
	smtpPass := os.Getenv("IAZIO_CALLBACK_SMTP_PASS")
	emailFrom := strings.TrimSpace(os.Getenv("IAZIO_CALLBACK_EMAIL_FROM"))
	if emailFrom == "" {
		emailFrom = smtpUser
	}
	if emailFrom == "" && smtpHost != "" {
		emailFrom = "ase-callback@iazio.local"
	}
	var emailTo []string
	for _, rawTo := range strings.Split(os.Getenv("IAZIO_CALLBACK_EMAIL_TO"), ",") {
		if trimmed := strings.TrimSpace(rawTo); trimmed != "" {
			emailTo = append(emailTo, trimmed)
		}
	}

	if smtpHost != "" && len(emailTo) > 0 {
		addr := smtpHost + ":" + smtpPort
		var auth smtp.Auth
		if smtpUser != "" && smtpPass != "" {
			auth = smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
		}
		_, _, rawMsg := FormatASECallbackEmail(emailFrom, emailTo, payload)
		if smtpSender != nil {
			if err := smtpSender(addr, auth, emailFrom, emailTo, rawMsg); err != nil {
				log.Printf("[ase-callback] SMTP callback failed for job %s: %v", job.ID, err)
			}
		} else {
			h.callbackWG.Add(1)
			go func() {
				defer h.callbackWG.Done()
				if err := smtp.SendMail(addr, auth, emailFrom, emailTo, rawMsg); err != nil {
					log.Printf("[ase-callback] SMTP callback failed for job %s: %v", job.ID, err)
				}
			}()
		}
	}

	// 3. Webhook Callback (callback_url + IAZIO_CALLBACK_WEBHOOK_URL fallback)
	var webhookURLs []string
	if u := strings.TrimSpace(job.CallbackURL); u != "" {
		webhookURLs = append(webhookURLs, u)
	}
	if fallbackURL := strings.TrimSpace(os.Getenv("IAZIO_CALLBACK_WEBHOOK_URL")); fallbackURL != "" {
		alreadyIncluded := false
		for _, existing := range webhookURLs {
			if existing == fallbackURL {
				alreadyIncluded = true
				break
			}
		}
		if !alreadyIncluded {
			webhookURLs = append(webhookURLs, fallbackURL)
		}
	}

	if len(webhookURLs) > 0 {
		if httpClient == nil {
			httpClient = &http.Client{Timeout: 5 * time.Second}
		}
		secret := os.Getenv("IAZIO_CALLBACK_WEBHOOK_SECRET")
		bodyBytes, err := json.Marshal(payload)
		if err == nil {
			sig := SignCallbackPayload(bodyBytes, secret)
			for _, targetURL := range webhookURLs {
				u := targetURL
				h.callbackWG.Add(1)
				go func() {
					defer h.callbackWG.Done()
					h.deliverWebhookWithRetry(httpClient, u, bodyBytes, sig, payload)
				}()
			}
		}
	}
}

func (h *JobLifecycleHandler) deliverWebhookWithRetry(client *http.Client, targetURL string, bodyBytes []byte, sig string, payload TerminalJobCallbackPayload) {
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("[ase-callback] invalid webhook URL %q for job %s: %v", targetURL, payload.JobID, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Iazio-Event", payload.Event)
		req.Header.Set("X-Iazio-Job-ID", payload.JobID)
		req.Header.Set("X-Iazio-Signature-256", sig)

		resp, err := client.Do(req)
		if err == nil && resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				log.Printf("[ase-callback] webhook %q returned non-retriable status %d for job %s", targetURL, resp.StatusCode, payload.JobID)
				return
			}
		}
		if attempt < maxAttempts {
			time.Sleep(time.Duration(attempt*25) * time.Millisecond)
		} else if err != nil {
			log.Printf("[ase-callback] webhook delivery to %q failed after %d attempts for job %s: %v", targetURL, maxAttempts, payload.JobID, err)
		}
	}
}

// SetRepoRoot overrides the workspace root directory used for story index resolution.
func (h *JobLifecycleHandler) SetRepoRoot(root string) {
	h.repoRoot = root
}

// SetEngine sets the control engine for dynamic tool priority allocation and story binding.
func (h *JobLifecycleHandler) SetEngine(engine *control.Engine) {
	h.engine = engine
}

// SetScheduler sets the global scheduler.
func (h *JobLifecycleHandler) SetScheduler(scheduler GlobalScheduler) {
	h.scheduler = scheduler
}

// SetHistoryFinder sets the historical job lookup handler.
func (h *JobLifecycleHandler) SetHistoryFinder(finder HistoryFinder) {
	h.historyFinder = finder
}

// SetStoryValidator sets the story validation provider.
func (h *JobLifecycleHandler) SetStoryValidator(v StoryValidator) {
	h.storyValidator = v
}

func (h *JobLifecycleHandler) isStoryAvailable(storyID, submodule string) bool {
	if h.storyValidator != nil {
		return h.storyValidator.StoryExists(storyID, submodule)
	}

	if h.engine != nil {
		if _, ok := h.engine.GetStory(storyID); ok {
			return true
		}
	}

	root := resolveStoriesRoot(h.repoRoot)
	all := loadIndexStories(root)
	for _, s := range all {
		if strings.EqualFold(s.ID, storyID) || strings.EqualFold(strings.TrimPrefix(s.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
			return true
		}
	}

	// Also check if any .story.md file matches this storyID in docs/stories, family hubs, or submodule/docs/stories
	storyCandidates := []string{
		filepath.Join(root, "docs", "stories"),
		filepath.Join(root, "iazio-infra", "docs", "stories"),
		filepath.Join(root, "autopilot-umbrella", "docs", "stories"),
		filepath.Join(root, "vigo", "docs", "stories"),
		filepath.Join(root, "eye-habit", "docs", "stories"),
	}
	if submodule != "" && submodule != "." {
		storyCandidates = append(storyCandidates,
			filepath.Join(root, submodule, "docs", "stories"),
			filepath.Join(submodule, "docs", "stories"),
		)
	}
	for _, hubDir := range []string{"iazio-infra", "autopilot-umbrella", "vigo", "eye-habit"} {
		for _, s := range loadIndexStories(filepath.Join(root, hubDir)) {
			if strings.EqualFold(s.ID, storyID) || strings.EqualFold(strings.TrimPrefix(s.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
				return true
			}
		}
	}

	for _, dir := range storyCandidates {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			eName := strings.ToUpper(e.Name())
			sID := strings.ToUpper(storyID)
			cleanID := strings.TrimPrefix(sID, "STORY-")
			if (strings.Contains(eName, sID) || (cleanID != "" && strings.Contains(eName, cleanID))) && (strings.HasSuffix(e.Name(), ".story.md") || strings.HasSuffix(e.Name(), ".md")) {
				return true
			}
		}
	}

	return false
}

// JobDetailResponse is the detailed JSON representation of a dispatch job.
type JobDetailResponse struct {
	ID                      string                        `json:"id"`
	ScheduleID              string                        `json:"schedule_id,omitempty"`
	Type                    string                        `json:"type"`
	Repo                    string                        `json:"repo"`
	TargetHost              string                        `json:"target_host,omitempty"`
	ClaimedHost             string                        `json:"claimed_host,omitempty"`
	HostID                  string                        `json:"host_id,omitempty"`
	TargetAgent             string                        `json:"target_agent,omitempty"`
	StoryID                 string                        `json:"story_id,omitempty"`
	StoryTitle              string                        `json:"story_title,omitempty"`
	StoryPath               string                        `json:"story_path,omitempty"`
	StoryAllocationStatus   string                        `json:"story_allocation_status,omitempty"`
	AllocationSource        string                        `json:"allocation_source,omitempty"`
	PriorityRankSelected    int                           `json:"priority_rank_selected,omitempty"`
	NormalizedPrompt        string                        `json:"normalized_prompt,omitempty"`
	EffectiveAction         string                        `json:"effective_action,omitempty"`
	SourceConversationTitle string                        `json:"source_conversation_title,omitempty"`
	ConversationID          string                        `json:"conversation_id,omitempty"`
	ActiveConversationID    string                        `json:"active_conversation_id,omitempty"`
	ConversationIDs         []string                      `json:"conversation_ids,omitempty"`
	ConversationURL         string                        `json:"conversation_url,omitempty"`
	AgentPID                int                           `json:"agent_pid,omitempty"`
	HarnessPID              int                           `json:"harness_pid,omitempty"`
	ToolPID                 int                           `json:"tool_pid,omitempty"`
	ElapsedSeconds          int64                         `json:"elapsed_seconds,omitempty"`
	LiveURL                 string                        `json:"live_url,omitempty"`
	Status                  string                        `json:"status"`
	Result                  json.RawMessage               `json:"result,omitempty"`
	Error                   string                        `json:"error,omitempty"`
	FailureCategory         string                        `json:"failure_category,omitempty"`
	FailureStage            string                        `json:"failure_stage,omitempty"`
	FailureDirection        string                        `json:"failure_direction,omitempty"`
	FailureService          string                        `json:"failure_service,omitempty"`
	FailureReasonSummary    string                        `json:"failure_reason_summary,omitempty"`
	Retriable               bool                          `json:"retriable,omitempty"`
	ParentID                string                        `json:"parent_id,omitempty"`
	InfraRequeueCount       int                           `json:"infra_requeue_count,omitempty"`
	AttemptHistory          []dispatch.AttemptFailure     `json:"attempt_history,omitempty"`
	TraceID                 string                        `json:"trace_id,omitempty"`
	ZipkinTraceID           string                        `json:"zipkin_trace_id,omitempty"`
	ZipkinTraceURL          string                        `json:"zipkin_trace_url,omitempty"`
	SearchID                string                        `json:"search_id,omitempty"`
	RelevanceEmitted        bool                          `json:"relevance_emitted"`
	PromotedMemories        []string                      `json:"promoted_memories,omitempty"`
	FencingToken            int64                         `json:"fencing_token,omitempty"`
	Attempts                int                           `json:"attempts,omitempty"`
	FinalTurnCount          int                           `json:"final_turn_count,omitempty"`
	FinalTranscriptSHA256   string                        `json:"final_transcript_sha256,omitempty"`
	HeartbeatAt             string                        `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt          string                        `json:"lease_expires_at,omitempty"`
	AgentAffinity           *dispatch.AgentAffinity       `json:"agent_affinity,omitempty"`
	DispatchDiagnostics     *dispatch.DispatchDiagnostics `json:"dispatch_diagnostics,omitempty"`
	DependsOnJobID          string                        `json:"depends_on_job_id,omitempty"`
	DependsOnJobIDs         []string                      `json:"depends_on_job_ids,omitempty"`
	CallbackURL             string                        `json:"callback_url,omitempty"`
	CreatedAt               string                        `json:"created_at"`
	ClaimedAt               string                        `json:"claimed_at,omitempty"`
	CompletedAt             string                        `json:"completed_at,omitempty"`
	DurationMs              int64                         `json:"duration_ms,omitempty"`
}

// ListQueueResponse contains the list of jobs matching a filter.
type ListQueueResponse struct {
	Items []JobDetailResponse `json:"items"`
	Total int                 `json:"total"`
}

// CancelJobRequest is the optional payload when canceling a job.
type CancelJobRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ExecuteStoryRequest specifies parameters for executing an existing or newly allocated story.
type ExecuteStoryRequest struct {
	Submodule               string   `json:"submodule,omitempty"`
	Repo                    string   `json:"repo,omitempty"`
	Backlog                 string   `json:"backlog,omitempty"`
	StoryID                 string   `json:"story_id,omitempty"`
	TargetAgent             string   `json:"target_agent,omitempty"`
	Host                    string   `json:"host,omitempty"`
	Prompt                  string   `json:"prompt,omitempty"`
	Request                 string   `json:"request,omitempty"`
	Specification           string   `json:"specification,omitempty"`
	Description             string   `json:"description,omitempty"`
	Action                  string   `json:"action,omitempty"`
	Command                 string   `json:"command,omitempty"`
	Mode                    string   `json:"mode,omitempty"`
	SourceConversationTitle string   `json:"source_conversation_title,omitempty"`
	SourceContext           string   `json:"source_context,omitempty"`
	TraceID                 string   `json:"trace_id,omitempty"`
	SearchID                string   `json:"search_id,omitempty"`
	DependsOnJobID          string   `json:"depends_on_job_id,omitempty"`
	DependsOnJobIDs         []string `json:"depends_on_job_ids,omitempty"`
	CallbackURL             string   `json:"callback_url,omitempty"`
}

// ExecuteStoryResponse is returned after queuing a story for execution.
type ExecuteStoryResponse struct {
	JobID                 string   `json:"job_id"`
	StoryID               string   `json:"story_id,omitempty"`
	Submodule             string   `json:"submodule"`
	Backlog               string   `json:"backlog,omitempty"`
	Mode                  string   `json:"mode,omitempty"`
	TargetAgent           string   `json:"target_agent"`
	TargetHost            string   `json:"target_host,omitempty"`
	TargetRepo            string   `json:"target_repo,omitempty"`
	Status                string   `json:"status"`
	StoryAllocationStatus string   `json:"story_allocation_status,omitempty"`
	AllocationSource      string   `json:"allocation_source,omitempty"`
	PriorityRankSelected  int      `json:"priority_rank_selected,omitempty"`
	NormalizedPrompt      string   `json:"normalized_prompt,omitempty"`
	EffectiveAction       string   `json:"effective_action,omitempty"`
	DependsOnJobID        string   `json:"depends_on_job_id,omitempty"`
	DependsOnJobIDs       []string `json:"depends_on_job_ids,omitempty"`
	CallbackURL           string   `json:"callback_url,omitempty"`
	ConversationID        string   `json:"conversation_id,omitempty"`
	ConversationURL       string   `json:"conversation_url,omitempty"`
	LiveURL               string   `json:"live_url,omitempty"`
	CreatedAt             string   `json:"created_at"`
}

// BindStoryRequest is the payload for PATCH /api/v1/jobs/{id}/story-binding.
type BindStoryRequest struct {
	FencingToken int64  `json:"fencing_token,omitempty"`
	StoryID      string `json:"story_id"`
	StoryTitle   string `json:"story_title,omitempty"`
	StoryPath    string `json:"story_path,omitempty"`
}

// GetJob serves GET /api/v1/jobs/{id}.
func (h *JobLifecycleHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	job, found := h.queue.Get(jobID)
	if (!found || job == nil) && strings.HasPrefix(jobID, "sch-") {
		altID := "job_" + strings.TrimPrefix(jobID, "sch-")
		if altJob, altFound := h.queue.Get(altID); altFound && altJob != nil {
			job = altJob
			found = true
			jobID = altID
		}
	}
	if !found || job == nil {
		if h.historyFinder != nil {
			if row, host, path, hFound := h.historyFinder.FindHistoryRow(jobID); hFound {
				resp := formatHistoryRowToJobDetail(row, host, path)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
		}
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
		return
	}

	if job.DispatchDiagnostics == nil && h.engine != nil && (job.Status == dispatch.JobStatusPending || job.Status == dispatch.JobStatusClaimed || job.Status == dispatch.JobStatusLeased) {
		diag := h.engine.EvaluateDispatchDiagnostics(job.TargetHost, job.TargetAgent)
		if job.Status == dispatch.JobStatusClaimed || job.Status == dispatch.JobStatusLeased {
			diag.QueueState = "running"
		}
		_ = h.queue.SetDispatchDiagnostics(jobID, diag)
		job.DispatchDiagnostics = diag
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ForceDispatchJob serves POST /api/v1/jobs/{id}/force-dispatch.
func (h *JobLifecycleHandler) ForceDispatchJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	// 1. Reconcile any stale workspace slots across the fleet
	if h.engine != nil {
		_ = h.engine.ReconcileStaleWorkspaceSlots(time.Now().UTC(), 45*time.Second)
	}

	// 2. Break claim on the job and re-queue it
	job, err := h.queue.ForceDispatch(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		if errors.Is(err, dispatch.ErrJobAlreadyCompleted) {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf("job %q has already completed or terminated", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to force dispatch job: %v", err))
		return
	}

	// 3. Evaluate live candidate diagnostics
	if h.engine != nil {
		diag := h.engine.EvaluateDispatchDiagnostics(job.TargetHost, job.TargetAgent)
		diag.QueueState = "dispatching"
		_ = h.queue.SetDispatchDiagnostics(jobID, diag)
		job.DispatchDiagnostics = diag
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ListQueue serves GET /api/v1/queue and GET /api/v1/jobs.
func (h *JobLifecycleHandler) ListQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	q := r.URL.Query()
	repo := strings.TrimSpace(q.Get("repo"))
	status := strings.TrimSpace(q.Get("status"))
	jobType := strings.TrimSpace(q.Get("type"))

	limit := 20
	if rawLimit := q.Get("limit"); rawLimit != "" {
		if l, err := strconv.Atoi(rawLimit); err == nil && l > 0 {
			limit = l
		}
	}

	filter := dispatch.JobFilter{
		Repo:   repo,
		Status: status,
		Type:   jobType,
		Limit:  limit,
	}
	jobs, err := h.queue.List(r.Context(), filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list queue: %v", err))
		return
	}

	items := make([]JobDetailResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, formatJobDetail(j))
	}

	total := len(items)
	if counter, ok := h.queue.(interface {
		Count(ctx context.Context, filter dispatch.JobFilter) (int, error)
	}); ok {
		if c, err := counter.Count(r.Context(), filter); err == nil {
			total = c
		}
	}

	resp := ListQueueResponse{
		Items: items,
		Total: total,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// CancelJob serves POST /api/v1/jobs/{id}/cancel.
func (h *JobLifecycleHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	reason := "cancelled by user"
	if r.Body != nil {
		var req CancelJobRequest
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
			if strings.TrimSpace(req.Reason) != "" {
				reason = strings.TrimSpace(req.Reason)
			}
		}
	}

	job, err := h.queue.Cancel(r.Context(), jobID, reason)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		if errors.Is(err, dispatch.ErrJobAlreadyCompleted) {
			writeJSONError(w, http.StatusConflict, fmt.Sprintf("job %q has already completed or terminated", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to cancel job: %v", err))
		return
	}

	resp := formatJobDetail(job)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// RetryJob serves POST /api/v1/jobs/{id}/retry.
func (h *JobLifecycleHandler) RetryJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	newJob, err := h.queue.Retry(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to retry job: %v", err))
		return
	}

	resp := formatJobDetail(newJob)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// ExecuteStory serves POST /api/v1/stories/{id}/execute or POST /api/v1/stories/execute.
func (h *JobLifecycleHandler) ExecuteStory(w http.ResponseWriter, r *http.Request) {
	traceID, parentID := telemetry.ExtractHTTP(r.Header)
	var spanOpts []telemetry.SpanOption
	if traceID != "" {
		spanOpts = append(spanOpts, telemetry.WithParentTrace(traceID, parentID))
	}
	spanOpts = append(spanOpts, telemetry.WithKind("SERVER"), telemetry.WithTag("http.method", r.Method), telemetry.WithTag("http.path", r.URL.Path))
	ctx, span := telemetry.StartSpan(r.Context(), "JobLifecycleHandler.ExecuteStory", spanOpts...)
	defer span.End()
	r = r.WithContext(ctx)

	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ExecuteStoryRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	storyID := strings.TrimSpace(r.PathValue("id"))
	if storyID == "" {
		storyID = strings.TrimSpace(req.StoryID)
	}

	mode := strings.TrimSpace(req.Mode)
	switch mode {
	case modeStoryCreateAndExecute, modeStoryCreate:
		// Modes 1 and 3 never mock or accept a pre-assigned story ID.
		storyID = ""
	case modeStoryExecute:
		if storyID == "" {
			writeJSONError(w, http.StatusUnprocessableEntity, "story_id is mandatory for story.execute")
			return
		}
	default:
		if storyID != "" {
			mode = modeStoryExecute
		} else {
			mode = modeStoryCreateAndExecute
		}
	}

	backlog := strings.TrimSpace(req.Backlog)
	submodule := strings.TrimSpace(req.Submodule)
	if submodule == "" {
		submodule = strings.TrimSpace(req.Repo)
	}

	var storyTitle, storyPath string
	root := resolveBacklogStoriesRoot(h.repoRoot, backlog, submodule)
	if storyID != "" {
		if h.engine != nil {
			if st, ok := h.engine.GetStory(storyID); ok {
				storyID = st.ID
				storyTitle = st.Title
				if submodule == "" && st.Submodule != "" {
					submodule = st.Submodule
				}
			}
		}
		for _, idxSt := range loadIndexStories(root) {
			if strings.EqualFold(idxSt.ID, storyID) || strings.EqualFold(strings.TrimPrefix(idxSt.ID, "STORY-"), strings.TrimPrefix(storyID, "STORY-")) {
				if storyTitle == "" {
					storyTitle = idxSt.Title
				}
				if idxSt.Path != "" {
					storyPath = idxSt.Path
				}
				if submodule == "" && len(idxSt.Submodules) > 0 && idxSt.Submodules[0] != "" {
					submodule = idxSt.Submodules[0]
				}
				break
			}
		}
		if storyPath == "" {
			storyPath = fmt.Sprintf("docs/stories/%s.story.md", strings.ToLower(storyID))
		}
	}

	if submodule == "" {
		submodule = "."
	} else if strings.EqualFold(submodule, "root") {
		submodule = "meta-repo"
	}

	storyAllocationStatus := "allocated"
	if storyID == "" {
		storyAllocationStatus = "pending_harness_creation"
	} else {
		if !h.isStoryAvailable(storyID, submodule) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("story %q not found in backlog: cannot execute nonexistent story. Create it first using create_story or verify it exists in docs/stories/.", storyID))
			return
		}
	}

	promptText := strings.TrimSpace(req.Prompt)
	if promptText == "" {
		promptText = strings.TrimSpace(req.Request)
	}
	if promptText == "" {
		promptText = strings.TrimSpace(req.Specification)
	}
	if promptText == "" {
		promptText = strings.TrimSpace(req.Description)
	}

	action := strings.TrimSpace(req.Command)
	if action == "" {
		action = strings.TrimSpace(req.Action)
	}
	if action == "" {
		if mode == modeStoryCreate {
			action = "/story-create"
		} else {
			action = defaultASECommand
		}
	}
	if !strings.HasPrefix(action, "/") {
		action = "/" + action
	}

	var normalizedPrompt string
	if mode == modeStoryExecute && storyID != "" {
		// Mode 2: concatenation of command ("/autonomous-story-execution") followed by story_id (and optional prompt)
		if strings.HasPrefix(promptText, action+" "+storyID) || strings.HasPrefix(promptText, action+"\n"+storyID) {
			normalizedPrompt = promptText
		} else {
			extra := strings.TrimSpace(strings.TrimPrefix(promptText, action))
			extra = strings.TrimSpace(strings.TrimPrefix(extra, storyID))
			if extra != "" {
				normalizedPrompt = action + " " + storyID + " " + extra
			} else {
				normalizedPrompt = action + " " + storyID
			}
		}
	} else {
		if promptText == "" {
			if mode == modeStoryCreate {
				promptText = fmt.Sprintf("Create story in backlog for %s.", submodule)
			} else {
				promptText = fmt.Sprintf("Execute autonomous story flow in %s.", submodule)
			}
		}
		normalizedPrompt = control.NormalizeASEPrompt(promptText, action)
	}

	targetAgent := strings.TrimSpace(req.TargetAgent)
	targetHost := strings.TrimSpace(req.Host)
	targetRepo := submodule
	allocationSource := "explicit_target"
	priorityRankSelected := 0

	if h.engine != nil {
		allocRepo := submodule
		if allocRepo == "meta-repo" {
			allocRepo = "."
		}
		alloc := h.engine.ResolveToolPriorityAllocation(targetAgent, targetHost, allocRepo)
		targetAgent = alloc.TargetAgent
		targetHost = alloc.TargetHost
		if alloc.TargetRepo != "" && alloc.TargetRepo != "root" && (submodule == "" || submodule == "." || submodule == "root" || submodule == "meta-repo") {
			targetRepo = alloc.TargetRepo
		}
		allocationSource = alloc.AllocationSource
		priorityRankSelected = alloc.PriorityRankSelected
	} else if targetAgent == "" {
		targetAgent = "agy"
	}
	if strings.EqualFold(targetRepo, "root") {
		targetRepo = "meta-repo"
	}

	if t := strings.TrimSpace(req.TraceID); t != "" {
		traceID = t
	}
	if traceID == "" {
		traceID = span.TraceID
	}

	promptID := fmt.Sprintf("prompt_%s_%d", storyID, time.Now().Unix())
	if storyID == "" {
		promptID = fmt.Sprintf("prompt_ase_%d", time.Now().Unix())
	}
	execTitle := fmt.Sprintf("Execute story %s", storyID)
	if storyID == "" {
		execTitle = "Autonomous Story Execution"
	}

	execPayload, err := json.Marshal(map[string]any{
		"prompt_id":                 promptID,
		"repo":                      targetRepo,
		"submodule":                 submodule,
		"backlog":                   backlog,
		"mode":                      mode,
		"command":                   action,
		"worktree_path":             targetRepo,
		"story_id":                  storyID,
		"story_title":               storyTitle,
		"story_path":                storyPath,
		"story_allocation_status":   storyAllocationStatus,
		"allocation_source":         allocationSource,
		"priority_rank_selected":    priorityRankSelected,
		"source_conversation_title": req.SourceConversationTitle,
		"title":                     execTitle,
		"prompt":                    normalizedPrompt,
		"normalized_prompt":         normalizedPrompt,
		"effective_action":          action,
		"target_agent":              targetAgent,
		"target_host":               targetHost,
		"trace_id":                  traceID,
		"depends_on_job_id":         strings.TrimSpace(req.DependsOnJobID),
		"depends_on_job_ids":        req.DependsOnJobIDs,
		"callback_url":              strings.TrimSpace(req.CallbackURL),
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal execution payload: %v", err))
		return
	}

	job, err := h.queue.Enqueue(r.Context(), &dispatch.Job{
		Type:                    dispatch.JobTypePromptExecute,
		Repo:                    targetRepo,
		TargetHost:              targetHost,
		TargetAgent:             targetAgent,
		StoryID:                 storyID,
		StoryTitle:              storyTitle,
		StoryPath:               storyPath,
		StoryAllocationStatus:   storyAllocationStatus,
		AllocationSource:        allocationSource,
		PriorityRankSelected:    priorityRankSelected,
		SourceConversationTitle: req.SourceConversationTitle,
		NormalizedPrompt:        normalizedPrompt,
		EffectiveAction:         action,
		TraceID:                 traceID,
		ParentID:                span.ID,
		SearchID:                strings.TrimSpace(req.SearchID),
		DependsOnJobID:          strings.TrimSpace(req.DependsOnJobID),
		DependsOnJobIDs:         req.DependsOnJobIDs,
		CallbackURL:             strings.TrimSpace(req.CallbackURL),
		Payload:                 execPayload,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to enqueue story execution: %v", err))
		return
	}

	if storyID != "" {
		if h.engine != nil {
			_ = h.engine.BindStoryToJob(job.ID, storyID, storyTitle, storyPath)
		}
		updateIndexStoryStatus(resolveStoriesRootForWrite(h.repoRoot), storyID, "IN_PROGRESS")
	}

	if h.queue == nil && h.scheduler != nil {
		_, _ = h.scheduler.EnqueueGlobalStorySchedule(storyID, submodule, targetAgent, normalizedPrompt)
	}

	liveURL := dispatch.BuildLiveURL("", job.ID)

	dispatch.PopulateJobConversationFields(job)
	resp := ExecuteStoryResponse{
		JobID:                 job.ID,
		StoryID:               storyID,
		Submodule:             submodule,
		Backlog:               backlog,
		Mode:                  mode,
		TargetAgent:           targetAgent,
		TargetHost:            targetHost,
		TargetRepo:            submodule,
		Status:                job.Status,
		StoryAllocationStatus: storyAllocationStatus,
		AllocationSource:      allocationSource,
		PriorityRankSelected:  priorityRankSelected,
		NormalizedPrompt:      normalizedPrompt,
		EffectiveAction:       action,
		DependsOnJobID:        job.DependsOnJobID,
		DependsOnJobIDs:       job.DependsOnJobIDs,
		CallbackURL:           job.CallbackURL,
		ConversationID:        job.ConversationID,
		ConversationURL:       job.ConversationURL,
		LiveURL:               liveURL,
		CreatedAt:             job.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// BindStory serves PATCH /api/v1/jobs/{id}/story-binding and POST /api/v1/jobs/{id}/story-binding.
func (h *JobLifecycleHandler) BindStory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch && r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req BindStoryRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	storyID := strings.TrimSpace(req.StoryID)
	if storyID == "" {
		writeJSONError(w, http.StatusBadRequest, "story_id is required")
		return
	}

	var job *dispatch.Job
	var err error
	if fencer, ok := h.queue.(interface {
		BindStoryWithFencing(ctx context.Context, jobID, storyID, storyTitle, storyPath string, fencingToken int64) (*dispatch.Job, error)
	}); ok {
		job, err = fencer.BindStoryWithFencing(r.Context(), jobID, storyID, req.StoryTitle, req.StoryPath, req.FencingToken)
	} else {
		job, err = h.queue.BindStory(r.Context(), jobID, storyID, req.StoryTitle, req.StoryPath)
	}
	if err != nil {
		if errors.Is(err, dispatch.ErrLeaseFenced) {
			writeJSONError(w, http.StatusConflict, "LEASE_FENCED: stale fencing token presented")
			return
		}
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if h.engine != nil {
		_ = h.engine.BindStoryToJob(jobID, storyID, req.StoryTitle, req.StoryPath)
	}
	updateIndexStoryStatus(resolveStoriesRootForWrite(h.repoRoot), storyID, "IN_PROGRESS")

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(formatJobDetail(job))
}

func formatHistoryRowToJobDetail(row control.HistoryRow, host, path string) JobDetailResponse {
	schedID := strings.TrimSpace(row.ScheduleID)
	if schedID == "" && strings.HasPrefix(row.JobID, "job_") {
		schedID = "sch-" + strings.TrimPrefix(row.JobID, "job_")
	}
	activeCID := strings.TrimSpace(row.ActiveConversationID)
	if activeCID == "" {
		activeCID = strings.TrimSpace(row.InitialConversationID)
	}
	convIDs := row.ConversationIDs
	if activeCID == "" && len(convIDs) > 0 {
		activeCID = strings.TrimSpace(convIDs[0])
	}
	if len(convIDs) == 0 && activeCID != "" {
		convIDs = []string{activeCID}
	}
	return JobDetailResponse{
		ID:                    row.JobID,
		ScheduleID:            schedID,
		Type:                  "story_execution",
		Repo:                  path,
		TargetHost:            host,
		TargetAgent:           row.Engine,
		StoryID:               row.StoryID,
		StoryAllocationStatus: row.StoryAllocationStatus,
		AllocationSource:      row.AllocationSource,
		PriorityRankSelected:  row.PriorityRankSelected,
		ConversationID:        activeCID,
		ActiveConversationID:  activeCID,
		ConversationIDs:       convIDs,
		ConversationURL:       dispatch.BuildConversationURL("", activeCID),
		LiveURL:               dispatch.BuildLiveURL("", row.JobID),
		Status:                row.Status,
		Error:                 row.ErrorMessage,
		TraceID:               row.TraceID,
		ZipkinTraceID:         row.ZipkinTraceID,
		ZipkinTraceURL:        row.ZipkinTraceURL,
		CreatedAt:             row.CreatedAt.Format(time.RFC3339),
		PromotedMemories:      []string{},
	}
}

func formatJobDetail(j *dispatch.Job) JobDetailResponse {
	if j == nil {
		return JobDetailResponse{}
	}
	cp := *j
	dispatch.PopulateJobConversationFields(&cp)
	j = &cp

	promoted := j.PromotedMemories
	if promoted == nil {
		promoted = []string{}
	}

	effectiveHost := strings.TrimSpace(j.TargetHost)
	if effectiveHost == "" || strings.EqualFold(effectiveHost, "auto") {
		if strings.TrimSpace(j.ClaimedHost) != "" {
			effectiveHost = strings.TrimSpace(j.ClaimedHost)
		} else if strings.TrimSpace(j.HostID) != "" {
			effectiveHost = strings.TrimSpace(j.HostID)
		}
	}

	convIDs := j.ConversationIDs
	if len(convIDs) == 0 && j.ActiveConversationID != "" {
		convIDs = []string{j.ActiveConversationID}
	}
	harnessPID := j.HarnessPID
	if harnessPID == 0 {
		harnessPID = j.AgentPID
	}
	elapsedSec := j.ElapsedSeconds
	if elapsedSec <= 0 && j.CompletedAt != nil && !j.CreatedAt.IsZero() {
		elapsedSec = int64(j.CompletedAt.Sub(j.CreatedAt).Seconds())
		if elapsedSec < 0 {
			elapsedSec = 0
		}
	}

	failureCat := j.FailureCategory
	failureStage := j.FailureStage
	failureDir := j.FailureDirection
	failureSvc := j.FailureService
	failureReason := j.FailureReasonSummary
	retriable := j.Retriable
	if strings.EqualFold(j.Status, dispatch.JobStatusCompleted) && strings.TrimSpace(j.Error) == "" {
		failureCat = ""
		failureStage = ""
		failureDir = ""
		failureSvc = ""
		failureReason = ""
		retriable = false
	} else if failureCat != "" && failureDir == "" {
		_, _, failureDir, failureSvc, _, _ = dispatch.ClassifyDispatchFailureWithDirection(failureCat + " " + failureStage + " " + failureReason)
	}

	diag := j.DispatchDiagnostics
	if diag != nil {
		stLower := strings.ToLower(strings.TrimSpace(j.Status))
		if stLower == dispatch.JobStatusRunning || stLower == dispatch.JobStatusCompleted || stLower == dispatch.JobStatusFailed || stLower == dispatch.JobStatusCanceled || stLower == "cancelled" || stLower == dispatch.JobStatusBlockedByDependency {
			diagCopy := *diag
			diagCopy.QueueState = stLower
			diag = &diagCopy
		}
	}

	schedID := ""
	if strings.HasPrefix(j.ID, "job_") {
		schedID = "sch-" + strings.TrimPrefix(j.ID, "job_")
	} else if strings.HasPrefix(j.ID, "job-") {
		schedID = "sch-" + strings.TrimPrefix(j.ID, "job-")
	}
	zID := strings.TrimSpace(j.ZipkinTraceID)
	if zID == "" {
		zID = strings.TrimSpace(j.TraceID)
	}
	zURL := strings.TrimSpace(j.ZipkinTraceURL)
	if zURL == "" && zID != "" {
		base := strings.TrimRight(strings.TrimSpace(os.Getenv("IAZIO_ZIPKIN_UI_URL")), "/")
		if base == "" {
			base = "https://tian.go.ro/zipkin"
		}
		zURL = fmt.Sprintf("%s/traces/%s", base, zID)
	}
	cleanStoryTitle := j.StoryTitle
	if j.StoryID != "" && cleanStoryTitle != "" {
		cleanStoryTitle, _ = dispatch.NormalizeStoryTitle(j.StoryID, cleanStoryTitle)
	}

	resp := JobDetailResponse{
		ID:                      j.ID,
		ScheduleID:              schedID,
		Type:                    j.Type,
		Repo:                    j.Repo,
		TargetHost:              effectiveHost,
		ClaimedHost:             j.ClaimedHost,
		HostID:                  j.HostID,
		TargetAgent:             j.TargetAgent,
		StoryID:                 j.StoryID,
		StoryTitle:              cleanStoryTitle,
		StoryPath:               j.StoryPath,
		StoryAllocationStatus:   j.StoryAllocationStatus,
		AllocationSource:        j.AllocationSource,
		PriorityRankSelected:    j.PriorityRankSelected,
		NormalizedPrompt:        j.NormalizedPrompt,
		EffectiveAction:         j.EffectiveAction,
		SourceConversationTitle: j.SourceConversationTitle,
		ConversationID:          j.ConversationID,
		ActiveConversationID:    j.ActiveConversationID,
		ConversationIDs:         convIDs,
		ConversationURL:         j.ConversationURL,
		AgentPID:                j.AgentPID,
		HarnessPID:              harnessPID,
		ToolPID:                 j.ToolPID,
		ElapsedSeconds:          elapsedSec,
		Status:                  j.Status,
		Result:                  j.Result,
		Error:                   j.Error,
		FailureCategory:         failureCat,
		FailureStage:            failureStage,
		FailureDirection:        failureDir,
		FailureService:          failureSvc,
		FailureReasonSummary:    failureReason,
		Retriable:               retriable,
		ParentID:                j.ParentID,
		InfraRequeueCount:       j.InfraRequeueCount,
		AttemptHistory:          j.AttemptHistory,
		TraceID:                 j.TraceID,
		ZipkinTraceID:           zID,
		ZipkinTraceURL:          zURL,
		SearchID:                j.SearchID,
		RelevanceEmitted:        j.RelevanceEmitted,
		PromotedMemories:        promoted,
		FencingToken:            j.FencingToken,
		Attempts:                j.Attempts,
		FinalTurnCount:          j.FinalTurnCount,
		FinalTranscriptSHA256:   j.FinalTranscriptSHA256,
		AgentAffinity:           j.AgentAffinity,
		DispatchDiagnostics:     diag,
		DependsOnJobID:          j.DependsOnJobID,
		DependsOnJobIDs:         j.DependsOnJobIDs,
		CallbackURL:             j.CallbackURL,
		CreatedAt:               j.CreatedAt.Format(time.RFC3339),
	}

	if j.ID != "" {
		resp.LiveURL = dispatch.BuildLiveURL("", j.ID)
	}

	if j.HeartbeatAt != nil {
		resp.HeartbeatAt = j.HeartbeatAt.Format(time.RFC3339)
	}
	if j.LeaseExpiresAt != nil {
		resp.LeaseExpiresAt = j.LeaseExpiresAt.Format(time.RFC3339)
	}

	if j.ClaimedAt != nil {
		resp.ClaimedAt = j.ClaimedAt.Format(time.RFC3339)
	}
	if j.CompletedAt != nil {
		resp.CompletedAt = j.CompletedAt.Format(time.RFC3339)
		if !j.CreatedAt.IsZero() {
			resp.DurationMs = j.CompletedAt.Sub(j.CreatedAt).Milliseconds()
		}
	}
	return resp
}

// GetJobAttribution serves GET /api/v1/jobs/{id}/attribution.
func (h *JobLifecycleHandler) GetJobAttribution(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	attr, err := h.queue.GetAttribution(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, dispatch.ErrJobNotFound) {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("job %q not found", jobID))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(attr)
}

// AckLeaseRequest is the payload for Phase 2 lease handshake.
type AckLeaseRequest struct {
	LeaseToken           string `json:"lease_token"`
	AgentPID             int    `json:"agent_pid"`
	ToolPID              int    `json:"tool_pid,omitempty"`
	ConversationID       string `json:"conversation_id,omitempty"`
	ActiveConversationID string `json:"active_conversation_id"`
	WorktreePath         string `json:"worktree_path,omitempty"`
	Repo                 string `json:"repo,omitempty"`
}

// AckLease serves POST /api/v1/global-queue/jobs/{id}/ack-lease and POST /api/v1/jobs/{id}/ack-lease.
func (h *JobLifecycleHandler) AckLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req AckLeaseRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}
	if strings.TrimSpace(req.ActiveConversationID) == "" && strings.TrimSpace(req.ConversationID) != "" {
		req.ActiveConversationID = strings.TrimSpace(req.ConversationID)
	}

	var qJob *dispatch.Job
	var errQueue error
	if h.queue != nil {
		qJob, errQueue = h.queue.AckLease(r.Context(), jobID, req.LeaseToken, req.AgentPID, req.ActiveConversationID)
	}

	var errEngine error
	if h.engine != nil {
		errEngine = h.engine.AckLease(jobID, req.LeaseToken, req.AgentPID, req.ActiveConversationID)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to acknowledge lease: %v", errQueue))
		return
	}

	effectiveWT := strings.TrimSpace(req.WorktreePath)
	if effectiveWT == "" {
		effectiveWT = strings.TrimSpace(req.Repo)
	}
	if qJob != nil {
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(qJob.Repo)
		}
		if h.engine != nil {
			hostID := qJob.ClaimedHost
			if hostID == "" {
				hostID = qJob.HostID
			}
			boundWT := h.engine.BindDispatchJobRepo(hostID, effectiveWT, jobID)
			if boundWT != "" {
				effectiveWT = boundWT
			}
		}
		if updater, ok := h.queue.(interface {
			UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*dispatch.Job, bool)
		}); ok {
			updater.UpdateRuntimeMetadata(jobID, effectiveWT, req.ActiveConversationID, req.AgentPID, req.ToolPID, 0)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":        "acknowledged",
		"job_id":        jobID,
		"worktree_path": effectiveWT,
		"message":       "Phase 2 lease handshake completed; job transitioned to running",
	})
}

// Heartbeat serves POST /api/v1/global-queue/jobs/{id}/heartbeat and POST /api/v1/jobs/{id}/heartbeat.
func (h *JobLifecycleHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req struct {
		LeaseToken           string `json:"lease_token"`
		FencingToken         int64  `json:"fencing_token"`
		WorktreePath         string `json:"worktree_path,omitempty"`
		Repo                 string `json:"repo,omitempty"`
		AgentPID             int    `json:"agent_pid,omitempty"`
		ToolPID              int    `json:"tool_pid,omitempty"`
		ActiveConversationID string `json:"active_conversation_id,omitempty"`
		ElapsedSeconds       int64  `json:"elapsed_seconds,omitempty"`
	}
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	var qJob *dispatch.Job
	var errQueue error
	if h.queue != nil {
		qJob, errQueue = h.queue.Heartbeat(r.Context(), jobID, req.FencingToken)
	}

	var errEngine error
	if h.engine != nil {
		errEngine = h.engine.HeartbeatJob(jobID, req.LeaseToken)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to heartbeat job: %v", errQueue))
		return
	}

	if qJob != nil {
		effectiveWT := strings.TrimSpace(req.WorktreePath)
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(req.Repo)
		}
		if effectiveWT == "" {
			effectiveWT = strings.TrimSpace(qJob.Repo)
		}
		if h.engine != nil {
			hostID := qJob.ClaimedHost
			if hostID == "" {
				hostID = qJob.HostID
			}
			boundWT := h.engine.BindDispatchJobRepo(hostID, effectiveWT, jobID)
			if boundWT != "" {
				effectiveWT = boundWT
			}
		}
		if updater, ok := h.queue.(interface {
			UpdateRuntimeMetadata(jobID, worktreePath, activeConvID string, harnessPID, toolPID int, elapsedSec int64) (*dispatch.Job, bool)
		}); ok {
			updater.UpdateRuntimeMetadata(jobID, effectiveWT, req.ActiveConversationID, req.AgentPID, req.ToolPID, req.ElapsedSeconds)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"job_id": jobID,
	})
}

// FailoverRequest carries details when a tool startup or pre-progress check fails.
type FailoverRequest struct {
	Host             string `json:"host"`
	Tool             string `json:"tool"`
	Workspace        string `json:"workspace"`
	ExitCode         int    `json:"exit_code"`
	StderrTail       string `json:"stderr_tail"`
	FailureCategory  string `json:"failure_category"`
	FailureStage     string `json:"failure_stage"`
	FailureDirection string `json:"failure_direction,omitempty"`
	FailureService   string `json:"failure_service,omitempty"`
	Reason           string `json:"reason"`
	Timestamp        string `json:"timestamp"`
}

// Failover serves POST /api/v1/global-queue/jobs/{id}/failover and POST /api/v1/jobs/{id}/failover.
func (h *JobLifecycleHandler) Failover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req FailoverRequest
	if r.Body != nil {
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<18))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &req)
		}
	}

	attempt := dispatch.AttemptFailure{
		Host:             req.Host,
		Tool:             req.Tool,
		Workspace:        req.Workspace,
		ExitCode:         req.ExitCode,
		StderrTail:       req.StderrTail,
		FailureCategory:  req.FailureCategory,
		FailureStage:     req.FailureStage,
		FailureDirection: req.FailureDirection,
		FailureService:   req.FailureService,
		Reason:           req.Reason,
		Timestamp:        req.Timestamp,
	}

	var errQueue error
	if h.queue != nil {
		_, errQueue = h.queue.Failover(r.Context(), jobID, attempt)
	}

	var errEngine error
	if h.engine != nil {
		h.engine.ReleaseDispatchJobRepo(jobID)
		_, errEngine = h.engine.FailoverJob(jobID, attempt)
	}

	if errQueue != nil && errEngine != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("failed to failover job: %v", errQueue))
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "failover_requeued",
		"job_id":  jobID,
		"message": "Tool failure recorded and cooled down; job requeued for next priority candidate",
	})
}

// RetryRetriable serves POST /api/v1/global-queue/jobs/retry-retriable.
func (h *JobLifecycleHandler) RetryRetriable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var retriedQueueCount int
	if h.queue != nil {
		retried, err := h.queue.RetryRetriable(r.Context())
		if err == nil {
			retriedQueueCount = len(retried)
		}
	}

	var retriedEngineCount int
	if h.engine != nil {
		retried, err := h.engine.RetryRetriableJobs()
		if err == nil {
			retriedEngineCount = len(retried)
		}
	}

	total := retriedQueueCount
	if retriedEngineCount > total {
		total = retriedEngineCount
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":        "ok",
		"retried_count": total,
		"message":       fmt.Sprintf("Re-enqueued %d retriable failed jobs", total),
	})
}
