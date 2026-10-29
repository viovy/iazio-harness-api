// Package httpserver exposes the control plane over HTTP.
package httpserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-harness-api/internal/api/handlers"
	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/gitlease"
	"github.com/viovy/iazio-harness-api/internal/sse"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
	"github.com/viovy/iazio-harness-api/internal/version"
)

// Server is the harness API.
type Server struct {
	Engine *control.Engine
	// Extract runs the share-page worker. Tests replace it.
	Extract func(shareURL string) (title, transcript string, err error)
	// SaveIdea persists an idea when a database is configured.
	SaveIdea func(idea control.Idea)
	// SavePrompt persists a prompt when a database is configured.
	SavePrompt func(p control.Prompt)
	// DeletePrompt removes a prompt when a database is configured.
	DeletePrompt func(id string)
	// SaveRepo persists a registered repo when a database is configured.
	SaveRepo func(r control.Repo)
	// DeleteRepo removes a registered repo when a database is configured.
	DeleteRepo func(hostID, path string)
	// SaveHost persists host inventory when a database is configured.
	SaveHost func(h control.Host)
	// SaveProfile persists a distribution profile when a database is configured.
	SaveProfile func(p control.DistributionProfile)
	// DeleteProfile removes a distribution profile when a database is configured.
	DeleteProfile func(name string)
	// DeleteHistory removes a single history record when a database is configured.
	DeleteHistory func(ctx context.Context, jobID string) error
	// ClearHistory removes all history records for a repo when a database is configured.
	ClearHistory func(ctx context.Context, hostID, worktreePath string) error
	// ReconcileLineage reconciles historical job and conversation lineages when a database is configured.
	ReconcileLineage func(ctx context.Context) error
	// AuthToken, when non-empty, requires Bearer token authentication on control-plane endpoints.
	AuthToken string
	// CodebaseStore manages fleet repository discovery and codebase RAG search chunks.
	CodebaseStore codebase.Store
	// EventHub manages cursor-resumable SSE streaming and heartbeat telemetry.
	EventHub *sse.Hub
	// Queue manages async job dispatching and execution for prompts and stories.
	Queue dispatch.Queue
	// GitLeaseManager coordinates distributed git commit serialization and submodule merge leases.
	GitLeaseManager *gitlease.Manager
	// Idempotency manages API idempotency caches.
	Idempotency *handlers.IdempotencyStore
	// StoryValidator validates story existence before execution.
	StoryValidator handlers.StoryValidator

	notificationsMu sync.RWMutex
	notifications   []NotificationRecord
}

// NotificationRecord captures system events, releases refreshes, and agent tool execution events.
type NotificationRecord struct {
	ID        string         `json:"id"`
	Title     string         `json:"title"`
	Message   string         `json:"message"`
	Category  string         `json:"category"` // "info", "success", "warning", "error", "userAction"
	Event     string         `json:"event,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source,omitempty"`
	JobID     string         `json:"job_id,omitempty"`
	HostID    string         `json:"host_id,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// RecordNotification records a notification in memory, capped to 500 entries.
func (s *Server) RecordNotification(rec NotificationRecord) {
	s.notificationsMu.Lock()
	defer s.notificationsMu.Unlock()
	if rec.ID == "" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		rec.ID = fmt.Sprintf("notif_%x", b)
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	if rec.Category == "" {
		rec.Category = "info"
	}
	s.notifications = append([]NotificationRecord{rec}, s.notifications...)
	if len(s.notifications) > 500 {
		s.notifications = s.notifications[:500]
	}
}

// GetNotifications returns recent notifications up to limit.
func (s *Server) GetNotifications(limit int) []NotificationRecord {
	s.notificationsMu.RLock()
	defer s.notificationsMu.RUnlock()
	if limit <= 0 || limit > len(s.notifications) {
		limit = len(s.notifications)
	}
	res := make([]NotificationRecord, limit)
	copy(res, s.notifications[:limit])
	return res
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	codebaseStore := s.CodebaseStore
	if codebaseStore == nil {
		codebaseStore = codebase.NewMemStore()
	}
	eventHub := s.EventHub
	if eventHub == nil {
		eventHub = sse.NewHub()
	}
	queue := s.Queue
	if queue == nil {
		queue = dispatch.NewInMemoryQueue()
		s.Queue = queue
	}
	idempotency := s.Idempotency
	if idempotency == nil {
		idempotency = handlers.NewIdempotencyStore(24 * time.Hour)
		s.Idempotency = idempotency
	}
	gitLeaseManager := s.GitLeaseManager
	if gitLeaseManager == nil {
		gitLeaseManager = gitlease.NewManager()
		s.GitLeaseManager = gitLeaseManager
	}

	mux.Handle("GET /api/v1/repos", handlers.NewReposHandler(codebaseStore))
	mux.Handle("POST /api/v1/codebase/search", handlers.NewCodebaseSearchHandler(codebaseStore))

	agentSyncHandler := handlers.NewAgentSyncHandler(codebaseStore, codebase.NewIngestor(codebaseStore, nil))
	mux.HandleFunc("POST /api/v1/agent/repos/sync", agentSyncHandler.SyncRepos)
	mux.HandleFunc("POST /api/v1/agent/codebase/sync", agentSyncHandler.SyncCodebase)

	// Dispatch & Story Lifecycle Endpoints
	storiesHandler := handlers.NewStoriesHandler(codebaseStore, queue, idempotency, s.Engine)
	if s.Engine != nil {
		storiesHandler.SetEngine(s.Engine)
	}
	mux.Handle("POST /api/v1/stories", storiesHandler)
	mux.HandleFunc("GET /api/v1/stories", storiesHandler.ListStories)
	mux.HandleFunc("GET /api/v1/stories/", storiesHandler.GetStory)
	mux.HandleFunc("POST /api/v1/stories/{id}/materialize", storiesHandler.MaterializeStory)

	promptsHandler := handlers.NewPromptsHandler(codebaseStore, queue, idempotency)
	if s.Engine != nil {
		promptsHandler.SetEngine(s.Engine)
	}
	mux.Handle("POST /api/v1/prompts", promptsHandler)

	jobLifecycle := handlers.NewJobLifecycleHandler(queue, codebaseStore)
	if s.Engine != nil {
		jobLifecycle.SetEngine(s.Engine)
		jobLifecycle.SetScheduler(s.Engine)
		jobLifecycle.SetHistoryFinder(s.Engine)
	}
	if s.StoryValidator != nil {
		jobLifecycle.SetStoryValidator(s.StoryValidator)
	}
	mux.HandleFunc("GET /api/v1/jobs/{id}", jobLifecycle.GetJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/attribution", jobLifecycle.GetJobAttribution)
	mux.HandleFunc("GET /api/v1/queue", jobLifecycle.ListQueue)
	mux.HandleFunc("GET /api/v1/jobs", jobLifecycle.ListQueue)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", jobLifecycle.CancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", jobLifecycle.RetryJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/ack-lease", jobLifecycle.AckLease)
	mux.HandleFunc("POST /v1/jobs/{id}/ack-lease", jobLifecycle.AckLease)
	mux.HandleFunc("POST /api/v1/global-queue/jobs/{id}/ack-lease", jobLifecycle.AckLease)
	mux.HandleFunc("POST /v1/global-queue/jobs/{id}/ack-lease", jobLifecycle.AckLease)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/ack-lease", jobLifecycle.AckLease)
	mux.HandleFunc("POST /v1/agent/jobs/{id}/ack-lease", jobLifecycle.AckLease)

	mux.HandleFunc("POST /api/v1/jobs/{id}/heartbeat", jobLifecycle.Heartbeat)
	mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", jobLifecycle.Heartbeat)
	mux.HandleFunc("POST /api/v1/global-queue/jobs/{id}/heartbeat", jobLifecycle.Heartbeat)
	mux.HandleFunc("POST /v1/global-queue/jobs/{id}/heartbeat", jobLifecycle.Heartbeat)

	mux.HandleFunc("POST /api/v1/jobs/{id}/failover", jobLifecycle.Failover)
	mux.HandleFunc("POST /v1/jobs/{id}/failover", jobLifecycle.Failover)
	mux.HandleFunc("POST /api/v1/global-queue/jobs/{id}/failover", jobLifecycle.Failover)
	mux.HandleFunc("POST /v1/global-queue/jobs/{id}/failover", jobLifecycle.Failover)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/failover", jobLifecycle.Failover)
	mux.HandleFunc("POST /v1/agent/jobs/{id}/failover", jobLifecycle.Failover)

	mux.HandleFunc("POST /api/v1/global-queue/jobs/retry-retriable", jobLifecycle.RetryRetriable)
	mux.HandleFunc("POST /v1/global-queue/jobs/retry-retriable", jobLifecycle.RetryRetriable)
	mux.HandleFunc("POST /api/v1/jobs/retry-retriable", jobLifecycle.RetryRetriable)
	mux.HandleFunc("POST /v1/jobs/retry-retriable", jobLifecycle.RetryRetriable)
	mux.HandleFunc("POST /api/v1/jobs/{id}/force-dispatch", jobLifecycle.ForceDispatchJob)
	mux.HandleFunc("POST /v1/jobs/{id}/force-dispatch", jobLifecycle.ForceDispatchJob)
	mux.HandleFunc("POST /api/v1/global-queue/jobs/{id}/force-dispatch", jobLifecycle.ForceDispatchJob)
	mux.HandleFunc("POST /v1/global-queue/jobs/{id}/force-dispatch", jobLifecycle.ForceDispatchJob)
	mux.HandleFunc("POST /api/v1/stories/{id}/execute", jobLifecycle.ExecuteStory)
	mux.HandleFunc("POST /api/v1/stories/execute", jobLifecycle.ExecuteStory)
	mux.HandleFunc("POST /v1/stories/{id}/execute", jobLifecycle.ExecuteStory)
	mux.HandleFunc("POST /v1/stories/execute", jobLifecycle.ExecuteStory)
	mux.HandleFunc("PATCH /api/v1/jobs/{id}/story-binding", jobLifecycle.BindStory)
	mux.HandleFunc("POST /api/v1/jobs/{id}/story-binding", jobLifecycle.BindStory)
	mux.HandleFunc("PATCH /v1/jobs/{id}/story-binding", jobLifecycle.BindStory)
	mux.HandleFunc("POST /v1/jobs/{id}/story-binding", jobLifecycle.BindStory)

	agentJobsHandler := handlers.NewAgentJobsHandler(queue)
	mux.HandleFunc("POST /api/v1/agent/jobs/claim", agentJobsHandler.ClaimHandler)
	mux.HandleFunc("POST /v1/agent/jobs/claim", agentJobsHandler.ClaimHandler)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/complete", agentJobsHandler.CompleteHandler)
	mux.HandleFunc("POST /v1/agent/jobs/{id}/complete", agentJobsHandler.CompleteHandler)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/heartbeat", agentJobsHandler.HeartbeatHandler)
	mux.HandleFunc("POST /v1/agent/jobs/{id}/heartbeat", agentJobsHandler.HeartbeatHandler)
	mux.HandleFunc("GET /v1/notifications", s.getNotifications)
	mux.HandleFunc("GET /api/v1/notifications", s.getNotifications)
	mux.HandleFunc("POST /v1/notifications", s.createNotification)
	mux.HandleFunc("POST /api/v1/notifications", s.createNotification)
	mux.HandleFunc("POST /v1/fleet/tools/update-all", s.queueAllToolsUpdate)
	mux.HandleFunc("POST /v1/hosts/all/tools/update", s.queueAllToolsUpdate)

	// Periodic 15s Global Queue Slot & Lease Reconciler (STORY-GOV-3690)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now().UTC()
			if s.Engine != nil {
				_ = s.Engine.ReconcileStaleWorkspaceSlots(now, 45*time.Second)
			}
			if queue != nil {
				_ = queue.ReconcileGlobalQueueSlots(now, 45*time.Second)
			}
		}
	}()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "iazio-harness-api",
			"version": version.Informational(),
		})
	})
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/observability/metrics", s.observabilityMetrics)
	mux.HandleFunc("GET /v1/observability/stats", s.observabilityMetrics)
	mux.HandleFunc("GET /api/v1/observability/metrics", s.observabilityMetrics)
	mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("GET /swagger/", s.swaggerUI)
	mux.HandleFunc("GET /swagger/openapi.json", s.openAPISpec)
	mux.HandleFunc("GET /v1/ideas", s.listIdeas)
	mux.HandleFunc("POST /v1/ideas/import-gemini", s.importGemini)
	mux.HandleFunc("POST /v1/ideas/{id}/triage", s.triage)
	mux.HandleFunc("POST /v1/ideas/{id}/refine", s.refine)
	mux.HandleFunc("GET /v1/prompts", s.listPrompts)
	mux.HandleFunc("POST /v1/prompts", s.createPrompt)
	mux.HandleFunc("PUT /v1/prompts/{id}", s.updatePrompt)
	mux.HandleFunc("DELETE /v1/prompts/{id}", s.deletePrompt)
	mux.HandleFunc("POST /v1/prompts/{id}/execute", s.executePrompt)
	mux.HandleFunc("GET /v1/schedules", s.listSchedules)
	mux.HandleFunc("GET /v1/schedules/{id}", s.getSchedule)
	mux.HandleFunc("POST /v1/schedules/{id}/priority", s.changePriority)
	mux.HandleFunc("POST /v1/schedules/{id}/increase-priority", s.increasePriority)
	mux.HandleFunc("POST /v1/schedules/{id}/decrease-priority", s.decreasePriority)
	mux.HandleFunc("POST /v1/schedules/{id}/cancel", s.cancelSchedule)
	mux.HandleFunc("DELETE /v1/schedules/{id}", s.cancelSchedule)
	mux.HandleFunc("GET /v1/history", s.listHistory)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/exit", s.postExit)
	mux.HandleFunc("POST /v1/jobs/{id}/decline", s.declineJob)
	mux.HandleFunc("POST /v1/jobs/{id}/story-draft", s.storyDraft)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /v1/jobs/{id}/abandon", s.abandonJob)
	mux.HandleFunc("POST /v1/jobs/{id}/resume", s.resumeJob)
	mux.HandleFunc("POST /v1/jobs/{id}/pause-auto-resume", s.pauseAutoResumeJob)
	mux.HandleFunc("POST /v1/jobs/{id}/force-resume", s.forceResumeJob)
	mux.HandleFunc("POST /v1/jobs/{id}/force-pause", s.forcePauseJob)
	mux.HandleFunc("POST /v1/jobs/{id}/kill-background-tasks", s.killBackgroundTasks)
	mux.HandleFunc("POST /api/v1/jobs/{id}/kill-background-tasks", s.killBackgroundTasks)
	mux.HandleFunc("POST /v1/jobs/{id}/stash-clean-restart", s.stashCleanRestart)
	mux.HandleFunc("POST /api/v1/jobs/{id}/stash-clean-restart", s.stashCleanRestart)
	mux.HandleFunc("POST /v1/jobs/{id}/retry-correlation", s.retryCorrelation)
	mux.HandleFunc("POST /v1/jobs/{id}/chunks", s.appendChunk)
	mux.HandleFunc("POST /v1/jobs/{id}/conversations", s.addJobConversation)
	mux.HandleFunc("POST /v1/jobs/{id}/readiness", s.postJobReadiness)
	mux.HandleFunc("POST /api/v1/jobs/{id}/readiness", s.postJobReadiness)
	mux.HandleFunc("GET /api/v1/conversations/{id}/rehydration-bundle", s.getRehydrationBundle)
	mux.HandleFunc("GET /v1/conversations/{id}/rehydration-bundle", s.getRehydrationBundle)
	mux.HandleFunc("GET /v1/jobs/{id}/logs", s.jobLogs)
	mux.HandleFunc("GET /v1/jobs/{id}/stream", s.jobStream)
	mux.HandleFunc("GET /api/v1/jobs/{id}/stream", s.jobStream)
	mux.HandleFunc("GET /v1/global-queue/jobs/{id}/stream", s.jobStream)
	mux.HandleFunc("GET /api/v1/global-queue/jobs/{id}/stream", s.jobStream)

	// Git merge lease coordinator endpoints (STORY-GOV-3705)
	mux.HandleFunc("POST /api/v1/git-leases/acquire", s.acquireGitLease)
	mux.HandleFunc("POST /v1/git-leases/acquire", s.acquireGitLease)
	mux.HandleFunc("POST /api/v1/git-leases/renew", s.renewGitLease)
	mux.HandleFunc("POST /v1/git-leases/renew", s.renewGitLease)
	mux.HandleFunc("POST /api/v1/git-leases/release", s.releaseGitLease)
	mux.HandleFunc("POST /v1/git-leases/release", s.releaseGitLease)
	mux.HandleFunc("GET /api/v1/git-leases", s.listGitLeases)
	mux.HandleFunc("GET /v1/git-leases", s.listGitLeases)
	mux.HandleFunc("GET /api/v1/git-leases/{repo...}", s.getGitLease)
	mux.HandleFunc("GET /v1/git-leases/{repo...}", s.getGitLease)
	mux.HandleFunc("GET /v1/jobs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		eventHub.ServeHTTP(w, r, id)
	})
	mux.HandleFunc("POST /v1/jobs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var body struct {
			Type string `json:"type"`
			Data any    `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		ev := eventHub.Publish(id, body.Type, body.Data)
		writeJSON(w, http.StatusCreated, ev)
	})
	mux.HandleFunc("POST /v1/hosts/register", s.register)
	mux.HandleFunc("GET /v1/hosts", s.listHosts)
	mux.HandleFunc("GET /v1/hosts/{id}", s.hostDetail)
	mux.HandleFunc("GET /api/v1/agents/{id}/tools", s.getAgentTools)
	mux.HandleFunc("GET /v1/hosts/{id}/tools", s.getAgentTools)
	mux.HandleFunc("DELETE /v1/hosts/{id}/tools/{tool}", s.deleteAgentTool)
	mux.HandleFunc("DELETE /api/v1/agents/{id}/tools/{tool}", s.deleteAgentTool)
	mux.HandleFunc("DELETE /api/agents/{id}/tools/{tool}", s.deleteAgentTool)
	mux.HandleFunc("GET /api/v1/agents/{host}/jobs/{id}", s.getJob)
	mux.HandleFunc("GET /api/v1/agents/{host}/{repo}/jobs/{id}", s.getJob)
	mux.HandleFunc("GET /api/v1/agents/{rest...}", s.getAgentRest)
	mux.HandleFunc("POST /api/v1/admin/jobs/reconcile-lineage", s.reconcileLineage)
	mux.HandleFunc("POST /v1/admin/jobs/reconcile-lineage", s.reconcileLineage)
	mux.HandleFunc("POST /v1/hosts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /api/v1/hosts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /api/hosts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/update", s.queueToolUpdate)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/{tool}/update", s.queueToolUpdate)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /v1/fleet/agents/{id}/tools/{tool}/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/{tool}/update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/{tool}/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/agents/{id}/tools/update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/agents/{id}/tools/{tool}/update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/agents/{id}/tools/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /api/agents/{id}/tools/{tool}/force-update", s.queueToolUpdate)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/{tool}/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /api/v1/agents/{id}/tools/{tool}/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /api/agents/{id}/tools/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /api/agents/{id}/tools/{tool}/cleanup-binaries", s.queueToolCleanupBinaries)
	mux.HandleFunc("POST /v1/hosts/{id}/poll", s.pollHost)
	mux.HandleFunc("GET /v1/hosts/{host}/repos", s.repoDetail)
	mux.HandleFunc("DELETE /v1/hosts/{host}/repos", s.unregisterRepo)
	mux.HandleFunc("DELETE /v1/hosts/{host}/repos/history", s.clearRepoHistory)
	mux.HandleFunc("DELETE /v1/history/{id}", s.deleteHistoryRow)
	mux.HandleFunc("GET /v1/history/{id}", s.getJob)
	mux.HandleFunc("POST /v1/hosts/{host}/repo-requests", s.repoRequest)
	mux.HandleFunc("POST /v1/hosts/{host}/docs-hub/discard", s.discardHub)
	mux.HandleFunc("POST /v1/repos", s.upsertRepo)
	mux.HandleFunc("POST /v1/repos/discard", s.discardRepo)
	mux.HandleFunc("POST /v1/repos/discard-all", s.discardAllRepos)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/discard-all", s.discardAllRepos)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/discard-all", s.discardAllRepos)
	mux.HandleFunc("POST /api/v1/agents/{host}/{repo}/actions/discard-changes", s.discardRepo)
	mux.HandleFunc("POST /v1/repos/intervention", s.intervention)
	mux.HandleFunc("POST /v1/repos/{host}/lease", s.lease)
	mux.HandleFunc("POST /v1/repos/{host}/release", s.releaseExternalLease)
	mux.HandleFunc("POST /v1/repos/{host}/preflight", s.preflight)
	mux.HandleFunc("POST /v1/repos/{host}/finish", s.finish)
	mux.HandleFunc("POST /v1/repos/{host}/force-pause", s.forcePause)
	mux.HandleFunc("POST /v1/repos/{host}/resume", s.resumeRepo)
	mux.HandleFunc("POST /v1/repos/{host}/resume-in-place", s.resumeRepoInPlace)
	mux.HandleFunc("POST /v1/repos/{host}/remediate", s.remediateRepo)
	mux.HandleFunc("POST /v1/repos/{host}/queue-allocation", s.setRepoQueueAllocation)
	mux.HandleFunc("GET /v1/repos/{host}", s.getRepo)
	mux.HandleFunc("GET /v1/fleet/profiles", s.listProfiles)
	mux.HandleFunc("POST /v1/fleet/profiles", s.createProfile)
	mux.HandleFunc("GET /v1/fleet/profiles/{name}", s.getProfile)
	mux.HandleFunc("PUT /v1/fleet/profiles/{name}", s.updateProfile)
	mux.HandleFunc("DELETE /v1/fleet/profiles/{name}", s.deleteProfile)
	mux.HandleFunc("POST /v1/fleet/profiles/generic/sync", s.syncGenericProfile)
	mux.HandleFunc("POST /v1/fleet/profiles/scan-repo", s.scanRepoProfile)
	mux.HandleFunc("GET /v1/fleet/manifest", s.getFleetManifest)
	mux.HandleFunc("GET /v1/fleet/profiles/{name}/releases", s.getProfileReleases)
	mux.HandleFunc("GET /v1/fleet/credentials/git", s.listGitCredentials)
	mux.HandleFunc("POST /v1/fleet/credentials/git", s.registerGitCredential)
	mux.HandleFunc("DELETE /v1/fleet/credentials/git/{id}", s.deleteGitCredential)
	mux.HandleFunc("POST /v1/fleet/credentials/git/{id}/test", s.testSavedGitCredential)
	mux.HandleFunc("POST /v1/fleet/credentials/git/test", s.testGitCredentialPayload)
	mux.HandleFunc("POST /api/v1/git-credentials/{id}/test", s.testSavedGitCredential)
	mux.HandleFunc("POST /api/v1/git-credentials/test", s.testGitCredentialPayload)
	mux.HandleFunc("POST /v1/fleet/profiles/{name}/refresh", s.refreshProfileReleases)
	mux.HandleFunc("POST /api/v1/binaries-profiles/{name}/refresh", s.refreshProfileReleases)
	mux.HandleFunc("POST /v1/fleet/profiles/sync", s.syncAllProfiles)
	mux.HandleFunc("POST /api/v1/binaries-profiles/sync", s.syncAllProfiles)
	mux.HandleFunc("POST /v1/hosts/{host}/ping", s.pingHost)
	mux.HandleFunc("POST /api/v1/agents/{host}/ping", s.pingHost)
	mux.HandleFunc("POST /v1/hosts/{host}/ping-ack", s.pingAck)
	mux.HandleFunc("POST /api/v1/agents/{host}/ping-ack", s.pingAck)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/check-fleet-status", s.checkFleetStatus)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/check-fleet-status", s.checkFleetStatus)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/heal-ase", s.healASERepo)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/heal-ase", s.healASERepo)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/retry-heal", s.retryHealRepo)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/retry-heal", s.retryHealRepo)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/force-clean-git", s.forceCleanGitRepo)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/force-clean-git", s.forceCleanGitRepo)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/clear-healing", s.clearHealingRepo)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/clear-healing", s.clearHealingRepo)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/clean-and-dispatch-next", s.cleanAndDispatchNext)
	mux.HandleFunc("POST /api/v1/agents/{host}/repos/clean-and-dispatch-next", s.cleanAndDispatchNext)
	mux.HandleFunc("POST /v1/hosts/{host}/repos/status", s.updateRepoRuntimeStatus)
	mux.HandleFunc("POST /v1/hosts/{host}/eager-readiness", s.updateEagerReadiness)
	mux.HandleFunc("POST /api/v1/agents/{host}/eager-readiness", s.updateEagerReadiness)
	mux.HandleFunc("POST /api/agents/{host}/eager-readiness", s.updateEagerReadiness)
	mux.HandleFunc("GET /v1/hosts/{host}/eager-readiness", s.getEagerReadiness)
	mux.HandleFunc("GET /api/v1/agents/{host}/eager-readiness", s.getEagerReadiness)
	mux.HandleFunc("GET /api/agents/{host}/eager-readiness", s.getEagerReadiness)
	mux.HandleFunc("POST /v1/hosts/{host}/eager-update/evaluate", s.evaluateEagerUpdate)
	mux.HandleFunc("POST /api/v1/agents/{host}/eager-update/evaluate", s.evaluateEagerUpdate)
	mux.HandleFunc("POST /api/agents/{host}/eager-update/evaluate", s.evaluateEagerUpdate)
	mux.HandleFunc("POST /v1/hosts/{host}/eager-update/force", s.forceEagerUpdate)
	mux.HandleFunc("POST /api/v1/agents/{host}/eager-update/force", s.forceEagerUpdate)
	mux.HandleFunc("POST /api/agents/{host}/eager-update/force", s.forceEagerUpdate)
	mux.HandleFunc("GET /v1/fleet/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /v1/fleet/tool-priority", s.setToolPriority)
	mux.HandleFunc("GET /api/v1/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /api/v1/tool-priority", s.setToolPriority)
	mux.HandleFunc("GET /v1/global-queue/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /v1/global-queue/tool-priority", s.setToolPriority)
	mux.HandleFunc("GET /api/v1/global-queue/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /api/v1/global-queue/tool-priority", s.setToolPriority)
	return withTracing(withCORS(withAuth(mux, s.AuthToken)))
}

func withTracing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/healthz" || path == "/readyz" || path == "/version" || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		traceID, parentID := telemetry.ExtractHTTP(r.Header)
		var spanOpts []telemetry.SpanOption
		if traceID != "" {
			spanOpts = append(spanOpts, telemetry.WithParentTrace(traceID, parentID))
		}
		spanOpts = append(spanOpts,
			telemetry.WithKind("SERVER"),
			telemetry.WithTag("http.method", r.Method),
			telemetry.WithTag("http.path", path),
		)
		ctx, span := telemetry.StartSpan(r.Context(), r.Method+" "+path, spanOpts...)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func withAuth(next http.Handler, token string) http.Handler {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" || path == "/healthz" || path == "/readyz" || path == "/version" ||
			strings.HasPrefix(path, "/swagger") || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(authHeader, prefix) {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		got := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, Last-Event-ID, X-Requested-With, Origin")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
		w.Header().Set("Access-Control-Expose-Headers", "*")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": version.Informational()})
}

func (s *Server) importGemini(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ShareURL string `json:"share_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := control.ValidateShareURL(body.ShareURL); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Engine.BeginExtract(); err != nil {
		writeErr(w, http.StatusTooManyRequests, err)
		return
	}
	defer s.Engine.EndExtract()
	title, transcript := "imported", ""
	if s.Extract != nil {
		var err error
		title, transcript, err = s.Extract(body.ShareURL)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err)
			return
		}
	}
	idea, err := s.Engine.ImportIdea(body.ShareURL, transcript, title)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveIdea != nil {
		s.SaveIdea(idea)
	}
	writeJSON(w, http.StatusCreated, ideaView(idea))
}

func (s *Server) listIdeas(w http.ResponseWriter, _ *http.Request) {
	ideas := s.Engine.ListIdeas()
	rows := make([]map[string]any, 0, len(ideas))
	for _, idea := range ideas {
		rows = append(rows, ideaView(idea))
	}
	writeJSON(w, http.StatusOK, map[string]any{"ideas": rows})
}

func (s *Server) triage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.Engine.Triage(id); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	idea, _ := s.Engine.GetIdea(id)
	if s.SaveIdea != nil {
		s.SaveIdea(idea)
	}
	writeJSON(w, http.StatusOK, ideaView(idea))
}

func (s *Server) refine(w http.ResponseWriter, r *http.Request) {
	var body struct {
		HostID      string `json:"host_id"`
		DocsHubPath string `json:"docs_hub_path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	id := r.PathValue("id")
	if _, err := s.Engine.ScheduleRefinement(id, body.HostID, body.DocsHubPath); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	idea, _ := s.Engine.GetIdea(id)
	if s.SaveIdea != nil {
		s.SaveIdea(idea)
	}
	writeJSON(w, http.StatusAccepted, ideaView(idea))
}

func (s *Server) storyDraft(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StoryID string `json:"story_id"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	holder := r.Header.Get("X-Lease-Holder")
	p, err := s.Engine.StoryDraft(r.PathValue("id"), holder, body.StoryID, body.Content)
	if err != nil {
		writeErr(w, http.StatusForbidden, err)
		return
	}
	if s.SavePrompt != nil {
		s.SavePrompt(p)
	}
	if s.SaveIdea != nil && p.SourceIdeaID != "" {
		if it, ok := s.Engine.GetIdea(p.SourceIdeaID); ok {
			s.SaveIdea(it)
		}
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.Engine.Cancel(id, "cancelled"); err != nil {
		if s.Queue != nil {
			if _, qErr := s.Queue.Cancel(r.Context(), id, "cancelled"); qErr == nil {
				writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED"})
				return
			}
		}
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "CANCEL_REQUESTED"})
}

func (s *Server) abandonJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	var raw struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&raw)
	if err := s.Engine.AbandonJob(jobID, raw.Reason); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ABANDONED",
		"job_id": jobID,
	})
}

func (s *Server) resumeJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	var opts control.ResumeOptions
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&opts)
	}
	if opts.ConversationID != "" && !control.IsValidConversationID(opts.ConversationID) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid conversation_id: contains illegal characters or path traversal"))
		return
	}
	// C5: Strip server-derived fields from client request body
	opts.ParentJobID = ""
	opts.RootJobID = ""
	opts.InitialConversationID = ""
	opts.InterruptedASEState = nil
	opts.ResumeFallbackReason = ""
	opts.HostID = ""
	opts.WorktreePath = ""
	if opts.ResumeMode != "fallback_new_conversation" {
		opts.ResumeMode = ""
	}
	sch, err := s.Engine.ResumeJob(jobID, opts)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) pauseAutoResumeJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	enabled, err := s.Engine.ToggleAutoResume(jobID)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":              "success",
		"job_id":              jobID,
		"auto_resume_enabled": enabled,
	})
}

func (s *Server) forceResumeJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	sch, err := s.Engine.ForceResumeJob(jobID)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "resumed",
		"job_id":      jobID,
		"schedule_id": sch.ID,
	})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID                  string `json:"id"`
		Kind                string `json:"kind"`
		DistributionProfile string `json:"distribution_profile,omitempty"`
		Profile             string `json:"profile,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	profile := body.DistributionProfile
	if profile == "" {
		profile = body.Profile
	}
	if profile == "" {
		profile = "generic"
	}
	h := s.Engine.RegisterHost(body.ID, body.Kind, profile)
	if s.SaveHost != nil {
		s.SaveHost(h)
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) upsertRepo(w http.ResponseWriter, r *http.Request) {
	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	repo := control.Repo{
		HostID:        firstString(raw, "host_id", "HostID"),
		WorktreePath:  firstString(raw, "worktree_path", "WorktreePath"),
		DocsHubPath:   firstString(raw, "docs_hub_path", "DocsHubPath"),
		CloneURL:      firstString(raw, "clone_url", "CloneURL"),
		DefaultBranch: firstString(raw, "default_branch", "DefaultBranch"),
		Queue:         firstString(raw, "queue", "Queue"),
		Lock:          firstString(raw, "lock", "Lock"),
		Reason:        firstString(raw, "reason", "Reason"),
		RepoAlias:     firstString(raw, "repo_alias", "alias", "RepoAlias", "Alias"),
	}
	if repo.RepoAlias == "" && repo.WorktreePath != "" {
		repo.RepoAlias = repo.EffectiveAlias()
	}
	if err := s.Engine.UpsertRepo(repo); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveRepo != nil {
		s.SaveRepo(repo)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func firstString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := raw[key]; ok && v != nil {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path       string `json:"worktree_path"`
		CloneURL   string `json:"clone_url"`
		Kind       string `json:"kind"`
		HolderID   string `json:"holder_id"`
		CallerType string `json:"caller_type"`
		StoryID    string `json:"story_id"`
		TTLSeconds int    `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	host := r.PathValue("host")
	if body.CallerType == "autopilot-cli" || body.HolderID != "" {
		ttl := time.Duration(body.TTLSeconds) * time.Second
		if ttl <= 0 {
			ttl = 300 * time.Second
		}
		repo, err := s.Engine.LeaseExternal(host, body.Path, body.HolderID, body.StoryID, ttl)
		if err != nil {
			writeErr(w, http.StatusConflict, err)
			return
		}
		if s.SaveRepo != nil {
			s.SaveRepo(repo)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":        "LEASED",
			"host":          host,
			"worktree_path": repo.WorktreePath,
			"holder_id":     repo.LockHolder,
			"lock":          repo.Lock,
			"story_id":      repo.DirtyStoryID,
			"expires_at":    repo.LockExpiresAt.UTC().Format(time.RFC3339),
		})
		return
	}

	var job control.Job
	var err error
	if body.Kind == control.KindIntervention {
		job, err = s.Engine.LeaseIntervention(host, body.Path, body.CloneURL)
	} else {
		job, err = s.Engine.LeaseOrdinary(host, body.Path, body.CloneURL)
	}
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) releaseExternalLease(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path     string `json:"worktree_path"`
		HolderID string `json:"holder_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	host := r.PathValue("host")
	repo, err := s.Engine.ReleaseExternal(host, body.Path, body.HolderID)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveRepo != nil {
		s.SaveRepo(repo)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "RELEASED",
		"host":          host,
		"worktree_path": repo.WorktreePath,
		"lock":          repo.Lock,
	})
}

func (s *Server) preflight(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string            `json:"worktree_path"`
		Pre  control.Preflight `json:"preflight"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	host := r.PathValue("host")
	halt := s.Engine.ApplyPreflight(host, body.Path, body.Pre)
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, body.Path); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, halt)
}

func (s *Server) finish(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path  string              `json:"worktree_path"`
		JobID string              `json:"job_id"`
		In    control.FinishInput `json:"finish"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	host := r.PathValue("host")
	report := s.Engine.ApplyFinish(host, body.Path, body.JobID, body.In)
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, body.Path); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) forcePause(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string `json:"worktree_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	host := r.PathValue("host")
	s.Engine.ForcePause(host, body.Path)
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, body.Path); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"queue": control.QueuePaused})
}

func (s *Server) resumeRepo(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path         string `json:"path"`
		WorktreePath string `json:"worktree_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	targetPath := body.WorktreePath
	if targetPath == "" {
		targetPath = body.Path
	}
	host := r.PathValue("host")
	if err := s.Engine.ResumeRepo(host, targetPath); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, targetPath); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"queue": control.QueueOpen})
}

func (s *Server) resumeRepoInPlace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path                  string         `json:"path"`
		WorktreePath          string         `json:"worktree_path"`
		StoryID               string         `json:"story_id"`
		ConversationID        string         `json:"conversation_id"`
		JobID                 string         `json:"job_id"`
		AllowDirty            bool           `json:"allow_dirty"`
		PromptID              string         `json:"prompt_id"`
		PromptTitle           string         `json:"prompt_title"`
		Prompt                string         `json:"prompt"`
		CustomPrompt          string         `json:"custom_prompt"`
		Engine                string         `json:"engine"`
		InitialConversationID string         `json:"initial_conversation_id"`
		ActiveConversationID  string         `json:"active_conversation_id"`
		ParentJobID           string         `json:"parent_job_id"`
		RootJobID             string         `json:"root_job_id"`
		ResumeMode            string         `json:"resume_mode"`
		ResumeFallbackReason  string         `json:"resume_fallback_reason"`
		InterruptedASEState   map[string]any `json:"interrupted_ase_state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	targetPath := body.WorktreePath
	if targetPath == "" {
		targetPath = body.Path
	}
	if targetPath == "" {
		targetPath = r.URL.Query().Get("path")
	}
	host := r.PathValue("host")
	customPrompt := strings.TrimSpace(body.CustomPrompt)
	if customPrompt == "" {
		customPrompt = strings.TrimSpace(body.Prompt)
	}
	parentJobID := body.ParentJobID
	if parentJobID == "" {
		parentJobID = body.JobID
	}
	initConvID := body.InitialConversationID
	if initConvID == "" {
		initConvID = body.ConversationID
	}
	if initConvID != "" && !control.IsValidConversationID(initConvID) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid initial_conversation_id: contains illegal characters or path traversal"))
		return
	}
	if body.ActiveConversationID != "" && !control.IsValidConversationID(body.ActiveConversationID) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid active_conversation_id: contains illegal characters or path traversal"))
		return
	}
	resumeMode := ""
	if body.ResumeMode == "fallback_new_conversation" {
		resumeMode = "fallback_new_conversation"
	}
	opts := control.ResumeOptions{
		ConversationID:        body.ConversationID,
		InitialConversationID: initConvID,
		ActiveConversationID:  body.ActiveConversationID,
		ParentJobID:           parentJobID,
		RootJobID:             body.RootJobID,
		ResumeMode:            resumeMode,
		ResumeFallbackReason:  body.ResumeFallbackReason,
		InterruptedASEState:   body.InterruptedASEState,
		AllowDirty:            body.AllowDirty,
		StoryID:               body.StoryID,
		JobID:                 body.JobID,
		PromptID:              body.PromptID,
		PromptTitle:           body.PromptTitle,
		Prompt:                customPrompt,
		CustomPrompt:          customPrompt,
		Engine:                body.Engine,
		HostID:                host,
		WorktreePath:          targetPath,
	}
	sch, err := s.Engine.ResumeRepoInPlace(host, targetPath, opts)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, targetPath); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) remediateRepo(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path         string `json:"path"`
		WorktreePath string `json:"worktree_path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	targetPath := body.WorktreePath
	if targetPath == "" {
		targetPath = body.Path
	}
	if targetPath == "" {
		targetPath = r.URL.Query().Get("path")
	}
	host := r.PathValue("host")
	if err := s.Engine.RemediateRepo(host, targetPath); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, targetPath); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "REMEDIATED",
		"host_id": host,
		"path":    targetPath,
		"queue":   control.QueueOpen,
		"lock":    control.LockIdle,
	})
}

func (s *Server) setRepoQueueAllocation(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path                         string `json:"path"`
		WorktreePath                 string `json:"worktree_path"`
		Enabled                      *bool  `json:"enabled"`
		GlobalQueueAllocationEnabled *bool  `json:"global_queue_allocation_enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	targetPath := body.WorktreePath
	if targetPath == "" {
		targetPath = body.Path
	}
	if targetPath == "" {
		targetPath = r.URL.Query().Get("path")
	}
	if targetPath == "" {
		writeErr(w, http.StatusBadRequest, errors.New("worktree_path is required"))
		return
	}
	if body.Enabled == nil && body.GlobalQueueAllocationEnabled == nil {
		writeErr(w, http.StatusBadRequest, errors.New("enabled or global_queue_allocation_enabled is required"))
		return
	}
	enabled := false
	if body.GlobalQueueAllocationEnabled != nil {
		enabled = *body.GlobalQueueAllocationEnabled
	} else if body.Enabled != nil {
		enabled = *body.Enabled
	}
	host := r.PathValue("host")
	if err := s.Engine.SetRepoQueueAllocation(host, targetPath, enabled); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(host, targetPath); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                          "ok",
		"host_id":                         host,
		"worktree_path":                   targetPath,
		"global_queue_allocation_enabled": enabled,
	})
}

func (s *Server) getToolPriority(w http.ResponseWriter, r *http.Request) {
	_, span := telemetry.StartSpan(r.Context(), "getToolPriority")
	defer span.End()

	priority := s.Engine.GetToolPriority()
	writeJSON(w, http.StatusOK, map[string]any{
		"tools": priority,
	})
}

func (s *Server) setToolPriority(w http.ResponseWriter, r *http.Request) {
	_, span := telemetry.StartSpan(r.Context(), "setToolPriority")
	defer span.End()

	var body struct {
		Tools []string `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Engine.SetToolPriority(body.Tools); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"tools":  s.Engine.GetToolPriority(),
	})
}

func (s *Server) listPrompts(w http.ResponseWriter, _ *http.Request) {
	prompts := s.Engine.ListPrompts()
	rows := make([]map[string]any, 0, len(prompts))
	for _, p := range prompts {
		rows = append(rows, promptView(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"prompts": rows})
}

func (s *Server) createPrompt(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Title        string `json:"title"`
		Body         string `json:"body"`
		Engine       string `json:"engine"`
		Status       string `json:"status"`
		StoryID      string `json:"story_id"`
		SourceIdeaID string `json:"source_idea_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	created := s.Engine.PutPrompt(control.Prompt{
		Title: raw.Title, Body: raw.Body, Engine: raw.Engine, Status: raw.Status,
		StoryID: raw.StoryID, SourceIdeaID: raw.SourceIdeaID,
	})
	if s.SavePrompt != nil {
		s.SavePrompt(created)
	}
	writeJSON(w, http.StatusCreated, promptView(created))
}

func (s *Server) updatePrompt(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Title        string `json:"title"`
		Body         string `json:"body"`
		Engine       string `json:"engine"`
		Status       string `json:"status"`
		StoryID      string `json:"story_id"`
		SourceIdeaID string `json:"source_idea_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	p, err := s.Engine.ReplacePrompt(r.PathValue("id"), control.Prompt{
		Title: raw.Title, Body: raw.Body, Engine: raw.Engine, Status: raw.Status,
		StoryID: raw.StoryID, SourceIdeaID: raw.SourceIdeaID,
	})
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SavePrompt != nil {
		s.SavePrompt(p)
	}
	writeJSON(w, http.StatusOK, promptView(p))
}

func (s *Server) deletePrompt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.Engine.DeletePrompt(id); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if s.DeletePrompt != nil {
		s.DeletePrompt(id)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) executePrompt(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		HostID               string            `json:"host_id"`
		RepoPath             string            `json:"repo_path"`
		Iterations           int               `json:"iterations"`
		Env                  map[string]string `json:"env_vars"`
		ResumeConversationID string            `json:"resume_conversation_id"`
		IsGlobal             bool              `json:"is_global"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var sch control.Schedule
	var err error
	if raw.IsGlobal || raw.HostID == "global" {
		sch, err = s.Engine.EnqueueGlobalSchedule(r.PathValue("id"), raw.Iterations, raw.Env)
	} else {
		sch, err = s.Engine.ExecutePromptWithResume(r.PathValue("id"), raw.HostID, raw.RepoPath, raw.Iterations, raw.Env, raw.ResumeConversationID)
	}
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusCreated, scheduleView(sch))
}

func (s *Server) changePriority(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Delta    *int `json:"delta"`
		Priority *int `json:"priority"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	id := r.PathValue("id")
	if body.Priority != nil {
		sch, err := s.Engine.SetSchedulePriority(id, *body.Priority)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		if s.SaveRepo != nil {
			if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
				s.SaveRepo(repo)
			}
		}
		writeJSON(w, http.StatusOK, scheduleView(sch))
		return
	}
	delta := 1
	if body.Delta != nil {
		delta = *body.Delta
	}
	sch, err := s.Engine.ChangeSchedulePriority(id, delta)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) increasePriority(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sch, err := s.Engine.ChangeSchedulePriority(id, 1)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) decreasePriority(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sch, err := s.Engine.ChangeSchedulePriority(id, -1)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) cancelSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sch, err := s.Engine.CancelSchedule(id)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			if s.Queue != nil {
				if job, qErr := s.Queue.Cancel(r.Context(), id, "cancelled by operator from schedule view"); qErr == nil && job != nil {
					writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED", "id": id})
					return
				}
			}
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED", "id": id})
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	hostID := r.URL.Query().Get("host_id")
	if hostID == "" {
		hostID = r.URL.Query().Get("host")
	}
	repoPath := r.URL.Query().Get("repo_path")
	if repoPath == "" {
		repoPath = r.URL.Query().Get("path")
	}
	status := r.URL.Query().Get("status")
	schedules := s.Engine.ListSchedules(hostID, repoPath, status)
	out := make([]map[string]any, 0, len(schedules))
	for _, sch := range schedules {
		out = append(out, scheduleView(sch))
	}
	if s.Queue != nil {
		qJobs, _ := s.Queue.List(r.Context(), dispatch.JobFilter{
			Repo: repoPath,
		})
		for _, qj := range qJobs {
			if qj.Status == dispatch.JobStatusPending || qj.Status == dispatch.JobStatusClaimed || qj.Status == dispatch.JobStatusLeased || qj.Status == dispatch.JobStatusRunning {
				if status != "" && !strings.EqualFold(status, qj.Status) && !strings.EqualFold(status, "QUEUED") && !strings.EqualFold(status, "RUNNING") && !strings.EqualFold(status, "LEASED") && !strings.EqualFold(status, "PENDING_EXECUTION") {
					continue
				}
				out = append(out, dispatchScheduleView(qj))
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
}

func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.Engine != nil {
		if sch, ok := s.Engine.GetSchedule(id); ok {
			writeJSON(w, http.StatusOK, scheduleView(sch))
			return
		}
	}
	if s.Queue != nil {
		if qJob, found := s.Queue.Get(id); found && qJob != nil {
			writeJSON(w, http.StatusOK, dispatchScheduleView(qJob))
			return
		}
	}
	writeErr(w, http.StatusNotFound, control.ErrNotFound)
}

func (s *Server) listHistory(w http.ResponseWriter, r *http.Request) {
	hostID := r.URL.Query().Get("host_id")
	if hostID == "" {
		hostID = r.URL.Query().Get("host")
	}
	repoPath := r.URL.Query().Get("repo_path")
	if repoPath == "" {
		repoPath = r.URL.Query().Get("path")
	}
	status := r.URL.Query().Get("status")
	limit := 50
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		if n, err := strconv.Atoi(rawLimit); err == nil && n > 0 {
			limit = n
		}
	}
	items := s.Engine.ListHistory(hostID, repoPath, status, limit)
	out := make([]map[string]any, 0, len(items))
	for _, h := range items {
		out = append(out, historyItemView(h))
	}
	if s.Queue != nil {
		qJobs, _ := s.Queue.List(r.Context(), dispatch.JobFilter{
			Repo: repoPath,
		})
		for _, qj := range qJobs {
			if qj.Status == dispatch.JobStatusCompleted || qj.Status == dispatch.JobStatusFailed || qj.Status == dispatch.JobStatusCanceled || qj.Status == dispatch.JobStatusRequeued {
				if status != "" && !strings.EqualFold(status, qj.Status) && !(strings.EqualFold(status, "CANCELLED") && qj.Status == dispatch.JobStatusCanceled) && !(strings.EqualFold(status, "CANCELED") && qj.Status == dispatch.JobStatusCanceled) {
					continue
				}
				out = append(out, dispatchHistoryView(qj))
			}
		}
	}

	// Sort unified history chronologically: newest first (created_at DESC)
	sort.SliceStable(out, func(i, j int) bool {
		strI, _ := out[i]["created_at"].(string)
		strJ, _ := out[j]["created_at"].(string)
		if strI == "" && strJ == "" {
			return false
		}
		if strI == "" {
			return false
		}
		if strJ == "" {
			return true
		}
		ti, errI := time.Parse(time.RFC3339, strI)
		tj, errJ := time.Parse(time.RFC3339, strJ)
		if errI != nil && errJ != nil {
			return strI > strJ
		}
		if errI != nil {
			return false
		}
		if errJ != nil {
			return true
		}
		return ti.After(tj)
	})

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	writeJSON(w, http.StatusOK, map[string]any{"history": out})
}

func (s *Server) getAgentRest(w http.ResponseWriter, r *http.Request) {
	rest := r.PathValue("rest")
	if strings.Contains(rest, "/jobs/") {
		parts := strings.Split(rest, "/jobs/")
		jobID := parts[len(parts)-1]
		r.SetPathValue("id", jobID)
		s.getJob(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// 1. Active in-memory jobs take highest precedence
	if s.Engine != nil {
		if job, ok := s.Engine.GetActiveJob(id); ok {
			writeJSON(w, http.StatusOK, jobDetailView(job))
			return
		}
	}
	// 2. Pending jobs in dispatch queue take precedence over archived history
	if s.Queue != nil {
		if qJob, found := s.Queue.Get(id); found && qJob != nil {
			writeJSON(w, http.StatusOK, dispatchJobDetailView(qJob))
			return
		}
	}
	// 3. Historical/archived jobs from engine history
	if s.Engine != nil {
		if job, ok := s.Engine.GetJob(id); ok {
			writeJSON(w, http.StatusOK, jobDetailView(job))
			return
		}
		if sch, found := s.Engine.GetSchedule(id); found {
			sv := scheduleView(sch)
			sv["prompt"] = sch.PromptTitle
			sv["worktree_path"] = sch.WorktreePath
			writeJSON(w, http.StatusOK, sv)
			return
		}
	}
	writeErr(w, http.StatusNotFound, control.ErrNotFound)
}

func (s *Server) postExit(w http.ResponseWriter, r *http.Request) {
	var body control.ExitOptions
	_ = json.NewDecoder(r.Body).Decode(&body)
	jobID := r.PathValue("id")
	if err := s.Engine.PostExit(jobID, body.ExitCode, body); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if body.ExitCode == 0 && (body.TerminationReasonCode == "" || body.TerminationReasonCode == "CLEAN") {
		s.RecordNotification(NotificationRecord{
			Timestamp: time.Now().UTC(),
			Category:  "info",
			Title:     "job.ase.completed",
			Source:    "harness",
			Event:     "job.ase.completed",
			JobID:     jobID,
			Message:   fmt.Sprintf("ASE job %s completed successfully", jobID),
		})
	} else {
		s.RecordNotification(NotificationRecord{
			Timestamp: time.Now().UTC(),
			Category:  "warning",
			Title:     "job.ase.intervention_required",
			Source:    "harness",
			Event:     "job.ase.intervention_required",
			JobID:     jobID,
			Message:   fmt.Sprintf("ASE job %s intervention required (exit: %d, reason: %s)", jobID, body.ExitCode, body.TerminationReasonCode),
		})
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepoForJob(jobID); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) declineJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	jobID := r.PathValue("id")
	if err := s.Engine.DeclineJob(jobID, body.Reason); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepoForJob(jobID); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listHosts(w http.ResponseWriter, _ *http.Request) {
	hosts := s.Engine.ListHosts()
	rows := make([]map[string]any, 0, len(hosts))
	for _, h := range hosts {
		rows = append(rows, hostView(h.Host, h.ReposPaused))
	}
	writeJSON(w, http.StatusOK, map[string]any{"hosts": rows})
}

func (s *Server) hostDetail(w http.ResponseWriter, r *http.Request) {
	detail, ok := s.Engine.GetHostDetail(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	paused := 0
	repos := make([]map[string]any, 0, len(detail.Repos))
	for _, repo := range detail.Repos {
		if repo.Queue == control.QueuePaused {
			paused++
		}
		repos = append(repos, repoSummary(repo))
	}
	tools := make([]map[string]any, 0, len(detail.Host.Tools))
	for _, tool := range detail.Host.Tools {
		tools = append(tools, toolView(tool))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"host":  hostView(detail.Host, paused),
		"repos": repos,
		"tools": tools,
	})
}

func (s *Server) pollHost(w http.ResponseWriter, r *http.Request) {
	job, sch, docs, ok := s.Engine.PollHost(r.PathValue("id"))
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetRepoDetail(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(detail.Repo)
		}
	}
	resumeConvID := sch.ResumeConversationID
	if resumeConvID == "" && job.ResumeConversationID != "" {
		resumeConvID = job.ResumeConversationID
	}
	engine := sch.Engine
	var promptText string
	if p, ok := s.Engine.GetPrompt(sch.PromptID); ok {
		promptText = p.Body
		if engine == "" && p.Engine != "" {
			engine = p.Engine
		}
	}
	if promptText == "" {
		promptText = sch.PromptTitle
	}
	if engine == "" {
		engine = "agent"
	}
	initConvID := job.InitialConversationID
	if initConvID == "" {
		initConvID = sch.InitialConversationID
	}
	activeConvID := job.ActiveConversationID
	if activeConvID == "" {
		activeConvID = sch.ActiveConversationID
	}
	if activeConvID == "" {
		activeConvID = initConvID
	}
	parentJobID := job.ParentJobID
	if parentJobID == "" {
		parentJobID = sch.ParentJobID
	}
	rootJobID := job.RootJobID
	if rootJobID == "" {
		rootJobID = sch.RootJobID
	}
	resumeMode := job.ResumeMode
	if resumeMode == "" {
		resumeMode = sch.ResumeMode
	}
	fallbackReason := job.ResumeFallbackReason
	if fallbackReason == "" {
		fallbackReason = sch.ResumeFallbackReason
	}
	interruptedASE := job.InterruptedASEState
	if interruptedASE == nil {
		interruptedASE = sch.InterruptedASEState
	}

	resp := map[string]any{
		"id":                      job.ID,
		"kind":                    job.Kind,
		"worktree_path":           sch.WorktreePath,
		"docs_hub_path":           docs,
		"story_id":                sch.StoryID,
		"schedule_id":             sch.ID,
		"engine":                  engine,
		"prompt":                  promptText,
		"resume_conversation_id":  resumeConvID,
		"trace_id":                job.TraceID,
		"parent_job_id":           parentJobID,
		"root_job_id":             rootJobID,
		"initial_conversation_id": initConvID,
		"active_conversation_id":  activeConvID,
		"resume_mode":             resumeMode,
		"resume_fallback_reason":  fallbackReason,
		"lease_token":             job.LeaseToken,
		"lease_ack_deadline_at":   job.LeaseAckDeadlineAt,
		"failure_category":        job.FailureCategory,
		"failure_stage":           job.FailureStage,
		"failure_reason_summary":  job.FailureReasonSummary,
		"retriable":               job.Retriable,
		"attempt_history":         job.AttemptHistory,
		"zipkin_trace_id":         job.ZipkinTraceID,
		"zipkin_trace_url":        job.ZipkinTraceURL,
		"traceparent":             job.Traceparent,
		"infra_requeue_count":     job.InfraRequeueCount,
	}
	if interruptedASE != nil {
		resp["interrupted_ase_state"] = interruptedASE
	}
	if len(job.ConversationIDs) > 0 {
		resp["conversation_ids"] = job.ConversationIDs
	}
	if len(job.ConversationLineage) > 0 {
		resp["conversation_lineage"] = job.ConversationLineage
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Presence              string   `json:"presence"`
		FetchFailed           bool     `json:"fetch_failed"`
		SupportedCapabilities []string `json:"supported_capabilities"`
		Version               string     `json:"version"`
		CommitSHA             string     `json:"commit_sha"`
		DiscoveredDevBinaries []string   `json:"discovered_dev_binaries"`
		EagerUpdateEnabled    bool       `json:"eager_update_enabled"`
		LastEagerUpdateAt     *time.Time `json:"last_eager_update_at"`
		Tools                 []struct {
			Name             string   `json:"name"`
			Path             string   `json:"path"`
			Version          string   `json:"version"`
			CommitSHA        string   `json:"commit_sha"`
			TargetVersion    string   `json:"target_version"`
			TargetCommitSHA  string   `json:"target_commit_sha"`
			Status           string   `json:"status"`
			ServiceInstalled bool     `json:"service_installed"`
			ServiceType      string   `json:"service_type"`
			ServiceStatus    string   `json:"service_status"`
			Capabilities     []string `json:"capabilities"`
			Installed        bool     `json:"installed"`
			InstallationType string   `json:"installation_type"`
			ActiveState      string   `json:"active_state"`
			WatchdogEnabled  bool     `json:"watchdog_enabled"`
			ConfigStatus     string   `json:"config_status"`
		} `json:"tools"`
		ActiveJobs []struct {
			JobID        string `json:"job_id"`
			WorktreePath string `json:"worktree_path"`
			PID          int    `json:"pid"`
		} `json:"active_jobs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	tools := make([]control.Tool, 0, len(raw.Tools))
	for _, t := range raw.Tools {
		tools = append(tools, control.Tool{
			Name:             t.Name,
			Path:             t.Path,
			Version:          t.Version,
			CommitSHA:        t.CommitSHA,
			TargetVersion:    t.TargetVersion,
			TargetCommitSHA:  t.TargetCommitSHA,
			Status:           t.Status,
			ServiceInstalled: t.ServiceInstalled,
			ServiceType:      t.ServiceType,
			ServiceStatus:    t.ServiceStatus,
			Capabilities:     t.Capabilities,
			Installed:        t.Installed,
			InstallationType: t.InstallationType,
			ActiveState:      t.ActiveState,
			WatchdogEnabled:  t.WatchdogEnabled,
			ConfigStatus:     t.ConfigStatus,
		})
	}
	var activeJobs []control.ActiveJob
	for _, aj := range raw.ActiveJobs {
		activeJobs = append(activeJobs, control.ActiveJob{
			JobID:        aj.JobID,
			WorktreePath: aj.WorktreePath,
			PID:          aj.PID,
		})
	}
	hostID := r.PathValue("id")
	if err := s.Engine.HeartbeatWithCapabilities(hostID, raw.Presence, tools, raw.FetchFailed, raw.SupportedCapabilities, raw.Version, raw.CommitSHA, activeJobs...); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.Queue != nil {
		activeJobIDs := make([]string, 0, len(activeJobs))
		for _, aj := range activeJobs {
			if aj.JobID != "" {
				activeJobIDs = append(activeJobIDs, aj.JobID)
			}
		}
		_, _ = s.Queue.ExtendLeasesForHost(r.Context(), hostID, activeJobIDs)
	}
	s.Engine.SetHostEagerUpdate(hostID, raw.EagerUpdateEnabled, raw.LastEagerUpdateAt)
	if len(raw.DiscoveredDevBinaries) > 0 {
		s.Engine.SetHostDevBinaries(hostID, raw.DiscoveredDevBinaries)
	}
	if s.SaveHost != nil {
		if detail, ok := s.Engine.GetHostDetail(hostID); ok {
			s.SaveHost(detail.Host)
		}
	}
	pending := s.Engine.ConsumePendingToolUpdate(hostID)
	pendingCmds := s.Engine.ConsumePendingCommands(hostID)
	res := map[string]any{"status": "ok"}
	if pending != nil {
		res["pending_tool_update"] = pending
	}
	if len(pendingCmds) > 0 {
		res["pending_commands"] = pendingCmds
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getAgentTools(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	if hostID == "" {
		hostID = r.PathValue("host")
	}
	detail, ok := s.Engine.GetHostDetail(hostID)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	tools := make([]map[string]any, 0, len(detail.Host.Tools))
	for _, tool := range detail.Host.Tools {
		tools = append(tools, toolView(tool))
	}
	supportedCaps := detail.Host.SupportedCapabilities
	if supportedCaps == nil {
		supportedCaps = []string{}
	}
	resp := map[string]any{
		"host":                   hostID,
		"supported_capabilities": supportedCaps,
		"version":                detail.Host.Version,
		"commit_sha":             detail.Host.CommitSHA,
		"tools":                  tools,
		"eager_update_enabled":   detail.Host.EagerUpdateEnabled,
	}
	if detail.Host.LastEagerUpdateAt != nil && !detail.Host.LastEagerUpdateAt.IsZero() {
		resp["last_eager_update_at"] = detail.Host.LastEagerUpdateAt.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) deleteAgentTool(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	if hostID == "" {
		hostID = r.PathValue("host")
	}
	toolName := r.PathValue("tool")
	if hostID == "" || toolName == "" {
		writeErr(w, http.StatusBadRequest, errors.New("host and tool name are required"))
		return
	}
	if err := s.Engine.DeleteTool(hostID, toolName); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.SaveHost != nil {
		if detail, ok := s.Engine.GetHostDetail(hostID); ok {
			s.SaveHost(detail.Host)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "deleted",
		"host":    hostID,
		"tool":    toolName,
		"message": fmt.Sprintf("tool %s removed from host %s", toolName, hostID),
	})
}

func (s *Server) reconcileLineage(w http.ResponseWriter, r *http.Request) {
	count, err := s.Engine.ReconcileHistoricalLineage()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.ReconcileLineage != nil {
		_ = s.ReconcileLineage(r.Context())
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "ok",
		"reconciled_count": count,
		"message":          "Historical job and conversation lineage successfully reconciled",
	})
}

func (s *Server) getNotifications(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	notifs := s.GetNotifications(limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"notifications": notifs,
		"count":         len(notifs),
	})
}

func (s *Server) createNotification(w http.ResponseWriter, r *http.Request) {
	var rec NotificationRecord
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}
	s.RecordNotification(rec)
	writeJSON(w, http.StatusCreated, rec)
}

func (s *Server) queueToolUpdate(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	hostDet, ok := s.Engine.GetHostDetail(hostID)
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Errorf("host not found: %s", hostID))
		return
	}

	var body struct {
		Tools         []string `json:"tools"`
		Tool          string   `json:"tool,omitempty"`
		Force         bool     `json:"force"`
		TargetVersion string   `json:"target_version,omitempty"`
		Version       string   `json:"version,omitempty"`
	}
	if r.Body != nil {
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid json body: %w", err))
			return
		}
	}
	if body.Tool != "" && len(body.Tools) == 0 {
		body.Tools = []string{body.Tool}
	}
	toolParam := strings.TrimSpace(r.PathValue("tool"))
	if toolParam != "" {
		body.Tools = []string{toolParam}
		// R3: only force when route is explicitly a force-update endpoint
		if strings.HasSuffix(r.URL.Path, "/force-update") {
			body.Force = true
		}
	} else if strings.HasSuffix(r.URL.Path, "/force-update") {
		body.Force = true
	}
	if body.TargetVersion == "" && body.Version != "" {
		body.TargetVersion = body.Version
	}

	// Single-tool guard (F2): reject target_version unless exactly one tool is specified
	if body.TargetVersion != "" && len(body.Tools) != 1 {
		writeErr(w, http.StatusBadRequest, errors.New("target_version can only be specified when exactly one tool is targeted"))
		return
	}

	// Resolve target profile for the host, falling back to generic default profile (F6, R8)
	profileName := hostDet.Host.DistributionProfile
	if profileName == "" {
		profileName = "generic"
	}
	p, err := s.Engine.GetProfile(profileName)
	if err != nil || p == nil {
		if profileName != "generic" {
			fmt.Fprintf(os.Stderr, "[server] host %s profile %q not found, falling back to generic: %v\n", hostID, profileName, err)
		}
		p, _ = s.Engine.GetProfile("generic")
	}

	isWildcard := len(body.Tools) == 1 && (body.Tools[0] == "*" || strings.EqualFold(body.Tools[0], "all"))
	if isWildcard || len(body.Tools) == 0 {
		if p != nil && len(p.Tools) > 0 {
			var allTools []string
			for _, pt := range p.Tools {
				allTools = append(allTools, pt.Name)
			}
			body.Tools = allTools
		} else {
			body.Tools = []string{"autopilot", "iazio-agent", "iazio-harvester", "iazio-mcp", "iazio-harness"}
		}
	}

	// Validate tool existence across all requested tools (F6, N4-R, R4, H2)
	if p != nil && len(p.Tools) > 0 {
		for _, toolName := range body.Tools {
			found := false
			for _, pt := range p.Tools {
				if strings.EqualFold(pt.Name, toolName) || strings.EqualFold(pt.BinaryName, toolName) {
					found = true
					break
				}
			}
			if !found {
				writeErr(w, http.StatusNotFound, fmt.Errorf("unknown tool: %s", toolName))
				return
			}
		}
	} else if toolParam != "" || body.TargetVersion != "" {
		// When no profile is configured or profile has no tools, explicit tool targets cannot be validated (F6, S4)
		for _, toolName := range body.Tools {
			writeErr(w, http.StatusNotFound, fmt.Errorf("unknown tool: %s", toolName))
			return
		}
	}

	if len(body.Tools) == 1 {
		toolName := body.Tools[0]
		var foundTool *control.ProfileTool
		if p != nil {
			for _, pt := range p.Tools {
				if strings.EqualFold(pt.Name, toolName) || strings.EqualFold(pt.BinaryName, toolName) {
					toolCopy := pt
					foundTool = &toolCopy
					break
				}
			}
		}

		// R2, N4: validate explicit target_version if supplied; return 422 if invalid or unknown
		if body.TargetVersion != "" {
			rawTV := strings.TrimSpace(body.TargetVersion)
			cleanTV := strings.TrimPrefix(rawTV, "v")
			if match := regexp.MustCompile(`([0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?(?:\+[a-zA-Z0-9.]+)?)|([0-9]+\.[0-9]+\.[0-9]+)`).FindString(rawTV); match != "" {
				cleanTV = match
				body.TargetVersion = match
			} else if strings.Contains(strings.ToLower(rawTV), "dev") || strings.EqualFold(cleanTV, "latest") {
				if foundTool != nil && foundTool.LatestAvailableVersion != "" {
					body.TargetVersion = foundTool.LatestAvailableVersion
					cleanTV = strings.TrimPrefix(strings.TrimSpace(body.TargetVersion), "v")
				} else if foundTool != nil && foundTool.TargetVersion != "" && foundTool.TargetVersion != "latest" {
					body.TargetVersion = foundTool.TargetVersion
					cleanTV = strings.TrimPrefix(strings.TrimSpace(body.TargetVersion), "v")
				}
			}
			if cleanTV == "" || strings.Contains(cleanTV, " ") {
				writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("invalid target_version: %s", body.TargetVersion))
				return
			}
			if foundTool != nil {
				normClean := strings.TrimPrefix(strings.TrimSpace(cleanTV), "v")
				knownLatest := strings.TrimPrefix(strings.TrimSpace(foundTool.LatestAvailableVersion), "v")
				knownTarget := strings.TrimPrefix(strings.TrimSpace(foundTool.TargetVersion), "v")

				isKnown := false
				if (knownLatest != "" && normClean == knownLatest) ||
					(knownTarget != "" && knownTarget != "latest" && normClean == knownTarget) ||
					(body.Force && (strings.Contains(strings.ToLower(rawTV), "dev") || strings.EqualFold(rawTV, "latest"))) {
					isKnown = true
				}
				if !isKnown && s.Engine != nil && s.Engine.Releases != nil && foundTool.Repository != "" {
					if rel, err := s.Engine.Releases.GetToolRelease(r.Context(), foundTool.Repository, foundTool.Name, cleanTV); err == nil && rel.Error == "" && rel.LastSyncStatus != "error" {
						relNorm := strings.TrimPrefix(strings.TrimSpace(rel.LatestVersion), "v")
						tagNorm := strings.TrimPrefix(strings.TrimSpace(rel.TagName), "v")
						if relNorm == normClean || tagNorm == normClean {
							isKnown = true
						}
					}
				}
				if !isKnown {
					writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("unknown target_version: %s (known latest: %s)", body.TargetVersion, foundTool.LatestAvailableVersion))
					return
				}
			}
		}

		// Prefer LatestAvailableVersion for force update when target_version is not explicitly supplied (F6, R2)
		if body.TargetVersion == "" && foundTool != nil {
			if foundTool.LatestAvailableVersion != "" {
				body.TargetVersion = foundTool.LatestAvailableVersion
			} else if foundTool.TargetVersion != "" && foundTool.TargetVersion != "latest" {
				body.TargetVersion = foundTool.TargetVersion
			}
		}
	}

	var jobID string
	if s.Queue != nil {
		payloadMap := map[string]any{
			"host_id": hostID,
			"tools":   body.Tools,
			"force":   body.Force,
		}
		if body.TargetVersion != "" {
			payloadMap["target_version"] = body.TargetVersion
		}
		payload, _ := json.Marshal(payloadMap)
		job := &dispatch.Job{
			Type:       "agent.tools.update",
			Repo:       "fleet",
			TargetHost: hostID,
			AgentAffinity: &dispatch.AgentAffinity{
				HostID:    hostID,
				AgentName: "iazio-agent",
			},
			Payload: payload,
			Status:  dispatch.JobStatusPending,
		}
		enqJob, err := s.Queue.Enqueue(r.Context(), job)
		if err != nil {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("failed to enqueue tool update job: %w", err))
			return
		}
		if enqJob != nil {
			jobID = enqJob.ID
		}
	}

	if err := s.Engine.QueueToolUpdate(hostID, body.Tools, body.Force, body.TargetVersion); err != nil {
		if s.Queue != nil && jobID != "" {
			_, _ = s.Queue.Cancel(r.Context(), jobID, fmt.Sprintf("failed to update host desired state: %v", err))
		}
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusInternalServerError, err)
		}
		return
	}

	meta := map[string]any{
		"tools": body.Tools,
		"force": body.Force,
	}
	if body.TargetVersion != "" {
		meta["target_version"] = body.TargetVersion
	}

	s.RecordNotification(NotificationRecord{
		Title:    "Tool Update Queued",
		Message:  fmt.Sprintf("Tool update queued for host %s (job: %s)", hostID, jobID),
		Category: "userAction",
		Source:   "harness-api",
		JobID:    jobID,
		HostID:   hostID,
		Metadata: meta,
	})

	resp := map[string]any{
		"status":  "queued",
		"host_id": hostID,
		"job_id":  jobID,
		"tools":   body.Tools,
		"force":   body.Force,
	}
	if body.TargetVersion != "" {
		resp["target_version"] = body.TargetVersion
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) queueToolCleanupBinaries(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	if hostID == "" {
		hostID = r.PathValue("host")
	}
	_, ok := s.Engine.GetHostDetail(hostID)
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Errorf("host not found: %s", hostID))
		return
	}

	var body struct {
		DryRun bool `json:"dry_run"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	cmdPayload, _ := json.Marshal(map[string]any{
		"action":  "cleanup-binaries",
		"dry_run": body.DryRun,
	})
	_ = s.Engine.QueueCommand(hostID, control.CommandEnvelope{
		ID:        fmt.Sprintf("cleanup-%d", time.Now().UnixNano()),
		Type:      "CLEANUP_BINARIES",
		Payload:   string(cmdPayload),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "queued",
		"host_id": hostID,
		"action":  "cleanup-binaries",
		"dry_run": body.DryRun,
		"message": fmt.Sprintf("dev binary cleanup queued for agent host %s", hostID),
	})
}

func (s *Server) queueAllToolsUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Tools []string `json:"tools"`
		Force bool     `json:"force"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	hosts := s.Engine.ListHosts()
	queuedHosts := make([]string, 0, len(hosts))
	for _, h := range hosts {
		hostID := h.Host.ID
		_ = s.Engine.QueueToolUpdate(hostID, body.Tools, body.Force)
		var jobID string
		if s.Queue != nil {
			payload, _ := json.Marshal(map[string]any{
				"host_id": hostID,
				"tools":   body.Tools,
				"force":   body.Force,
			})
			job := &dispatch.Job{
				Type:       "agent.tools.update",
				Repo:       "fleet",
				TargetHost: hostID,
				AgentAffinity: &dispatch.AgentAffinity{
					HostID:    hostID,
					AgentName: "iazio-agent",
				},
				Payload: payload,
				Status:  dispatch.JobStatusPending,
			}
			if enqJob, err := s.Queue.Enqueue(r.Context(), job); err == nil && enqJob != nil {
				jobID = enqJob.ID
			}
		}
		queuedHosts = append(queuedHosts, hostID)
		s.RecordNotification(NotificationRecord{
			Title:    "Tool Update Queued",
			Message:  fmt.Sprintf("Tool update queued for host %s across fleet (job: %s)", hostID, jobID),
			Category: "info",
			Source:   "harness-api",
			JobID:    jobID,
			HostID:   hostID,
		})
	}

	s.RecordNotification(NotificationRecord{
		Title:    "Fleet Tool Update Initiated",
		Message:  fmt.Sprintf("Enqueued tool updates across %d hosts", len(queuedHosts)),
		Category: "userAction",
		Source:   "harness-api",
		Metadata: map[string]any{
			"hosts": queuedHosts,
			"tools": body.Tools,
			"force": body.Force,
		},
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "queued",
		"hosts":  queuedHosts,
		"count":  len(queuedHosts),
		"tools":  body.Tools,
		"force":  body.Force,
	})
}

func (s *Server) getProfileReleases(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		name = "generic"
	}
	profile, err := s.Engine.GetProfile(name)
	if err != nil {
		if name == "generic" {
			profile = &control.DistributionProfile{
				Name: "generic",
				Tools: []control.ProfileTool{
					{Name: "autopilot", Repository: "viovy/autopilot-plugins-internal"},
					{Name: "iazio-harness", Repository: "viovy/iazio-harness"},
					{Name: "iazio-agent", Repository: "viovy/iazio-agent"},
					{Name: "iazio-mcp", Repository: "viovy/iazio-mcp"},
				},
			}
		} else {
			writeErr(w, http.StatusNotFound, err)
			return
		}
	}
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	releases, err := s.Engine.Releases.GetProfileReleases(r.Context(), *profile)
	if err != nil && len(releases) == 0 {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	writeJSON(w, http.StatusOK, map[string]any{
		"profile":  name,
		"releases": releases,
	})
}

func (s *Server) listGitCredentials(w http.ResponseWriter, _ *http.Request) {
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	writeJSON(w, http.StatusOK, s.Engine.Releases.ListCredentials())
}

func (s *Server) registerGitCredential(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Pattern string `json:"pattern"`
		Token   string `json:"token"`
		Type    string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(body.Pattern) == "" {
		body.Pattern = "*"
	}
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	cred, err := s.Engine.Releases.RegisterCredential(control.GitCredential{
		Name:    body.Name,
		Pattern: body.Pattern,
		Token:   body.Token,
		Type:    body.Type,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "registered",
		"credential": cred,
	})
}

func (s *Server) deleteGitCredential(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	if err := s.Engine.Releases.DeleteCredential(id); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) testSavedGitCredential(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	cred, ok := s.Engine.Releases.GetCredential(id)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	res, err := s.Engine.Releases.TestCredential(r.Context(), cred.Token)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) testGitCredentialPayload(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.Engine.Releases == nil {
		s.Engine.Releases = control.NewReleaseService()
	}
	res, err := s.Engine.Releases.TestCredential(r.Context(), body.Token)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) refreshProfileReleases(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.Engine.GetProfile(name); err != nil {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	relSvc := s.Engine.GetReleaseService()
	if relSvc == nil {
		relSvc = control.NewReleaseService()
		s.Engine.SetReleaseService(relSvc)
	}

	advanced, err := relSvc.SyncProfile(r.Context(), s.Engine, name, func(prof string, tool control.ProfileTool, fromVer, toVer string) {
		s.RecordNotification(NotificationRecord{
			Title:    "Release Detected",
			Message:  fmt.Sprintf("New release detected for %s: %s (was %s). Profile '%s' updated.", tool.Name, toVer, fromVer, prof),
			Category: "release",
			Source:   "release-watcher",
		})
	})
	status := "ok"
	var errStr string
	if err != nil {
		errStr = err.Error()
		if advanced > 0 {
			status = "partial"
		} else {
			status = "error"
		}
	}

	updatedProfile, _ := s.Engine.GetProfile(name)
	s.RecordNotification(NotificationRecord{
		Title:    "Profile Releases Refreshed",
		Message:  fmt.Sprintf("Refreshed release versions for profile '%s'. Status: %s, advanced tools: %d", name, status, advanced),
		Category: "release",
		Source:   "harness-api",
	})
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	resp := map[string]any{
		"status":        status,
		"tools_updated": advanced,
		"profile":       updatedProfile,
	}
	if errStr != "" {
		resp["error"] = errStr
	}
	httpCode := http.StatusOK
	if status == "error" {
		httpCode = http.StatusBadGateway
	}
	writeJSON(w, httpCode, resp)
}

func (s *Server) syncAllProfiles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	if s.Engine == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "engine not initialized"})
		return
	}
	relSvc := s.Engine.GetReleaseService()
	if relSvc == nil {
		relSvc = control.NewReleaseService()
		s.Engine.SetReleaseService(relSvc)
	}

	syncRes := relSvc.SyncAllProfilesDetailed(r.Context(), s.Engine, func(profile string, tool control.ProfileTool, fromVersion, toVersion string) {
		s.RecordNotification(NotificationRecord{
			Title:    "Release Detected",
			Message:  fmt.Sprintf("New release detected for %s: %s (was %s). Profile '%s' updated.", tool.Name, toVersion, fromVersion, profile),
			Category: "release",
			Source:   "release-sync",
			Metadata: map[string]any{
				"event":        "release.detected",
				"profile":      profile,
				"tool":         tool.Name,
				"from_version": fromVersion,
				"to_version":   toVersion,
				"url":          tool.LatestReleaseURL,
			},
		})
	})

	status := "ok"
	if len(syncRes.Errors) > 0 {
		if syncRes.TotalAdvanced > 0 || len(syncRes.Errors) < len(s.Engine.ListProfiles()) {
			status = "partial"
		} else {
			status = "error"
		}
	}

	resp := map[string]any{
		"status":        status,
		"tools_updated": syncRes.TotalAdvanced,
		"synced_at":     time.Now().UTC().Format(time.RFC3339),
	}
	if len(syncRes.Errors) > 0 {
		resp["errors"] = syncRes.Errors
	}
	httpCode := http.StatusOK
	if status == "error" {
		httpCode = http.StatusBadGateway
	}
	writeJSON(w, httpCode, resp)
}

func (s *Server) pingAck(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PingID       string `json:"ping_id"`
		AgentVersion string `json:"agent_version"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	if body.PingID != "" {
		s.Engine.NotifyPingAck(body.PingID, body.AgentVersion)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) pingHost(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("host")
	detail, ok := s.Engine.GetHostDetail(hostID)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	start := time.Now()
	pingID := fmt.Sprintf("ping-%d", time.Now().UnixNano())
	ackChan := s.Engine.RegisterPingWaiter(pingID)
	defer s.Engine.DeregisterPingWaiter(pingID)

	// Queue the live PING command to the agent stream
	_ = s.Engine.QueueCommand(hostID, control.CommandEnvelope{
		ID:        pingID,
		Type:      "PING",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	var agentVersion string
	for _, t := range detail.Host.Tools {
		if t.Name == "iazio-agent" {
			agentVersion = t.Version
			break
		}
	}

	waitTimeout := 8 * time.Second
	if tStr := r.URL.Query().Get("timeout_sec"); tStr != "" {
		if sec, err := strconv.Atoi(tStr); err == nil && sec > 0 && sec <= 30 {
			waitTimeout = time.Duration(sec) * time.Second
		}
	} else if tMsStr := r.URL.Query().Get("timeout_ms"); tMsStr != "" {
		if ms, err := strconv.Atoi(tMsStr); err == nil && ms > 0 && ms <= 30000 {
			waitTimeout = time.Duration(ms) * time.Millisecond
		}
	}

	reachable := false
	liveAck := false
	latency := int64(1)

	// Wait for live agent ack round-trip
	select {
	case ack := <-ackChan:
		reachable = true
		liveAck = true
		latency = ack.ReceivedAt.Sub(start).Milliseconds()
		if latency <= 0 {
			latency = 1
		}
		if ack.AgentVersion != "" {
			agentVersion = ack.AgentVersion
		}
	case <-time.After(waitTimeout):
		fmt.Fprintf(os.Stderr, "[SIR: PROBE_TIMEOUT] live ping to host %s timed out after %v without agent ack\n", hostID, waitTimeout)
	}

	presence := "OFFLINE"
	statusStr := "TIMEOUT"
	if reachable {
		presence = "ONLINE"
		statusStr = "ONLINE"
	}

	res := map[string]any{
		"host_id":       hostID,
		"reachable":     reachable,
		"live_ack":      liveAck,
		"presence":      presence,
		"latency_ms":    latency,
		"agent_version": agentVersion,
		"kind":          detail.Host.Kind,
		"active_jobs":   0,
		"tools_count":   len(detail.Host.Tools),
		"last_seen":     detail.Host.LastSeen.UTC().Format(time.RFC3339),
		"status":        statusStr,
	}
	if !reachable {
		res["error"] = fmt.Sprintf("ping timed out after %v waiting for live agent ack", waitTimeout)
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) updateRepoRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("host")
	var statuses []control.RepoRuntimeStatus
	if err := json.NewDecoder(r.Body).Decode(&statuses); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	s.Engine.UpdateRepoRuntimeStatus(hostID, statuses)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func resolveHostID(r *http.Request) string {
	hostID := r.PathValue("host")
	if hostID == "" {
		hostID = r.PathValue("id")
	}
	if hostID == "" {
		p := strings.TrimPrefix(r.URL.Path, "/v1/hosts/")
		p = strings.TrimPrefix(p, "/api/v1/agents/")
		p = strings.TrimPrefix(p, "/api/agents/")
		if idx := strings.Index(p, "/"); idx != -1 {
			hostID = p[:idx]
		} else {
			hostID = p
		}
	}
	return hostID
}

func (s *Server) updateEagerReadiness(w http.ResponseWriter, r *http.Request) {
	hostID := resolveHostID(r)
	if hostID == "" {
		http.Error(w, "host is required", http.StatusBadRequest)
		return
	}
	var report control.EagerUpdateReadinessReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if report.HostID == "" {
		report.HostID = hostID
	}
	s.Engine.UpdateEagerReadiness(hostID, report)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) getEagerReadiness(w http.ResponseWriter, r *http.Request) {
	hostID := resolveHostID(r)
	if hostID == "" {
		http.Error(w, "host is required", http.StatusBadRequest)
		return
	}
	report, ok := s.Engine.GetEagerReadiness(hostID)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) evaluateEagerUpdate(w http.ResponseWriter, r *http.Request) {
	hostID := resolveHostID(r)
	if hostID == "" {
		http.Error(w, "host is required", http.StatusBadRequest)
		return
	}
	if err := s.Engine.EvaluateEagerUpdate(hostID); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "evaluation_dispatched"})
}

func (s *Server) forceEagerUpdate(w http.ResponseWriter, r *http.Request) {
	hostID := resolveHostID(r)
	if hostID == "" {
		http.Error(w, "host is required", http.StatusBadRequest)
		return
	}
	if err := s.Engine.ForceEagerUpdate(hostID); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "force_update_dispatched"})
}

func (s *Server) checkFleetStatus(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("host")
	targetPath := r.URL.Query().Get("path")
	if targetPath == "" {
		targetPath = r.URL.Query().Get("worktree_path")
	}
	if targetPath == "" && r.Body != nil {
		var body struct {
			Path         string `json:"path"`
			WorktreePath string `json:"worktree_path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.WorktreePath != "" {
			targetPath = body.WorktreePath
		} else {
			targetPath = body.Path
		}
	}
	detail, ok := s.Engine.GetRepoDetail(hostID, targetPath)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	repo := detail.Repo
	actualPath := repo.WorktreePath
	if actualPath == "" {
		actualPath = targetPath
	}

	probeID := fmt.Sprintf("ephemeral-probe-%d", time.Now().UnixNano())
	waiterKey, waiterCh := s.Engine.RegisterRepoStatusWaiter(hostID, actualPath)
	defer s.Engine.DeregisterRepoStatusWaiter(waiterKey)

	_ = s.Engine.QueueCommand(hostID, control.CommandEnvelope{
		ID:        probeID,
		Type:      "CHECK_FLEET_GIT_STATUS",
		Payload:   actualPath,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	waitTimeout := 8 * time.Second
	if tStr := r.URL.Query().Get("timeout_sec"); tStr != "" {
		if sec, err := strconv.Atoi(tStr); err == nil && sec > 0 && sec <= 30 {
			waitTimeout = time.Duration(sec) * time.Second
		}
	} else if tMsStr := r.URL.Query().Get("timeout_ms"); tMsStr != "" {
		if ms, err := strconv.Atoi(tMsStr); err == nil && ms > 0 && ms <= 30000 {
			waitTimeout = time.Duration(ms) * time.Millisecond
		}
	}

	var liveStatus *control.RepoRuntimeStatus
	isLiveProbe := false
	stale := false
	probeStatus := "LIVE_ACK"

	select {
	case st := <-waiterCh:
		liveStatus = &st
		isLiveProbe = true
	case <-time.After(waitTimeout):
		fmt.Fprintf(os.Stderr, "[SIR: PROBE_TIMEOUT] repo status probe for host %s repo %s timed out after %v; falling back to cache\n", hostID, actualPath, waitTimeout)
		stale = true
		probeStatus = "TIMEOUT_CACHED_FALLBACK"
	}

	isMainBranch := func(b string) bool {
		return b == "main" || b == "master"
	}
	repoMain := isMainBranch(repo.CurrentBranch)

	res := control.RepoFleetStatusResult{
		RepoPath:             actualPath,
		Branch:               repo.CurrentBranch,
		IsClean:              repoMain && repo.Porcelain == "" && repo.SubmodulesClean && len(repo.DirtySubmodules) == 0 && len(repo.NonMainSubmodules) == 0,
		SubmodulesClean:      repo.SubmodulesClean,
		DirtySubmodules:      repo.DirtySubmodules,
		NonMainSubmodules:    repo.NonMainSubmodules,
		ASEState:             repo.ASEState,
		ActiveStoryID:        repo.DirtyStoryID,
		AgentTool:            repo.ActiveTool,
		ToolLiveness:         repo.ToolLiveness,
		ExecutionProvenance:  repo.ExecutionProvenance,
		ActiveConversationID: repo.DetectedConversationID,
		IsLiveProbe:          isLiveProbe,
		Stale:                stale,
		ProbeStatus:          probeStatus,
		CheckedAt:            time.Now().UTC().Format(time.RFC3339),
	}
	if liveStatus != nil {
		if liveStatus.Branch != "" {
			res.Branch = liveStatus.Branch
		}
		if len(liveStatus.NonMainSubmodules) > 0 {
			res.NonMainSubmodules = liveStatus.NonMainSubmodules
		}
		res.SubmodulesClean = liveStatus.SubmodulesClean
		res.DirtySubmodules = liveStatus.DirtySubmodules
		res.IsClean = isMainBranch(res.Branch) && liveStatus.IsClean && liveStatus.SubmodulesClean && len(liveStatus.DirtySubmodules) == 0 && len(res.NonMainSubmodules) == 0 && (liveStatus.Porcelain == "")
		if liveStatus.ASEState != "" {
			res.ASEState = liveStatus.ASEState
		}
		res.ActiveStoryID = liveStatus.ActiveStoryID
		res.AgentTool = liveStatus.ActiveTool
		if liveStatus.ToolLiveness != "" {
			res.ToolLiveness = liveStatus.ToolLiveness
		}
		if liveStatus.ExecutionProvenance != "" {
			res.ExecutionProvenance = liveStatus.ExecutionProvenance
		}
		res.ActiveConversationID = liveStatus.ActiveConversationID
		if !liveStatus.LastCheckedAt.IsZero() {
			res.CheckedAt = liveStatus.LastCheckedAt.UTC().Format(time.RFC3339)
		}
	} else {
		if res.ASEState == "" {
			if repo.Lock == "RUNNING_HARNESS" {
				res.ASEState = "RUNNING"
			} else if repo.Porcelain != "" {
				res.ASEState = "FAILED_DIRTY"
			} else {
				res.ASEState = "IDLE"
			}
		}
		if res.ToolLiveness == "" {
			if detail.Running != nil {
				res.ToolLiveness = "LIVE"
				res.AgentTool = detail.RunningEngine
				res.ExecutionProvenance = "COORDINATED_HARNESS"
			} else {
				res.ToolLiveness = "NONE"
				res.ExecutionProvenance = "NONE"
			}
		}
	}

	var sb strings.Builder
	if liveStatus != nil {
		sb.WriteString(fmt.Sprintf("[probe %s] Live probe telemetry received from agent\n", probeID))
	} else {
		sb.WriteString(fmt.Sprintf("[probe %s] [SIR: PROBE_TIMEOUT] Live probe timed out after %v; displaying cached status\n", probeID, waitTimeout))
	}
	sb.WriteString(fmt.Sprintf("[probe %s] Repository: %s\n", probeID, actualPath))
	sb.WriteString(fmt.Sprintf("[probe %s] Branch: %s\n", probeID, res.Branch))
	porcelain := repo.Porcelain
	if liveStatus != nil && liveStatus.Porcelain != "" {
		porcelain = liveStatus.Porcelain
	}
	var modifiedFiles []string
	var untrackedFiles []string
	if strings.TrimSpace(porcelain) != "" {
		for _, line := range strings.Split(porcelain, "\n") {
			line = strings.TrimRight(line, "\r")
			if len(line) < 3 {
				continue
			}
			status := line[:2]
			file := strings.TrimSpace(line[2:])
			if strings.HasPrefix(status, "??") {
				untrackedFiles = append(untrackedFiles, file)
			} else {
				modifiedFiles = append(modifiedFiles, file)
			}
		}
	}
	res.ModifiedFiles = modifiedFiles
	res.UntrackedFiles = untrackedFiles
	if len(modifiedFiles) > 0 || len(untrackedFiles) > 0 {
		res.IsClean = false
	}

	if res.IsClean && len(modifiedFiles) == 0 && len(untrackedFiles) == 0 {
		sb.WriteString(fmt.Sprintf("[probe %s] Working Tree: CLEAN\n", probeID))
	} else {
		sb.WriteString(fmt.Sprintf("[probe %s] Working Tree: DIRTY (%d modified, %d untracked)\n", probeID, len(modifiedFiles), len(untrackedFiles)))
		for _, f := range modifiedFiles {
			sb.WriteString(fmt.Sprintf("  M  %s\n", f))
		}
		for _, f := range untrackedFiles {
			sb.WriteString(fmt.Sprintf("  ?? %s\n", f))
		}
	}
	if !isMainBranch(res.Branch) {
		sb.WriteString(fmt.Sprintf("[probe %s] Warning: Primary repo branch is %q (fleet cleanliness requires main/master; git-flow incomplete)\n", probeID, res.Branch))
	}
	if res.SubmodulesClean {
		sb.WriteString(fmt.Sprintf("[probe %s] Submodules: ALL CLEAN\n", probeID))
	} else {
		sb.WriteString(fmt.Sprintf("[probe %s] Submodules: DIRTY (%s)\n", probeID, strings.Join(res.DirtySubmodules, ", ")))
	}
	if len(res.NonMainSubmodules) > 0 {
		sb.WriteString(fmt.Sprintf("[probe %s] Submodules on Non-Main branches: %s\n", probeID, strings.Join(res.NonMainSubmodules, ", ")))
	}
	sb.WriteString(fmt.Sprintf("[probe %s] ASE State: %s\n", probeID, res.ASEState))
	if res.AgentTool != "" && res.ToolLiveness != "NONE" {
		sb.WriteString(fmt.Sprintf("[probe %s] Active Agent Tool: %s (Liveness: %s, Provenance: %s)\n",
			probeID, res.AgentTool, res.ToolLiveness, res.ExecutionProvenance))
	}
	sb.WriteString(fmt.Sprintf("[probe %s] Ephemeral diagnostic probe completed successfully.\n", probeID))

	res.DiagnosticSummary = sb.String()

	if s.EventHub != nil {
		s.EventHub.Publish(probeID, "stdout", map[string]any{
			"type": "stdout",
			"data": sb.String(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"probe_id": probeID,
		"status":   "COMPLETED",
		"result":   res,
		"output":   res.DiagnosticSummary,
	})
}

func (s *Server) repoDetail(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	detail, ok := s.Engine.GetRepoDetail(r.PathValue("host"), path)
	if !ok {
		writeErr(w, http.StatusNotFound, control.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, repoDetailView(detail))
}

func (s *Server) unregisterRepo(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	path := r.URL.Query().Get("path")
	if err := s.Engine.UnregisterRepo(host, path); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	if s.DeleteRepo != nil {
		s.DeleteRepo(host, path)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteHistoryRow(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing job id"))
		return
	}
	deleted := s.Engine.DeleteHistoryRow(jobID)
	if s.DeleteHistory != nil {
		_ = s.DeleteHistory(r.Context(), jobID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted, "job_id": jobID})
}

func (s *Server) clearRepoHistory(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	repoPath := r.URL.Query().Get("path")
	if repoPath == "" && r.Body != nil {
		var body struct {
			Path         string `json:"path"`
			WorktreePath string `json:"worktree_path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		repoPath = body.WorktreePath
		if repoPath == "" {
			repoPath = body.Path
		}
	}
	if host == "" || repoPath == "" {
		writeErr(w, http.StatusBadRequest, errors.New("missing host or path"))
		return
	}
	count := s.Engine.ClearHistory(host, repoPath)
	if s.ClearHistory != nil {
		_ = s.ClearHistory(r.Context(), host, repoPath)
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": count, "host": host, "path": repoPath})
}

func (s *Server) repoRequest(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Mode     string `json:"mode"`
		Path     string `json:"path"`
		CloneURL string `json:"clone_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	host := r.PathValue("host")
	if err := s.Engine.RequestCheckout(control.RepoRequest{
		HostID: host, Mode: raw.Mode, Path: raw.Path, CloneURL: raw.CloneURL,
	}); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveRepo != nil {
		if detail, ok := s.Engine.GetHostDetail(host); ok {
			for _, repo := range detail.Repos {
				if repo.WorktreePath == raw.Path {
					s.SaveRepo(repo)
				}
			}
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "requested"})
}

func (s *Server) discardRepo(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Query().Get("host")
	path := r.URL.Query().Get("path")
	if host == "" {
		host = r.PathValue("host")
	}
	if path == "" {
		path = r.PathValue("repo")
	}
	force := r.URL.Query().Get("force") == "true" || r.URL.Query().Get("force") == "1"
	if r.Body != nil {
		var raw struct {
			Host  string `json:"host"`
			Path  string `json:"path"`
			Force bool   `json:"force"`
		}
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &raw)
			if host == "" && raw.Host != "" {
				host = raw.Host
			}
			if path == "" && raw.Path != "" {
				path = raw.Path
			}
			if raw.Force {
				force = true
			}
		}
	}

	if host == "" || path == "" {
		writeErr(w, http.StatusBadRequest, errors.New("host and path are required"))
		return
	}

	if err := s.Engine.RequestDiscard(host, path, force); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "DISCARD_REQUESTED",
		"host":   host,
		"path":   path,
		"force":  force,
	})
}

func (s *Server) discardAllRepos(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if host == "" {
		host = r.URL.Query().Get("host")
	}
	force := r.URL.Query().Get("force") == "true" || r.URL.Query().Get("force") == "1"
	if r.Body != nil {
		var raw struct {
			Host  string `json:"host"`
			Force bool   `json:"force"`
		}
		bodyBytes, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &raw)
			if host == "" && raw.Host != "" {
				host = raw.Host
			}
			if raw.Force {
				force = true
			}
		}
	}
	if host == "" {
		writeErr(w, http.StatusBadRequest, errors.New("host is required"))
		return
	}

	repos, err := s.Engine.RequestDiscardAll(host, force)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	repoPaths := make([]string, 0, len(repos))
	for _, repo := range repos {
		repoPaths = append(repoPaths, repo.WorktreePath)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "DISCARD_REQUESTED",
		"host":           host,
		"force":          force,
		"count":          len(repos),
		"affected_repos": repoPaths,
	})
}

func (s *Server) discardHub(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		DocsHubPath string `json:"docs_hub_path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&raw)
	if err := s.Engine.RequestDiscardHub(r.PathValue("host"), raw.DocsHubPath); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "DISCARD_REQUESTED"})
}

func (s *Server) intervention(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		PromptID string `json:"prompt_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	sch, err := s.Engine.ScheduleIntervention(r.URL.Query().Get("host"), r.URL.Query().Get("path"), raw.PromptID)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, scheduleView(sch))
}

func (s *Server) forcePauseJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if err := s.Engine.ForcePauseJob(jobID); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepoForJob(jobID); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"queue": control.QueuePaused})
}

func (s *Server) retryCorrelation(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.RetryCorrelation(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "PENDING_CORRELATION"})
}

func (s *Server) appendChunk(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Type             string   `json:"type"`
		Stream           string   `json:"stream"`
		Text             string   `json:"text"`
		SilentForMs      int      `json:"silent_for_ms"`
		HarnessPID       int      `json:"harness_pid"`
		ToolPID          int      `json:"tool_pid"`
		ElapsedSeconds   int64    `json:"elapsed_seconds"`
		ContinuationTurn int      `json:"continuation_turn"`
		ConversationID   string   `json:"conversation_id"`
		UnmetGates       []string `json:"unmet_gates"`
		ActiveChildTasks string   `json:"active_child_tasks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if raw.Type == "TURN_AUTO_CONTINUED" {
		if raw.Text == "" {
			raw.Stream = "stdout"
			raw.Text = fmt.Sprintf("[iazio-harness] Turn auto-continued (turn %d) on conversation %s (unmet gates: %v; active: %s)\n",
				raw.ContinuationTurn, raw.ConversationID, raw.UnmetGates, raw.ActiveChildTasks)
		}
		s.RecordNotification(NotificationRecord{
			Timestamp: time.Now().UTC(),
			Category:  "info",
			Title:     "job.turn.auto_continued",
			Source:    "harness",
			Event:     "job.turn.auto_continued",
			JobID:     r.PathValue("id"),
			Message:   fmt.Sprintf("Turn auto-continued (turn %d) on job %s: conversation %s (unmet gates: %v)", raw.ContinuationTurn, r.PathValue("id"), raw.ConversationID, raw.UnmetGates),
		})
	}
	if strings.Contains(raw.Text, "root agent idle; waiting up to") {
		lines := strings.Split(raw.Text, "\n")
		var kept []string
		for _, line := range lines {
			if !strings.Contains(line, "root agent idle; waiting up to") {
				kept = append(kept, line)
			}
		}
		raw.Text = strings.Join(kept, "\n")
	}
	if raw.Type == "OUTPUT_CHUNK" && raw.Text == "" {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "filtered"})
		return
	}
	s.Engine.AppendLog(r.PathValue("id"), control.LogChunk{
		Type:             raw.Type,
		Stream:           raw.Stream,
		Text:             raw.Text,
		SilentForMs:      raw.SilentForMs,
		HarnessPID:       raw.HarnessPID,
		ToolPID:          raw.ToolPID,
		ElapsedSeconds:   raw.ElapsedSeconds,
		ContinuationTurn: raw.ContinuationTurn,
		ConversationID:   raw.ConversationID,
		UnmetGates:       raw.UnmetGates,
		ActiveChildTasks: raw.ActiveChildTasks,
	})
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stored"})
}

func (s *Server) addJobConversation(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		ConversationID    string         `json:"conversation_id"`
		Fallback          bool           `json:"fallback"`
		FallbackReason    string         `json:"fallback_reason"`
		Role              string         `json:"role"`
		CLIArgs           string         `json:"cli_args"`
		ASEPhase          string         `json:"ase_phase"`
		RehydrationStatus map[string]any `json:"rehydration_status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cid := strings.TrimSpace(raw.ConversationID)
	if cid == "" {
		writeErr(w, http.StatusBadRequest, errors.New("conversation_id is required"))
		return
	}
	if !control.IsValidConversationID(cid) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid conversation_id: contains illegal characters or path traversal"))
		return
	}
	opt := control.ConversationAttachOptions{
		Fallback:          raw.Fallback,
		FallbackReason:    raw.FallbackReason,
		Role:              raw.Role,
		CLIArgs:           raw.CLIArgs,
		ASEPhase:          raw.ASEPhase,
		RehydrationStatus: raw.RehydrationStatus,
	}
	if err := s.Engine.AddJobConversationWithOptions(r.PathValue("id"), cid, opt); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded", "conversation_id": cid})
}

func (s *Server) postJobReadiness(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		ReapResult             map[string]any `json:"reap_result"`
		WorkspaceStateManifest map[string]any `json:"workspace_state_manifest"`
		RehydrationStatus      map[string]any `json:"rehydration_status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	if err := s.Engine.SetJobReadiness(id, raw.ReapResult, raw.WorkspaceStateManifest, raw.RehydrationStatus); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) getRehydrationBundle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !control.IsValidConversationID(id) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid conversation_id: contains illegal characters or path traversal"))
		return
	}
	bundle, err := s.Engine.GetRehydrationBundle(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (s *Server) jobLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := atoiQuery(q.Get("page"))
	fromSeq := atoiQuery(q.Get("from_seq"))
	toSeq := atoiQuery(q.Get("to_seq"))
	pageOut := s.Engine.JobLogs(r.PathValue("id"), page, q.Get("cursor"), fromSeq, toSeq)
	chunks := make([]map[string]any, 0, len(pageOut.Chunks))
	for _, c := range pageOut.Chunks {
		chunks = append(chunks, chunkView(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"chunks": chunks, "next_cursor": pageOut.NextCursor})
}

func (s *Server) jobStream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	status, ok := s.Engine.JobStatus(id)
	if ok && (status == "RUN_FINISHED" || status == "CANCELLED") {
		writeErr(w, http.StatusConflict, errors.New("job is finished"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Send an initial SSE comment and flush to establish connection through reverse proxies
	_, _ = w.Write([]byte(": ping\n\n"))
	flusher.Flush()
	tail := atoiQuery(r.URL.Query().Get("tail"))
	after := atoiQuery(r.URL.Query().Get("after_seq"))
	if after == 0 {
		if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
			after = atoiQuery(lastID)
		}
	}
	chunks, gapFrom, gapTo := s.Engine.TailLogs(id, tail, after)
	if gapFrom > 0 {
		writeSSE(w, "GAP", map[string]any{"type": "GAP", "from_seq": gapFrom, "to_seq": gapTo})
	}
	for _, c := range chunks {
		writeSSE(w, c.Type, chunkView(c))
	}
	flusher.Flush()
	sub, cancel := s.Engine.SubscribeLogs(id)
	defer cancel()
	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepAliveTicker.C:
			_, _ = w.Write([]byte(": keep-alive\n\n"))
			flusher.Flush()
		case c, open := <-sub:
			if !open {
				return
			}
			writeSSE(w, c.Type, chunkView(c))
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, payload any) {
	b, _ := json.Marshal(payload)
	if m, ok := payload.(map[string]any); ok {
		if seq, ok := m["seq"].(int); ok && seq > 0 {
			_, _ = fmt.Fprintf(w, "id: %d\n", seq)
		} else if seq64, ok := m["seq"].(int64); ok && seq64 > 0 {
			_, _ = fmt.Fprintf(w, "id: %d\n", seq64)
		}
	}
	_, _ = w.Write([]byte("event: " + event + "\ndata: " + string(b) + "\n\n"))
}

// Git merge lease coordinator requests and handlers (STORY-GOV-3705)

type gitLeaseRequest struct {
	Repo       string `json:"repo"`
	Holder     string `json:"holder"`
	LeaseID    string `json:"lease_id,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

func (s *Server) acquireGitLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req gitLeaseRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.GitLeaseManager == nil {
		s.GitLeaseManager = gitlease.NewManager()
	}
	lease, acquired, err := s.GitLeaseManager.Acquire(req.Repo, req.Holder, req.TTLSeconds)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !acquired {
		writeJSON(w, http.StatusConflict, map[string]any{
			"acquired":      false,
			"error":         "repository merge lease held by another workspace",
			"current_lease": lease,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"acquired": true,
		"lease":    lease,
	})
}

func (s *Server) renewGitLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req gitLeaseRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.GitLeaseManager == nil {
		s.GitLeaseManager = gitlease.NewManager()
	}
	lease, renewed, err := s.GitLeaseManager.Renew(req.Repo, req.Holder, req.LeaseID, req.TTLSeconds)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !renewed {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"renewed": false,
			"error":   "lease not active or expired",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"renewed": true,
		"lease":   lease,
	})
}

func (s *Server) releaseGitLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	var req gitLeaseRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.GitLeaseManager == nil {
		s.GitLeaseManager = gitlease.NewManager()
	}
	ok, err := s.GitLeaseManager.Release(req.Repo, req.Holder, req.LeaseID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"released": ok,
	})
}

func (s *Server) listGitLeases(w http.ResponseWriter, r *http.Request) {
	if s.GitLeaseManager == nil {
		s.GitLeaseManager = gitlease.NewManager()
	}
	leases := s.GitLeaseManager.List()
	writeJSON(w, http.StatusOK, map[string]any{
		"leases": leases,
	})
}

func (s *Server) getGitLease(w http.ResponseWriter, r *http.Request) {
	repo := r.PathValue("repo")
	if s.GitLeaseManager == nil {
		s.GitLeaseManager = gitlease.NewManager()
	}
	lease := s.GitLeaseManager.Get(repo)
	if lease == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"found": false,
			"error": "no active lease found for repository",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"found": true,
		"lease": lease,
	})
}

func atoiQuery(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func (s *Server) getRepo(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	repo, ok := s.Engine.GetRepo(r.PathValue("host"), path)
	if !ok {
		writeErr(w, http.StatusNotFound, errors.New("not found"))
		return
	}
	writeJSON(w, http.StatusOK, repo)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) listProfiles(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	writeJSON(w, http.StatusOK, s.Engine.ListProfiles())
}

func (s *Server) createProfile(w http.ResponseWriter, r *http.Request) {
	var p control.DistributionProfile
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	created, err := s.Engine.UpsertProfile(p)
	if err != nil {
		if strings.Contains(err.Error(), "persist profile") {
			writeErr(w, http.StatusInternalServerError, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) getProfile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	name := r.PathValue("name")
	p, err := s.Engine.GetProfile(name)
	if err != nil {
		if errors.Is(err, control.ErrProfileNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) updateProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var p control.DistributionProfile
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	p.Name = name
	updated, err := s.Engine.UpsertProfile(p)
	if err != nil {
		if strings.Contains(err.Error(), "persist profile") {
			writeErr(w, http.StatusInternalServerError, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) deleteProfile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.Engine.DeleteProfile(name); err != nil {
		if errors.Is(err, control.ErrProtectedProfile) {
			writeErr(w, http.StatusForbidden, err)
			return
		}
		if errors.Is(err, control.ErrProfileNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if s.DeleteProfile != nil {
		s.DeleteProfile(name)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) syncGenericProfile(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	profile, err := s.Engine.SyncGenericProfile(raw)
	if err != nil {
		if strings.Contains(err.Error(), "persist profile") {
			writeErr(w, http.StatusInternalServerError, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (s *Server) scanRepoProfile(w http.ResponseWriter, r *http.Request) {
	var body struct {
		HostID      string `json:"host_id"`
		RepoPath    string `json:"repo_path"`
		ProfileName string `json:"profile_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(body.HostID) == "" || strings.TrimSpace(body.RepoPath) == "" {
		writeErr(w, http.StatusBadRequest, errors.New("host_id and repo_path are required"))
		return
	}
	profile, err := s.Engine.ScanRepoProfile(body.HostID, body.RepoPath, body.ProfileName)
	if err != nil {
		if strings.Contains(err.Error(), "persist profile") {
			writeErr(w, http.StatusInternalServerError, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(body.HostID, body.RepoPath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, profile)
}

func (s *Server) getFleetManifest(w http.ResponseWriter, r *http.Request) {
	profile := r.URL.Query().Get("profile")
	targetOS := r.URL.Query().Get("os")
	targetArch := r.URL.Query().Get("arch")

	manifest, err := s.Engine.GetManifest(profile, targetOS, targetArch)
	if err != nil {
		if errors.Is(err, control.ErrProfileNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) healASERepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hostID := r.PathValue("host")
	if hostID == "" {
		hostID = strings.TrimPrefix(r.URL.Path, "/v1/hosts/")
		hostID = strings.TrimPrefix(hostID, "/api/v1/agents/")
		if idx := strings.Index(hostID, "/"); idx != -1 {
			hostID = hostID[:idx]
		}
	}
	if hostID == "" {
		http.Error(w, "host is required", http.StatusBadRequest)
		return
	}

	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" {
		var req struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		targetPath = strings.TrimSpace(req.Path)
	}
	if targetPath == "" {
		targetPath = "."
	}

	actualPath := targetPath
	detail, ok := s.Engine.GetRepoDetail(hostID, targetPath)
	if ok && detail.Repo.WorktreePath != "" {
		actualPath = detail.Repo.WorktreePath
	}

	cmdID := fmt.Sprintf("heal-ase-%d", time.Now().UnixNano())
	_ = s.Engine.QueueCommand(hostID, control.CommandEnvelope{
		ID:        cmdID,
		Type:      "REAP_ORPHANS_AND_RESET_ASE",
		Payload:   actualPath,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})

	var locksCleared []string
	if ok {
		if detail.Repo.Lock != control.LockIdle {
			detail.Repo.Lock = control.LockIdle
			detail.Repo.Reason = ""
			_ = s.Engine.UpsertRepo(detail.Repo)
			locksCleared = append(locksCleared, "repo_lock_reset")
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"host_id":       hostID,
		"repo_path":     actualPath,
		"command_id":    cmdID,
		"locks_cleared": locksCleared,
		"status":        "dispatched",
		"healed_at":     time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) retryHealRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hostID := r.PathValue("host")
	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" {
		var req struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		targetPath = strings.TrimSpace(req.Path)
	}
	if err := s.Engine.RetryHealRepo(hostID, targetPath); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(hostID, targetPath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"host_id":   hostID,
		"repo_path": targetPath,
		"status":    "healing_retried",
	})
}

func (s *Server) forceCleanGitRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hostID := r.PathValue("host")
	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" {
		var req struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		targetPath = strings.TrimSpace(req.Path)
	}
	if err := s.Engine.ForceCleanGitRepo(hostID, targetPath); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(hostID, targetPath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"host_id":   hostID,
		"repo_path": targetPath,
		"status":    "force_clean_dispatched",
	})
}

func (s *Server) clearHealingRepo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hostID := r.PathValue("host")
	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" {
		var req struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		targetPath = strings.TrimSpace(req.Path)
	}
	if err := s.Engine.ClearHealingRepo(hostID, targetPath); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(hostID, targetPath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"host_id":   hostID,
		"repo_path": targetPath,
		"status":    "healing_cleared",
	})
}

func (s *Server) killBackgroundTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jobID := r.PathValue("id")
	if err := s.Engine.KillBackgroundTasks(jobID); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": jobID,
		"status": "background_tasks_killed",
	})
}

func (s *Server) stashCleanRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jobID := r.PathValue("id")
	sch, err := s.Engine.StashCleanRestart(jobID)
	if err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(sch.HostID, sch.WorktreePath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, scheduleView(sch))
}

func (s *Server) cleanAndDispatchNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	host := r.PathValue("host")
	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" {
		var req struct {
			Path         string `json:"path"`
			WorktreePath string `json:"worktree_path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.WorktreePath != "" {
			targetPath = strings.TrimSpace(req.WorktreePath)
		} else {
			targetPath = strings.TrimSpace(req.Path)
		}
	}
	if err := s.Engine.CleanAndDispatchNext(host, targetPath); err != nil {
		if errors.Is(err, control.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
		} else {
			writeErr(w, http.StatusBadRequest, err)
		}
		return
	}
	if s.SaveRepo != nil {
		if repo, ok := s.Engine.GetRepo(host, targetPath); ok {
			s.SaveRepo(repo)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"host_id":   host,
		"repo_path": targetPath,
		"status":    "cleaned_and_dispatch_reconciled",
	})
}

func (s *Server) observabilityMetrics(w http.ResponseWriter, r *http.Request) {
	history := s.Engine.ListHistory("", "", "", 0)
	statusCounts := make(map[string]int)
	reasonCounts := make(map[string]int)
	engineCounts := make(map[string]int)
	traceCounts := 0
	sameConvSuccessCount := 0
	fallbackConvCount := 0
	fallbackReasonCounts := make(map[string]int)
	for _, h := range history {
		status := h.Status
		if status == "" {
			status = "UNKNOWN"
		}
		statusCounts[status]++
		reason := h.Reason
		if reason == "" {
			reason = h.DiagnosticCode
		}
		if reason != "" {
			reasonCounts[reason]++
		}
		if h.Engine != "" {
			engineCounts[h.Engine]++
		}
		if h.TraceID != "" {
			traceCounts++
		}
		if (h.ResumeMode == "same_conversation" || h.SameConversationConfirmed) && (h.AttachConfirmation == "marker" || (h.AttachConfirmation == "implicit" && (h.Status == "RUN_FINISHED" || h.Status == "FINISHED" || h.Status == "COMPLETED"))) {
			sameConvSuccessCount++
		} else if h.ResumeMode == "fallback_new_conversation" {
			fallbackConvCount++
			fbReason := h.ResumeFallbackReason
			if fbReason == "" {
				fbReason = "UNKNOWN"
			}
			fallbackReasonCounts[fbReason]++
		}
	}

	hosts := s.Engine.ListHosts()
	onlineHosts := 0
	offlineHosts := 0
	for _, h := range hosts {
		if h.Host.Presence == "ONLINE" {
			onlineHosts++
		} else {
			offlineHosts++
		}
	}

	idleRepos, runningRepos, pausedRepos := s.Engine.RepoCounts()
	activeJobs := s.Engine.ActiveJobsCount()

	zipkinEndpoint := os.Getenv("ZIPKIN_ENDPOINT")
	if zipkinEndpoint == "" {
		zipkinEndpoint = "http://localhost:9411/api/v2"
	}
	esEndpoint := os.Getenv("ELASTICSEARCH_ENDPOINT")
	if esEndpoint == "" {
		esEndpoint = "http://localhost:9200"
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total_executions":  len(history),
		"status_counts":     statusCounts,
		"failure_reasons":   reasonCounts,
		"engine_counts":     engineCounts,
		"active_jobs_count": activeJobs,
		"traces_correlated": traceCounts,
		"hosts": map[string]any{
			"total":   len(hosts),
			"online":  onlineHosts,
			"offline": offlineHosts,
		},
		"repositories": map[string]any{
			"idle":    idleRepos,
			"running": runningRepos,
			"paused":  pausedRepos,
		},
		"resume_metrics": map[string]any{
			"same_conversation_success_total": sameConvSuccessCount,
			"fallback_new_conversation_total": fallbackConvCount,
			"fallback_reasons":                fallbackReasonCounts,
		},
		"telemetry": map[string]any{
			"zipkin_endpoint":        zipkinEndpoint,
			"elasticsearch_endpoint": esEndpoint,
			"service_name":           "iazio-harness-api",
			"tracing_enabled":        true,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// DecodeLimit reads a small JSON body.
func DecodeLimit(r io.Reader, v any) error {
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	return dec.Decode(v)
}

// NowUnix is a test helper clock.
func NowUnix() time.Time { return time.Unix(0, 0) }
