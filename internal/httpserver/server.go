// Package httpserver exposes the control plane over HTTP.
package httpserver

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/api/handlers"
	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
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
	// AuthToken, when non-empty, requires Bearer token authentication on control-plane endpoints.
	AuthToken string
	// CodebaseStore manages fleet repository discovery and codebase RAG search chunks.
	CodebaseStore codebase.Store
	// EventHub manages cursor-resumable SSE streaming and heartbeat telemetry.
	EventHub *sse.Hub
	// Queue manages async job dispatching and execution for prompts and stories.
	Queue dispatch.Queue
	// Idempotency manages API idempotency caches.
	Idempotency *handlers.IdempotencyStore
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

	mux.Handle("GET /api/v1/repos", handlers.NewReposHandler(codebaseStore))
	mux.Handle("POST /api/v1/codebase/search", handlers.NewCodebaseSearchHandler(codebaseStore))

	agentSyncHandler := handlers.NewAgentSyncHandler(codebaseStore, codebase.NewIngestor(codebaseStore, nil))
	mux.HandleFunc("POST /api/v1/agent/repos/sync", agentSyncHandler.SyncRepos)
	mux.HandleFunc("POST /api/v1/agent/codebase/sync", agentSyncHandler.SyncCodebase)

	// Dispatch & Story Lifecycle Endpoints
	storiesHandler := handlers.NewStoriesHandler(codebaseStore, queue, idempotency, s.Engine)
	mux.Handle("POST /api/v1/stories", storiesHandler)
	mux.HandleFunc("GET /api/v1/stories", storiesHandler.ListStories)
	mux.HandleFunc("GET /api/v1/stories/", storiesHandler.GetStory)

	promptsHandler := handlers.NewPromptsHandler(codebaseStore, queue, idempotency)
	mux.Handle("POST /api/v1/prompts", promptsHandler)

	jobLifecycle := handlers.NewJobLifecycleHandler(queue, codebaseStore)
	if s.Engine != nil {
		jobLifecycle.SetScheduler(s.Engine)
	}
	mux.HandleFunc("GET /api/v1/jobs/{id}", jobLifecycle.GetJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/attribution", jobLifecycle.GetJobAttribution)
	mux.HandleFunc("GET /api/v1/queue", jobLifecycle.ListQueue)
	mux.HandleFunc("GET /api/v1/jobs", jobLifecycle.ListQueue)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", jobLifecycle.CancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", jobLifecycle.RetryJob)
	mux.HandleFunc("POST /api/v1/stories/{id}/execute", jobLifecycle.ExecuteStory)

	agentJobsHandler := handlers.NewAgentJobsHandler(queue)
	mux.HandleFunc("POST /api/v1/agent/jobs/{id}/complete", agentJobsHandler.CompleteHandler)

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
	mux.HandleFunc("POST /v1/jobs/{id}/force-pause", s.forcePauseJob)
	mux.HandleFunc("POST /v1/jobs/{id}/retry-correlation", s.retryCorrelation)
	mux.HandleFunc("POST /v1/jobs/{id}/chunks", s.appendChunk)
	mux.HandleFunc("POST /v1/jobs/{id}/conversations", s.addJobConversation)
	mux.HandleFunc("GET /v1/jobs/{id}/logs", s.jobLogs)
	mux.HandleFunc("GET /v1/jobs/{id}/stream", s.jobStream)
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
	mux.HandleFunc("POST /v1/hosts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /v1/hosts/{id}/tools/update", s.queueToolUpdate)
	mux.HandleFunc("POST /v1/hosts/{id}/poll", s.pollHost)
	mux.HandleFunc("GET /v1/hosts/{host}/repos", s.repoDetail)
	mux.HandleFunc("DELETE /v1/hosts/{host}/repos", s.unregisterRepo)
	mux.HandleFunc("DELETE /v1/hosts/{host}/repos/history", s.clearRepoHistory)
	mux.HandleFunc("DELETE /v1/history/{id}", s.deleteHistoryRow)
	mux.HandleFunc("POST /v1/hosts/{host}/repo-requests", s.repoRequest)
	mux.HandleFunc("POST /v1/hosts/{host}/docs-hub/discard", s.discardHub)
	mux.HandleFunc("POST /v1/repos", s.upsertRepo)
	mux.HandleFunc("POST /v1/repos/discard", s.discardRepo)
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
	mux.HandleFunc("GET /v1/fleet/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /v1/fleet/tool-priority", s.setToolPriority)
	mux.HandleFunc("GET /api/v1/tool-priority", s.getToolPriority)
	mux.HandleFunc("PUT /api/v1/tool-priority", s.setToolPriority)
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
		Path           string `json:"path"`
		WorktreePath   string `json:"worktree_path"`
		StoryID        string `json:"story_id"`
		ConversationID string `json:"conversation_id"`
		JobID          string `json:"job_id"`
		AllowDirty     bool   `json:"allow_dirty"`
		PromptID       string `json:"prompt_id"`
		PromptTitle    string `json:"prompt_title"`
		Engine         string `json:"engine"`
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
	opts := control.ResumeOptions{
		ConversationID: body.ConversationID,
		AllowDirty:     body.AllowDirty,
		StoryID:        body.StoryID,
		JobID:          body.JobID,
		PromptID:       body.PromptID,
		PromptTitle:    body.PromptTitle,
		Engine:         body.Engine,
		HostID:         host,
		WorktreePath:   targetPath,
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
			if qj.Status == dispatch.JobStatusPending || qj.Status == dispatch.JobStatusClaimed {
				if status != "" && !strings.EqualFold(status, qj.Status) && !strings.EqualFold(status, "QUEUED") && !strings.EqualFold(status, "RUNNING") {
					continue
				}
				out = append(out, dispatchScheduleView(qj))
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
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
			if qj.Status == dispatch.JobStatusCompleted || qj.Status == dispatch.JobStatusFailed || qj.Status == dispatch.JobStatusCanceled {
				if status != "" && !strings.EqualFold(status, qj.Status) {
					continue
				}
				out = append(out, dispatchHistoryView(qj))
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"history": out})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, ok := s.Engine.GetJob(id)
	if ok {
		writeJSON(w, http.StatusOK, jobDetailView(job))
		return
	}
	if s.Queue != nil {
		if qJob, found := s.Queue.Get(id); found && qJob != nil {
			writeJSON(w, http.StatusOK, dispatchJobDetailView(qJob))
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
	writeJSON(w, http.StatusOK, map[string]string{
		"id": job.ID, "kind": job.Kind,
		"worktree_path": sch.WorktreePath, "docs_hub_path": docs,
		"story_id": sch.StoryID,
	})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		Presence    string `json:"presence"`
		FetchFailed bool   `json:"fetch_failed"`
		Tools       []struct {
			Name    string `json:"name"`
			Path    string `json:"path"`
			Version string `json:"version"`
			Status  string `json:"status"`
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
		tools = append(tools, control.Tool{Name: t.Name, Path: t.Path, Version: t.Version, Status: t.Status})
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
	if err := s.Engine.HeartbeatDetailed(hostID, raw.Presence, tools, raw.FetchFailed, activeJobs...); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if s.SaveHost != nil {
		if detail, ok := s.Engine.GetHostDetail(hostID); ok {
			s.SaveHost(detail.Host)
		}
	}
	pending := s.Engine.ConsumePendingToolUpdate(hostID)
	res := map[string]any{"status": "ok"}
	if pending != nil {
		res["pending_tool_update"] = pending
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) queueToolUpdate(w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	var body struct {
		Tools []string `json:"tools"`
		Force bool     `json:"force"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	if err := s.Engine.QueueToolUpdate(hostID, body.Tools, body.Force); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "queued",
		"host_id": hostID,
		"tools":   body.Tools,
		"force":   body.Force,
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
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
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
	if err := s.Engine.RequestDiscard(host, path); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "DISCARD_REQUESTED"})
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
		Type        string `json:"type"`
		Stream      string `json:"stream"`
		Text        string `json:"text"`
		SilentForMs int    `json:"silent_for_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	s.Engine.AppendLog(r.PathValue("id"), control.LogChunk{
		Type:        raw.Type,
		Stream:      raw.Stream,
		Text:        raw.Text,
		SilentForMs: raw.SilentForMs,
	})
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stored"})
}

func (s *Server) addJobConversation(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		ConversationID string `json:"conversation_id"`
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
	if err := s.Engine.AddJobConversation(r.PathValue("id"), cid); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded", "conversation_id": cid})
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
	_, _ = w.Write([]byte("event: " + event + "\ndata: " + string(b) + "\n\n"))
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
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveProfile != nil {
		s.SaveProfile(*created)
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) getProfile(w http.ResponseWriter, r *http.Request) {
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
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveProfile != nil {
		s.SaveProfile(*updated)
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
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveProfile != nil {
		s.SaveProfile(*profile)
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
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if s.SaveProfile != nil {
		s.SaveProfile(*profile)
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

// DecodeLimit reads a small JSON body.
func DecodeLimit(r io.Reader, v any) error {
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	return dec.Decode(v)
}

// NowUnix is a test helper clock.
func NowUnix() time.Time { return time.Unix(0, 0) }
