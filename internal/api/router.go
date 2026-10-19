// Package api configures the REST API routes and authentication middleware.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/api/handlers"
	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

// RouterConfig contains dependencies and settings for the API router.
type RouterConfig struct {
	Store       codebase.Store
	Ingestor    codebase.Ingestor
	Queue       dispatch.Queue
	Idempotency *handlers.IdempotencyStore
	AuthToken   string
}

// NewRouter constructs an http.Handler with all /api/v1 routes and auth middleware.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	if cfg.Queue == nil {
		cfg.Queue = dispatch.NewInMemoryQueue()
	}
	if cfg.Idempotency == nil {
		cfg.Idempotency = handlers.NewIdempotencyStore(24 * time.Hour)
	}
	if cfg.Ingestor == nil {
		cfg.Ingestor = codebase.NewIngestor(cfg.Store, nil)
	}

	reposHandler := handlers.NewReposHandler(cfg.Store)
	searchHandler := handlers.NewCodebaseSearchHandler(cfg.Store)
	promptsHandler := handlers.NewPromptsHandler(cfg.Store, cfg.Queue, cfg.Idempotency)
	storiesHandler := handlers.NewStoriesHandler(cfg.Store, cfg.Queue, cfg.Idempotency)
	agentJobsHandler := handlers.NewAgentJobsHandler(cfg.Queue)
	agentSyncHandler := handlers.NewAgentSyncHandler(cfg.Store, cfg.Ingestor)

	mux.Handle("/api/v1/repos", reposHandler)
	mux.Handle("/api/v1/codebase/search", searchHandler)
	mux.Handle("/api/v1/prompts", promptsHandler)
	mux.Handle("/api/v1/stories", storiesHandler)
	mux.HandleFunc("/api/v1/agent/repos/sync", agentSyncHandler.SyncRepos)
	mux.HandleFunc("/api/v1/agent/codebase/sync", agentSyncHandler.SyncCodebase)
	mux.HandleFunc("/api/v1/agent/jobs/claim", agentJobsHandler.ClaimHandler)
	mux.HandleFunc("/api/v1/agent/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/complete") {
			agentJobsHandler.CompleteHandler(w, r)
			return
		}
		http.NotFound(w, r)
	})

	return withAuthMiddleware(mux, cfg.AuthToken)
}

func withAuthMiddleware(next http.Handler, token string) http.Handler {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(authHeader, prefix) {
			writeAuthError(w)
			return
		}

		got := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			writeAuthError(w)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeAuthError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
}
