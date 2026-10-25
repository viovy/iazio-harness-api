// Command iazio-harness-api serves the control plane.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/viovy/iazio-harness-api/internal/api/handlers"
	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/extract"
	"github.com/viovy/iazio-harness-api/internal/httpserver"
	"github.com/viovy/iazio-harness-api/internal/pgstore"
	"github.com/viovy/iazio-harness-api/internal/telemetry"
	"github.com/viovy/iazio-harness-api/internal/version"
)

var (
	versionStamp   = "0.1.0-dev"
	commitStamp    = "unknown"
	branchStamp    = "unknown"
	setVersionOnce sync.Once
)

func run(ctx context.Context, args []string) error {
	setVersionOnce.Do(func() {
		version.Version = versionStamp
		version.Commit = commitStamp
		version.Branch = branchStamp
	})
	if len(args) > 1 && args[1] == "version" {
		fmt.Println("iazio-harness-api " + version.Informational())
		return nil
	}
	addr := ":8080"
	if v := os.Getenv("IAZIO_HARNESS_API_ADDR"); v != "" {
		addr = v
	}
	codebaseStore := codebase.NewMemStore()
	srv := &httpserver.Server{
		Engine:        control.NewEngine(nil),
		CodebaseStore: codebaseStore,
		Queue:         dispatch.NewInMemoryQueue(),
		Idempotency:   handlers.NewIdempotencyStore(24 * time.Hour),
	}
	if tok := strings.TrimSpace(os.Getenv("IAZIO_AUTH_TOKEN")); tok != "" {
		srv.AuthToken = tok
	} else if tok := strings.TrimSpace(os.Getenv("IAZIO_HARNESS_API_TOKEN")); tok != "" {
		srv.AuthToken = tok
	}
	srv.Engine.StartReaper(ctx, 10*time.Second)
	if srv.Queue != nil {
		srv.Queue.StartReaper(ctx, 10*time.Second)
	}
	if script := os.Getenv("IAZIO_EXTRACT_WORKER"); script != "" {
		srv.Extract = func(shareURL string) (string, string, error) {
			return extract.Run(ctx, script, shareURL)
		}
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		store, err := pgstore.Open(ctx, dsn)
		if err != nil {
			return fmt.Errorf("pgstore.Open: %w", err)
		}
		defer store.Close()

		if ideas, err := store.LoadIdeas(ctx); err == nil {
			for _, it := range ideas {
				srv.Engine.PutIdea(it)
			}
			log.Printf("loaded %d ideas from database", len(ideas))
		}
		if prompts, err := store.LoadPrompts(ctx); err == nil {
			for _, p := range prompts {
				srv.Engine.PutPrompt(p)
			}
			log.Printf("loaded %d prompts from database", len(prompts))
		}
		if hosts, err := store.LoadHosts(ctx); err == nil {
			for _, h := range hosts {
				srv.Engine.SetHost(h)
			}
			log.Printf("loaded %d hosts from database", len(hosts))
		}
		if repos, err := store.LoadRepos(ctx); err == nil {
			for _, r := range repos {
				_ = srv.Engine.UpsertRepo(r)
			}
			log.Printf("loaded %d repos from database", len(repos))
		}
		if hist, err := store.LoadHistory(ctx); err != nil {
			log.Printf("warn: load history: %v", err)
		} else {
			for key, rows := range hist {
				srv.Engine.SetHistory(key, rows)
			}
			log.Printf("loaded %d repo history records from database", len(hist))
			reconciled := srv.Engine.ReconcileHistoricalFailureTaxonomy()
			if reconciled > 0 {
				log.Printf("reconciled failure taxonomy for %d historical history records", reconciled)
			}
		}
		if stories, err := store.LoadStories(ctx); err == nil {
			for _, st := range stories {
				_ = srv.Engine.SaveStory(st)
			}
			log.Printf("loaded %d stories from database", len(stories))
		}

		srv.ReconcileLineage = func(ctx context.Context) error {
			return store.ReconcileHistoricalLineage(ctx)
		}
		if err := store.ReconcileHistoricalLineage(ctx); err != nil {
			log.Printf("warn: reconcile historical lineage on db: %v", err)
		}
		if count, err := srv.Engine.ReconcileHistoricalLineage(); err != nil {
			log.Printf("warn: reconcile historical lineage on engine: %v", err)
		} else if count > 0 {
			log.Printf("reconciled %d historical job lineages on startup", count)
		}

		seedDefaults(ctx, srv.Engine, store, srv.CodebaseStore)

		if cRepos, err := store.LoadCodebaseRepos(ctx); err == nil && len(cRepos) > 0 {
			for _, r := range cRepos {
				_ = srv.CodebaseStore.UpsertRepository(ctx, r)
			}
			log.Printf("loaded %d codebase repos from database", len(cRepos))
		}

		srv.SaveIdea = func(idea control.Idea) {
			_ = store.SaveIdea(context.Background(), idea)
		}
		srv.SavePrompt = func(p control.Prompt) {
			_ = store.SavePrompt(context.Background(), p)
		}
		srv.DeletePrompt = func(id string) {
			_ = store.DeletePrompt(context.Background(), id)
		}
		srv.SaveRepo = func(r control.Repo) {
			_ = store.SaveRepo(context.Background(), r)
		}
		srv.Engine.OnRepoChange = srv.SaveRepo
		srv.Engine.OnStoryChange = func(s control.Story) {
			_ = store.SaveStory(context.Background(), s)
		}
		srv.Engine.OnHistoryChange = func(host, path string, row control.HistoryRow) {
			if err := store.SaveHistory(context.Background(), host, path, row); err != nil {
				log.Printf("warn: save history for %s %s: %v", host, path, err)
			}
		}
		srv.Engine.OnHistoryDelete = func(host, path, jobID string) {
			if err := store.DeleteHistory(context.Background(), jobID); err != nil {
				log.Printf("warn: delete history %s for %s %s: %v", jobID, host, path, err)
			}
		}
		srv.DeleteHistory = store.DeleteHistory
		srv.ClearHistory = store.ClearRepoHistory
		srv.DeleteRepo = func(hostID, path string) {
			_ = store.DeleteRepo(context.Background(), hostID, path)
		}
		srv.SaveHost = func(h control.Host) {
			_ = store.SaveHost(context.Background(), h)
		}
		srv.Engine.OnSaveProfile = func(p control.DistributionProfile) error {
			return store.SaveProfile(context.Background(), p)
		}
		srv.DeleteProfile = func(name string) {
			_ = store.DeleteProfile(context.Background(), name)
		}
		if profiles, err := store.LoadProfiles(ctx); err == nil {
			for _, p := range profiles {
				srv.Engine.LoadProfile(p)
			}
			log.Printf("loaded %d distribution profiles from database", len(profiles))
		} else {
			log.Printf("failed to load distribution profiles from database: %v", err)
		}
		if srv.Engine.Releases != nil {
			if err := srv.Engine.Releases.SetStore(store); err != nil {
				return fmt.Errorf("failed to load git credentials into release service: %w", err)
			}
			log.Printf("attached git credentials store to release service")
		}
	} else {
		seedDefaults(context.Background(), srv.Engine, nil, srv.CodebaseStore)
	}

	if srv.Engine.Releases != nil {
		syncInterval := 60 * time.Second
		if raw := os.Getenv("IAZIO_RELEASE_SYNC_INTERVAL"); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d > 0 {
				syncInterval = d
			}
		}
		srv.Engine.Releases.StartBackgroundSync(ctx, srv.Engine, syncInterval, func(profileName string, tool control.ProfileTool, oldVersion, newVersion string) {
			srv.RecordNotification(httpserver.NotificationRecord{
				Title:    "Release Detected",
				Message:  fmt.Sprintf("New release detected for %s: %s (was %s). Profile '%s' updated.", tool.Name, newVersion, oldVersion, profileName),
				Category: "release",
				Source:   "release-watcher",
				Metadata: map[string]any{
					"event":        "release.detected",
					"profile":      profileName,
					"tool":         tool.Name,
					"from_version": oldVersion,
					"to_version":   newVersion,
					"url":          tool.LatestReleaseURL,
				},
			})
		})
		log.Printf("started background release sync watcher (interval=%v)", syncInterval)
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("listening %s %s", addr, version.Informational())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
	case <-ctx.Done():
		log.Printf("shutting down gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}
	return nil
}

func main() {
	flushTracer := telemetry.InitGlobalTracer("iazio-harness-api", "")
	defer flushTracer()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func seedDefaults(ctx context.Context, e *control.Engine, store *pgstore.Store, cbStore codebase.Store) {
	if len(e.ListPrompts()) == 0 {
		seedPrompts := []control.Prompt{
			{
				ID:       "prompt-1",
				Title:    "Ready ASE prompt",
				Body:     "run {{.StoryID}}",
				Engine:   "agent",
				Status:   "READY",
				Revision: 1,
			},
			{
				ID:       "prompt-2",
				Title:    "E2E AGY Prompt",
				Body:     "Respond with the exact word: AGY_E2E_VERIFIED",
				Engine:   "agy",
				Status:   "READY",
				Revision: 1,
			},
		}
		for _, p := range seedPrompts {
			created := e.PutPrompt(p)
			if store != nil {
				_ = store.SavePrompt(ctx, created)
			}
		}
		log.Printf("seeded %d default prompts", len(seedPrompts))
	}

	if rawHosts := os.Getenv("IAZIO_KNOWN_HOSTS"); rawHosts != "" {
		for _, id := range strings.Split(rawHosts, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				if _, ok := e.GetHostDetail(id); !ok {
					h := e.RegisterHost(id, "permanent")
					if store != nil {
						_ = store.SaveHost(ctx, h)
					}
				}
			}
		}
	}

	if raw := os.Getenv("IAZIO_DEFAULT_REPOS"); raw != "" {
		var defaultRepos []control.Repo
		if err := json.Unmarshal([]byte(raw), &defaultRepos); err == nil {
			seededCount := 0
			for _, r := range defaultRepos {
				if r.DefaultBranch == "" {
					r.DefaultBranch = "main"
				}
				if r.Queue == "" {
					r.Queue = control.QueueOpen
				}
				if r.Lock == "" {
					r.Lock = control.LockIdle
				}
				if _, ok := e.GetRepo(r.HostID, r.WorktreePath); !ok {
					_ = e.UpsertRepo(r)
					if store != nil {
						_ = store.SaveRepo(ctx, r)
					}
					seededCount++
				}
			}
			if seededCount > 0 {
				log.Printf("seeded %d default repos from environment", seededCount)
			}
		}
	}

	if cbStore != nil {
		res, _ := cbStore.ListRepositories(ctx, codebase.ListReposRequest{Limit: 1})
		if res == nil || res.Total == 0 {
			now := time.Now().UTC()
			defaultFleet := []codebase.Repository{
				{
					Name:          "iazio-mcp",
					Path:          "iazio-mcp",
					DefaultBranch: "main",
					Description:   "MCP server bridging iazio-api and iazio-harness-api",
					UpdatedAt:     now,
				},
				{
					Name:          "iazio-harness-api",
					Path:          "iazio-harness-api",
					DefaultBranch: "main",
					Description:   "Agent execution control plane and codebase RAG gateway",
					UpdatedAt:     now,
				},
				{
					Name:          "iazio-api",
					Path:          "iazio-api",
					DefaultBranch: "main",
					Description:   "Conversation history and distilled memory API",
					UpdatedAt:     now,
				},
				{
					Name:          "iazio-harness",
					Path:          "iazio-harness",
					DefaultBranch: "main",
					Description:   "Local runner execution harness",
					UpdatedAt:     now,
				},
				{
					Name:          "iazio-agent",
					Path:          "iazio-agent",
					DefaultBranch: "main",
					Description:   "Autonomous host agent",
					UpdatedAt:     now,
				},
				{
					Name:          "iazio-rag-api",
					Path:          "iazio-rag-api",
					DefaultBranch: "main",
					Description:   "Dedicated webhook-driven codebase RAG indexing service",
					UpdatedAt:     now,
				},
				{
					Name:          "autopilot",
					Path:          "autopilot",
					DefaultBranch: "main",
					Description:   "Autonomous software development lifecycle engine",
					UpdatedAt:     now,
				},
			}
			for _, r := range defaultFleet {
				_ = cbStore.UpsertRepository(ctx, r)
				if store != nil {
					_ = store.SaveCodebaseRepo(ctx, r)
				}
			}
			log.Printf("seeded %d default fleet repositories into codebase store", len(defaultFleet))
		}
	}
}
