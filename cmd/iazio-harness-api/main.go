// Command iazio-harness-api serves the control plane.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/extract"
	"github.com/viovy/iazio-harness-api/internal/httpserver"
	"github.com/viovy/iazio-harness-api/internal/pgstore"
	"github.com/viovy/iazio-harness-api/internal/version"
)

var (
	versionStamp = "0.1.0-dev"
	commitStamp  = "unknown"
	branchStamp  = "unknown"
)

func main() {
	version.Version = versionStamp
	version.Commit = commitStamp
	version.Branch = branchStamp
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("iazio-harness-api " + version.Informational())
		return
	}
	addr := ":8080"
	if v := os.Getenv("IAZIO_HARNESS_API_ADDR"); v != "" {
		addr = v
	}
	srv := &httpserver.Server{Engine: control.NewEngine(nil)}
	if script := os.Getenv("IAZIO_EXTRACT_WORKER"); script != "" {
		srv.Extract = func(shareURL string) (string, string, error) {
			return extract.Run(context.Background(), script, shareURL)
		}
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		store, err := pgstore.Open(context.Background(), dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer store.Close()

		ctx := context.Background()
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
		}

		seedDefaults(ctx, srv.Engine, store)

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
		srv.DeleteRepo = func(hostID, path string) {
			_ = store.DeleteRepo(context.Background(), hostID, path)
		}
		srv.SaveHost = func(h control.Host) {
			_ = store.SaveHost(context.Background(), h)
		}
		srv.SaveProfile = func(p control.DistributionProfile) {
			_ = store.SaveProfile(context.Background(), p)
		}
		srv.DeleteProfile = func(name string) {
			_ = store.DeleteProfile(context.Background(), name)
		}
		if profiles, err := store.LoadProfiles(ctx); err == nil {
			for _, p := range profiles {
				_, _ = srv.Engine.UpsertProfile(p)
			}
			log.Printf("loaded %d distribution profiles from database", len(profiles))
		}
	}
	log.Printf("listening %s %s", addr, version.Informational())
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

func seedDefaults(ctx context.Context, e *control.Engine, store *pgstore.Store) {
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
			_ = store.SavePrompt(ctx, created)
		}
		log.Printf("seeded %d default prompts", len(seedPrompts))
	}

	if rawHosts := os.Getenv("IAZIO_KNOWN_HOSTS"); rawHosts != "" {
		for _, id := range strings.Split(rawHosts, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				if _, ok := e.GetHostDetail(id); !ok {
					h := e.RegisterHost(id, "permanent")
					_ = store.SaveHost(ctx, h)
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
					_ = store.SaveRepo(ctx, r)
					seededCount++
				}
			}
			if seededCount > 0 {
				log.Printf("seeded %d default repos from environment", seededCount)
			}
		}
	}
}
