// Command iazio-harness-api serves the control plane.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

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
		srv.SaveIdea = func(idea control.Idea) {
			_ = store.SaveIdea(context.Background(), idea.ID, idea.Title, idea.ShareURL, idea.Transcript, idea.Status, idea.StoryID)
		}
	}
	log.Printf("listening %s %s", addr, version.Informational())
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
