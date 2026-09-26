// Package httpserver exposes the control plane over HTTP.
package httpserver

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/version"
)

// Server is the harness API.
type Server struct {
	Engine *control.Engine
	// Extract runs the share-page worker. Tests replace it.
	Extract func(shareURL string) (title, transcript string, err error)
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/ideas/import-gemini", s.importGemini)
	mux.HandleFunc("POST /v1/ideas/{id}/triage", s.triage)
	mux.HandleFunc("POST /v1/ideas/{id}/refine", s.refine)
	mux.HandleFunc("POST /v1/jobs/{id}/story-draft", s.storyDraft)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /v1/hosts/register", s.register)
	mux.HandleFunc("POST /v1/repos", s.upsertRepo)
	mux.HandleFunc("POST /v1/repos/{host}/lease", s.lease)
	mux.HandleFunc("POST /v1/repos/{host}/preflight", s.preflight)
	mux.HandleFunc("POST /v1/repos/{host}/finish", s.finish)
	mux.HandleFunc("POST /v1/repos/{host}/force-pause", s.forcePause)
	mux.HandleFunc("GET /v1/repos/{host}", s.getRepo)
	return mux
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
	writeJSON(w, http.StatusCreated, idea)
}

func (s *Server) triage(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.Triage(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": control.IdeaTriaged})
}

func (s *Server) refine(w http.ResponseWriter, r *http.Request) {
	var body struct {
		HostID      string `json:"host_id"`
		DocsHubPath string `json:"docs_hub_path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	job, err := s.Engine.ScheduleRefinement(r.PathValue("id"), body.HostID, body.DocsHubPath)
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrHubBusy) {
			code = http.StatusConflict
		}
		writeErr(w, code, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
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
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.Cancel(r.PathValue("id"), "cancelled"); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "CANCEL_REQUESTED"})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID   string `json:"id"`
		Kind string `json:"kind"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.Engine.RegisterHost(body.ID, body.Kind))
}

func (s *Server) upsertRepo(w http.ResponseWriter, r *http.Request) {
	var repo control.Repo
	if err := json.NewDecoder(r.Body).Decode(&repo); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Engine.UpsertRepo(repo); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path     string `json:"worktree_path"`
		CloneURL string `json:"clone_url"`
		Kind     string `json:"kind"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var job control.Job
	var err error
	if body.Kind == control.KindIntervention {
		job, err = s.Engine.LeaseIntervention(r.PathValue("host"), body.Path, body.CloneURL)
	} else {
		job, err = s.Engine.LeaseOrdinary(r.PathValue("host"), body.Path, body.CloneURL)
	}
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
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
	writeJSON(w, http.StatusOK, s.Engine.ApplyPreflight(r.PathValue("host"), body.Path, body.Pre))
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
	writeJSON(w, http.StatusOK, s.Engine.ApplyFinish(r.PathValue("host"), body.Path, body.JobID, body.In))
}

func (s *Server) forcePause(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string `json:"worktree_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	s.Engine.ForcePause(r.PathValue("host"), body.Path)
	writeJSON(w, http.StatusOK, map[string]string{"queue": control.QueuePaused})
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

// DecodeLimit reads a small JSON body.
func DecodeLimit(r io.Reader, v any) error {
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	return dec.Decode(v)
}

// NowUnix is a test helper clock.
func NowUnix() time.Time { return time.Unix(0, 0) }
