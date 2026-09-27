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
	// SaveIdea persists an idea when a database is configured.
	SaveIdea func(idea control.Idea)
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /version", s.version)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/ideas", s.listIdeas)
	mux.HandleFunc("POST /v1/ideas/import-gemini", s.importGemini)
	mux.HandleFunc("POST /v1/ideas/{id}/triage", s.triage)
	mux.HandleFunc("POST /v1/ideas/{id}/refine", s.refine)
	mux.HandleFunc("GET /v1/prompts", s.listPrompts)
	mux.HandleFunc("POST /v1/prompts", s.createPrompt)
	mux.HandleFunc("PUT /v1/prompts/{id}", s.updatePrompt)
	mux.HandleFunc("DELETE /v1/prompts/{id}", s.deletePrompt)
	mux.HandleFunc("POST /v1/prompts/{id}/execute", s.executePrompt)
	mux.HandleFunc("POST /v1/jobs/{id}/story-draft", s.storyDraft)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /v1/jobs/{id}/force-pause", s.forcePauseJob)
	mux.HandleFunc("POST /v1/jobs/{id}/retry-correlation", s.retryCorrelation)
	mux.HandleFunc("POST /v1/jobs/{id}/chunks", s.appendChunk)
	mux.HandleFunc("GET /v1/jobs/{id}/logs", s.jobLogs)
	mux.HandleFunc("GET /v1/jobs/{id}/stream", s.jobStream)
	mux.HandleFunc("POST /v1/hosts/register", s.register)
	mux.HandleFunc("GET /v1/hosts", s.listHosts)
	mux.HandleFunc("GET /v1/hosts/{id}", s.hostDetail)
	mux.HandleFunc("POST /v1/hosts/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /v1/hosts/{id}/poll", s.pollHost)
	mux.HandleFunc("GET /v1/hosts/{host}/repos", s.repoDetail)
	mux.HandleFunc("DELETE /v1/hosts/{host}/repos", s.unregisterRepo)
	mux.HandleFunc("POST /v1/hosts/{host}/repo-requests", s.repoRequest)
	mux.HandleFunc("POST /v1/hosts/{host}/docs-hub/discard", s.discardHub)
	mux.HandleFunc("POST /v1/repos", s.upsertRepo)
	mux.HandleFunc("POST /v1/repos/discard", s.discardRepo)
	mux.HandleFunc("POST /v1/repos/intervention", s.intervention)
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
	}
	if err := s.Engine.UpsertRepo(repo); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
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
	writeJSON(w, http.StatusCreated, promptView(s.Engine.PutPrompt(control.Prompt{
		Title: raw.Title, Body: raw.Body, Engine: raw.Engine, Status: raw.Status,
		StoryID: raw.StoryID, SourceIdeaID: raw.SourceIdeaID,
	})))
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
	writeJSON(w, http.StatusOK, promptView(p))
}

func (s *Server) deletePrompt(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.DeletePrompt(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) executePrompt(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		HostID     string            `json:"host_id"`
		RepoPath   string            `json:"repo_path"`
		Iterations int               `json:"iterations"`
		Env        map[string]string `json:"env_vars"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	keys := make([]string, 0, len(raw.Env))
	for k := range raw.Env {
		keys = append(keys, k)
	}
	sch, err := s.Engine.ExecutePrompt(r.PathValue("id"), raw.HostID, raw.RepoPath, raw.Iterations, keys)
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
	writeJSON(w, http.StatusOK, map[string]string{
		"id": job.ID, "kind": job.Kind,
		"worktree_path": sch.WorktreePath, "docs_hub_path": docs,
	})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		FetchFailed bool `json:"fetch_failed"`
		Tools       []struct {
			Name    string `json:"name"`
			Path    string `json:"path"`
			Version string `json:"version"`
			Status  string `json:"status"`
		} `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	tools := make([]control.Tool, 0, len(raw.Tools))
	for _, t := range raw.Tools {
		tools = append(tools, control.Tool{Name: t.Name, Path: t.Path, Version: t.Version, Status: t.Status})
	}
	if err := s.Engine.Heartbeat(r.PathValue("id"), tools, raw.FetchFailed); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
	if err := s.Engine.UnregisterRepo(r.PathValue("host"), r.URL.Query().Get("path")); err != nil {
		code := http.StatusConflict
		if errors.Is(err, control.ErrNotFound) {
			code = http.StatusNotFound
		}
		writeErr(w, code, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	if err := s.Engine.RequestCheckout(control.RepoRequest{
		HostID: r.PathValue("host"), Mode: raw.Mode, Path: raw.Path, CloneURL: raw.CloneURL,
	}); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
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
	if err := s.Engine.ForcePauseJob(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
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
		Type   string `json:"type"`
		Stream string `json:"stream"`
		Text   string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	s.Engine.AppendLog(r.PathValue("id"), control.LogChunk{Type: raw.Type, Stream: raw.Stream, Text: raw.Text})
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "stored"})
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
	w.WriteHeader(http.StatusOK)
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
	for {
		select {
		case <-r.Context().Done():
			return
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

// DecodeLimit reads a small JSON body.
func DecodeLimit(r io.Reader, v any) error {
	dec := json.NewDecoder(io.LimitReader(r, 1<<20))
	return dec.Decode(v)
}

// NowUnix is a test helper clock.
func NowUnix() time.Time { return time.Unix(0, 0) }
