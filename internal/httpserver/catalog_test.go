package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestWebCatalogRoundTrip(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	h := s.Handler()

	post(t, h, "/v1/hosts/register", `{"id":"runner-1","kind":"permanent"}`, http.StatusOK)
	post(t, h, "/v1/hosts/runner-1/heartbeat", `{"fetch_failed":true,"tools":[{"name":"iazio-harness","path":"/bin/iazio-harness","version":"1.2.3+abc","status":"OK"}]}`, http.StatusOK)
	hostsBody := get(t, h, "/v1/hosts", http.StatusOK)
	if !strings.Contains(hostsBody, "iazio-harness") || !strings.Contains(hostsBody, "1.2.3+abc") {
		t.Fatalf("expected tools in /v1/hosts: %s", hostsBody)
	}
	post(t, h, "/v1/repos", `{"host_id":"runner-1","worktree_path":"/repos/app","docs_hub_path":"/repos/docs-hub","clone_url":"https://example.test/app.git","queue":"OPEN","lock":"IDLE"}`, http.StatusOK)

	rr := post(t, h, "/v1/ideas/import-gemini", `{"share_url":"https://gemini.google.com/share/abc"}`, http.StatusCreated)
	if !strings.Contains(rr.Body.String(), `"status":"NEW"`) || strings.Contains(rr.Body.String(), `"ID"`) {
		t.Fatalf("idea json: %s", rr.Body.String())
	}
	var idea map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &idea); err != nil {
		t.Fatal(err)
	}
	id := idea["id"].(string)
	post(t, h, "/v1/ideas/"+id+"/triage", `{}`, http.StatusOK)
	post(t, h, "/v1/ideas/"+id+"/refine", `{"host_id":"runner-1","docs_hub_path":"/repos/docs-hub"}`, http.StatusAccepted)
	post(t, h, "/v1/ideas/"+id+"/refine", `{}`, http.StatusConflict)

	body := get(t, h, "/v1/ideas", http.StatusOK)
	if !strings.Contains(body, `"status":"REFINING"`) {
		t.Fatalf("list ideas: %s", body)
	}

	rr = post(t, h, "/v1/prompts", `{"title":"ASE","body":"run {{.StoryID}}","engine":"agent","status":"READY"}`, http.StatusCreated)
	var prompt map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &prompt); err != nil {
		t.Fatal(err)
	}
	pid := prompt["id"].(string)
	rr = post(t, h, "/v1/prompts/"+pid+"/execute", `{"host_id":"runner-1","repo_path":"/repos/app","iterations":2,"env_vars":{"TOKEN":"s3cret-value"}}`, http.StatusCreated)
	if strings.Contains(rr.Body.String(), "s3cret-value") {
		t.Fatalf("env value leaked: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"TOKEN"`) {
		t.Fatalf("env key missing: %s", rr.Body.String())
	}

	hostBody := get(t, h, "/v1/hosts/runner-1", http.StatusOK)
	if !strings.Contains(hostBody, "iazio-harness") || !strings.Contains(hostBody, `"fetch_failed":true`) {
		t.Fatalf("host detail: %s", hostBody)
	}
	repoBody := get(t, h, "/v1/hosts/runner-1/repos?path=/repos/app", http.StatusOK)
	if !strings.Contains(repoBody, `"queue":"OPEN"`) {
		t.Fatalf("repo: %s", repoBody)
	}

	post(t, h, "/v1/repos/discard?host=runner-1&path=/repos/app", `{}`, http.StatusOK)
	req := httptest.NewRequest(http.MethodDelete, "/v1/hosts/runner-1/repos?path=/repos/app", nil)
	del := httptest.NewRecorder()
	h.ServeHTTP(del, req)
	if del.Code != http.StatusNoContent {
		t.Fatalf("unregister %d %s", del.Code, del.Body.String())
	}
}

func TestFinishedJobDoesNotOpenStream(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("h", "permanent")
	if err := s.Engine.UpsertRepo(control.Repo{HostID: "h", WorktreePath: "/w", CloneURL: "https://example.test/a.git"}); err != nil {
		t.Fatal(err)
	}
	job, err := s.Engine.LeaseOrdinary("h", "/w", "https://example.test/a.git")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Engine.PostExit(job.ID, 0); err != nil {
		t.Fatal(err)
	}
	s.Engine.AppendLog(job.ID, control.LogChunk{Type: "OUTPUT_CHUNK", Text: "hello"})
	post(t, s.Handler(), "/v1/jobs/"+job.ID+"/chunks", `{"type":"OUTPUT_CHUNK","stream":"stdout","text":"hello"}`, http.StatusAccepted)
	body := get(t, s.Handler(), "/v1/jobs/"+job.ID+"/logs", http.StatusOK)
	if !strings.Contains(body, "hello") {
		t.Fatalf("logs: %s", body)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/stream", nil)
	// Status is still RUNNING until finish. Mark finished.
	s.Engine.ApplyFinish("h", "/w", job.ID, control.FinishInput{ASEComplete: true})
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("stream code %d", rr.Code)
	}
}

func TestLiveStreamTail(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.AppendLog("job-live", control.LogChunk{Type: "OUTPUT_CHUNK", Text: "boot"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/jobs/job-live/stream?tail=100", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Fatal(err)
	}
	if resp != nil {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(b), "boot") {
			t.Fatalf("sse: %s", b)
		}
	}
}

func TestStreamCORSAndHeaders(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.AppendLog("job-stream-cors", control.LogChunk{Type: "OUTPUT_CHUNK", Text: "cors-check"})

	// 1. OPTIONS preflight
	reqOptions := httptest.NewRequest(http.MethodOptions, "/v1/jobs/job-stream-cors/stream?tail=100", nil)
	reqOptions.Header.Set("Origin", "https://example.com")
	reqOptions.Header.Set("Access-Control-Request-Method", "GET")
	reqOptions.Header.Set("Access-Control-Request-Headers", "authorization, accept")
	rrOptions := httptest.NewRecorder()
	s.Handler().ServeHTTP(rrOptions, reqOptions)

	if rrOptions.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS code = %d, want 204", rrOptions.Code)
	}
	if got := rrOptions.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want https://example.com", got)
	}
	if got := rrOptions.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "GET") {
		t.Errorf("Access-Control-Allow-Methods = %q, want containing GET", got)
	}
	if got := rrOptions.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "Authorization") {
		t.Errorf("Access-Control-Allow-Headers = %q, want containing Authorization", got)
	}

	// 2. GET streaming headers
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	reqGet, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/jobs/job-stream-cors/stream?tail=100", nil)
	if err != nil {
		t.Fatal(err)
	}
	reqGet.Header.Set("Origin", "https://example.com")
	reqGet.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(reqGet)
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Fatal(err)
	}
	if resp != nil {
		defer resp.Body.Close()
		if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
			t.Errorf("Content-Type = %q, want text/event-stream", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://example.com" {
			t.Errorf("Access-Control-Allow-Origin = %q, want https://example.com", got)
		}
		if got := resp.Header.Get("Cache-Control"); !strings.Contains(got, "no-cache") {
			t.Errorf("Cache-Control = %q, want containing no-cache", got)
		}
		if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
			t.Errorf("X-Accel-Buffering = %q, want no", got)
		}
		b, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(b), ": ping\n\n") {
			t.Errorf("expected initial ping comment in body: %s", string(b))
		}
		if !strings.Contains(string(b), "cors-check") {
			t.Errorf("expected chunk log in body: %s", string(b))
		}
	}
}

func post(t *testing.T, h http.Handler, path, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != want {
		t.Fatalf("%s code %d body %s", path, rr.Code, rr.Body.String())
	}
	return rr
}

func put(t *testing.T, h http.Handler, path, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewBufferString(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != want {
		t.Fatalf("%s code %d body %s", path, rr.Code, rr.Body.String())
	}
	return rr
}

func get(t *testing.T, h http.Handler, path string, want int) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != want {
		t.Fatalf("%s code %d body %s", path, rr.Code, rr.Body.String())
	}
	return rr.Body.String()
}

func TestGetJobEndpoint(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	h := s.Handler()

	s.Engine.RegisterHost("runner-1", "permanent")
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID: "runner-1", WorktreePath: "/repos/app", DocsHubPath: "/repos/docs-hub",
		CloneURL: "https://example.test/app.git", Queue: "OPEN", Lock: "IDLE",
	})
	p := s.Engine.PutPrompt(control.Prompt{
		Title: "ASE", Body: "run {{.StoryID}}", Engine: "agent", Status: "READY", StoryID: "STORY-123",
	})
	_, err := s.Engine.ExecutePrompt(p.ID, "runner-1", "/repos/app", 1, map[string]string{"ENV_A": "VAL_A"})
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := s.Engine.PollHost("runner-1")
	if !ok {
		t.Fatal("expected polled job")
	}

	res := get(t, h, "/v1/jobs/"+job.ID, http.StatusOK)
	var detail map[string]any
	if err := json.Unmarshal([]byte(res), &detail); err != nil {
		t.Fatal(err)
	}
	if detail["id"] != job.ID {
		t.Fatalf("unexpected id: %v", detail["id"])
	}
	if detail["engine"] != "agent" {
		t.Fatalf("unexpected engine: %v", detail["engine"])
	}
	if detail["prompt"] != "run {{.StoryID}}" {
		t.Fatalf("unexpected prompt: %v", detail["prompt"])
	}
	if detail["story_id"] != "STORY-123" {
		t.Fatalf("unexpected story_id: %v", detail["story_id"])
	}
	if detail["worktree_path"] != "/repos/app" {
		t.Fatalf("unexpected worktree_path: %v", detail["worktree_path"])
	}
	if detail["docs_hub_path"] != "/repos/docs-hub" {
		t.Fatalf("unexpected docs_hub_path: %v", detail["docs_hub_path"])
	}
	envVars, ok := detail["env_vars"].(map[string]any)
	if !ok || envVars["ENV_A"] != "VAL_A" {
		t.Fatalf("unexpected env_vars: %v", detail["env_vars"])
	}

	// Missing job returns 404
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/non-existent", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing job, got %d", rr.Code)
	}
}

func TestRepoRequestsAndUpsert(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("host-test", "permanent")
	h := s.Handler()

	// 1. Post repo-request with existing-checkout
	post(t, h, "/v1/hosts/host-test/repo-requests", `{"mode":"existing-checkout","path":"/repos/existing-1"}`, http.StatusAccepted)

	// Verify hostDetail now returns this repo
	hostRes := get(t, h, "/v1/hosts/host-test", http.StatusOK)
	var hostDetail map[string]any
	if err := json.Unmarshal([]byte(hostRes), &hostDetail); err != nil {
		t.Fatal(err)
	}
	repos, ok := hostDetail["repos"].([]any)
	if !ok || len(repos) != 1 {
		t.Fatalf("expected 1 repo in hostDetail, got: %v", hostDetail["repos"])
	}
	r0 := repos[0].(map[string]any)
	if r0["path"] != "/repos/existing-1" {
		t.Fatalf("unexpected repo path: %v", r0["path"])
	}

	// 2. Post /v1/repos without clone_url (existing local checkout)
	post(t, h, "/v1/repos", `{"host_id":"host-test","worktree_path":"/repos/existing-2"}`, http.StatusOK)
	hostRes = get(t, h, "/v1/hosts/host-test", http.StatusOK)
	if err := json.Unmarshal([]byte(hostRes), &hostDetail); err != nil {
		t.Fatal(err)
	}
	repos = hostDetail["repos"].([]any)
	if len(repos) != 2 {
		t.Fatalf("expected 2 repos in hostDetail, got: %v", repos)
	}
}

func TestPersistenceHooks(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	var savedPrompts []control.Prompt
	var deletedPromptID string
	var savedRepos []control.Repo
	var deletedRepoHost, deletedRepoPath string
	var savedHosts []control.Host

	s.SavePrompt = func(p control.Prompt) { savedPrompts = append(savedPrompts, p) }
	s.DeletePrompt = func(id string) { deletedPromptID = id }
	s.SaveRepo = func(r control.Repo) { savedRepos = append(savedRepos, r) }
	s.DeleteRepo = func(h, p string) { deletedRepoHost = h; deletedRepoPath = p }
	s.SaveHost = func(h control.Host) { savedHosts = append(savedHosts, h) }

	h := s.Handler()

	// 1. Host register & heartbeat trigger SaveHost
	post(t, h, "/v1/hosts/register", `{"id":"hook-host","kind":"permanent"}`, http.StatusOK)
	if len(savedHosts) != 1 || savedHosts[0].ID != "hook-host" {
		t.Fatalf("expected SaveHost called on register: %+v", savedHosts)
	}
	post(t, h, "/v1/hosts/hook-host/heartbeat", `{"tools":[{"name":"autopilot","path":"/bin/autopilot","status":"OK"}]}`, http.StatusOK)
	if len(savedHosts) != 2 {
		t.Fatalf("expected SaveHost called on heartbeat: %+v", savedHosts)
	}

	// 2. Prompt create, update, delete trigger hooks
	res := post(t, h, "/v1/prompts", `{"title":"P1","body":"do task","engine":"agent","status":"READY"}`, http.StatusCreated)
	if len(savedPrompts) != 1 || savedPrompts[0].Title != "P1" {
		t.Fatalf("expected SavePrompt called on createPrompt: %+v", savedPrompts)
	}
	var createdPrompt map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &createdPrompt); err != nil {
		t.Fatal(err)
	}
	pID := createdPrompt["id"].(string)

	put(t, h, "/v1/prompts/"+pID, `{"title":"P1-updated","body":"do new task","engine":"agent","status":"READY"}`, http.StatusOK)
	if len(savedPrompts) != 2 || savedPrompts[1].Title != "P1-updated" {
		t.Fatalf("expected SavePrompt called on updatePrompt: %+v", savedPrompts)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/prompts/"+pID, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on deletePrompt, got %d", rr.Code)
	}
	if deletedPromptID != pID {
		t.Fatalf("expected DeletePrompt called with %s, got %s", pID, deletedPromptID)
	}

	// 3. Repo upsert & unregister trigger hooks
	post(t, h, "/v1/repos", `{"host_id":"hook-host","worktree_path":"/work/test-1","clone_url":"local:///work/test-1"}`, http.StatusOK)
	if len(savedRepos) != 1 || savedRepos[0].WorktreePath != "/work/test-1" {
		t.Fatalf("expected SaveRepo called on upsertRepo: %+v", savedRepos)
	}

	post(t, h, "/v1/repos/hook-host/preflight", `{"worktree_path":"/work/test-1","preflight":{"FreeBytes":100000000,"GitWorkTree":true,"HeadAttached":true,"Branch":"main","DefaultBranch":"main"}}`, http.StatusOK)
	if len(savedRepos) != 2 || savedRepos[1].WorktreePath != "/work/test-1" {
		t.Fatalf("expected SaveRepo called on preflight: %+v", savedRepos)
	}

	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts/hook-host/repos?path=/work/test-1", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on unregisterRepo, got %d", rr.Code)
	}
	if deletedRepoHost != "hook-host" || deletedRepoPath != "/work/test-1" {
		t.Fatalf("expected DeleteRepo called with hook-host /work/test-1, got %s %s", deletedRepoHost, deletedRepoPath)
	}
}

func TestDeclineJobEndpoint(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/test-1", CloneURL: "https://example.test/test.git"})
	p := e.PutPrompt(control.Prompt{Title: "Task", Body: "do work", Status: "READY"})
	_, err := e.ExecutePrompt(p.ID, "host-1", "/work/test-1", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("expected job leased")
	}

	var savedRepos []control.Repo
	s := &Server{
		Engine:   e,
		SaveRepo: func(r control.Repo) { savedRepos = append(savedRepos, r) },
	}
	h := s.Handler()

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/decline", strings.NewReader(`{"reason":"worktree_busy"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	jobDetail, ok := e.GetJob(job.ID)
	if !ok || jobDetail.Status != "DECLINED" {
		t.Fatalf("expected job DECLINED, got: %+v", jobDetail)
	}

	if len(savedRepos) == 0 {
		t.Fatalf("expected SaveRepo called on decline")
	}
	if savedRepos[len(savedRepos)-1].Lock != control.LockIdle {
		t.Fatalf("expected saved repo lock to be LockIdle, got %s", savedRepos[len(savedRepos)-1].Lock)
	}
}

func TestResumeRepoEndpoint(t *testing.T) {
	e := control.NewEngine(time.Now)
	repo := control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/app",
		Queue:        control.QueuePaused,
		Lock:         control.LockIdle,
		Reason:       control.ReasonDirty,
		Porcelain:    "M dirty.txt",
	}
	_ = e.UpsertRepo(repo)

	var savedRepos []control.Repo
	s := &Server{
		Engine: e,
		SaveRepo: func(r control.Repo) {
			savedRepos = append(savedRepos, r)
		},
	}
	h := s.Handler()

	req := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/resume", strings.NewReader(`{"worktree_path":"/work/app"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"queue":"OPEN"`) {
		t.Fatalf("expected queue OPEN in response, got %s", rr.Body.String())
	}
	detail, ok := e.GetRepoDetail("host-1", "/work/app")
	if !ok || detail.Repo.Queue != control.QueueOpen || detail.Repo.Reason != "" {
		t.Fatalf("expected repo queue to be OPEN and reason cleared, got: %+v", detail.Repo)
	}
	if len(savedRepos) == 0 || savedRepos[len(savedRepos)-1].Queue != control.QueueOpen {
		t.Fatalf("expected SaveRepo called with queue OPEN")
	}

	// 404 for non-existent repo
	req404 := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/resume", strings.NewReader(`{"worktree_path":"/work/nonexistent"}`))
	req404.Header.Set("Content-Type", "application/json")
	rr404 := httptest.NewRecorder()
	h.ServeHTTP(rr404, req404)
	if rr404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent repo, got %d", rr404.Code)
	}
}

func TestSchedulesAndHistoryEndpoints(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	h := s.Handler()

	post(t, h, "/v1/hosts/register", `{"id":"runner-1","kind":"permanent"}`, http.StatusOK)
	post(t, h, "/v1/repos", `{"host_id":"runner-1","worktree_path":"/repos/app","clone_url":"https://example.test/app.git","queue":"OPEN","lock":"IDLE"}`, http.StatusOK)

	// Create prompt
	rr := post(t, h, "/v1/prompts", `{"title":"Integration Task","body":"echo test","engine":"agent","status":"READY"}`, http.StatusCreated)
	var prompt map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &prompt); err != nil {
		t.Fatal(err)
	}
	pid := prompt["id"].(string)

	// Execute prompt to schedule 2 iterations
	rr = post(t, h, "/v1/prompts/"+pid+"/execute", `{"host_id":"runner-1","repo_path":"/repos/app","iterations":2}`, http.StatusCreated)
	var createdSch map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &createdSch); err != nil {
		t.Fatal(err)
	}
	schID := createdSch["id"].(string)

	// 1. GET /v1/schedules
	schedulesBody := get(t, h, "/v1/schedules?host_id=runner-1&status=QUEUED", http.StatusOK)
	if !strings.Contains(schedulesBody, schID) || !strings.Contains(schedulesBody, `"iterations_total":2`) {
		t.Fatalf("expected schedule %s in /v1/schedules: %s", schID, schedulesBody)
	}

	// 2. Poll host leases iteration 1
	pollResp := post(t, h, "/v1/hosts/runner-1/poll", `{"kind":"ordinary"}`, http.StatusOK)
	var pollJson map[string]any
	if err := json.Unmarshal(pollResp.Body.Bytes(), &pollJson); err != nil {
		t.Fatal(err)
	}
	jobID, ok := pollJson["id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("expected job assigned in poll response: %s", pollResp.Body.String())
	}

	// 3. GET /v1/history should show in-flight RUNNING row
	historyBody := get(t, h, "/v1/history?host_id=runner-1&status=RUNNING", http.StatusOK)
	if !strings.Contains(historyBody, jobID) || !strings.Contains(historyBody, `"status":"RUNNING"`) {
		t.Fatalf("expected running job %s in /v1/history: %s", jobID, historyBody)
	}

	// 4. Post log chunk
	post(t, h, "/v1/jobs/"+jobID+"/chunks", `{"stream":"stdout","text":"chunk 1 data\n"}`, http.StatusAccepted)

	// Verify chunk via /v1/jobs/{id}/logs
	logsBody := get(t, h, "/v1/jobs/"+jobID+"/logs", http.StatusOK)
	if !strings.Contains(logsBody, "chunk 1 data") {
		t.Fatalf("expected chunk 1 data in logs: %s", logsBody)
	}

	// 5. Post Exit (completion)
	post(t, h, "/v1/jobs/"+jobID+"/exit", `{"exit_code":0}`, http.StatusOK)

	// 6. GET /v1/history should now show RUN_FINISHED
	historyFinishedBody := get(t, h, "/v1/history?host_id=runner-1&status=RUN_FINISHED", http.StatusOK)
	if !strings.Contains(historyFinishedBody, jobID) || !strings.Contains(historyFinishedBody, `"status":"RUN_FINISHED"`) {
		t.Fatalf("expected finished job in /v1/history: %s", historyFinishedBody)
	}

	// 7. GET /v1/schedules should show iteration 2 queued
	schedulesRemainingBody := get(t, h, "/v1/schedules?host_id=runner-1", http.StatusOK)
	if !strings.Contains(schedulesRemainingBody, schID) || !strings.Contains(schedulesRemainingBody, `"iterations_remaining":1`) {
		t.Fatalf("expected iteration 2 remaining in /v1/schedules: %s", schedulesRemainingBody)
	}

	// 8. Test repo_path with trailing slash matches normalized worktreePath
	historyTrailingSlash := get(t, h, "/v1/history?repo_path=/repos/app/", http.StatusOK)
	if !strings.Contains(historyTrailingSlash, jobID) {
		t.Fatalf("expected trailing slash repo_path to match in /v1/history: %s", historyTrailingSlash)
	}
	schedulesTrailingSlash := get(t, h, "/v1/schedules?repo_path=/repos/app/", http.StatusOK)
	if !strings.Contains(schedulesTrailingSlash, schID) {
		t.Fatalf("expected trailing slash repo_path to match in /v1/schedules: %s", schedulesTrailingSlash)
	}
}


