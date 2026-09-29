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

