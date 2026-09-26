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
	post(t, h, "/v1/repos", `{"HostID":"runner-1","WorktreePath":"/repos/app","DocsHubPath":"/repos/docs-hub","CloneURL":"https://example.test/app.git","Queue":"OPEN","Lock":"IDLE"}`, http.StatusOK)

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
