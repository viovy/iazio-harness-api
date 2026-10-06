package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestImportRejectsAppURLAndBusy(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	req := httptest.NewRequest(http.MethodPost, "/v1/ideas/import-gemini", bytes.NewBufferString(`{"share_url":"https://gemini.google.com/app/abc"}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("code %d", rr.Code)
	}
	if err := s.Engine.BeginExtract(); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/ideas/import-gemini", bytes.NewBufferString(`{"share_url":"https://gemini.google.com/share/abc"}`))
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("busy code %d body %s", rr.Code, rr.Body.String())
	}
	s.Engine.EndExtract()
}

func TestVersion(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	req := httptest.NewRequest(http.MethodGet, "/version", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatal(rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["version"] == "" {
		t.Fatal("empty version")
	}
}

func TestRoot(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" || body["service"] != "iazio-harness-api" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestSchedulePriorityEndpoints(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task", Body: "run", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{Engine: e}
	handler := s.Handler()

	// 1. increase-priority
	req := httptest.NewRequest(http.MethodPost, "/v1/schedules/"+sch.ID+"/increase-priority", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}
	var res map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if int(res["priority"].(float64)) != 1 {
		t.Fatalf("expected priority 1, got %v", res["priority"])
	}

	// 2. decrease-priority
	req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+sch.ID+"/decrease-priority", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if int(res["priority"].(float64)) != 0 {
		t.Fatalf("expected priority 0, got %v", res["priority"])
	}

	// 3. priority with delta
	req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+sch.ID+"/priority", bytes.NewBufferString(`{"delta": 5}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if int(res["priority"].(float64)) != 5 {
		t.Fatalf("expected priority 5, got %v", res["priority"])
	}

	// 4. priority with exact value
	req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+sch.ID+"/priority", bytes.NewBufferString(`{"priority": 42}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if int(res["priority"].(float64)) != 42 {
		t.Fatalf("expected priority 42, got %v", res["priority"])
	}
}

func TestCancelScheduleEndpoint(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task", Body: "run", Status: "READY"})
	sch1, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	sch2, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{Engine: e}
	handler := s.Handler()

	// 1. POST /v1/schedules/{id}/cancel
	req := httptest.NewRequest(http.MethodPost, "/v1/schedules/"+sch1.ID+"/cancel", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}
	var res map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if res["status"] != "CANCELLED" || res["id"] != sch1.ID {
		t.Fatalf("unexpected cancel response: %v", res)
	}

	// 2. DELETE /v1/schedules/{id}
	req = httptest.NewRequest(http.MethodDelete, "/v1/schedules/"+sch2.ID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got: %d", rr.Code)
	}

	// 3. Cancel not found schedule
	req = httptest.NewRequest(http.MethodPost, "/v1/schedules/non-existent/cancel", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got: %d", rr.Code)
	}
}

func TestSwaggerEndpoints(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	// 1. GET /swagger redirect
	req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/swagger/" {
		t.Fatalf("expected Location /swagger/, got %s", loc)
	}

	// 2. GET /swagger/ UI
	req = httptest.NewRequest(http.MethodGet, "/swagger/", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte("SwaggerUIBundle")) {
		t.Fatalf("expected SwaggerUIBundle in body")
	}

	// 3. GET /swagger/openapi.json
	req = httptest.NewRequest(http.MethodGet, "/swagger/openapi.json", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var spec map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatalf("invalid json spec: %v", err)
	}
	if spec["openapi"] != "3.0.3" {
		t.Fatalf("expected openapi 3.0.3, got %v", spec["openapi"])
	}
}

func TestRepoResumeAndHistory(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/repo",
		Queue:        control.QueuePaused,
		Lock:         control.LockIdle,
		Reason:       "HALTED_DIRTY",
		Porcelain:    " M file.go",
	})
	p := e.PutPrompt(control.Prompt{
		ID:     "prompt-1",
		Title:  "Test Prompt",
		Body:   "echo 1",
		Status: "READY",
	})
	s := &Server{Engine: e}
	handler := s.Handler()

	// 1. GET /v1/hosts/host-1/repos?path=/work/repo -> PAUSED
	req := httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/repo", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var detail map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &detail)
	if detail["queue"] != "PAUSED" || detail["reason"] != "HALTED_DIRTY" {
		t.Fatalf("expected PAUSED HALTED_DIRTY, got: %v", detail)
	}

	// 2. Resume repo: POST /v1/repos/host-1/resume
	resumeBody, _ := json.Marshal(map[string]string{"path": "/work/repo"})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/resume", bytes.NewReader(resumeBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resume failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// Verify repo queue is now OPEN
	repo, ok := e.GetRepo("host-1", "/work/repo")
	if !ok || repo.Queue != control.QueueOpen || repo.Reason != "" || repo.Porcelain != "" {
		t.Fatalf("repo not resumed properly: %+v", repo)
	}

	// 3. Schedule prompt execution
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/repo", 1, nil)
	if err != nil {
		t.Fatalf("execute prompt: %v", err)
	}

	// 4. PollHost to lease schedule
	job, leasedSch, _, leased := e.PollHost("host-1")
	if !leased || job.ID == "" || leasedSch.ID != sch.ID {
		t.Fatalf("poll host failed: leased=%v job=%+v", leased, job)
	}

	// Check repo detail view includes running job and history
	req = httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/repo", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	_ = json.Unmarshal(rr.Body.Bytes(), &detail)
	running, ok := detail["running_job"].(map[string]any)
	if !ok || running["id"] != job.ID {
		t.Fatalf("expected running job %s, got: %v", job.ID, detail["running_job"])
	}
	history, ok := detail["history"].([]any)
	if !ok || len(history) != 1 {
		t.Fatalf("expected 1 history item, got: %v", detail["history"])
	}

	// 5. Finish job
	// 5. Post job exit
	exitBody, _ := json.Marshal(map[string]any{"exit_code": 0})
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/exit", bytes.NewReader(exitBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("exit failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// 6. Finish job
	finishBody, _ := json.Marshal(map[string]any{
		"worktree_path": "/work/repo",
		"job_id":        job.ID,
		"finish": map[string]any{
			"Kind":          "regular",
			"ASEComplete":   true,
			"WorkPorcelain": "",
			"HubPorcelain":  "",
			"HubAhead":      0,
		},
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/finish", bytes.NewReader(finishBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("finish failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// Verify repo detail history is updated
	req = httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/repo", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	_ = json.Unmarshal(rr.Body.Bytes(), &detail)
	history = detail["history"].([]any)
	firstHist := history[0].(map[string]any)
	if firstHist["status"] != "RUN_FINISHED" || firstHist["clean"] != true || firstHist["ase_complete"] != true {
		t.Fatalf("unexpected finished history: %v", firstHist)
	}
}

func TestJobConversationEndpoints(t *testing.T) {
	eng := control.NewEngine(nil)
	eng.RegisterHost("host-1", "permanent")
	_ = eng.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/repo",
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})
	p := eng.PutPrompt(control.Prompt{
		ID:     "prompt-1",
		Title:  "Test Prompt",
		Body:   "do work",
		Engine: "agy",
		Status: "READY",
	})

	s := &Server{Engine: eng}
	handler := s.Handler()

	// 1. Execute with resume_conversation_id
	execBody, _ := json.Marshal(map[string]any{
		"host_id":                "host-1",
		"repo_path":              "/work/repo",
		"iterations":             1,
		"resume_conversation_id": "initial-conv-123",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/prompts/"+p.ID+"/execute", bytes.NewReader(execBody))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("execute failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// 2. Poll host to lease job
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/host-1/poll", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("poll failed: code %d body %s", rr.Code, rr.Body.String())
	}
	var pollResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &pollResp); err != nil {
		t.Fatal(err)
	}
	jobID := pollResp.ID
	if jobID == "" {
		t.Fatal("empty job id from poll")
	}

	// 3. Get job details and verify resume_conversation_id
	req = httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get job failed: code %d", rr.Code)
	}
	var jobDet map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &jobDet); err != nil {
		t.Fatal(err)
	}
	if jobDet["resume_conversation_id"] != "initial-conv-123" {
		t.Fatalf("expected resume_conversation_id 'initial-conv-123', got %v", jobDet["resume_conversation_id"])
	}

	// 4. Post an early/streamed conversation ID
	convBody, _ := json.Marshal(map[string]string{
		"conversation_id": "streamed-conv-456",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/"+jobID+"/conversations", bytes.NewReader(convBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("post conversation failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// 5. Verify conversation appears in GET /v1/jobs/{id}
	req = httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	_ = json.Unmarshal(rr.Body.Bytes(), &jobDet)
	cids := jobDet["conversation_ids"].([]any)
	foundInitial, foundStreamed := false, false
	for _, c := range cids {
		if c == "initial-conv-123" {
			foundInitial = true
		}
		if c == "streamed-conv-456" {
			foundStreamed = true
		}
	}
	if !foundInitial || !foundStreamed {
		t.Fatalf("expected both initial and streamed conversation IDs, got: %v", cids)
	}

	// 6. Verify conversation IDs appear in GET /v1/history
	req = httptest.NewRequest(http.MethodGet, "/v1/history?host_id=host-1", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get history failed: code %d", rr.Code)
	}
	var histResp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &histResp); err != nil {
		t.Fatal(err)
	}
	if len(histResp.History) == 0 {
		t.Fatal("empty history")
	}
	histCids := histResp.History[0]["conversation_ids"].([]any)
	if len(histCids) < 2 {
		t.Fatalf("expected at least 2 conversation IDs in history row, got %v", histCids)
	}
}



