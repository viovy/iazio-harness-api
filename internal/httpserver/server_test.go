package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
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

func TestHealthzAndReadyz(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s expected 200, got %d", path, rr.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s unmarshal: %v", path, err)
		}
		if body["status"] != "ok" {
			t.Fatalf("%s expected status ok, got %v", path, body)
		}
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
	if running["conversation_ids"] == nil {
		t.Fatalf("expected running job to include conversation_ids, got: %v", running)
	}

	// Add conversation ID to running job
	convBody, _ := json.Marshal(map[string]string{"conversation_id": "conv-test-123"})
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/conversations", bytes.NewReader(convBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /conversations, got: %d", rr.Code)
	}

	// Verify repo detail running_job now includes the conversation ID
	req = httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/repo", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	_ = json.Unmarshal(rr.Body.Bytes(), &detail)
	running = detail["running_job"].(map[string]any)
	cids, _ := running["conversation_ids"].([]any)
	if len(cids) != 1 || cids[0] != "conv-test-123" {
		t.Fatalf("expected running_job conversation_ids to include conv-test-123, got: %v", running["conversation_ids"])
	}

	// Verify GET /api/v1/conversations/:id/rehydration-bundle
	req = httptest.NewRequest(http.MethodGet, "/api/v1/conversations/conv-test-123/rehydration-bundle", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/v1/conversations/conv-test-123/rehydration-bundle, got: %d", rr.Code)
	}
	var bundle map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &bundle); err != nil {
		t.Fatalf("failed to unmarshal rehydration bundle: %v", err)
	}
	if bundle["conversation_id"] != "conv-test-123" {
		t.Errorf("expected conversation_id conv-test-123, got: %v", bundle["conversation_id"])
	}
	if bundle["bootstrap_prompt"] == "" {
		t.Errorf("expected non-empty bootstrap_prompt in rehydration bundle")
	}

	// Also verify /v1/conversations/:id/rehydration-bundle
	req = httptest.NewRequest(http.MethodGet, "/v1/conversations/conv-test-123/rehydration-bundle", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/conversations/conv-test-123/rehydration-bundle, got: %d", rr.Code)
	}

	// Verify POST /v1/jobs/:id/readiness
	readinessPayload, _ := json.Marshal(map[string]any{
		"reap_result": map[string]any{
			"orphans_reaped": 2,
			"locks_released": 1,
		},
		"workspace_state_manifest": map[string]any{
			"clean_locks":           true,
			"preserved_files_count": 3,
			"modified_submodules":   []string{"iazio-agent"},
		},
		"rehydration_status": map[string]any{
			"hydrated":               true,
			"source_conversation_id": "conv-test-123",
			"turns_count":            15,
			"ase_phase":              "IMPLEMENTATION",
		},
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/readiness", bytes.NewReader(readinessPayload))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/jobs/%s/readiness, got: %d (%s)", job.ID, rr.Code, rr.Body.String())
	}

	// Verify GET /v1/jobs/:id includes populated workspace_state_manifest, reap_result, and rehydration_status
	req = httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/jobs/%s, got: %d", job.ID, rr.Code)
	}
	var jobView map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &jobView); err != nil {
		t.Fatalf("failed to unmarshal job view: %v", err)
	}
	if ws, ok := jobView["workspace_state_manifest"].(map[string]any); !ok || ws["clean_locks"] != true {
		t.Errorf("expected clean_locks=true in job view workspace_state_manifest, got: %v", jobView["workspace_state_manifest"])
	}
	if rrMap, ok := jobView["reap_result"].(map[string]any); !ok || rrMap["orphans_reaped"] != float64(2) {
		t.Errorf("expected orphans_reaped=2 in job view reap_result, got: %v", jobView["reap_result"])
	}
	if rhMap, ok := jobView["rehydration_status"].(map[string]any); !ok || rhMap["hydrated"] != true {
		t.Errorf("expected hydrated=true in job view rehydration_status, got: %v", jobView["rehydration_status"])
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

func TestJobStreamKeepaliveAndOutputTick(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task", Body: "run", Status: "READY"})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatal("poll host failed")
	}

	s := &Server{Engine: e}
	handler := s.Handler()

	initialExpiry, ok := e.GetJobLeaseExpiry(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	// 1. Verify stream ping on connect
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	streamReq := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/stream", nil).WithContext(streamCtx)
	streamRR := httptest.NewRecorder()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancelStream()
	}()
	handler.ServeHTTP(streamRR, streamReq)
	if streamRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", streamRR.Code)
	}
	body := streamRR.Body.String()
	if !strings.Contains(body, ": ping") {
		t.Fatalf("expected initial ping in stream body, got: %q", body)
	}

	// 2. Post OUTPUT_TICK with silent_for_ms
	tickReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/chunks", bytes.NewBufferString(`{"type":"OUTPUT_TICK","silent_for_ms":10000}`))
	tickReq.Header.Set("Content-Type", "application/json")
	tickRR := httptest.NewRecorder()
	handler.ServeHTTP(tickRR, tickReq)
	if tickRR.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", tickRR.Code)
	}

	newExpiry, _ := e.GetJobLeaseExpiry(job.ID)
	if !newExpiry.After(initialExpiry) {
		t.Fatalf("expected LeaseExpiry to be extended, initial: %v, new: %v", initialExpiry, newExpiry)
	}

	// 3. Decline job and verify reason in GET /v1/history
	declineReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/decline", bytes.NewBufferString(`{"reason":"preflight_rejected: HALTED_DIRTY"}`))
	declineReq.Header.Set("Content-Type", "application/json")
	declineRR := httptest.NewRecorder()
	handler.ServeHTTP(declineRR, declineReq)
	if declineRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", declineRR.Code)
	}

	histReq := httptest.NewRequest(http.MethodGet, "/v1/history?host_id=host-1", nil)
	histRR := httptest.NewRecorder()
	handler.ServeHTTP(histRR, histReq)
	if histRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", histRR.Code)
	}
	var histResp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.Unmarshal(histRR.Body.Bytes(), &histResp); err != nil {
		t.Fatal(err)
	}
	if len(histResp.History) == 0 {
		t.Fatal("expected history")
	}
	reason, _ := histResp.History[0]["reason"].(string)
	if reason != "preflight_rejected: HALTED_DIRTY" {
		t.Fatalf("expected reason 'preflight_rejected: HALTED_DIRTY', got: %q", reason)
	}
	_ = sch
}

func TestAppendChunk_TurnAutoContinued(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	sch, _ := e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	job, _, _, _ := e.PollHost("host-1")

	s := &Server{Engine: e}
	handler := s.Handler()

	payload := `{"type":"TURN_AUTO_CONTINUED","continuation_turn":2,"conversation_id":"conv-12345","unmet_gates":["clean_working_tree"],"active_child_tasks":"make run-review-scheme"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/chunks", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr.Code)
	}

	detail, ok := e.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job found")
	}
	if detail.ExecutionAttempt != 3 {
		t.Fatalf("expected ExecutionAttempt=3, got %d", detail.ExecutionAttempt)
	}
	if detail.AutoResumeCount != 2 {
		t.Fatalf("expected AutoResumeCount=2, got %d", detail.AutoResumeCount)
	}
	if detail.ContinuationTurn != 2 {
		t.Fatalf("expected ContinuationTurn=2, got %d", detail.ContinuationTurn)
	}
	if detail.ActiveChildTasks != "make run-review-scheme" {
		t.Fatalf("expected ActiveChildTasks='make run-review-scheme', got %q", detail.ActiveChildTasks)
	}
	if detail.ActiveConversationID != "conv-12345" {
		t.Fatalf("expected ActiveConversationID='conv-12345', got %q", detail.ActiveConversationID)
	}

	foundTurnNotif := false
	for _, n := range s.notifications {
		if n.Event == "job.turn.auto_continued" && n.JobID == job.ID {
			foundTurnNotif = true
			break
		}
	}
	if !foundTurnNotif {
		t.Fatal("expected notification job.turn.auto_continued to be recorded")
	}
	_ = sch
}

func TestPostExit_Notifications(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, _ = e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	job, _, _, _ := e.PollHost("host-1")

	s := &Server{Engine: e}
	handler := s.Handler()

	exitReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/exit", bytes.NewBufferString(`{"exit_code":0,"termination_reason_code":"CLEAN"}`))
	exitReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, exitReq)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	foundCompleted := false
	for _, n := range s.notifications {
		if n.Event == "job.ase.completed" && n.JobID == job.ID {
			foundCompleted = true
			break
		}
	}
	if !foundCompleted {
		t.Fatal("expected notification job.ase.completed to be recorded")
	}
}

func TestAppendChunk_FilterRootAgentIdle(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := e.PutPrompt(control.Prompt{Title: "Task 1", Body: "do task 1", Status: "READY"})
	_, _ = e.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	job, _, _, _ := e.PollHost("host-1")

	s := &Server{Engine: e}
	handler := s.Handler()

	// Chunk containing only the misleading timeout message should be filtered completely
	payload1 := `{"type":"OUTPUT_CHUNK","stream":"stderr","text":"root agent idle; waiting up to 1h0m0s for 1 background task(s)\n"}`
	req1 := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/chunks", bytes.NewBufferString(payload1))
	req1.Header.Set("Content-Type", "application/json")
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr1.Code)
	}

	// Chunk with mixed lines should have the idle line removed
	payload2 := `{"type":"OUTPUT_CHUNK","stream":"stdout","text":"starting task\nroot agent idle; waiting up to 1h0m0s for 1 background task(s)\ntask finished\n"}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/chunks", bytes.NewBufferString(payload2))
	req2.Header.Set("Content-Type", "application/json")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rr2.Code)
	}

	page := e.JobLogs(job.ID, 0, "", 0, 0)
	for _, c := range page.Chunks {
		if strings.Contains(c.Text, "root agent idle; waiting up to") {
			t.Fatalf("expected idle line to be filtered from chunk, got: %q", c.Text)
		}
	}
}


func TestAbandonResumeAndRemediateEndpoints(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	s.Engine.RegisterHost("host-1", "permanent")
	_ = s.Engine.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/app"})
	p := s.Engine.PutPrompt(control.Prompt{Title: "Task", Body: "run task", Status: "READY"})
	_, _ = s.Engine.ExecutePrompt(p.ID, "host-1", "/work/app", 1, nil)
	job, _, _, ok := s.Engine.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	// 1. Abandon endpoint
	abandonReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/abandon", bytes.NewBufferString(`{"reason":"process_died"}`))
	abandonRR := httptest.NewRecorder()
	handler.ServeHTTP(abandonRR, abandonReq)
	if abandonRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", abandonRR.Code, abandonRR.Body.String())
	}

	// 2. Resume endpoint
	resumeReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/resume", nil)
	resumeRR := httptest.NewRecorder()
	handler.ServeHTTP(resumeRR, resumeReq)
	if resumeRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", resumeRR.Code, resumeRR.Body.String())
	}
	var resSch map[string]any
	_ = json.Unmarshal(resumeRR.Body.Bytes(), &resSch)
	if resSch["kind"] != "resume" {
		t.Fatalf("expected kind 'resume', got %v", resSch["kind"])
	}

	// 2b. Resume endpoint with custom conversation ID body
	resumeWithBodyReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/resume", bytes.NewBufferString(`{"conversation_id":"custom-conv-from-body"}`))
	resumeWithBodyRR := httptest.NewRecorder()
	handler.ServeHTTP(resumeWithBodyRR, resumeWithBodyReq)
	if resumeWithBodyRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", resumeWithBodyRR.Code, resumeWithBodyRR.Body.String())
	}
	var resBodySch map[string]any
	_ = json.Unmarshal(resumeWithBodyRR.Body.Bytes(), &resBodySch)
	if resBodySch["resume_conversation_id"] != "custom-conv-from-body" {
		t.Fatalf("expected custom conversation id 'custom-conv-from-body', got %v", resBodySch["resume_conversation_id"])
	}

	// 3. Remediate endpoint
	remReq := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/remediate", bytes.NewBufferString(`{"worktree_path":"/work/app"}`))
	remRR := httptest.NewRecorder()
	handler.ServeHTTP(remRR, remReq)
	if remRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", remRR.Code, remRR.Body.String())
	}

	// 4. Heartbeat with active_jobs
	hbBody := `{"fetch_failed":false,"tools":[],"active_jobs":[{"job_id":"job-live","worktree_path":"/work/app","pid":1234}]}`
	hbReq := httptest.NewRequest(http.MethodPost, "/v1/hosts/host-1/heartbeat", bytes.NewBufferString(hbBody))
	hbRR := httptest.NewRecorder()
	handler.ServeHTTP(hbRR, hbReq)
	if hbRR.Code != http.StatusOK {
		t.Fatalf("expected 200 from heartbeat, got %d body %s", hbRR.Code, hbRR.Body.String())
	}
}

func TestResumeRepoInPlaceEndpoint(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	s.Engine.RegisterHost("mac-mini", "permanent")
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:                 "mac-mini",
		WorktreePath:           "/Users/alice/work/test-repo-04",
		Queue:                  control.QueuePaused,
		Reason:                 control.ReasonDirty,
		DirtyStoryID:           "STORY-SAMPLE-0098",
		DetectedConversationID: "conv-mac-mini-0098",
	})
	p := s.Engine.PutPrompt(control.Prompt{Title: "Ready ASE prompt", Body: "run {{.StoryID}}", Status: "READY"})

	// Call POST /v1/repos/{host}/resume-in-place
	payload := `{"worktree_path":"/Users/alice/work/test-repo-04","allow_dirty":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/repos/mac-mini/resume-in-place", bytes.NewBufferString(payload))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", rr.Code, rr.Body.String())
	}

	var res map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if res["kind"] != "resume" {
		t.Fatalf("expected kind 'resume', got %v", res["kind"])
	}
	if res["story_id"] != "STORY-SAMPLE-0098" {
		t.Fatalf("expected story_id 'STORY-SAMPLE-0098', got %v", res["story_id"])
	}
	if res["resume_conversation_id"] != "conv-mac-mini-0098" {
		t.Fatalf("expected resume_conversation_id 'conv-mac-mini-0098', got %v", res["resume_conversation_id"])
	}
	if res["prompt_id"] != p.ID {
		t.Fatalf("expected prompt_id %s, got %v", p.ID, res["prompt_id"])
	}

	detail, ok := s.Engine.GetRepoDetail("mac-mini", "/Users/alice/work/test-repo-04")
	if !ok || detail.Repo.Queue != control.QueueOpen || detail.Repo.Reason != "" {
		t.Fatalf("expected repo queue OPEN and reason cleared: %+v", detail.Repo)
	}
}

func TestPostExitDiagnosticsHTTP(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	h := s.Handler()

	s.Engine.RegisterHost("host-1", "permanent", "generic")
	p := s.Engine.PutPrompt(control.Prompt{
		Title:   "Story Exec",
		Body:    "body",
		Engine:  "agent",
		Status:  "READY",
		StoryID: "STORY-555",
	})
	if err := s.Engine.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/test-repo",
		CloneURL:     "https://github.com/org/repo.git",
		Queue:        control.QueueOpen,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Engine.ExecutePrompt(p.ID, "host-1", "/work/test-repo", 1, nil); err != nil {
		t.Fatal(err)
	}

	job, _, _, ok := s.Engine.PollHost("host-1")
	if !ok {
		t.Fatal("poll failed")
	}

	exitBody, _ := json.Marshal(map[string]any{
		"exit_code":       1,
		"reason":          "QUOTA_EXHAUSTED",
		"diagnostic_code": "QUOTA_EXHAUSTED",
		"error_message":   "ActionRequiredError: Increase limits for faster responses You're out of usage. Switch to Auto, or ask your admin to increase your limit to continue.",
		"model":           "auto",
		"engine":          "agent",
		"remediation":     "switch_to_agy",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/exit", bytes.NewReader(exitBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("exit failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// Verify repo detail endpoint includes diagnostic fields
	req = httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/test-repo", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get repo detail failed: code %d body %s", rr.Code, rr.Body.String())
	}
	var repoResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &repoResp); err != nil {
		t.Fatal(err)
	}
	if repoResp["reason"] != "QUOTA_EXHAUSTED" {
		t.Fatalf("expected repo reason QUOTA_EXHAUSTED, got %v", repoResp["reason"])
	}
	historyList, ok := repoResp["history"].([]any)
	if !ok || len(historyList) == 0 {
		t.Fatalf("expected non-empty history list: %v", repoResp["history"])
	}
	firstHistory := historyList[0].(map[string]any)
	if firstHistory["diagnostic_code"] != "QUOTA_EXHAUSTED" {
		t.Fatalf("expected diagnostic_code QUOTA_EXHAUSTED, got %v", firstHistory["diagnostic_code"])
	}
	if firstHistory["model"] != "auto" {
		t.Fatalf("expected model auto, got %v", firstHistory["model"])
	}
	if firstHistory["remediation"] != "switch_to_agy" {
		t.Fatalf("expected remediation switch_to_agy, got %v", firstHistory["remediation"])
	}
	if firstHistory["created_at"] == nil || firstHistory["created_at"] == "" {
		t.Fatalf("expected created_at timestamp in history, got %v", firstHistory["created_at"])
	}

	// Resume repo via HTTP endpoint -> verify auto-fallback to agy
	resumeBody, _ := json.Marshal(map[string]any{
		"path":        "/work/test-repo",
		"allow_dirty": true,
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/resume-in-place", bytes.NewReader(resumeBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("resume failed: code %d body %s", rr.Code, rr.Body.String())
	}
	var resumeResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resumeResp); err != nil {
		t.Fatal(err)
	}
	if resumeResp["engine"] != "agy" {
		t.Fatalf("expected engine 'agy' on resume, got %v", resumeResp["engine"])
	}

	// Delete one history entry via DELETE /v1/history/{id}
	histJobID := firstHistory["job_id"].(string)
	req = httptest.NewRequest(http.MethodDelete, "/v1/history/"+histJobID, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete history failed: code %d body %s", rr.Code, rr.Body.String())
	}

	// Clear history endpoint test
	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts/host-1/repos/history?path=/work/test-repo", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("clear history failed: code %d body %s", rr.Code, rr.Body.String())
	}
}

func TestServer_ExternalLeaseAndRelease(t *testing.T) {
	e := control.NewEngine(func() time.Time { return time.Now().UTC() })
	_ = e.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/work/test-repo", Queue: control.QueueOpen, Lock: control.LockIdle})

	s := &Server{Engine: e}
	h := s.Handler()

	// 1. Acquire external lease
	body, _ := json.Marshal(map[string]any{
		"worktree_path": "/work/test-repo",
		"holder_id":     "autopilot-cli-100",
		"caller_type":   "autopilot-cli",
		"story_id":      "TASK-42",
		"ttl_seconds":   600,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/lease", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on external lease, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["lock"] != "AUTOPILOT_LOCAL" || resp["holder_id"] != "autopilot-cli-100" {
		t.Fatalf("unexpected lease response: %+v", resp)
	}

	// 2. Conflicting external lease
	body2, _ := json.Marshal(map[string]any{
		"worktree_path": "/work/test-repo",
		"holder_id":     "autopilot-cli-200",
		"caller_type":   "autopilot-cli",
	})
	req2 := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/lease", bytes.NewReader(body2))
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusConflict {
		t.Fatalf("expected HTTP 409 Conflict, got %d: %s", rr2.Code, rr2.Body.String())
	}

	// 3. Inspect repo detail view reflects lock_holder and AUTOPILOT_LOCAL
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/test-repo", nil)
	rrGet := httptest.NewRecorder()
	h.ServeHTTP(rrGet, reqGet)
	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d: %s", rrGet.Code, rrGet.Body.String())
	}
	var getResp map[string]any
	_ = json.Unmarshal(rrGet.Body.Bytes(), &getResp)
	if getResp["lock"] != "AUTOPILOT_LOCAL" || getResp["lock_holder"] != "autopilot-cli-100" {
		t.Fatalf("expected lock AUTOPILOT_LOCAL with holder, got %+v", getResp)
	}

	// 4. Release external lease
	relBody, _ := json.Marshal(map[string]any{
		"worktree_path": "/work/test-repo",
		"holder_id":     "autopilot-cli-100",
	})
	reqRel := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/release", bytes.NewReader(relBody))
	rrRel := httptest.NewRecorder()
	h.ServeHTTP(rrRel, reqRel)
	if rrRel.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on release, got %d: %s", rrRel.Code, rrRel.Body.String())
	}

	// 5. Verify restored to IDLE
	reqGet2 := httptest.NewRequest(http.MethodGet, "/v1/hosts/host-1/repos?path=/work/test-repo", nil)
	rrGet2 := httptest.NewRecorder()
	h.ServeHTTP(rrGet2, reqGet2)
	var getResp2 map[string]any
	_ = json.Unmarshal(rrGet2.Body.Bytes(), &getResp2)
	if getResp2["lock"] != "IDLE" || getResp2["lock_holder"] != "" {
		t.Fatalf("expected lock IDLE after release, got %+v", getResp2)
	}
}

func TestRepoQueueAllocationEndpoint(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	s.Engine.RegisterHost("host-1", "permanent")
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/test-repo",
	})

	// 1. Initial state check: default should be true
	det, ok := s.Engine.GetRepoDetail("host-1", "/work/test-repo")
	if !ok || !det.Repo.GlobalQueueAllocationEnabled {
		t.Fatalf("expected default globalQueueAllocationEnabled true, got: %+v", det)
	}

	// 2. Toggle to false: POST /v1/repos/host-1/queue-allocation
	falseVal := false
	body, _ := json.Marshal(map[string]any{
		"worktree_path": "/work/test-repo",
		"enabled":       falseVal,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/queue-allocation", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", rr.Code, rr.Body.String())
	}

	det, ok = s.Engine.GetRepoDetail("host-1", "/work/test-repo")
	if !ok || det.Repo.GlobalQueueAllocationEnabled {
		t.Fatalf("expected globalQueueAllocationEnabled false after toggle, got: %+v", det)
	}

	// 3. Toggle back to true
	trueVal := true
	body, _ = json.Marshal(map[string]any{
		"worktree_path": "/work/test-repo",
		"enabled":       trueVal,
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/queue-allocation", bytes.NewReader(body))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", rr.Code, rr.Body.String())
	}

	det, ok = s.Engine.GetRepoDetail("host-1", "/work/test-repo")
	if !ok || !det.Repo.GlobalQueueAllocationEnabled {
		t.Fatalf("expected globalQueueAllocationEnabled true after toggle back, got: %+v", det)
	}
}

func TestServerMissingRoutes(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/test-repo",
		DocsHubPath:  "/docs/hub",
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})

	s := &Server{Engine: e}
	h := s.Handler()

	// 1. GET /v1/repos/{host}?path=...
	req := httptest.NewRequest(http.MethodGet, "/v1/repos/host-1?path=/work/test-repo", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET /v1/repos/host-1 expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Missing path param
	req = httptest.NewRequest(http.MethodGet, "/v1/repos/host-1", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("GET /v1/repos/host-1 without path expected 404, got %d", rr.Code)
	}

	// 2. POST /v1/jobs/{id}/story-draft (nonexistent job)
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/story-draft", bytes.NewReader([]byte("{}")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("POST story-draft expected 403, got %d", rr.Code)
	}

	// 3. POST /v1/jobs/{id}/cancel (nonexistent job)
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/cancel", bytes.NewReader([]byte("{}")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Errorf("POST cancel expected 409, got %d", rr.Code)
	}

	// 4. POST /v1/jobs/{id}/force-pause (nonexistent job)
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/force-pause", bytes.NewReader([]byte("{}")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("POST force-pause expected 404, got %d", rr.Code)
	}

	// 5. POST /v1/jobs/{id}/retry-correlation (nonexistent job)
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/retry-correlation", bytes.NewReader([]byte("{}")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("POST retry-correlation expected 404, got %d", rr.Code)
	}

	// 6. POST /v1/repos/{host}/force-pause
	pauseBody, _ := json.Marshal(map[string]string{"worktree_path": "/work/test-repo"})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/force-pause", bytes.NewReader(pauseBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST force-pause expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// 7. POST /v1/hosts/{host}/docs-hub/discard
	discardBody, _ := json.Marshal(map[string]string{"docs_hub_path": "/docs/hub"})
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/host-1/docs-hub/discard", bytes.NewReader(discardBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST docs-hub/discard expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// 8. POST /v1/repos/intervention (queue is not leasable -> 409)
	intervBody, _ := json.Marshal(map[string]any{
		"host_id":       "host-1",
		"worktree_path": "/work/test-repo",
		"prompt_title":  "Intervention Test",
		"prompt_body":   "fix something",
		"engine":        "agent",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/intervention", bytes.NewReader(intervBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Errorf("POST intervention expected 409, got %d: %s", rr.Code, rr.Body.String())
	}

	// 9. POST /v1/hosts/{host}/repo-requests
	repoReqBody, _ := json.Marshal(map[string]string{
		"path":      "/work/requested-repo",
		"clone_url": "https://git/requested",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/host-1/repo-requests", bytes.NewReader(repoReqBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Errorf("POST repo-requests expected 202 Accepted, got %d: %s", rr.Code, rr.Body.String())
	}

	// 10. PUT /v1/fleet/profiles/{name}
	profBody, _ := json.Marshal(map[string]any{
		"description": "updated profile",
		"is_default":  false,
	})
	req = httptest.NewRequest(http.MethodPut, "/v1/fleet/profiles/prof-1", bytes.NewReader(profBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("PUT profile expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// 11. DecodeLimit and NowUnix helpers
	var val map[string]string
	if err := DecodeLimit(strings.NewReader(`{"key":"value"}`), &val); err != nil || val["key"] != "value" {
		t.Errorf("DecodeLimit failed: %v", err)
	}
	if NowUnix().Unix() != 0 {
		t.Error("expected 0 from NowUnix")
	}
}

func TestServerMoreEndpoints(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("host-1", "permanent")
	_ = e.UpsertRepo(control.Repo{
		HostID:       "host-1",
		WorktreePath: "/work/test-repo",
		DocsHubPath:  "/docs/hub",
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})
	p := e.PutPrompt(control.Prompt{ID: "p-test", Title: "Test Prompt", Body: "run something", Status: "READY"})
	idea := e.PutIdea(control.Idea{ID: "i-test", Title: "Test Idea", ShareURL: "https://gemini.google.com/share/abc_DEF-1", Status: "NEW"})

	s := &Server{
		Engine:       e,
		SavePrompt:   func(_ control.Prompt) {},
		DeletePrompt: func(_ string) {},
		SaveRepo:     func(_ control.Repo) {},
		SaveIdea:     func(_ control.Idea) {},
		Extract: func(_ string) (string, string, error) {
			return "Imported Title", "Imported Transcript", nil
		},
	}
	h := s.Handler()

	// 1. PUT /v1/prompts/{id}
	putBody, _ := json.Marshal(map[string]string{"title": "Updated Title", "body": "updated body", "status": "READY"})
	req := httptest.NewRequest(http.MethodPut, "/v1/prompts/"+p.ID, bytes.NewReader(putBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("PUT /v1/prompts expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Bad JSON to PUT /v1/prompts
	req = httptest.NewRequest(http.MethodPut, "/v1/prompts/"+p.ID, bytes.NewReader([]byte("invalid")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("PUT /v1/prompts invalid JSON expected 400, got %d", rr.Code)
	}

	// 2. POST /v1/prompts/{id}/execute
	execBody, _ := json.Marshal(map[string]any{
		"host_id":   "host-1",
		"repo_path": "/work/test-repo",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/prompts/"+p.ID+"/execute", bytes.NewReader(execBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("POST execute prompt expected 201, got %d: %s", rr.Code, rr.Body.String())
	}

	// Get schedule ID created
	var schedView map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &schedView)
	schedID, _ := schedView["id"].(string)

	if schedID != "" {
		// 3. POST /v1/schedules/{id}/increase-priority and decrease-priority
		req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+schedID+"/increase-priority", nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("increase-priority expected 200, got %d", rr.Code)
		}

		req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+schedID+"/decrease-priority", nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("decrease-priority expected 200, got %d", rr.Code)
		}

		// 4. Cancel schedule: POST and DELETE
		req = httptest.NewRequest(http.MethodPost, "/v1/schedules/"+schedID+"/cancel", nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("cancel schedule expected 200, got %d", rr.Code)
		}

		req = httptest.NewRequest(http.MethodDelete, "/v1/schedules/"+schedID, nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK && rr.Code != http.StatusNotFound {
			t.Errorf("delete schedule expected 200/404, got %d", rr.Code)
		}
	}

	// 5. DELETE /v1/prompts/{id}
	req = httptest.NewRequest(http.MethodDelete, "/v1/prompts/"+p.ID, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("DELETE prompt expected 204, got %d", rr.Code)
	}

	// 6. POST /v1/ideas/{id}/triage
	triageBody, _ := json.Marshal(map[string]string{"action": "ARCHIVE"})
	req = httptest.NewRequest(http.MethodPost, "/v1/ideas/"+idea.ID+"/triage", bytes.NewReader(triageBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST triage expected 200, got %d", rr.Code)
	}

	// 7. POST /v1/repos/{host}/resume-in-place
	resBody, _ := json.Marshal(map[string]string{"worktree_path": "/work/test-repo"})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/resume-in-place", bytes.NewReader(resBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK && rr.Code != http.StatusConflict {
		t.Errorf("POST resume-in-place unexpected %d", rr.Code)
	}

	// 8. POST /v1/repos/{host}/remediate
	remBody, _ := json.Marshal(map[string]string{
		"worktree_path": "/work/test-repo",
		"story_id":      "STORY-1",
		"review_file":   "review.md",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/host-1/remediate", bytes.NewReader(remBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK && rr.Code != http.StatusConflict {
		t.Errorf("POST remediate unexpected %d", rr.Code)
	}

	// 9. POST /v1/jobs/{id}/abandon and resume
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/abandon", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("POST abandon expected 404, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/resume", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("POST resume expected 404, got %d", rr.Code)
	}

	// 10. POST /v1/jobs/{id}/conversations
	convBody, _ := json.Marshal(map[string]string{"conversation_id": "c-123"})
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-unknown/conversations", bytes.NewReader(convBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("POST conversations expected 404, got %d", rr.Code)
	}

	// 11. POST /v1/ideas/import-gemini
	importBody, _ := json.Marshal(map[string]string{"share_url": "https://gemini.google.com/share/abc_DEF-1"})
	req = httptest.NewRequest(http.MethodPost, "/v1/ideas/import-gemini", bytes.NewReader(importBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("POST import-gemini expected 201, got %d: %s", rr.Code, rr.Body.String())
	}

	// 12. POST /v1/repos/discard
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/discard?host=host-1&path=/work/test-repo", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST discard expected 200, got %d", rr.Code)
	}

	// 13. DELETE /v1/fleet/credentials/git/{id}
	req = httptest.NewRequest(http.MethodDelete, "/v1/fleet/credentials/git/cred-unknown", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("DELETE credential expected 404, got %d", rr.Code)
	}

	// 14. DELETE /v1/hosts/{host}/repos?path=/work/test-repo
	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts/host-1/repos?path=/work/test-repo", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("DELETE repo expected 204, got %d", rr.Code)
	}

	// 15. POST /v1/hosts/host-1/tools/update
	toolUpBody, _ := json.Marshal(map[string]any{"tools": []string{"git"}, "force": true})
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/host-1/tools/update", bytes.NewReader(toolUpBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST tools/update expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// 16. GET and POST git credentials
	regCredBody, _ := json.Marshal(map[string]string{
		"name":    "test-cred",
		"pattern": "github.com/*",
		"token":   "ghp_123",
		"type":    "pat",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/fleet/credentials/git", bytes.NewReader(regCredBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("POST git credentials expected 200, got %d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/credentials/git", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET git credentials expected 200, got %d", rr.Code)
	}
}

func TestServerExtendedHandlers(t *testing.T) {
	e := control.NewEngine(nil)
	savedRepos := make([]control.Repo, 0)
	savedHosts := make([]control.Host, 0)
	savedPrompts := make([]control.Prompt, 0)
	deletedPrompts := make([]string, 0)
	savedProfiles := make([]control.DistributionProfile, 0)
	deletedProfiles := make([]string, 0)

	s := &Server{
		Engine: e,
		SaveRepo: func(r control.Repo) {
			savedRepos = append(savedRepos, r)
		},
		SaveHost: func(h control.Host) {
			savedHosts = append(savedHosts, h)
		},
		SavePrompt: func(p control.Prompt) {
			savedPrompts = append(savedPrompts, p)
		},
		DeletePrompt: func(id string) {
			deletedPrompts = append(deletedPrompts, id)
		},
		SaveProfile: func(p control.DistributionProfile) {
			savedProfiles = append(savedProfiles, p)
		},
		DeleteProfile: func(name string) {
			deletedProfiles = append(deletedProfiles, name)
		},
		DeleteHistory: func(_ context.Context, _ string) error {
			return nil
		},
		ClearHistory: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	h := s.Handler()

	// Seed host and repo
	hostID := "host-ext-1"
	repoPath := "/work/ext-repo"
	e.RegisterHost(hostID, "linux")
	_ = e.UpsertRepo(control.Repo{HostID: hostID, WorktreePath: repoPath, GlobalQueueAllocationEnabled: true})

	// 1. Queue allocation
	// Invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader([]byte("{invalid")))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad json, got %d", rr.Code)
	}

	// Missing path
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader([]byte(`{}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing path, got %d", rr.Code)
	}

	// Missing enabled
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader([]byte(`{"path":"`+repoPath+`"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing enabled, got %d", rr.Code)
	}

	// Repo not found
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader([]byte(`{"path":"/not/found","enabled":true}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing repo, got %d", rr.Code)
	}

	// Valid with global_queue_allocation_enabled
	bodyGlobal, _ := json.Marshal(map[string]any{"path": repoPath, "global_queue_allocation_enabled": true})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader(bodyGlobal))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for global queue allocation, got %d", rr.Code)
	}

	// Valid with enabled
	bodyEnabled, _ := json.Marshal(map[string]any{"worktree_path": repoPath, "enabled": false})
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/queue-allocation", bytes.NewReader(bodyEnabled))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for enabled false, got %d", rr.Code)
	}

	// 2. Resume repo in place
	// Bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/resume-in-place", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad json, got %d", rr.Code)
	}

	// Not found
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/resume-in-place?path=/not/found", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for not found repo, got %d", rr.Code)
	}

	// Valid resume in place
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/resume-in-place", bytes.NewReader([]byte(`{"worktree_path":"`+repoPath+`","story_id":"STORY-1"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for resume in place, got %d: %s", rr.Code, rr.Body.String())
	}

	// 3. Remediate repo
	// Not found
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/remediate?path=/not/found", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for remediate not found, got %d", rr.Code)
	}

	// Valid remediate
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/"+hostID+"/remediate", bytes.NewReader([]byte(`{"path":"`+repoPath+`"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for remediate, got %d: %s", rr.Code, rr.Body.String())
	}

	// 4. Poll host
	// After resume in place, a job was scheduled!
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/poll", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for poll with job, got %d: %s", rr.Code, rr.Body.String())
	}
	var polledJob map[string]string
	_ = json.Unmarshal(rr.Body.Bytes(), &polledJob)
	activeJobID := polledJob["id"]

	// Next poll is 204
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/poll", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204 for empty poll, got %d", rr.Code)
	}

	// 5. Heartbeat
	// Bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/heartbeat", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for heartbeat bad json, got %d", rr.Code)
	}

	// Not found host
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/unknown-host/heartbeat", bytes.NewReader([]byte(`{"presence":"ONLINE"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown host heartbeat, got %d", rr.Code)
	}

	// Valid heartbeat with tools and active jobs
	hbData := map[string]any{
		"presence":     "ONLINE",
		"fetch_failed": false,
		"tools": []map[string]string{
			{"name": "git", "path": "/usr/bin/git", "version": "2.40", "status": "ok"},
		},
		"active_jobs": []map[string]any{
			{"job_id": activeJobID, "worktree_path": repoPath, "pid": 4567},
		},
	}
	hbBytes, _ := json.Marshal(hbData)
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/heartbeat", bytes.NewReader(hbBytes))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for valid heartbeat, got %d: %s", rr.Code, rr.Body.String())
	}

	// 6. Force pause job
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/unknown-job/force-pause", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown job force-pause, got %d", rr.Code)
	}

	if activeJobID != "" {
		req = httptest.NewRequest(http.MethodPost, "/v1/jobs/"+activeJobID+"/force-pause", nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("expected 200 for valid force-pause, got %d: %s", rr.Code, rr.Body.String())
		}
	}

	// 7. Clear repo history & delete history row
	// Clear history missing host/path
	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts//repos/history", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	// Query path
	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts/"+hostID+"/repos/history?path="+repoPath, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for clear history, got %d: %s", rr.Code, rr.Body.String())
	}

	// Clear history with body
	req = httptest.NewRequest(http.MethodDelete, "/v1/hosts/"+hostID+"/repos/history", bytes.NewReader([]byte(`{"worktree_path":"`+repoPath+`"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for clear history with body, got %d", rr.Code)
	}

	// Delete history row
	req = httptest.NewRequest(http.MethodDelete, "/v1/history/"+activeJobID, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for delete history row, got %d", rr.Code)
	}

	// 8. Repo request
	// Bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/repo-requests", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad repo request, got %d", rr.Code)
	}

	// Valid repo request
	reqBody, _ := json.Marshal(map[string]string{
		"mode":      "checkout",
		"path":      repoPath,
		"clone_url": "https://github.com/org/repo.git",
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/repo-requests", bytes.NewReader(reqBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Errorf("expected 202 for valid repo request, got %d: %s", rr.Code, rr.Body.String())
	}

	// 9. Discard hub
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/docs-hub/discard", bytes.NewReader([]byte(`{"docs_hub_path":"/nonexistent"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound && rr.Code != http.StatusOK {
		t.Errorf("expected 404 or 200 for discard hub, got %d", rr.Code)
	}

	// 10. Execute prompt
	p := e.PutPrompt(control.Prompt{Title: "Run Test", Body: "do testing", Status: "READY"})
	// Bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/prompts/"+p.ID+"/execute", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad json in execute prompt, got %d", rr.Code)
	}

	// Unknown prompt
	req = httptest.NewRequest(http.MethodPost, "/v1/prompts/unknown-p/execute", bytes.NewReader([]byte(`{"is_global":true}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown prompt execute, got %d", rr.Code)
	}

	// Execute global
	execGlobalBody, _ := json.Marshal(map[string]any{"is_global": true, "iterations": 1})
	req = httptest.NewRequest(http.MethodPost, "/v1/prompts/"+p.ID+"/execute", bytes.NewReader(execGlobalBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201 for global execute, got %d: %s", rr.Code, rr.Body.String())
	}

	// Execute on specific host
	execHostBody, _ := json.Marshal(map[string]any{"host_id": hostID, "repo_path": repoPath, "iterations": 1})
	req = httptest.NewRequest(http.MethodPost, "/v1/prompts/"+p.ID+"/execute", bytes.NewReader(execHostBody))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201 for host execute, got %d: %s", rr.Code, rr.Body.String())
	}

	// 11. Story draft on job
	// Bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/unknown-job/story-draft", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for story draft bad json, got %d", rr.Code)
	}

	// Forbidden / job not found
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/unknown-job/story-draft", bytes.NewReader([]byte(`{"story_id":"S1"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("expected 403 for unknown job story draft, got %d", rr.Code)
	}

	// 12. Post exit
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/unknown-job/exit", bytes.NewReader([]byte(`{"exit_code":0}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Errorf("expected 409 for unknown job exit, got %d", rr.Code)
	}

	// 13. Decline job
	// Unknown job
	req = httptest.NewRequest(http.MethodPost, "/v1/jobs/unknown-job/decline", bytes.NewReader([]byte(`{"reason":"busy"}`)))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown job decline, got %d", rr.Code)
	}

	// 14. Profiles: create, get, update, delete
	// Create bad JSON
	req = httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for profile bad json, got %d", rr.Code)
	}

	// Create valid profile
	prof := control.DistributionProfile{Name: "ext-profile", Description: "ext test"}
	profBytes, _ := json.Marshal(prof)
	req = httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles", bytes.NewReader(profBytes))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201 for create profile, got %d: %s", rr.Code, rr.Body.String())
	}

	// Get profile
	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles/ext-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for get profile, got %d", rr.Code)
	}

	// Get profile not found
	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles/not-found-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for get unknown profile, got %d", rr.Code)
	}

	// Update profile bad JSON
	req = httptest.NewRequest(http.MethodPut, "/v1/fleet/profiles/ext-profile", bytes.NewReader([]byte("{bad")))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for update profile bad json, got %d", rr.Code)
	}

	// Update profile valid
	prof.Description = "updated description"
	profBytes, _ = json.Marshal(prof)
	req = httptest.NewRequest(http.MethodPut, "/v1/fleet/profiles/ext-profile", bytes.NewReader(profBytes))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for update profile, got %d", rr.Code)
	}

	// 15. Fleet manifest on existing profile
	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/manifest?profile=ext-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for manifest ext-profile, got %d: %s", rr.Code, rr.Body.String())
	}

	// Not found profile
	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/manifest?profile=unknown-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for manifest unknown profile, got %d", rr.Code)
	}

	// Delete profile not found
	req = httptest.NewRequest(http.MethodDelete, "/v1/fleet/profiles/unknown-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for delete unknown profile, got %d", rr.Code)
	}

	// Delete profile valid
	req = httptest.NewRequest(http.MethodDelete, "/v1/fleet/profiles/ext-profile", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for delete profile, got %d", rr.Code)
	}

	// 16. Profile releases
	e.Releases.SetCachedRelease("viovy/autopilot-plugins-internal", "latest", control.ToolRelease{Name: "autopilot", Repository: "viovy/autopilot-plugins-internal", LatestVersion: "0.1.0", TagName: "v0.1.0"}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-mcp", "latest", control.ToolRelease{Name: "iazio-mcp", Repository: "viovy/iazio-mcp", LatestVersion: "0.0.42", TagName: "v0.0.42"}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-agent", "latest", control.ToolRelease{Name: "iazio-agent", Repository: "viovy/iazio-agent", LatestVersion: "0.1.0", TagName: "v0.1.0"}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-harness", "latest", control.ToolRelease{Name: "iazio-harness", Repository: "viovy/iazio-harness", LatestVersion: "0.1.0", TagName: "v0.1.0"}, 10*time.Minute)
	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles/generic/releases", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for generic releases, got %d: %s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles/unknown-prof/releases", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown profile releases, got %d", rr.Code)
	}

	_ = savedRepos
	_ = savedHosts
	_ = savedPrompts
	_ = deletedPrompts
	_ = savedProfiles
	_ = deletedProfiles
}

type testStoryValidator struct {
	stories map[string]bool
}

func (v *testStoryValidator) StoryExists(storyID, submodule string) bool {
	return v.stories[storyID]
}

func TestServer_DispatchAndStoryRoutesMounted(t *testing.T) {
	s := &Server{
		Engine: control.NewEngine(nil),
		StoryValidator: &testStoryValidator{
			stories: map[string]bool{"STORY-TEST": true},
		},
	}
	handler := s.Handler()

	// 1. Check GET /api/v1/queue -> 200 OK (not 404)
	reqQueue := httptest.NewRequest(http.MethodGet, "/api/v1/queue", nil)
	rrQueue := httptest.NewRecorder()
	handler.ServeHTTP(rrQueue, reqQueue)
	if rrQueue.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /api/v1/queue, got %d", rrQueue.Code)
	}

	// 2. Check POST /api/v1/prompts with invalid body -> 422/400 (proves route is mounted, not 404!)
	reqPrompt := httptest.NewRequest(http.MethodPost, "/api/v1/prompts", bytes.NewBufferString(`{}`))
	rrPrompt := httptest.NewRecorder()
	handler.ServeHTTP(rrPrompt, reqPrompt)
	if rrPrompt.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity for empty prompt body, got %d", rrPrompt.Code)
	}

	// 3. Check POST /api/v1/stories with invalid body -> 422/400 (proves route is mounted, not 404!)
	reqStory := httptest.NewRequest(http.MethodPost, "/api/v1/stories", bytes.NewBufferString(`{}`))
	rrStory := httptest.NewRecorder()
	handler.ServeHTTP(rrStory, reqStory)
	if rrStory.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity for empty story body, got %d", rrStory.Code)
	}

	// 4. Check POST /api/v1/stories/STORY-TEST/execute
	reqExec := httptest.NewRequest(http.MethodPost, "/api/v1/stories/STORY-TEST/execute", bytes.NewBufferString(`{"submodule":"."}`))
	rrExec := httptest.NewRecorder()
	handler.ServeHTTP(rrExec, reqExec)
	if rrExec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for story execution, got %d: %s", rrExec.Code, rrExec.Body.String())
	}
}

func TestServer_DispatchQueueUnifiedWithHarnessRoutes(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  q,
	}
	handler := s.Handler()

	// 1. Enqueue a pending dispatch job
	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		ID:          "job_bedddabcda059343",
		Type:        dispatch.JobTypePromptExecute,
		Repo:        ".",
		TargetAgent: "autopilot",
		Status:      dispatch.JobStatusPending,
		Payload:     []byte(`{"title":"Execute Story STORY-GOV-0001","story_id":"STORY-GOV-0001"}`),
		TraceID:     "10caa73be67f84f18b7c09142b73c767",
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 2. Query GET /v1/schedules -> must contain the dispatch job
	reqSched := httptest.NewRequest(http.MethodGet, "/v1/schedules", nil)
	rrSched := httptest.NewRecorder()
	handler.ServeHTTP(rrSched, reqSched)
	if rrSched.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/schedules, got %d", rrSched.Code)
	}
	var schedResp struct {
		Schedules []map[string]any `json:"schedules"`
	}
	if err := json.Unmarshal(rrSched.Body.Bytes(), &schedResp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	foundSched := false
	for _, sc := range schedResp.Schedules {
		if sc["id"] == job.ID {
			foundSched = true
			if sc["engine"] != "autopilot" {
				t.Errorf("expected engine 'autopilot', got %v", sc["engine"])
			}
			if sc["status"] != "QUEUED" {
				t.Errorf("expected status 'QUEUED', got %v", sc["status"])
			}
			if sc["trace_id"] != "10caa73be67f84f18b7c09142b73c767" {
				t.Errorf("expected trace_id, got %v", sc["trace_id"])
			}
		}
	}
	if !foundSched {
		t.Fatalf("job %s not found in /v1/schedules", job.ID)
	}

	// 3. Query GET /v1/jobs/job_bedddabcda059343 -> must return 200 OK with jobDetailView
	reqJob := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil)
	rrJob := httptest.NewRecorder()
	handler.ServeHTTP(rrJob, reqJob)
	if rrJob.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/jobs/%s, got %d: %s", job.ID, rrJob.Code, rrJob.Body.String())
	}
	var jobResp map[string]any
	if err := json.Unmarshal(rrJob.Body.Bytes(), &jobResp); err != nil {
		t.Fatalf("unmarshal job detail error: %v", err)
	}
	if jobResp["id"] != job.ID {
		t.Errorf("expected id %s, got %v", job.ID, jobResp["id"])
	}
	if jobResp["target_agent"] != "autopilot" {
		t.Errorf("expected target_agent autopilot, got %v", jobResp["target_agent"])
	}
	if jobResp["trace_id"] != "10caa73be67f84f18b7c09142b73c767" {
		t.Errorf("expected trace_id, got %v", jobResp["trace_id"])
	}

	// 4. Complete the job and verify it appears in /v1/history
	if err := q.Complete(context.Background(), job.ID, []byte(`{"outcome":"success"}`), ""); err != nil {
		t.Fatalf("complete error: %v", err)
	}

	reqHist := httptest.NewRequest(http.MethodGet, "/v1/history", nil)
	rrHist := httptest.NewRecorder()
	handler.ServeHTTP(rrHist, reqHist)
	if rrHist.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/history, got %d", rrHist.Code)
	}
	var histResp struct {
		History []map[string]any `json:"history"`
	}
	if err := json.Unmarshal(rrHist.Body.Bytes(), &histResp); err != nil {
		t.Fatalf("unmarshal history error: %v", err)
	}
	foundHist := false
	for _, h := range histResp.History {
		if h["job_id"] == job.ID {
			foundHist = true
			if h["status"] != "COMPLETED" {
				t.Errorf("expected status 'COMPLETED', got %v", h["status"])
			}
			if h["clean"] != true {
				t.Errorf("expected clean true, got %v", h["clean"])
			}
			if h["trace_id"] != "10caa73be67f84f18b7c09142b73c767" {
				t.Errorf("expected trace_id, got %v", h["trace_id"])
			}
		}
	}
	if !foundHist {
		t.Fatalf("job %s not found in /v1/history", job.ID)
	}
}

func TestServer_CancelDispatchScheduleBridge(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  q,
	}
	handler := s.Handler()

	job, err := q.Enqueue(context.Background(), &dispatch.Job{
		ID:     "job_cancel_bridge_123",
		Type:   dispatch.JobTypePromptExecute,
		Repo:   "repo-test",
		Status: dispatch.JobStatusPending,
	})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Cancel via /v1/schedules/{id}/cancel
	reqCancel := httptest.NewRequest(http.MethodPost, "/v1/schedules/"+job.ID+"/cancel", strings.NewReader("{}"))
	rrCancel := httptest.NewRecorder()
	handler.ServeHTTP(rrCancel, reqCancel)

	if rrCancel.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for cancel schedule bridge, got %d: %s", rrCancel.Code, rrCancel.Body.String())
	}

	// Verify job is marked canceled in dispatch queue
	j, ok := q.Get(job.ID)
	if !ok {
		t.Fatal("expected job to be in queue")
	}
	if j.Status != dispatch.JobStatusCanceled {
		t.Fatalf("expected status canceled, got %s", j.Status)
	}

	// Also verify POST /v1/jobs/{id}/cancel on a dispatch job
	job2, _ := q.Enqueue(context.Background(), &dispatch.Job{
		ID:     "job_cancel_job_route_456",
		Type:   dispatch.JobTypePromptExecute,
		Repo:   "repo-test-2",
		Status: dispatch.JobStatusPending,
	})
	reqCancel2 := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job2.ID+"/cancel", strings.NewReader("{}"))
	rrCancel2 := httptest.NewRecorder()
	handler.ServeHTTP(rrCancel2, reqCancel2)
	if rrCancel2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for cancel job route, got %d: %s", rrCancel2.Code, rrCancel2.Body.String())
	}
	j2, ok := q.Get(job2.ID)
	if !ok || j2.Status != dispatch.JobStatusCanceled {
		t.Fatalf("expected job2 canceled, got %+v", j2)
	}
}

func TestGetScheduleEndpointAndJobFallback(t *testing.T) {
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

	// 2. Query GET /v1/schedules/{id} -> must return 200 OK with schedule details
	reqSched := httptest.NewRequest(http.MethodGet, "/v1/schedules/"+sch.ID, nil)
	rrSched := httptest.NewRecorder()
	handler.ServeHTTP(rrSched, reqSched)
	if rrSched.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/schedules/%s, got %d: %s", sch.ID, rrSched.Code, rrSched.Body.String())
	}
	var schedMap map[string]any
	if err := json.Unmarshal(rrSched.Body.Bytes(), &schedMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if schedMap["id"] != sch.ID {
		t.Errorf("expected schedule ID %s, got %v", sch.ID, schedMap["id"])
	}
	if schedMap["host_id"] != "host-1" {
		t.Errorf("expected host_id 'host-1', got %v", schedMap["host_id"])
	}

	// 3. Query GET /v1/jobs/{id} using the schedule ID -> must gracefully return 200 OK without 404
	reqJob := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+sch.ID, nil)
	rrJob := httptest.NewRecorder()
	handler.ServeHTTP(rrJob, reqJob)
	if rrJob.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for fallback /v1/jobs/%s, got %d: %s", sch.ID, rrJob.Code, rrJob.Body.String())
	}
	var jobMap map[string]any
	if err := json.Unmarshal(rrJob.Body.Bytes(), &jobMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if jobMap["id"] != sch.ID {
		t.Errorf("expected job ID %s, got %v", sch.ID, jobMap["id"])
	}

	// 4. Query GET /v1/schedules/nonexistent -> must return 404 Not Found
	req404 := httptest.NewRequest(http.MethodGet, "/v1/schedules/sch-nonexistent", nil)
	rr404 := httptest.NewRecorder()
	handler.ServeHTTP(rr404, req404)
	if rr404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent schedule, got %d", rr404.Code)
	}
}

func TestGetScheduleEndpoint_HistoricalFallbackAndLineage(t *testing.T) {
	e := control.NewEngine(nil)
	e.RegisterHost("mac-mini", "permanent")
	_ = e.UpsertRepo(control.Repo{HostID: "mac-mini", WorktreePath: "/work/fleet-repo-03"})

	// Seed history with legacy runs (e.g. job-1311 and job-1313) where sch-1312 is not in e.schedules
	e.SetHistory("mac-mini\x00/work/fleet-repo-03", []control.HistoryRow{
		{
			JobID:           "job-1311",
			ScheduleID:      "sch-1310",
			PromptTitle:     "autopilot update-all",
			Engine:          "agy",
			Status:          "execution_timeout",
			ConversationIDs: []string{"aad902c8-bda6-44c7-8a68-c468a1562399"},
		},
		{
			JobID:           "job-1313",
			ScheduleID:      "sch-1312",
			PromptTitle:     "autopilot update-all",
			Engine:          "agy",
			Status:          "execution_timeout",
			ConversationIDs: []string{"1d446877-5bf9-44eb-9db5-e1c007a9fe31", "14de9a1f-5679-4f11-a08f-f32bc7e47663"},
		},
	})

	s := &Server{Engine: e}
	handler := s.Handler()

	// 1. GET /v1/schedules/sch-1312 must return 200 OK via historical fallback
	req := httptest.NewRequest(http.MethodGet, "/v1/schedules/sch-1312", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for historical schedule /v1/schedules/sch-1312, got %d: %s", rr.Code, rr.Body.String())
	}

	var schedMap map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &schedMap); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if schedMap["id"] != "sch-1312" {
		t.Errorf("expected schedule ID sch-1312, got %v", schedMap["id"])
	}
	if schedMap["host_id"] != "mac-mini" {
		t.Errorf("expected host_id mac-mini, got %v", schedMap["host_id"])
	}

	// Verify associated_jobs contains exactly job-1311 and job-1313 in chronological order (AC1)
	assocJobs, ok := schedMap["associated_jobs"].([]any)
	if !ok {
		t.Fatalf("expected associated_jobs array, got %T: %v", schedMap["associated_jobs"], schedMap["associated_jobs"])
	}
	if len(assocJobs) != 2 {
		t.Fatalf("expected exactly 2 associated jobs, got %d: %v", len(assocJobs), assocJobs)
	}
	if assocJobs[0] != "job-1311" || assocJobs[1] != "job-1313" {
		t.Errorf("expected associated_jobs [job-1311, job-1313], got: %v", assocJobs)
	}

	// 2. GET /v1/schedules must list historical schedules sch-1310 and sch-1312
	reqList := httptest.NewRequest(http.MethodGet, "/v1/schedules", nil)
	rrList := httptest.NewRecorder()
	handler.ServeHTTP(rrList, reqList)
	if rrList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/schedules, got %d: %s", rrList.Code, rrList.Body.String())
	}
	var listResp struct {
		Schedules []map[string]any `json:"schedules"`
	}
	if err := json.Unmarshal(rrList.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal list error: %v", err)
	}
	schedIDs := make(map[string]bool)
	for _, sc := range listResp.Schedules {
		if id, ok := sc["id"].(string); ok {
			schedIDs[id] = true
		}
	}
	if !schedIDs["sch-1312"] {
		t.Errorf("expected /v1/schedules to contain sch-1312, got: %+v", listResp.Schedules)
	}
	if !schedIDs["sch-1310"] {
		t.Errorf("expected /v1/schedules to contain sch-1310, got: %+v", listResp.Schedules)
	}
}

func TestGetProfileReleasesEndpoint_Graceful200OnToolErrors(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	// Register profile with a tool whose repository is unreachable / 404
	_, err := s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "mac-mini-profile",
		Tools: []control.ProfileTool{
			{Name: "autopilot", Repository: "viovy/autopilot", TargetVersion: "0.0.255"},
			{Name: "iazio-agent", Repository: "viovy/iazio-agent", TargetVersion: "0.0.63"},
		},
	})
	if err != nil {
		t.Fatalf("failed to put profile: %v", err)
	}

	// Mock releases service with an offline/404 server
	offlineServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer offlineServer.Close()

	relSvc := control.NewReleaseService()
	// inject client pointing to offline server
	relSvc.SetCachedRelease("viovy/autopilot", "0.0.255", control.ToolRelease{
		Name:          "autopilot",
		Repository:    "viovy/autopilot",
		LatestVersion: "0.0.255",
		TagName:       "0.0.255",
	}, 10*time.Minute)
	s.Engine.Releases = relSvc

	req := httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles/mac-mini-profile/releases", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/fleet/profiles/mac-mini-profile/releases even with tool errors, got %d: %s", rr.Code, rr.Body.String())
	}

	var res map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	releases, ok := res["releases"].([]any)
	if !ok || len(releases) != 2 {
		t.Fatalf("expected 2 releases in response, got %v", res["releases"])
	}
}

func TestObservabilityMetricsEndpoint(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("mac-mini", "permanent")
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: "/work/sample-repo",
	})

	// Add sample history entries
	s.Engine.SetHistory("mac-mini\x00/work/sample-repo", []control.HistoryRow{
		{
			JobID:   "job-101",
			Status:  "RUN_FINISHED",
			Engine:  "autopilot",
			Clean:   true,
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		},
		{
			JobID:          "job-102",
			Status:         "FAILED",
			Reason:         "DIRTY_WORKTREE_POST_EXIT",
			Engine:         "agy",
			DiagnosticCode: "DIRTY_WORKTREE_POST_EXIT",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/observability/metrics", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/observability/metrics, got %d: %s", rr.Code, rr.Body.String())
	}

	var res map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode json: %v", err)
	}

	if tot, ok := res["total_executions"].(float64); !ok || int(tot) != 2 {
		t.Errorf("expected total_executions=2, got %v", res["total_executions"])
	}

	statuses, ok := res["status_counts"].(map[string]any)
	if !ok || statuses["RUN_FINISHED"] != float64(1) || statuses["FAILED"] != float64(1) {
		t.Errorf("expected status_counts with RUN_FINISHED=1 and FAILED=1, got %v", res["status_counts"])
	}

	reasons, ok := res["failure_reasons"].(map[string]any)
	if !ok || reasons["DIRTY_WORKTREE_POST_EXIT"] != float64(1) {
		t.Errorf("expected failure_reasons with DIRTY_WORKTREE_POST_EXIT=1, got %v", res["failure_reasons"])
	}

	telem, ok := res["telemetry"].(map[string]any)
	if !ok || telem["service_name"] != "iazio-harness-api" {
		t.Errorf("expected telemetry block with service_name, got %v", res["telemetry"])
	}
}

func TestHostHeartbeat_ToolServiceMetadata(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	h := s.Handler()
	hostID := "debian-host"

	// Register host
	regBody := `{"id":"debian-host","kind":"permanent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/hosts/register", bytes.NewReader([]byte(regBody)))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("register failed: %d", rr.Code)
	}

	// Send heartbeat with tool service metadata & capabilities
	hbData := map[string]any{
		"presence":     "ONLINE",
		"fetch_failed": false,
		"tools": []map[string]any{
			{
				"name":              "iazio-agent",
				"path":              "/home/romeo/.iazio/bin/iazio-agent",
				"version":           "0.0.67",
				"status":            "OK",
				"service_installed": true,
				"service_type":      "user_service",
				"service_status":    "active",
				"capabilities":      []string{"watchdog", "auto_restart", "path_watcher"},
			},
		},
	}
	hbBytes, _ := json.Marshal(hbData)
	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/"+hostID+"/heartbeat", bytes.NewReader(hbBytes))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("heartbeat failed: %d", rr.Code)
	}

	// GET /v1/hosts/:id and verify tool service fields
	req = httptest.NewRequest(http.MethodGet, "/v1/hosts/"+hostID, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get host failed: %d", rr.Code)
	}

	var detail map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	tools, ok := detail["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("expected 1 tool, got %v", detail["tools"])
	}
	toolMap, ok := tools[0].(map[string]any)
	if !ok {
		t.Fatalf("expected tool map, got %T", tools[0])
	}
	if toolMap["name"] != "iazio-agent" {
		t.Errorf("expected tool name iazio-agent, got %v", toolMap["name"])
	}
	if toolMap["service_installed"] != true {
		t.Errorf("expected service_installed=true, got %v", toolMap["service_installed"])
	}
	if toolMap["service_type"] != "user_service" {
		t.Errorf("expected service_type=user_service, got %v", toolMap["service_type"])
	}
	if toolMap["service_status"] != "active" {
		t.Errorf("expected service_status=active, got %v", toolMap["service_status"])
	}
	caps, ok := toolMap["capabilities"].([]any)
	if !ok || len(caps) != 3 {
		t.Errorf("expected 3 capabilities, got %v", toolMap["capabilities"])
	}
}

func TestSyncAllProfilesEndpoint_AndCacheControl(t *testing.T) {
	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"mock-etag-492"`)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.492",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.492",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer mockGH.Close()

	s := &Server{Engine: control.NewEngine(nil)}
	handler := s.Handler()

	_, err := s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "generic",
		Tools: []control.ProfileTool{
			{Name: "autopilot-fleet", Repository: "viovy/autopilot-plugins-internal", TargetVersion: "0.0.482", TrackLatest: control.BoolPtr(true)},
		},
	})
	if err != nil {
		t.Fatalf("failed to upsert profile: %v", err)
	}

	relSvc := control.NewReleaseService()
	relSvc.SetBaseURL(mockGH.URL)
	s.Engine.SetReleaseService(relSvc)

	// 1. Check GET /v1/fleet/profiles has Cache-Control: no-store
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/fleet/profiles", nil)
	rrGet := httptest.NewRecorder()
	handler.ServeHTTP(rrGet, reqGet)
	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rrGet.Code)
	}
	if !strings.Contains(rrGet.Header().Get("Cache-Control"), "no-store") {
		t.Errorf("expected Cache-Control no-store, got %s", rrGet.Header().Get("Cache-Control"))
	}

	// 2. Call POST /v1/fleet/profiles/sync
	reqSync := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles/sync", nil)
	rrSync := httptest.NewRecorder()
	handler.ServeHTTP(rrSync, reqSync)
	if rrSync.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rrSync.Code, rrSync.Body.String())
	}
	if !strings.Contains(rrSync.Header().Get("Cache-Control"), "no-store") {
		t.Errorf("expected Cache-Control no-store on sync, got %s", rrSync.Header().Get("Cache-Control"))
	}

	var syncResp map[string]any
	if err := json.Unmarshal(rrSync.Body.Bytes(), &syncResp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if syncResp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", syncResp["status"])
	}

	// 3. Verify target version auto-advanced to 0.0.492
	prof, _ := s.Engine.GetProfile("generic")
	if prof.Tools[0].TargetVersion != "0.0.492" {
		t.Errorf("expected TargetVersion 0.0.492, got %s", prof.Tools[0].TargetVersion)
	}
	if prof.Tools[0].LatestAvailableVersion != "0.0.492" {
		t.Errorf("expected LatestAvailableVersion 0.0.492, got %s", prof.Tools[0].LatestAvailableVersion)
	}

	// 4. Verify notification was recorded
	notifs := s.GetNotifications(10)
	if len(notifs) == 0 {
		t.Errorf("expected release notification recorded")
	} else if notifs[0].Category != "release" {
		t.Errorf("expected category release, got %s", notifs[0].Category)
	}
}

func TestServer_RefreshPinnedProfileCannotDowngrade(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name":     "0.0.490",
			"html_url":     "https://github.com/viovy/autopilot-plugins-internal/releases/tag/0.0.490",
			"published_at": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer ts.Close()

	s := &Server{Engine: control.NewEngine(nil)}
	relSvc := control.NewReleaseService()
	relSvc.SetBaseURL(ts.URL)
	s.Engine.SetReleaseService(relSvc)

	// Seed profile with explicit pinned TargetVersion 0.0.492 (ahead of upstream 0.0.490) and TrackLatest false
	_, _ = s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "pinned-profile",
		Tools: []control.ProfileTool{
			{
				Name:          "autopilot-plugins-internal",
				Repository:    "viovy/autopilot-plugins-internal",
				TargetVersion: "0.0.492",
				TrackLatest:   control.BoolPtr(false),
			},
		},
	})

	handler := s.Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles/pinned-profile/refresh", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	prof, _ := s.Engine.GetProfile("pinned-profile")
	// TargetVersion must NOT be downgraded to 0.0.490
	if prof.Tools[0].TargetVersion != "0.0.492" {
		t.Fatalf("expected pinned target 0.0.492 preserved, but got %s", prof.Tools[0].TargetVersion)
	}
	// LatestAvailableVersion reflects upstream
	if prof.Tools[0].LatestAvailableVersion != "0.0.490" {
		t.Fatalf("expected latest available version 0.0.490, got %s", prof.Tools[0].LatestAvailableVersion)
	}
}

func TestServer_RefreshProfileReleases_MixedToolOutcomesPartialStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "repo-good/releases/latest"):
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"tag_name": "v2.0.0", "html_url": "https://github.com/viovy/repo-good/releases/tag/v2.0.0"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.Releases.SetBaseURL(ts.URL)
	s.Engine.Releases.SetHTTPClient(ts.Client())

	_, err := s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "mixed-profile",
		Tools: []control.ProfileTool{
			{
				Name:          "tool-good",
				Repository:    "viovy/repo-good",
				TargetVersion: "1.0.0",
				TrackLatest:   control.BoolPtr(true),
			},
			{
				Name:          "tool-bad",
				Repository:    "viovy/repo-bad",
				TargetVersion: "1.0.0",
				TrackLatest:   control.BoolPtr(true),
			},
		},
	})
	if err != nil {
		t.Fatalf("create profile failed: %v", err)
	}

	handler := s.Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles/mixed-profile/refresh", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// F2: Must return 200 OK (NOT 502) on partial failure
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for partial refresh, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status       string                      `json:"status"`
		ToolsUpdated int                         `json:"tools_updated"`
		Error        string                      `json:"error"`
		Profile      control.DistributionProfile `json:"profile"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "partial" {
		t.Fatalf("expected status 'partial', got '%s'", resp.Status)
	}
	if resp.ToolsUpdated != 1 {
		t.Fatalf("expected tools_updated=1, got %d", resp.ToolsUpdated)
	}
	if resp.Error == "" {
		t.Fatalf("expected non-empty error field describing failed tool")
	}

	// Verify good tool was advanced and bad tool remained at target
	prof, _ := s.Engine.GetProfile("mixed-profile")
	if prof.Tools[0].TargetVersion != "2.0.0" {
		t.Fatalf("expected tool-good target to advance to 2.0.0, got %s", prof.Tools[0].TargetVersion)
	}
	if prof.Tools[1].TargetVersion != "1.0.0" {
		t.Fatalf("expected tool-bad target to remain 1.0.0, got %s", prof.Tools[1].TargetVersion)
	}
}

func TestRefreshProfileReleases_TotalFailureReturns502(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	mockRelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockRelServer.Close()

	relSvc := control.NewReleaseService()
	relSvc.SetBaseURL(mockRelServer.URL)
	relSvc.SetHTTPClient(mockRelServer.Client())
	s.Engine.Releases = relSvc

	_, err := s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "fail-profile",
		Tools: []control.ProfileTool{
			{
				Name:          "tool-fail",
				Repository:    "viovy/repo-fail",
				TargetVersion: "1.0.0",
				TrackLatest:   control.BoolPtr(true),
			},
		},
	})
	if err != nil {
		t.Fatalf("create profile failed: %v", err)
	}

	handler := s.Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles/fail-profile/refresh", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// E8: Total failure must return 502 Bad Gateway
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway for total failure, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "error" {
		t.Fatalf("expected status 'error', got %v", resp["status"])
	}
}

func TestProfileHandlers_SinglePersistencePathNoRedundantWrite(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	var savedState control.DistributionProfile
	var saveCount int
	var mu sync.Mutex

	s.Engine.OnSaveProfile = func(p control.DistributionProfile) error {
		mu.Lock()
		savedState = p
		saveCount++
		mu.Unlock()
		return nil
	}

	handler := s.Handler()

	// 1. Create profile via POST /v1/fleet/profiles
	createPayload := `{"name":"test-single-persist","description":"initial","tools":[{"name":"tool1","repository":"viovy/tool1","target_version":"1.0.0"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles", strings.NewReader(createPayload))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rr.Code, rr.Body.String())
	}

	mu.Lock()
	if saveCount != 1 {
		t.Fatalf("expected exactly 1 save on create, got %d (E1)", saveCount)
	}
	if savedState.Description != "initial" {
		t.Fatalf("expected saved description 'initial', got %q", savedState.Description)
	}
	mu.Unlock()

	// 2. Update profile via PUT /v1/fleet/profiles/test-single-persist
	updatePayload := `{"description":"updated-desc","tools":[{"name":"tool1","repository":"viovy/tool1","target_version":"2.0.0"}]}`
	req = httptest.NewRequest(http.MethodPut, "/v1/fleet/profiles/test-single-persist", strings.NewReader(updatePayload))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on update, got %d: %s", rr.Code, rr.Body.String())
	}

	mu.Lock()
	if saveCount != 2 {
		t.Fatalf("expected exactly 2 total saves after update, got %d (E1 redundant write detected)", saveCount)
	}
	if savedState.Description != "updated-desc" {
		t.Fatalf("expected saved description 'updated-desc', got %q", savedState.Description)
	}
	mu.Unlock()

	inMem, err := s.Engine.GetProfile("test-single-persist")
	if err != nil {
		t.Fatalf("failed to get profile from engine: %v", err)
	}
	mu.Lock()
	if inMem.Description != savedState.Description || len(inMem.Tools) != len(savedState.Tools) || inMem.Tools[0].TargetVersion != savedState.Tools[0].TargetVersion {
		t.Fatalf("in-memory state does not match persisted state: inMem=%+v, saved=%+v", inMem, savedState)
	}
	mu.Unlock()
}

func TestSyncAllProfiles_TotalFailureReturns502(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	mockRelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockRelServer.Close()

	relSvc := control.NewReleaseService()
	relSvc.SetBaseURL(mockRelServer.URL)
	relSvc.SetHTTPClient(mockRelServer.Client())
	s.Engine.Releases = relSvc

	_, err := s.Engine.UpsertProfile(control.DistributionProfile{
		Name: "fail-all-profile",
		Tools: []control.ProfileTool{
			{
				Name:          "tool-fail",
				Repository:    "viovy/repo-fail",
				TargetVersion: "1.0.0",
				TrackLatest:   control.BoolPtr(true),
			},
		},
	})
	if err != nil {
		t.Fatalf("create profile failed: %v", err)
	}

	handler := s.Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/profiles/sync", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// L1, N3: Total failure across profiles must return 502 Bad Gateway
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway for total sync-all failure, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "error" {
		t.Fatalf("expected status 'error', got %v", resp["status"])
	}
}

func TestServer_DiscardRepo_ForceAndDiscardAll(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("mac-mini", "macos")

	// Setup repos
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: "/work/repo-a",
		RepoAlias:    "repo-a",
	})
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: "/work/repo-b",
		RepoAlias:    "repo-b",
	})
	p1 := s.Engine.PutPrompt(control.Prompt{Title: "Task 1", Body: "run task 1", Status: "READY"})
	_, _ = s.Engine.ExecutePrompt(p1.ID, "mac-mini", "/work/repo-b", 1, nil)
	_, _, _, _ = s.Engine.PollHost("mac-mini")

	handler := s.Handler()

	// 1. POST /v1/repos/discard on busy repo without force -> 409
	req := httptest.NewRequest(http.MethodPost, "/v1/repos/discard?host=mac-mini&path=/work/repo-b", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d: %s", rr.Code, rr.Body.String())
	}

	// 2. POST /v1/repos/discard on busy repo with force=true -> 200
	req = httptest.NewRequest(http.MethodPost, "/v1/repos/discard?host=mac-mini&path=/work/repo-b&force=true", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	// 3. POST /v1/hosts/mac-mini/repos/discard-all
	// Re-dirty repoB with another executing prompt
	p2 := s.Engine.PutPrompt(control.Prompt{Title: "Task 2", Body: "run task 2", Status: "READY"})
	_, _ = s.Engine.ExecutePrompt(p2.ID, "mac-mini", "/work/repo-b", 1, nil)
	_, _, _, _ = s.Engine.PollHost("mac-mini")

	req = httptest.NewRequest(http.MethodPost, "/v1/hosts/mac-mini/repos/discard-all?force=true", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}
	var discardAllResp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &discardAllResp); err != nil {
		t.Fatalf("failed to parse discard-all response: %v", err)
	}
	if discardAllResp["status"] != "DISCARD_REQUESTED" {
		t.Errorf("expected DISCARD_REQUESTED, got %v", discardAllResp["status"])
	}
	if int(discardAllResp["count"].(float64)) != 2 {
		t.Errorf("expected count 2, got %v", discardAllResp["count"])
	}

	// 4. POST /api/v1/agents/mac-mini/repo-a/actions/discard-changes
	req = httptest.NewRequest(http.MethodPost, "/api/v1/agents/mac-mini/repo-a/actions/discard-changes", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on actions/discard-changes, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestServer_QueueAllToolsUpdateDefaultProfileTools(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	s := &Server{
		Engine: control.NewEngine(nil),
		Queue:  q,
	}
	s.Engine.RegisterHost("mac-mini", "permanent")

	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/tools/update-all", bytes.NewBufferString(`{"target_version":"1.2.3","force":true}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status        string   `json:"status"`
		Tools         []string `json:"tools"`
		TargetVersion string   `json:"target_version"`
		Count         int      `json:"count"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response failed: %v", err)
	}
	if len(resp.Tools) == 0 {
		t.Fatal("expected default profile tools to be resolved when tools is omitted, got empty")
	}
	if resp.TargetVersion != "1.2.3" {
		t.Fatalf("expected target_version 1.2.3, got %q", resp.TargetVersion)
	}

	jobs, err := q.List(context.Background(), dispatch.JobFilter{Type: "agent.tools.update"})
	if err != nil || len(jobs) == 0 {
		t.Fatalf("expected agent.tools.update job in queue, got len=%d err=%v", len(jobs), err)
	}
	var payload struct {
		Tools         []string `json:"tools"`
		TargetVersion string   `json:"target_version"`
	}
	if err := json.Unmarshal(jobs[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal job payload: %v", err)
	}
	if len(payload.Tools) == 0 || payload.TargetVersion != "1.2.3" {
		t.Fatalf("expected non-empty tools and target_version=1.2.3 in job payload, got %+v", payload)
	}
}

func TestQueueAllToolsUpdate_PropagatesTargetVersionToEngine(t *testing.T) {
	q := dispatch.NewInMemoryQueue()
	eng := control.NewEngine(nil)
	s := &Server{
		Engine: eng,
		Queue:  q,
	}
	eng.RegisterHost("mac-mini", "permanent")

	req := httptest.NewRequest(http.MethodPost, "/v1/fleet/tools/update-all", bytes.NewBufferString(`{"tools":["iazio-agent","iazio-mcp"],"target_version":"2.4.0","force":true}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	pending := eng.ConsumePendingToolUpdate("mac-mini")
	if pending == nil {
		t.Fatal("expected PendingToolUpdate queued on Engine for mac-mini")
	}
	if pending.TargetVersion != "2.4.0" {
		t.Fatalf("expected PendingToolUpdate.TargetVersion=2.4.0, got %q", pending.TargetVersion)
	}
}


