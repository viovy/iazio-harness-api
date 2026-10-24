package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestServer_GitCredentialsTestEndpoints(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	// 1. Test payload endpoint with empty token -> returns valid: false
	req := httptest.NewRequest("POST", "/v1/fleet/credentials/git/test", bytes.NewReader([]byte(`{"token":""}`)))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var res control.GitCredentialTestResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if res.Valid {
		t.Fatalf("expected invalid for empty token")
	}

	// 2. Register credential
	cred, err := e.Releases.RegisterCredential(control.GitCredential{
		Name:    "ci-pat",
		Pattern: "*",
		Token:   "ghp_test_token_123",
	})
	if err != nil {
		t.Fatalf("register credential: %v", err)
	}

	// 3. Test saved credential endpoint
	reqSaved := httptest.NewRequest("POST", "/v1/fleet/credentials/git/"+cred.ID+"/test", nil)
	wSaved := httptest.NewRecorder()
	handler.ServeHTTP(wSaved, reqSaved)
	if wSaved.Code != http.StatusOK {
		t.Fatalf("expected 200 for saved cred test, got %d", wSaved.Code)
	}
}

func TestServer_ProfileRefreshAndHostPing(t *testing.T) {
	e := control.NewEngine(time.Now)
	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name": "1.0.0",
			"html_url": "https://github.com/viovy/iazio-agent/releases/tag/1.0.0",
		})
	}))
	defer mockGH.Close()
	relSvc := control.NewReleaseService()
	relSvc.SetBaseURL(mockGH.URL)
	relSvc.SetHTTPClient(mockGH.Client())
	e.Releases = relSvc

	srv := &Server{Engine: e}
	handler := srv.Handler()

	// Register generic profile
	_, err := e.UpsertProfile(control.DistributionProfile{
		Name: "generic",
		Tools: []control.ProfileTool{
			{Name: "iazio-agent", BinaryName: "iazio-agent", Repository: "viovy/iazio-agent", Tier: "baseline"},
		},
	})
	if err != nil {
		t.Fatalf("create profile: %v", err)
	}

	// 1. Refresh profile
	reqRefresh := httptest.NewRequest("POST", "/v1/fleet/profiles/generic/refresh", nil)
	wRefresh := httptest.NewRecorder()
	handler.ServeHTTP(wRefresh, reqRefresh)
	if wRefresh.Code != http.StatusOK {
		t.Fatalf("expected 200 for refresh profile, got %d: %s", wRefresh.Code, wRefresh.Body.String())
	}

	// Register host
	e.RegisterHost("debian", "linux", "generic")
	err = e.Heartbeat("debian", []control.Tool{
		{Name: "iazio-agent", Version: "1.0.0", Status: "OK"},
	}, false)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// 2. Ping host with simulated live agent ack round-trip
	go func() {
		for i := 0; i < 20; i++ {
			cmds := e.ConsumePendingCommands("debian")
			for _, cmd := range cmds {
				if cmd.Type == "PING" {
					e.NotifyPingAck(cmd.ID, "1.0.0")
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	reqPing := httptest.NewRequest("POST", "/v1/hosts/debian/ping?timeout_ms=500", nil)
	wPing := httptest.NewRecorder()
	handler.ServeHTTP(wPing, reqPing)
	if wPing.Code != http.StatusOK {
		t.Fatalf("expected 200 for ping, got %d", wPing.Code)
	}
	var pingRes map[string]any
	if err := json.Unmarshal(wPing.Body.Bytes(), &pingRes); err != nil {
		t.Fatalf("unmarshal ping: %v", err)
	}
	if pingRes["reachable"] != true {
		t.Fatalf("expected reachable host, got %v", pingRes)
	}
	if pingRes["live_ack"] != true {
		t.Fatalf("expected live_ack true, got %v", pingRes["live_ack"])
	}
	if pingRes["agent_version"] != "1.0.0" {
		t.Fatalf("expected agent_version 1.0.0, got %v", pingRes["agent_version"])
	}

	// 3. Ping timeout test: host without agent ack must fail live check (reachable: false, live_ack: false)
	reqTimeout := httptest.NewRequest("POST", "/v1/hosts/debian/ping?timeout_ms=50", nil)
	wTimeout := httptest.NewRecorder()
	handler.ServeHTTP(wTimeout, reqTimeout)
	if wTimeout.Code != http.StatusOK {
		t.Fatalf("expected 200 for ping timeout, got %d", wTimeout.Code)
	}
	var timeoutRes map[string]any
	if err := json.Unmarshal(wTimeout.Body.Bytes(), &timeoutRes); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if timeoutRes["reachable"] == true {
		t.Fatalf("expected reachable false when live ack times out, got %v", timeoutRes)
	}
	if timeoutRes["live_ack"] == true {
		t.Fatalf("expected live_ack false when live ack times out, got %v", timeoutRes)
	}
}

func TestServer_CheckFleetStatusAndRepoRuntimeStatus(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	_ = e.RegisterHost("debian", "linux", "generic")
	err := e.UpsertRepo(control.Repo{
		HostID:        "debian",
		WorktreePath:  "/work/sample-repo",
		DocsHubPath:   "/work/docs",
		CloneURL:      "https://github.com/example/sample-repo.git",
		DefaultBranch: "main",
		CurrentBranch: "main",
		RepoAlias:     "sample-repo",
	})
	if err != nil {
		t.Fatalf("register repo: %v", err)
	}

	// 1. Post Repo Runtime Status from agent
	runtimePayload := `[
		{
			"repo_path": "/work/sample-repo",
			"alias": "sample-repo",
			"is_clean": true,
			"branch": "main",
			"submodules_clean": true,
			"ase_state": "IDLE",
			"active_tool": "autopilot",
			"tool_liveness": "LIVE",
			"execution_provenance": "INDEPENDENT_USER_SESSION"
		}
	]`
	reqStatus := httptest.NewRequest("POST", "/v1/hosts/debian/repos/status", bytes.NewReader([]byte(runtimePayload)))
	wStatus := httptest.NewRecorder()
	handler.ServeHTTP(wStatus, reqStatus)
	if wStatus.Code != http.StatusOK {
		t.Fatalf("expected 200 for repo status update, got %d", wStatus.Code)
	}

	// Verify repo contains updated telemetry
	r, ok := e.GetRepo("debian", "sample-repo")
	if !ok {
		t.Fatalf("repo not found")
	}
	if r.ASEState != "IDLE" {
		t.Fatalf("expected ASEState IDLE, got %s", r.ASEState)
	}
	if r.ActiveTool != "autopilot" {
		t.Fatalf("expected tool autopilot, got %s", r.ActiveTool)
	}

	// 2. Ephemeral Check Fleet Git Status with live agent telemetry response
	go func() {
		for i := 0; i < 20; i++ {
			cmds := e.ConsumePendingCommands("debian")
			for _, cmd := range cmds {
				if cmd.Type == "CHECK_FLEET_GIT_STATUS" {
					e.UpdateRepoRuntimeStatus("debian", []control.RepoRuntimeStatus{
						{
							RepoPath:            "/work/sample-repo",
							Branch:              "main",
							IsClean:             true,
							SubmodulesClean:     true,
							ASEState:            "IDLE",
							ActiveTool:          "autopilot",
							ToolLiveness:        "LIVE",
							ExecutionProvenance: "COORDINATED_HARNESS",
							LastCheckedAt:       time.Now(),
						},
					})
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	reqProbe := httptest.NewRequest("POST", "/v1/hosts/debian/repos/check-fleet-status?path=sample-repo&timeout_ms=500", nil)
	wProbe := httptest.NewRecorder()
	handler.ServeHTTP(wProbe, reqProbe)
	if wProbe.Code != http.StatusOK {
		t.Fatalf("expected 200 for probe, got %d: %s", wProbe.Code, wProbe.Body.String())
	}
	var probeRes map[string]any
	if err := json.Unmarshal(wProbe.Body.Bytes(), &probeRes); err != nil {
		t.Fatalf("unmarshal probe: %v", err)
	}
	if probeRes["status"] != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %v", probeRes["status"])
	}
	resultObj, ok := probeRes["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected result object in probe response, got %T", probeRes["result"])
	}
	if resultObj["is_live_probe"] != true {
		t.Fatalf("expected is_live_probe == true for live probe response, got %v", resultObj["is_live_probe"])
	}
	if resultObj["probe_status"] != "LIVE_ACK" {
		t.Fatalf("expected probe_status LIVE_ACK, got %v", resultObj["probe_status"])
	}
	if !strings.HasPrefix(probeRes["probe_id"].(string), "ephemeral-probe-") {
		t.Fatalf("expected ephemeral probe ID, got %v", probeRes["probe_id"])
	}

	// Verify ephemeral probe was NEVER saved to jobs table!
	probeID := probeRes["probe_id"].(string)
	if _, found := e.GetJob(probeID); found {
		t.Fatalf("ephemeral probe leaked into persistent jobs table!")
	}

	// 3. Test probe timeout fallback to cached Repo
	reqFallback := httptest.NewRequest("POST", "/v1/hosts/debian/repos/check-fleet-status?path=sample-repo&timeout_ms=50", nil)
	wFallback := httptest.NewRecorder()
	handler.ServeHTTP(wFallback, reqFallback)
	if wFallback.Code != http.StatusOK {
		t.Fatalf("expected 200 for fallback probe, got %d", wFallback.Code)
	}
	var fallbackRes map[string]any
	if err := json.Unmarshal(wFallback.Body.Bytes(), &fallbackRes); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	fallbackResultObj, ok := fallbackRes["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected result object in fallback probe response, got %T", fallbackRes["result"])
	}
	if fallbackResultObj["is_live_probe"] == true {
		t.Fatalf("expected is_live_probe == false when timed out")
	}
	if fallbackResultObj["stale"] != true {
		t.Fatalf("expected stale == true when timed out")
	}
	if fallbackResultObj["probe_status"] != "TIMEOUT_CACHED_FALLBACK" {
		t.Fatalf("expected TIMEOUT_CACHED_FALLBACK, got %v", fallbackResultObj["probe_status"])
	}
}

func TestServer_AdditionalEndpointsCoverage(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	// 1. Tool Priority coverage
	e.SetToolPriority([]string{"autopilot", "agy"})
	prio := e.GetToolPriority()
	if len(prio) != 2 || prio[0] != "autopilot" {
		t.Fatalf("expected tool priority [autopilot agy], got %v", prio)
	}

	// 2. pingAck endpoint coverage
	// a. Valid ack
	ackBody := bytes.NewReader([]byte(`{"ping_id":"p-1","agent_version":"2.0.0"}`))
	reqAck := httptest.NewRequest("POST", "/v1/hosts/debian/ping-ack", ackBody)
	wAck := httptest.NewRecorder()
	handler.ServeHTTP(wAck, reqAck)
	if wAck.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wAck.Code)
	}

	// b. Empty body ack
	reqAckEmpty := httptest.NewRequest("POST", "/v1/hosts/debian/ping-ack", nil)
	wAckEmpty := httptest.NewRecorder()
	handler.ServeHTTP(wAckEmpty, reqAckEmpty)
	if wAckEmpty.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wAckEmpty.Code)
	}

	// 3. Ping non-existent host -> 404
	reqPing404 := httptest.NewRequest("POST", "/v1/hosts/non-existent/ping", nil)
	wPing404 := httptest.NewRecorder()
	handler.ServeHTTP(wPing404, reqPing404)
	if wPing404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown host ping, got %d", wPing404.Code)
	}

	// 4. Git Credentials list, register, delete
	reqListCreds := httptest.NewRequest("GET", "/v1/fleet/credentials/git", nil)
	wListCreds := httptest.NewRecorder()
	handler.ServeHTTP(wListCreds, reqListCreds)
	if wListCreds.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", wListCreds.Code)
	}

	cred, _ := e.Releases.RegisterCredential(control.GitCredential{
		Name:    "delete-me",
		Pattern: "*",
		Token:   "tok",
	})
	reqDelCred := httptest.NewRequest("DELETE", "/v1/fleet/credentials/git/"+cred.ID, nil)
	wDelCred := httptest.NewRecorder()
	handler.ServeHTTP(wDelCred, reqDelCred)
	if wDelCred.Code != http.StatusNoContent && wDelCred.Code != http.StatusOK {
		t.Fatalf("expected 204 or 200, got %d", wDelCred.Code)
	}

	// Delete unknown cred -> 404
	reqDel404 := httptest.NewRequest("DELETE", "/v1/fleet/credentials/git/unknown-id", nil)
	wDel404 := httptest.NewRecorder()
	handler.ServeHTTP(wDel404, reqDel404)
	if wDel404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wDel404.Code)
	}

	// Test saved unknown cred -> 404
	reqTestSaved404 := httptest.NewRequest("POST", "/v1/fleet/credentials/git/unknown-id/test", nil)
	wTestSaved404 := httptest.NewRecorder()
	handler.ServeHTTP(wTestSaved404, reqTestSaved404)
	if wTestSaved404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wTestSaved404.Code)
	}

	// Refresh unknown profile -> 404
	reqRefresh404 := httptest.NewRequest("POST", "/v1/fleet/profiles/non-existent-profile/refresh", nil)
	wRefresh404 := httptest.NewRecorder()
	handler.ServeHTTP(wRefresh404, reqRefresh404)
	if wRefresh404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wRefresh404.Code)
	}

	// Check fleet status unknown host -> 404
	reqProbeHost404 := httptest.NewRequest("POST", "/v1/hosts/unknown-host/repos/check-fleet-status?path=sample", nil)
	wProbeHost404 := httptest.NewRecorder()
	handler.ServeHTTP(wProbeHost404, reqProbeHost404)
	if wProbeHost404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wProbeHost404.Code)
	}

	// Check fleet status unknown repo -> 404
	e.RegisterHost("host-ok", "linux", "generic")
	reqProbeRepo404 := httptest.NewRequest("POST", "/v1/hosts/host-ok/repos/check-fleet-status?path=unknown-repo", nil)
	wProbeRepo404 := httptest.NewRecorder()
	handler.ServeHTTP(wProbeRepo404, reqProbeRepo404)
	if wProbeRepo404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", wProbeRepo404.Code)
	}
}

func TestServer_AgentJobsClaim(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{
		Engine: e,
		Queue:  dispatch.NewInMemoryQueue(),
	}
	handler := srv.Handler()

	// 1. Claim when queue is empty -> 204 No Content
	claimBody := []byte(`{"host_id":"debian","job_types":["story.create","prompt.execute"]}`)
	reqEmpty := httptest.NewRequest("POST", "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	wEmpty := httptest.NewRecorder()
	handler.ServeHTTP(wEmpty, reqEmpty)
	if wEmpty.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content for empty queue, got %d: %s", wEmpty.Code, wEmpty.Body.String())
	}

	// 2. Enqueue a job into dispatch.Queue
	job, err := srv.Queue.Enqueue(reqEmpty.Context(), &dispatch.Job{
		Type: dispatch.JobTypeStoryCreate,
		Repo: "iazio-mcp",
	})
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	// 3. Claim job -> 200 OK with claimed job
	reqClaim := httptest.NewRequest("POST", "/api/v1/agent/jobs/claim", bytes.NewReader(claimBody))
	wClaim := httptest.NewRecorder()
	handler.ServeHTTP(wClaim, reqClaim)
	if wClaim.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for claiming job, got %d: %s", wClaim.Code, wClaim.Body.String())
	}

	var claimed dispatch.Job
	if err := json.Unmarshal(wClaim.Body.Bytes(), &claimed); err != nil {
		t.Fatalf("unmarshal claimed job: %v", err)
	}
	if claimed.ID != job.ID {
		t.Errorf("expected claimed ID %s, got %s", job.ID, claimed.ID)
	}
	if claimed.Status != dispatch.JobStatusClaimed {
		t.Errorf("expected status claimed, got %s", claimed.Status)
	}
}

func TestServer_ProfileRefreshUpdatesTargetVersion(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{
		Engine: e,
	}
	handler := srv.Handler()

	// Create profile with tool having empty TargetVersion
	_, err := e.UpsertProfile(control.DistributionProfile{
		Name: "generic",
		Tools: []control.ProfileTool{
			{Name: "iazio-agent", BinaryName: "iazio-agent", Repository: "viovy/iazio-agent", Tier: "baseline"},
		},
	})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}

	// Mock release service
	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tag_name": "1.0.0",
			"html_url": "https://github.com/viovy/iazio-agent/releases/tag/1.0.0",
		})
	}))
	defer mockGH.Close()
	mockRelService := control.NewReleaseService()
	mockRelService.SetBaseURL(mockGH.URL)
	mockRelService.SetHTTPClient(mockGH.Client())
	e.Releases = mockRelService

	// Call refresh endpoint
	req := httptest.NewRequest("POST", "/api/v1/binaries-profiles/generic/refresh", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	// Verify profile is still accessible and was processed
	p, err := e.GetProfile("generic")
	if err != nil {
		t.Fatalf("get profile: %v", err)
	}
	if len(p.Tools) == 0 {
		t.Fatalf("expected tools in profile")
	}
}

func TestServer_HealASERepo(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{
		Engine: e,
	}
	handler := srv.Handler()

	// 1. Method not allowed
	reqWrongMethod := httptest.NewRequest(http.MethodGet, "/v1/hosts/debian/repos/heal-ase", nil)
	wWrongMethod := httptest.NewRecorder()
	handler.ServeHTTP(wWrongMethod, reqWrongMethod)
	if wWrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", wWrongMethod.Code)
	}

	// 2. Empty host
	reqEmptyHost := httptest.NewRequest(http.MethodPost, "/v1/hosts//repos/heal-ase", nil)
	wEmptyHost := httptest.NewRecorder()
	srv.healASERepo(wEmptyHost, reqEmptyHost)
	if wEmptyHost.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty host, got %d", wEmptyHost.Code)
	}

	// 3. Register host and repo with active lock
	e.RegisterHost("debian", "linux", "generic")
	repo := control.Repo{
		HostID:       "debian",
		WorktreePath: "/work/repo",
		Lock:         control.LockASE,
		Reason:       "ase story running",
	}
	if err := e.UpsertRepo(repo); err != nil {
		t.Fatalf("upsert repo: %v", err)
	}

	// POST /v1/hosts/debian/repos/heal-ase?path=/work/repo
	req := httptest.NewRequest(http.MethodPost, "/v1/hosts/debian/repos/heal-ase?path=/work/repo", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["host_id"] != "debian" {
		t.Errorf("expected host_id debian, got %v", resp["host_id"])
	}
	if resp["repo_path"] != "/work/repo" {
		t.Errorf("expected repo_path /work/repo, got %v", resp["repo_path"])
	}
	if resp["status"] != "dispatched" {
		t.Errorf("expected status dispatched, got %v", resp["status"])
	}
	locksCleared, ok := resp["locks_cleared"].([]any)
	if !ok || len(locksCleared) == 0 || locksCleared[0] != "repo_lock_reset" {
		t.Errorf("expected locks_cleared to contain repo_lock_reset, got %v", resp["locks_cleared"])
	}

	// Verify repo lock was reset in engine
	detail, ok := e.GetRepoDetail("debian", "/work/repo")
	if !ok || detail.Repo.Lock != control.LockIdle {
		t.Errorf("expected repo lock to be LockIdle, got %v", detail.Repo.Lock)
	}

	// Verify command was queued
	cmds := e.ConsumePendingCommands("debian")
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command queued, got %d", len(cmds))
	}
	if cmds[0].Type != "REAP_ORPHANS_AND_RESET_ASE" {
		t.Errorf("expected command REAP_ORPHANS_AND_RESET_ASE, got %s", cmds[0].Type)
	}
	if cmds[0].Payload != "/work/repo" {
		t.Errorf("expected command payload /work/repo, got %v", cmds[0].Payload)
	}

	// 4. Test alternate route POST /api/v1/agents/debian/repos/heal-ase with JSON body
	body := bytes.NewReader([]byte(`{"path":"/work/repo2"}`))
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/agents/debian/repos/heal-ase", body)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for alternate route, got %d: %s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]any
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("unmarshal response 2: %v", err)
	}
	if resp2["repo_path"] != "/work/repo2" {
		t.Errorf("expected repo_path /work/repo2, got %v", resp2["repo_path"])
	}
}

func TestServer_ToolPriority(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{
		Engine: e,
	}
	handler := srv.Handler()

	// GET /v1/fleet/tool-priority
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/fleet/tool-priority", nil)
	wGet := httptest.NewRecorder()
	handler.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wGet.Code, wGet.Body.String())
	}

	// PUT /v1/fleet/tool-priority with invalid json
	reqPutInvalid := httptest.NewRequest(http.MethodPut, "/v1/fleet/tool-priority", bytes.NewReader([]byte(`invalid-json`)))
	wPutInvalid := httptest.NewRecorder()
	handler.ServeHTTP(wPutInvalid, reqPutInvalid)
	if wPutInvalid.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", wPutInvalid.Code)
	}

	// PUT /v1/fleet/tool-priority with valid tools
	reqPut := httptest.NewRequest(http.MethodPut, "/v1/fleet/tool-priority", bytes.NewReader([]byte(`{"tools":["opencode","agy"]}`)))
	wPut := httptest.NewRecorder()
	handler.ServeHTTP(wPut, reqPut)
	if wPut.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wPut.Code, wPut.Body.String())
	}

	// GET /api/v1/tool-priority
	reqGetAPI := httptest.NewRequest(http.MethodGet, "/api/v1/tool-priority", nil)
	wGetAPI := httptest.NewRecorder()
	handler.ServeHTTP(wGetAPI, reqGetAPI)
	if wGetAPI.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wGetAPI.Code, wGetAPI.Body.String())
	}
}

func TestServer_CheckFleetStatus_DirtyFilesFormatting(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{
		Engine: e,
	}
	handler := srv.Handler()

	// Ingest repo with dirty porcelain
	_ = e.UpsertRepo(control.Repo{
		HostID:       "pc1",
		WorktreePath: "C:\\work\\sample-repo",
	})
	e.ApplyPreflight("pc1", "C:\\work\\sample-repo", control.Preflight{
		Kind:          control.KindOrdinary,
		Branch:        "chore/gov-3655",
		WorkPorcelain: " M docs/stories/index.yml\n?? docs/stories/new.story.md",
	})

	req := httptest.NewRequest("POST", "/v1/hosts/pc1/repos/check-fleet-status?path=C:\\work\\sample-repo&timeout_ms=10", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var envelope struct {
		ProbeID string                        `json:"probe_id"`
		Status  string                        `json:"status"`
		Result  control.RepoFleetStatusResult `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal probe response: %v", err)
	}

	if len(envelope.Result.ModifiedFiles) != 1 || envelope.Result.ModifiedFiles[0] != "docs/stories/index.yml" {
		t.Errorf("unexpected ModifiedFiles: %+v", envelope.Result.ModifiedFiles)
	}
	if len(envelope.Result.UntrackedFiles) != 1 || envelope.Result.UntrackedFiles[0] != "docs/stories/new.story.md" {
		t.Errorf("unexpected UntrackedFiles: %+v", envelope.Result.UntrackedFiles)
	}
	if !strings.Contains(envelope.Result.DiagnosticSummary, "Working Tree: DIRTY (1 modified, 1 untracked)") {
		t.Errorf("summary missing count line: %s", envelope.Result.DiagnosticSummary)
	}
	if !strings.Contains(envelope.Result.DiagnosticSummary, "  M  docs/stories/index.yml") {
		t.Errorf("summary missing modified file: %s", envelope.Result.DiagnosticSummary)
	}
	if !strings.Contains(envelope.Result.DiagnosticSummary, "  ?? docs/stories/new.story.md") {
		t.Errorf("summary missing untracked file: %s", envelope.Result.DiagnosticSummary)
	}
}
