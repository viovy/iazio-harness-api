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
		WorktreePath:  "/home/romeo/work/meta-repo",
		DocsHubPath:   "/home/romeo/work/docs",
		CloneURL:      "https://github.com/viovy/meta-repo.git",
		DefaultBranch: "main",
		CurrentBranch: "main",
		RepoAlias:     "meta-repo",
	})
	if err != nil {
		t.Fatalf("register repo: %v", err)
	}

	// 1. Post Repo Runtime Status from agent
	runtimePayload := `[
		{
			"repo_path": "/home/romeo/work/meta-repo",
			"alias": "meta-repo",
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
	r, ok := e.GetRepo("debian", "meta-repo")
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
							RepoPath:             "/home/romeo/work/meta-repo",
							Branch:               "main",
							IsClean:              true,
							SubmodulesClean:      true,
							ASEState:             "IDLE",
							ActiveTool:           "autopilot",
							ToolLiveness:         "LIVE",
							ExecutionProvenance:  "COORDINATED_HARNESS",
							LastCheckedAt:        time.Now(),
						},
					})
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	reqProbe := httptest.NewRequest("POST", "/v1/hosts/debian/repos/check-fleet-status?path=meta-repo&timeout_ms=500", nil)
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
	reqFallback := httptest.NewRequest("POST", "/v1/hosts/debian/repos/check-fleet-status?path=meta-repo&timeout_ms=50", nil)
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
