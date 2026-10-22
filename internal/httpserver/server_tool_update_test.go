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

func TestServer_ToolUpdateAndReleases(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	// 1. Register host
	e.RegisterHost("test-runner", "runner", "generic")

	// 2. Queue tool update
	reqBody := `{"tools": ["autopilot", "iazio-agent"], "force": true}`
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/hosts/test-runner/tools/update", bytes.NewReader([]byte(reqBody))))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on tool update queue, got %d: %s", rec.Code, rec.Body.String())
	}

	var qRes struct {
		Status string   `json:"status"`
		HostID string   `json:"host_id"`
		Tools  []string `json:"tools"`
		Force  bool     `json:"force"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &qRes); err != nil {
		t.Fatalf("unmarshal queue response: %v", err)
	}
	if qRes.Status != "queued" || len(qRes.Tools) != 2 || !qRes.Force {
		t.Fatalf("unexpected queue response: %+v", qRes)
	}

	// 3. Heartbeat sends ONLINE_UPDATING_TOOLS presence and receives pending tool update
	hbBody := `{"presence": "ONLINE_UPDATING_TOOLS", "tools": [{"name": "autopilot", "version": "0.1.0", "status": "OK"}]}`
	recHB := httptest.NewRecorder()
	handler.ServeHTTP(recHB, httptest.NewRequest("POST", "/v1/hosts/test-runner/heartbeat", bytes.NewReader([]byte(hbBody))))
	if recHB.Code != http.StatusOK {
		t.Fatalf("expected 200 on heartbeat, got %d: %s", recHB.Code, recHB.Body.String())
	}

	var hbRes struct {
		Status            string                     `json:"status"`
		PendingToolUpdate *control.PendingToolUpdate `json:"pending_tool_update"`
	}
	if err := json.Unmarshal(recHB.Body.Bytes(), &hbRes); err != nil {
		t.Fatalf("unmarshal heartbeat response: %v", err)
	}
	if hbRes.PendingToolUpdate == nil {
		t.Fatal("expected non-nil pending_tool_update in heartbeat response")
	}
	if len(hbRes.PendingToolUpdate.Tools) != 2 || !hbRes.PendingToolUpdate.Force {
		t.Fatalf("unexpected pending update: %+v", hbRes.PendingToolUpdate)
	}

	// 4. Verify host presence was updated to ONLINE_UPDATING_TOOLS
	host, ok := e.GetHostDetail("test-runner")
	if !ok {
		t.Fatal("host not found")
	}
	if host.Host.Presence != "ONLINE_UPDATING_TOOLS" {
		t.Fatalf("expected presence ONLINE_UPDATING_TOOLS, got %s", host.Host.Presence)
	}

	// 5. Subsequent heartbeat should have consumed the pending update
	recHB2 := httptest.NewRecorder()
	handler.ServeHTTP(recHB2, httptest.NewRequest("POST", "/v1/hosts/test-runner/heartbeat", bytes.NewReader([]byte(`{"presence": "ONLINE"}`))))
	var hbRes2 struct {
		Status            string                     `json:"status"`
		PendingToolUpdate *control.PendingToolUpdate `json:"pending_tool_update"`
	}
	_ = json.Unmarshal(recHB2.Body.Bytes(), &hbRes2)
	if hbRes2.PendingToolUpdate != nil {
		t.Fatal("expected pending update to be consumed and nil on second heartbeat")
	}

	// 6. Test GET /v1/fleet/profiles/generic/releases
	e.Releases.SetCachedRelease("viovy/autopilot-plugins-internal", "latest", control.ToolRelease{
		Name:          "autopilot",
		Repository:    "viovy/autopilot-plugins-internal",
		LatestVersion: "0.1.0",
		TagName:       "v0.1.0",
	}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-mcp", "latest", control.ToolRelease{
		Name:          "iazio-mcp",
		Repository:    "viovy/iazio-mcp",
		LatestVersion: "0.0.42",
		TagName:       "v0.0.42",
	}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-agent", "latest", control.ToolRelease{
		Name:          "iazio-agent",
		Repository:    "viovy/iazio-agent",
		LatestVersion: "0.1.0",
		TagName:       "v0.1.0",
	}, 10*time.Minute)
	e.Releases.SetCachedRelease("viovy/iazio-harness", "latest", control.ToolRelease{
		Name:          "iazio-harness",
		Repository:    "viovy/iazio-harness",
		LatestVersion: "0.1.0",
		TagName:       "v0.1.0",
	}, 10*time.Minute)
	recRel := httptest.NewRecorder()
	handler.ServeHTTP(recRel, httptest.NewRequest("GET", "/v1/fleet/profiles/generic/releases", nil))
	if recRel.Code != http.StatusOK {
		t.Fatalf("expected 200 on releases endpoint, got %d: %s", recRel.Code, recRel.Body.String())
	}
	var relRes struct {
		Profile  string                `json:"profile"`
		Releases []control.ToolRelease `json:"releases"`
	}
	if err := json.Unmarshal(recRel.Body.Bytes(), &relRes); err != nil {
		t.Fatalf("unmarshal releases response: %v", err)
	}
	if relRes.Profile != "generic" {
		t.Fatalf("expected profile generic, got %s", relRes.Profile)
	}
	if len(relRes.Releases) == 0 {
		t.Fatal("expected non-empty releases list for generic profile")
	}

	// 7. Verify host in ONLINE_UPDATING_TOOLS cannot receive leases via PollHost
	_, _, _, pollOk := e.PollHost("test-runner")
	if pollOk {
		t.Fatal("expected PollHost to return ok=false when host is in ONLINE_UPDATING_TOOLS")
	}
}

func TestServer_RepoAliasAndGitCredentials(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	e.RegisterHost("host-1", "runner", "generic")

	// 1. Register repo with explicit alias
	reqBody := `{"host_id":"host-1","worktree_path":"/work/repos/project-alpha","repo_alias":"alpha-repo"}`
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/repos", bytes.NewReader([]byte(reqBody))))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on repo registration, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Query repo by alias
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, httptest.NewRequest("GET", "/v1/hosts/host-1/repos?path=alpha-repo", nil))
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 querying repo by alias, got %d: %s", recGet.Code, recGet.Body.String())
	}
	var repoRes map[string]any
	if err := json.Unmarshal(recGet.Body.Bytes(), &repoRes); err != nil {
		t.Fatalf("unmarshal repo response: %v", err)
	}
	if repoRes["repo_alias"] != "alpha-repo" || repoRes["alias"] != "alpha-repo" {
		t.Fatalf("expected repo_alias alpha-repo, got %+v", repoRes)
	}

	// 3. Register repo without explicit alias -> effective alias is basename of worktree_path
	reqBody2 := `{"host_id":"host-1","worktree_path":"/work/repos/beta-service"}`
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest("POST", "/v1/repos", bytes.NewReader([]byte(reqBody2))))
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on repo registration, got %d: %s", rec2.Code, rec2.Body.String())
	}

	recGet2 := httptest.NewRecorder()
	handler.ServeHTTP(recGet2, httptest.NewRequest("GET", "/v1/hosts/host-1/repos?path=beta-service", nil))
	if recGet2.Code != http.StatusOK {
		t.Fatalf("expected 200 querying repo by fallback alias, got %d: %s", recGet2.Code, recGet2.Body.String())
	}

	// 3a. Register repo with reserved alias 'tools' -> must fail
	reqTools := `{"host_id":"host-1","worktree_path":"/work/repos/tools-helper","repo_alias":"tools"}`
	recTools := httptest.NewRecorder()
	handler.ServeHTTP(recTools, httptest.NewRequest("POST", "/v1/repos", bytes.NewReader([]byte(reqTools))))
	if recTools.Code != http.StatusBadRequest || !strings.Contains(recTools.Body.String(), "reserved") {
		t.Fatalf("expected 400 for reserved alias 'tools', got %d: %s", recTools.Code, recTools.Body.String())
	}

	// 3b. Register repo with colliding alias 'alpha-repo' -> must fail
	reqDup := `{"host_id":"host-1","worktree_path":"/work/repos/another-alpha","repo_alias":"alpha-repo"}`
	recDup := httptest.NewRecorder()
	handler.ServeHTTP(recDup, httptest.NewRequest("POST", "/v1/repos", bytes.NewReader([]byte(reqDup))))
	if recDup.Code != http.StatusBadRequest || !strings.Contains(recDup.Body.String(), "already in use") {
		t.Fatalf("expected 400 for duplicate alias 'alpha-repo', got %d: %s", recDup.Code, recDup.Body.String())
	}

	// 4. Test Git credentials endpoints
	// Initial GET should be empty list
	recCreds := httptest.NewRecorder()
	handler.ServeHTTP(recCreds, httptest.NewRequest("GET", "/v1/fleet/credentials/git", nil))
	if recCreds.Code != http.StatusOK {
		t.Fatalf("expected 200 on get git creds, got %d: %s", recCreds.Code, recCreds.Body.String())
	}

	// POST credential
	credBody := `{"name":"bot","pattern":"viovy/*","token":"ghp_secrettokenlongenough123","type":"pat"}`
	recPostCred := httptest.NewRecorder()
	handler.ServeHTTP(recPostCred, httptest.NewRequest("POST", "/v1/fleet/credentials/git", bytes.NewReader([]byte(credBody))))
	if recPostCred.Code != http.StatusOK {
		t.Fatalf("expected 200 on post git creds, got %d: %s", recPostCred.Code, recPostCred.Body.String())
	}
	var postRes struct {
		Status     string                `json:"status"`
		Credential control.GitCredential `json:"credential"`
	}
	if err := json.Unmarshal(recPostCred.Body.Bytes(), &postRes); err != nil {
		t.Fatalf("unmarshal created cred: %v", err)
	}
	createdCred := postRes.Credential
	if createdCred.ID == "" || createdCred.Pattern != "viovy/*" {
		t.Fatalf("unexpected created cred: %+v", createdCred)
	}

	// GET credentials - token must be masked
	recCredsList := httptest.NewRecorder()
	handler.ServeHTTP(recCredsList, httptest.NewRequest("GET", "/v1/fleet/credentials/git", nil))
	if recCredsList.Code != http.StatusOK {
		t.Fatalf("expected 200 on get git creds list, got %d: %s", recCredsList.Code, recCredsList.Body.String())
	}
	var credsList []control.GitCredential
	if err := json.Unmarshal(recCredsList.Body.Bytes(), &credsList); err != nil {
		t.Fatalf("unmarshal creds list: %v", err)
	}
	if len(credsList) != 1 {
		t.Fatalf("expected 1 credential, got %d", len(credsList))
	}
	if credsList[0].Token == "ghp_secrettokenlongenough123" {
		t.Fatal("token was not masked in GET response")
	}

	// DELETE credential
	recDel := httptest.NewRecorder()
	handler.ServeHTTP(recDel, httptest.NewRequest("DELETE", "/v1/fleet/credentials/git/"+createdCred.ID, nil))
	if recDel.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete git creds, got %d: %s", recDel.Code, recDel.Body.String())
	}

	// GET again - should be empty
	recCredsAfter := httptest.NewRecorder()
	handler.ServeHTTP(recCredsAfter, httptest.NewRequest("GET", "/v1/fleet/credentials/git", nil))
	var listAfter []control.GitCredential
	_ = json.Unmarshal(recCredsAfter.Body.Bytes(), &listAfter)
	if len(listAfter) != 0 {
		t.Fatalf("expected 0 credentials after deletion, got %d", len(listAfter))
	}
}

func TestServer_NotificationsAndToolUpdateQueue(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	e.RegisterHost("host-notif", "runner", "generic")

	// 1. Queue tool update
	reqBody := `{"tools": ["autopilot"], "force": true}`
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/hosts/host-notif/tools/update", bytes.NewReader([]byte(reqBody))))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on tool update queue, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Check GET /v1/notifications contains tool update notification
	recNotifs := httptest.NewRecorder()
	handler.ServeHTTP(recNotifs, httptest.NewRequest("GET", "/v1/notifications", nil))
	if recNotifs.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET /v1/notifications, got %d: %s", recNotifs.Code, recNotifs.Body.String())
	}
	var notifsRes struct {
		Notifications []NotificationRecord `json:"notifications"`
		Count         int                  `json:"count"`
	}
	if err := json.Unmarshal(recNotifs.Body.Bytes(), &notifsRes); err != nil {
		t.Fatalf("unmarshal notifications error: %v", err)
	}
	if notifsRes.Count == 0 || len(notifsRes.Notifications) == 0 {
		t.Fatal("expected notifications to be recorded")
	}

	// 3. Test POST /v1/fleet/tools/update-all
	recUpAll := httptest.NewRecorder()
	handler.ServeHTTP(recUpAll, httptest.NewRequest("POST", "/v1/fleet/tools/update-all", bytes.NewReader([]byte(`{"tools":["autopilot"]}`))))
	if recUpAll.Code != http.StatusOK {
		t.Fatalf("expected 200 on update-all, got %d: %s", recUpAll.Code, recUpAll.Body.String())
	}

	// 4. Test POST /v1/notifications
	recPostNotif := httptest.NewRecorder()
	postPayload := `{"title":"Agent Tool Updated","message":"Updated autopilot on host-1","category":"success","source":"iazio-agent","host_id":"host-1"}`
	handler.ServeHTTP(recPostNotif, httptest.NewRequest("POST", "/v1/notifications", bytes.NewReader([]byte(postPayload))))
	if recPostNotif.Code != http.StatusCreated {
		t.Fatalf("expected 201 on POST /v1/notifications, got %d: %s", recPostNotif.Code, recPostNotif.Body.String())
	}
}
