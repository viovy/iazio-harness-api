package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
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

func TestServer_ForceUpdateToolEndpoint(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	// Register host and seed generic profile with a tool
	e.RegisterHost("mac-mini", "desktop", "generic")
	_, _ = e.UpsertProfile(control.DistributionProfile{
		Name:      "generic",
		IsDefault: true,
		Tools: []control.ProfileTool{
			{
				Name:                   "autopilot-plugins-internal",
				Repository:             "viovy/autopilot-plugins-internal",
				BinaryName:             "autopilot",
				TargetVersion:          "0.0.492",
				LatestAvailableVersion: "0.0.492",
				Tier:                   "baseline",
			},
		},
	})

	// 1. POST /api/agents/mac-mini/tools/autopilot-plugins-internal/force-update with explicit target_version
	rec1 := httptest.NewRecorder()
	body1 := `{"target_version":"0.0.492"}`
	req1 := httptest.NewRequest("POST", "/api/agents/mac-mini/tools/autopilot-plugins-internal/force-update", bytes.NewReader([]byte(body1)))
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 from force-update, got %d: %s", rec1.Code, rec1.Body.String())
	}
	var res1 map[string]any
	if err := json.Unmarshal(rec1.Body.Bytes(), &res1); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res1["status"] != "queued" || res1["target_version"] != "0.0.492" {
		t.Fatalf("unexpected force update response: %v", res1)
	}

	// Verify pending tool update on host
	pending := e.ConsumePendingToolUpdate("mac-mini")
	if pending == nil {
		t.Fatal("expected pending tool update on host")
	}
	if !pending.Force || len(pending.Tools) != 1 || pending.Tools[0] != "autopilot-plugins-internal" || pending.TargetVersion != "0.0.492" {
		t.Fatalf("unexpected pending tool update: %+v", pending)
	}

	// 2. POST /api/v1/agents/mac-mini/tools/autopilot-plugins-internal/force-update without body, resolving from profile
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("POST", "/api/v1/agents/mac-mini/tools/autopilot-plugins-internal/force-update", nil)
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 from profile-resolved force-update, got %d: %s", rec2.Code, rec2.Body.String())
	}
	var res2 map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &res2); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if res2["target_version"] != "0.0.492" {
		t.Fatalf("expected profile-resolved target_version 0.0.492, got %v", res2["target_version"])
	}

	// 3. POST /v1/fleet/agents/mac-mini/tools/autopilot-plugins-internal/force-update with s.Queue populated (F7, N4)
	e.Releases.SetCachedRelease("viovy/autopilot-plugins-internal", "0.0.493", control.ToolRelease{
		Name:          "autopilot-plugins-internal",
		Repository:    "viovy/autopilot-plugins-internal",
		LatestVersion: "0.0.493",
	}, time.Hour)
	q := dispatch.NewInMemoryQueue()
	srv.Queue = q
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest("POST", "/v1/fleet/agents/mac-mini/tools/autopilot-plugins-internal/force-update", bytes.NewReader([]byte(`{"target_version":"0.0.493"}`)))
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 from fleet force-update route, got %d: %s", rec3.Code, rec3.Body.String())
	}
	var res3 map[string]any
	if err := json.Unmarshal(rec3.Body.Bytes(), &res3); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	jobID, ok := res3["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("expected job_id returned in response when Queue is present, got %v", res3)
	}
	job, ok := q.Get(jobID)
	if !ok || job == nil {
		t.Fatalf("expected enqueued job %s in queue, found=%v", jobID, ok)
	}
	if job.Type != "agent.tools.update" || job.TargetHost != "mac-mini" {
		t.Fatalf("unexpected job properties: %+v", job)
	}
	var payloadMap map[string]any
	if err := json.Unmarshal(job.Payload, &payloadMap); err != nil {
		t.Fatalf("failed to unmarshal job payload: %v", err)
	}
	if payloadMap["target_version"] != "0.0.493" {
		t.Fatalf("expected target_version 0.0.493 in job payload, got %v", payloadMap["target_version"])
	}

	// 4. Unknown host must return 404 (F6, F7)
	rec4 := httptest.NewRecorder()
	req4 := httptest.NewRequest("POST", "/api/agents/unknown-host/tools/autopilot-plugins-internal/force-update", nil)
	handler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown host, got %d: %s", rec4.Code, rec4.Body.String())
	}

	// 5. Unknown tool must return 404 (F6, F7)
	rec5 := httptest.NewRecorder()
	req5 := httptest.NewRequest("POST", "/api/agents/mac-mini/tools/unknown-tool/force-update", nil)
	handler.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown tool, got %d: %s", rec5.Code, rec5.Body.String())
	}

	// 6. Multiple tools with target_version must return 400 (F2, F7)
	rec6 := httptest.NewRecorder()
	req6 := httptest.NewRequest("POST", "/v1/hosts/mac-mini/tools/update", bytes.NewReader([]byte(`{"tools":["toolA","toolB"],"target_version":"1.0.0"}`)))
	handler.ServeHTTP(rec6, req6)
	if rec6.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when target_version is provided with multiple tools, got %d: %s", rec6.Code, rec6.Body.String())
	}

	// 7. Malformed JSON body must return 400 (F6, F7)
	rec7 := httptest.NewRecorder()
	req7 := httptest.NewRequest("POST", "/api/agents/mac-mini/tools/autopilot-plugins-internal/force-update", bytes.NewReader([]byte(`{not valid json`)))
	handler.ServeHTTP(rec7, req7)
	if rec7.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed json body, got %d: %s", rec7.Code, rec7.Body.String())
	}

	// 8. Non-generic host profile resolution (R2, R6)
	e.RegisterHost("edge-runner", "edge", "edge-profile")
	_, _ = e.UpsertProfile(control.DistributionProfile{
		Name: "edge-profile",
		Tools: []control.ProfileTool{
			{
				Name:                   "autopilot-plugins-internal",
				Repository:             "viovy/autopilot-plugins-internal",
				BinaryName:             "autopilot",
				TargetVersion:          "0.0.999",
				LatestAvailableVersion: "0.0.999",
				Tier:                   "baseline",
			},
		},
	})
	rec8 := httptest.NewRecorder()
	req8 := httptest.NewRequest("POST", "/api/agents/edge-runner/tools/autopilot-plugins-internal/force-update", nil)
	handler.ServeHTTP(rec8, req8)
	if rec8.Code != http.StatusOK {
		t.Fatalf("expected 200 for edge-runner force update, got %d: %s", rec8.Code, rec8.Body.String())
	}
	var res8 map[string]any
	_ = json.Unmarshal(rec8.Body.Bytes(), &res8)
	if res8["target_version"] != "0.0.999" {
		t.Fatalf("expected target_version 0.0.999 from edge-profile, got %v", res8["target_version"])
	}

	// 9. Standard /update route is unforced unless specified (R3)
	rec9 := httptest.NewRecorder()
	req9 := httptest.NewRequest("POST", "/v1/hosts/mac-mini/tools/autopilot-plugins-internal/update", bytes.NewReader([]byte(`{}`)))
	handler.ServeHTTP(rec9, req9)
	if rec9.Code != http.StatusOK {
		t.Fatalf("expected 200 on /update route, got %d: %s", rec9.Code, rec9.Body.String())
	}
	var res9 map[string]any
	_ = json.Unmarshal(rec9.Body.Bytes(), &res9)
	if res9["force"] != false {
		t.Fatalf("expected force=false for standard /update route (R3), got %v", res9["force"])
	}

	// 10. Invalid target_version returns 422 (R2)
	rec10 := httptest.NewRecorder()
	req10 := httptest.NewRequest("POST", "/api/agents/mac-mini/tools/autopilot-plugins-internal/force-update", bytes.NewReader([]byte(`{"target_version":"invalid version with spaces"}`)))
	handler.ServeHTTP(rec10, req10)
	if rec10.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for invalid target_version, got %d: %s", rec10.Code, rec10.Body.String())
	}

	// 11. Unknown target_version returns 422 (N4)
	rec11 := httptest.NewRecorder()
	req11 := httptest.NewRequest("POST", "/api/agents/mac-mini/tools/autopilot-plugins-internal/force-update", bytes.NewReader([]byte(`{"target_version":"9.9.9"}`)))
	handler.ServeHTTP(rec11, req11)
	if rec11.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for unknown target_version (N4), got %d: %s", rec11.Code, rec11.Body.String())
	}
	if !strings.Contains(rec11.Body.String(), "unknown target_version") {
		t.Fatalf("expected error message to mention unknown target_version, got: %s", rec11.Body.String())
	}

	// 12. Unknown tool on body-only path returns 404 (N4-R)
	rec12 := httptest.NewRecorder()
	req12 := httptest.NewRequest("POST", "/v1/hosts/mac-mini/tools/update", bytes.NewReader([]byte(`{"tools":["nonexistent-tool"]}`)))
	handler.ServeHTTP(rec12, req12)
	if rec12.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown tool on body-only path (N4-R), got %d: %s", rec12.Code, rec12.Body.String())
	}
	if !strings.Contains(rec12.Body.String(), "unknown tool") {
		t.Fatalf("expected error message to mention unknown tool, got: %s", rec12.Body.String())
	}

	// 13. Enqueue error returns 503 and leaves no residual desired state (N4-R, H3)
	_ = e.ConsumePendingToolUpdate("mac-mini") // clear prior pending update from earlier subtest
	failingQueue := &failingMockQueue{err: errors.New("simulated queue storage failure")}
	srv.Queue = failingQueue
	rec13 := httptest.NewRecorder()
	req13 := httptest.NewRequest("POST", "/v1/hosts/mac-mini/tools/autopilot-plugins-internal/update", bytes.NewReader([]byte(`{}`)))
	handler.ServeHTTP(rec13, req13)
	if rec13.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on queue failure (N4-R), got %d: %s", rec13.Code, rec13.Body.String())
	}
	if !strings.Contains(rec13.Body.String(), "failed to enqueue tool update job") {
		t.Fatalf("expected error message to mention failed to enqueue, got: %s", rec13.Body.String())
	}
	h13, ok := e.GetHostDetail("mac-mini")
	if !ok || h13.Host.PendingToolUpdate != nil {
		t.Fatalf("expected no residual desired state on enqueue failure (H3), got: %+v", h13.Host.PendingToolUpdate)
	}

	// 14. Unknown tool in multi-tool body returns 404 (R4)
	srv.Queue = nil // restore queue
	rec14 := httptest.NewRecorder()
	req14 := httptest.NewRequest("POST", "/v1/hosts/mac-mini/tools/update", bytes.NewReader([]byte(`{"tools":["autopilot-plugins-internal", "nonexistent-tool-2"]}`)))
	handler.ServeHTTP(rec14, req14)
	if rec14.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown tool in multi-tool body (R4), got %d: %s", rec14.Code, rec14.Body.String())
	}
	if !strings.Contains(rec14.Body.String(), "unknown tool") {
		t.Fatalf("expected error message to mention unknown tool, got: %s", rec14.Body.String())
	}
}

type failingMockQueue struct {
	dispatch.Queue
	err error
}

func (f *failingMockQueue) Enqueue(ctx context.Context, job *dispatch.Job) (*dispatch.Job, error) {
	return nil, f.err
}

func TestServer_EagerUpdateTelemetry(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	_ = e.RegisterHost("pc1", "workstation")

	// Post heartbeat with eager update telemetry
	hbBody := `{"presence":"ONLINE","eager_update_enabled":true,"last_eager_update_at":"2026-10-25T14:00:00Z","tools":[{"name":"autopilot","version":"0.1.0","status":"OK"}]}`
	recHB := httptest.NewRecorder()
	handler.ServeHTTP(recHB, httptest.NewRequest("POST", "/v1/hosts/pc1/heartbeat", bytes.NewReader([]byte(hbBody))))
	if recHB.Code != http.StatusOK {
		t.Fatalf("expected 200 on heartbeat, got %d: %s", recHB.Code, recHB.Body.String())
	}

	// Query /api/v1/agents/pc1/tools
	recTools := httptest.NewRecorder()
	handler.ServeHTTP(recTools, httptest.NewRequest("GET", "/api/v1/agents/pc1/tools", nil))
	if recTools.Code != http.StatusOK {
		t.Fatalf("expected 200 on /api/v1/agents/pc1/tools, got %d: %s", recTools.Code, recTools.Body.String())
	}

	var toolsResp struct {
		Host               string `json:"host"`
		EagerUpdateEnabled bool   `json:"eager_update_enabled"`
		LastEagerUpdateAt  string `json:"last_eager_update_at"`
	}
	if err := json.Unmarshal(recTools.Body.Bytes(), &toolsResp); err != nil {
		t.Fatalf("unmarshal tools response: %v", err)
	}
	if !toolsResp.EagerUpdateEnabled {
		t.Fatalf("expected eager_update_enabled true, got false")
	}
	if toolsResp.LastEagerUpdateAt != "2026-10-25T14:00:00Z" {
		t.Fatalf("expected last_eager_update_at 2026-10-25T14:00:00Z, got %s", toolsResp.LastEagerUpdateAt)
	}

	// Query /v1/hosts/pc1/tools
	recHostTools := httptest.NewRecorder()
	handler.ServeHTTP(recHostTools, httptest.NewRequest("GET", "/v1/hosts/pc1/tools", nil))
	if recHostTools.Code != http.StatusOK {
		t.Fatalf("expected 200 on /v1/hosts/pc1/tools, got %d: %s", recHostTools.Code, recHostTools.Body.String())
	}
	var hostToolsResp struct {
		EagerUpdateEnabled bool   `json:"eager_update_enabled"`
		LastEagerUpdateAt  string `json:"last_eager_update_at"`
	}
	if err := json.Unmarshal(recHostTools.Body.Bytes(), &hostToolsResp); err != nil {
		t.Fatalf("unmarshal host tools response: %v", err)
	}
	if !hostToolsResp.EagerUpdateEnabled {
		t.Fatalf("expected eager_update_enabled true in host tools, got false")
	}
}


func TestServer_DeleteAgentTool(t *testing.T) {
	e := control.NewEngine(time.Now)
	savedHosts := 0
	srv := &Server{
		Engine: e,
		SaveHost: func(h control.Host) {
			savedHosts++
		},
	}
	handler := srv.Handler()

	_ = e.RegisterHost("pc1", "workstation")
	hbBody := `{"presence":"ONLINE","tools":[{"name":"tool1","version":"1.0.0","status":"OK"},{"name":"tool2","version":"2.0.0","status":"OK"}]}`
	recHB := httptest.NewRecorder()
	handler.ServeHTTP(recHB, httptest.NewRequest("POST", "/v1/hosts/pc1/heartbeat", bytes.NewReader([]byte(hbBody))))
	if recHB.Code != http.StatusOK {
		t.Fatalf("expected 200 on heartbeat, got %d: %s", recHB.Code, recHB.Body.String())
	}

	// 1. DELETE /api/v1/agents/pc1/tools/tool1
	recDel := httptest.NewRecorder()
	handler.ServeHTTP(recDel, httptest.NewRequest("DELETE", "/api/v1/agents/pc1/tools/tool1", nil))
	if recDel.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete tool1, got %d: %s", recDel.Code, recDel.Body.String())
	}
	if savedHosts == 0 {
		t.Fatalf("expected SaveHost to be invoked on tool deletion")
	}

	// 2. Query /api/v1/agents/pc1/tools -> tool1 is gone, tool2 remains
	recTools := httptest.NewRecorder()
	handler.ServeHTTP(recTools, httptest.NewRequest("GET", "/api/v1/agents/pc1/tools", nil))
	if recTools.Code != http.StatusOK {
		t.Fatalf("expected 200 on get tools, got %d", recTools.Code)
	}
	var toolsResp struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(recTools.Body.Bytes(), &toolsResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(toolsResp.Tools) != 1 || toolsResp.Tools[0].Name != "tool2" {
		t.Fatalf("expected 1 remaining tool named tool2, got %+v", toolsResp.Tools)
	}

	// 3. DELETE /v1/hosts/pc1/tools/tool2
	recDel2 := httptest.NewRecorder()
	handler.ServeHTTP(recDel2, httptest.NewRequest("DELETE", "/v1/hosts/pc1/tools/tool2", nil))
	if recDel2.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete tool2, got %d: %s", recDel2.Code, recDel2.Body.String())
	}

	// 4. Query again -> 0 tools
	recTools2 := httptest.NewRecorder()
	handler.ServeHTTP(recTools2, httptest.NewRequest("GET", "/api/v1/agents/pc1/tools", nil))
	_ = json.Unmarshal(recTools2.Body.Bytes(), &toolsResp)
	if len(toolsResp.Tools) != 0 {
		t.Fatalf("expected 0 tools, got %+v", toolsResp.Tools)
	}

	// 5. DELETE non-existent tool -> 404
	recDel3 := httptest.NewRecorder()
	handler.ServeHTTP(recDel3, httptest.NewRequest("DELETE", "/api/v1/agents/pc1/tools/nonexistent", nil))
	if recDel3.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on nonexistent tool, got %d", recDel3.Code)
	}

	// 6. DELETE on nonexistent host -> 404
	recDel4 := httptest.NewRecorder()
	handler.ServeHTTP(recDel4, httptest.NewRequest("DELETE", "/api/v1/agents/unknown-host/tools/tool1", nil))
	if recDel4.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on nonexistent host, got %d", recDel4.Code)
	}
}