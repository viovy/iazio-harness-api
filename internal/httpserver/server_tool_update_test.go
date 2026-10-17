package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
}
