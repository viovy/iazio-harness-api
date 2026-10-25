package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestServer_ReconcileLineage_Endpoint(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/repo-03"
	_ = s.Engine.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
	})

	initialConv := "aad902c8-bda6-44c7-8a68-c468a1562399"
	secConv1 := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"
	secConv2 := "14de9a1f-5679-4f11-a08f-f32bc7e47663"

	s.Engine.SetHistory("mac-mini:"+repoPath, []control.HistoryRow{
		{
			JobID:           "job-1311",
			Status:          "FAILED",
			Engine:          "agy",
			ConversationIDs: []string{initialConv},
			CreatedAt:       time.Now().Add(-2 * time.Hour),
		},
		{
			JobID:           "job-1313",
			Status:          "COMPLETED",
			Engine:          "agy",
			ConversationIDs: []string{secConv1, secConv2},
			CreatedAt:       time.Now().Add(-1 * time.Hour),
		},
	})

	dbReconciled := false
	s.ReconcileLineage = func(ctx context.Context) error {
		dbReconciled = true
		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/jobs/reconcile-lineage", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !dbReconciled {
		t.Fatal("expected s.ReconcileLineage callback to have been called")
	}

	var resp struct {
		Status          string `json:"status"`
		ReconciledCount int    `json:"reconciled_count"`
		Message         string `json:"message"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if resp.Status != "ok" || resp.ReconciledCount < 2 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// Also verify via GET /api/v1/agents/mac-mini/repo-03/jobs/job-1313
	jobReq := httptest.NewRequest(http.MethodGet, "/api/v1/agents/mac-mini/repo-03/jobs/job-1313", nil)
	jobRR := httptest.NewRecorder()
	s.Handler().ServeHTTP(jobRR, jobReq)

	if jobRR.Code != http.StatusOK {
		t.Fatalf("expected job status 200, got %d: %s", jobRR.Code, jobRR.Body.String())
	}

	var jobResp struct {
		ID                    string `json:"id"`
		ParentJobID           string `json:"parent_job_id"`
		InitialConversationID string `json:"initial_conversation_id"`
	}
	if err := json.Unmarshal(jobRR.Body.Bytes(), &jobResp); err != nil {
		t.Fatalf("unmarshal job error: %v", err)
	}
	if jobResp.InitialConversationID != initialConv {
		t.Errorf("job-1313 InitialConversationID = %q, want %q", jobResp.InitialConversationID, initialConv)
	}
	if jobResp.ParentJobID != "job-1311" {
		t.Errorf("job-1313 ParentJobID = %q, want job-1311", jobResp.ParentJobID)
	}

	// Verify multi-segment worktree path routing (Finding M1)
	multiReq := httptest.NewRequest(http.MethodGet, "/api/v1/agents/mac-mini/Users/romeo/work/repo-03/jobs/job-1313", nil)
	multiRR := httptest.NewRecorder()
	s.Handler().ServeHTTP(multiRR, multiReq)
	if multiRR.Code != http.StatusOK {
		t.Fatalf("expected multi-segment job status 200, got %d: %s", multiRR.Code, multiRR.Body.String())
	}
}

func TestServer_GetAgentTools_Payload(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	s.Engine.RegisterHost("debian", "linux")

	tools := []control.Tool{
		{
			Name:             "iazio-agent",
			Version:          "0.2.1",
			Installed:        true,
			InstallationType: "systemd-user",
			ActiveState:      "active",
			WatchdogEnabled:  true,
			ConfigStatus:     "synced",
			Capabilities:     []string{"user_service_watchdog_v2", "deterministic_resume_4tuple_v1", "agy_conversation_pinning_v1"},
		},
		{
			Name:             "iazio-harvester",
			Version:          "0.2.1",
			Installed:        true,
			InstallationType: "systemd-user",
			ActiveState:      "active",
			WatchdogEnabled:  true,
			ConfigStatus:     "synced",
			Capabilities:     []string{"user_service_watchdog_v2"},
		},
	}

	_ = s.Engine.HeartbeatWithCapabilities("debian", "ONLINE", tools, false, []string{
		"user_service_watchdog_v2",
		"deterministic_resume_4tuple_v1",
		"agy_conversation_pinning_v1",
	}, "0.2.1", "git-sha-debian-test")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents/debian/tools", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Host                  string   `json:"host"`
		SupportedCapabilities []string `json:"supported_capabilities"`
		Version               string   `json:"version"`
		CommitSHA             string   `json:"commit_sha"`
		Tools                 []struct {
			Name             string `json:"name"`
			Installed        bool   `json:"installed"`
			InstallationType string `json:"installation_type"`
			ActiveState      string `json:"active_state"`
			WatchdogEnabled  bool   `json:"watchdog_enabled"`
			ConfigStatus     string `json:"config_status"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if resp.Host != "debian" {
		t.Errorf("host = %q, want debian", resp.Host)
	}
	if resp.Version != "0.2.1" {
		t.Errorf("version = %q, want 0.2.1", resp.Version)
	}
	if resp.CommitSHA != "git-sha-debian-test" {
		t.Errorf("commit_sha = %q, want git-sha-debian-test", resp.CommitSHA)
	}

	agentFound := false
	harvesterFound := false
	for _, tool := range resp.Tools {
		if tool.Name == "iazio-agent" {
			agentFound = true
			if !tool.Installed || tool.InstallationType != "systemd-user" || tool.ActiveState != "active" || !tool.WatchdogEnabled || tool.ConfigStatus != "synced" {
				t.Errorf("iazio-agent fields mismatch: %+v", tool)
			}
		}
		if tool.Name == "iazio-harvester" {
			harvesterFound = true
			if !tool.Installed || tool.InstallationType != "systemd-user" || tool.ActiveState != "active" || !tool.WatchdogEnabled || tool.ConfigStatus != "synced" {
				t.Errorf("iazio-harvester fields mismatch: %+v", tool)
			}
		}
	}
	if !agentFound || !harvesterFound {
		t.Errorf("expected both iazio-agent and iazio-harvester tools, got %+v", resp.Tools)
	}
}
