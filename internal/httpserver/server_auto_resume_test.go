package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestPauseAndForceResumeEndpoints(t *testing.T) {
	engine := control.NewEngine(nil)
	engine.RegisterHost("host-1", "permanent")
	_ = engine.UpsertRepo(control.Repo{HostID: "host-1", WorktreePath: "/tmp/repo"})
	p := engine.PutPrompt(control.Prompt{Title: "Test Story", Body: "run", Status: "READY"})
	_, err := engine.ExecutePrompt(p.ID, "host-1", "/tmp/repo", 1, nil)
	if err != nil {
		t.Fatalf("ExecutePrompt: %v", err)
	}

	job, _, _, ok := engine.PollHost("host-1")
	if !ok {
		t.Fatalf("PollHost failed")
	}

	s := &Server{Engine: engine}

	// 1. Test GET /v1/jobs/{id} includes auto_resume fields
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET job failed: %d, body=%s", rr.Code, rr.Body.String())
	}

	var jobDetail map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &jobDetail); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if _, ok := jobDetail["circuit_breaker_tripped"]; !ok {
		t.Errorf("expected circuit_breaker_tripped in job detail view")
	}
	if _, ok := jobDetail["auto_resume_count"]; !ok {
		t.Errorf("expected auto_resume_count in job detail view")
	}

	// 2. Test POST /v1/jobs/{id}/pause-auto-resume
	pauseReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/pause-auto-resume", nil)
	pauseRR := httptest.NewRecorder()
	s.Handler().ServeHTTP(pauseRR, pauseReq)
	if pauseRR.Code != http.StatusOK {
		t.Fatalf("pause-auto-resume failed: %d, body=%s", pauseRR.Code, pauseRR.Body.String())
	}
	var pauseResp map[string]any
	_ = json.Unmarshal(pauseRR.Body.Bytes(), &pauseResp)
	if pauseResp["status"] != "success" {
		t.Errorf("expected status success, got %v", pauseResp["status"])
	}

	// 3. Test POST /v1/jobs/{id}/force-resume
	forceReq := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/force-resume", nil)
	forceRR := httptest.NewRecorder()
	s.Handler().ServeHTTP(forceRR, forceReq)
	if forceRR.Code != http.StatusOK {
		t.Fatalf("force-resume failed: %d, body=%s", forceRR.Code, forceRR.Body.String())
	}
	var forceResp map[string]any
	_ = json.Unmarshal(forceRR.Body.Bytes(), &forceResp)
	if forceResp["status"] != "resumed" {
		t.Errorf("expected status resumed, got %v", forceResp["status"])
	}
}

func TestViews_AutoResumeFields(t *testing.T) {
	policy := &control.AutoResumePolicy{
		Enabled:                true,
		MaxAutoResumes:         5,
		BackoffInitialSec:      10,
		BackoffMaxSec:          120,
		RequireForwardProgress: true,
	}

	detail := control.JobDetail{
		ID:                      "job-test-1",
		AutoResumePolicy:        policy,
		AutoResumeCount:         2,
		LastProgressFingerprint: "fingerprint-123",
		CircuitBreakerTripped:   false,
		ASEGateStatus: map[string]any{
			"passed":    true,
			"git_clean": true,
		},
		StallTimeoutSec:      300,
		LivenessPulseSeconds: 15,
	}

	v := jobDetailView(detail)
	if v["auto_resume_count"] != 2 {
		t.Errorf("expected auto_resume_count 2, got %v", v["auto_resume_count"])
	}
	if v["last_progress_fingerprint"] != "fingerprint-123" {
		t.Errorf("expected fingerprint-123, got %v", v["last_progress_fingerprint"])
	}
	if v["stall_timeout_sec"] != 300 {
		t.Errorf("expected stall_timeout_sec 300, got %v", v["stall_timeout_sec"])
	}
	if v["liveness_pulse_seconds"] != 15 {
		t.Errorf("expected liveness_pulse_seconds 15, got %v", v["liveness_pulse_seconds"])
	}

	row := control.HistoryRow{
		JobID:                   "job-hist-1",
		AutoResumePolicy:        policy,
		AutoResumeCount:         1,
		LastProgressFingerprint: "fp-hist",
		CircuitBreakerTripped:   true,
		LivenessPulseSeconds:    45,
		CreatedAt:               time.Now().UTC(),
	}

	hv := historyJobDetailView(row, "mac-mini", "/tmp/repo")
	if hv["circuit_breaker_tripped"] != true {
		t.Errorf("expected circuit_breaker_tripped true in historyJobDetailView")
	}
	if hv["liveness_pulse_seconds"] != 45 {
		t.Errorf("expected liveness_pulse_seconds 45 in historyJobDetailView")
	}
}
