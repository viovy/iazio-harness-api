package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestDeleteHistoryRow(t *testing.T) {
	s := &Server{Engine: control.NewEngine(nil)}
	deletedInDB := ""
	s.DeleteHistory = func(ctx context.Context, jobID string) error {
		deletedInDB = jobID
		return nil
	}

	// Register a repo and history row in memory
	_ = s.Engine.UpsertRepo(control.Repo{HostID: "h1", WorktreePath: "/work"})
	s.Engine.SetHistory("h1:/work", []control.HistoryRow{
		{JobID: "job-del-1", Engine: "agy", Status: "COMPLETED"},
	})

	// 1. Delete existing row
	req := httptest.NewRequest(http.MethodDelete, "/v1/history/job-del-1", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	var res map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["deleted"] != true || res["job_id"] != "job-del-1" {
		t.Errorf("unexpected body: %+v", res)
	}
	if deletedInDB != "job-del-1" {
		t.Errorf("expected db callback with job-del-1, got %q", deletedInDB)
	}

	// 2. Idempotent delete on non-existent row
	req2 := httptest.NewRequest(http.MethodDelete, "/v1/history/job-del-1", nil)
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for already-deleted job, got %d", rr2.Code)
	}
}
