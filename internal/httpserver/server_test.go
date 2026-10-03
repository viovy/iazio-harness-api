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

