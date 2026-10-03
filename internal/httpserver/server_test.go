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
