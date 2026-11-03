package httpserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestAuthMiddleware(t *testing.T) {
	srv := &Server{
		Engine:    control.NewEngine(nil),
		AuthToken: "enterprise-secret-token-123",
	}
	handler := srv.Handler()

	// 1. Public route /healthz must succeed without auth
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rrHealth := httptest.NewRecorder()
	handler.ServeHTTP(rrHealth, reqHealth)
	if rrHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for public /healthz, got %d", rrHealth.Code)
	}

	// 2. Public route /readyz must succeed without auth
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rrReady := httptest.NewRecorder()
	handler.ServeHTTP(rrReady, reqReady)
	if rrReady.Code != http.StatusOK {
		t.Fatalf("expected 200 for public /readyz, got %d", rrReady.Code)
	}

	// 3. Protected route /v1/prompts without token must be rejected with 401
	reqPrompt := httptest.NewRequest(http.MethodGet, "/v1/prompts", nil)
	rrPrompt := httptest.NewRecorder()
	handler.ServeHTTP(rrPrompt, reqPrompt)
	if rrPrompt.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", rrPrompt.Code)
	}
	if h := rrPrompt.Header().Get("WWW-Authenticate"); h != `Bearer error="invalid_token"` {
		t.Fatalf("expected WWW-Authenticate header with Bearer error=\"invalid_token\", got %q", h)
	}

	// 4. Protected route with invalid token must be rejected with 401
	reqInvalid := httptest.NewRequest(http.MethodGet, "/v1/prompts", nil)
	reqInvalid.Header.Set("Authorization", "Bearer wrong-token")
	rrInvalid := httptest.NewRecorder()
	handler.ServeHTTP(rrInvalid, reqInvalid)
	if rrInvalid.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid token, got %d", rrInvalid.Code)
	}
	if h := rrInvalid.Header().Get("WWW-Authenticate"); h != `Bearer error="invalid_token"` {
		t.Fatalf("expected WWW-Authenticate header with Bearer error=\"invalid_token\", got %q", h)
	}

	// 4b. Telemetry config and /health public routes must succeed without auth
	reqHealth2 := httptest.NewRequest(http.MethodGet, "/health", nil)
	rrHealth2 := httptest.NewRecorder()
	handler.ServeHTTP(rrHealth2, reqHealth2)
	if rrHealth2.Code != http.StatusOK {
		t.Fatalf("expected 200 for public /health, got %d", rrHealth2.Code)
	}

	reqTelem := httptest.NewRequest(http.MethodGet, "/v1/telemetry/config", nil)
	rrTelem := httptest.NewRecorder()
	handler.ServeHTTP(rrTelem, reqTelem)
	if rrTelem.Code != http.StatusOK {
		t.Fatalf("expected 200 for public /v1/telemetry/config, got %d", rrTelem.Code)
	}

	// 5. Protected route with valid token must succeed
	reqValid := httptest.NewRequest(http.MethodGet, "/v1/prompts", nil)
	reqValid.Header.Set("Authorization", "Bearer enterprise-secret-token-123")
	rrValid := httptest.NewRecorder()
	handler.ServeHTTP(rrValid, reqValid)
	if rrValid.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid token, got %d: %s", rrValid.Code, rrValid.Body.String())
	}

	// 6. Mutating POST with valid token
	reqPost := httptest.NewRequest(http.MethodPost, "/v1/prompts", bytes.NewBufferString(`{"title":"test prompt","body":"hello","engine":"agent"}`))
	reqPost.Header.Set("Authorization", "Bearer enterprise-secret-token-123")
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusCreated && rrPost.Code != http.StatusOK {
		t.Fatalf("expected 201 Created for authorized prompt creation, got %d: %s", rrPost.Code, rrPost.Body.String())
	}
}
