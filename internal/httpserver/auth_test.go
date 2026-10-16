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

	// 4. Protected route with invalid token must be rejected with 401
	reqInvalid := httptest.NewRequest(http.MethodGet, "/v1/prompts", nil)
	reqInvalid.Header.Set("Authorization", "Bearer wrong-token")
	rrInvalid := httptest.NewRecorder()
	handler.ServeHTTP(rrInvalid, reqInvalid)
	if rrInvalid.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid token, got %d", rrInvalid.Code)
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
