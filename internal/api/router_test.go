package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

func TestRouter_AuthMiddleware(t *testing.T) {
	store := codebase.NewMemStore()
	_ = store.UpsertRepository(context.Background(), codebase.Repository{
		Name: "iazio-mcp",
		Path: "iazio-mcp",
	})

	validToken := "secret-bearer-token-123"
	router := NewRouter(RouterConfig{
		Store:     store,
		AuthToken: validToken,
	})

	// 1. Missing Authorization header -> 401
	reqNoAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	recNoAuth := httptest.NewRecorder()
	router.ServeHTTP(recNoAuth, reqNoAuth)

	if recNoAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", recNoAuth.Code)
	}

	// 2. Invalid Bearer token -> 401
	reqBadAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	reqBadAuth.Header.Set("Authorization", "Bearer wrong-token")
	recBadAuth := httptest.NewRecorder()
	router.ServeHTTP(recBadAuth, reqBadAuth)

	if recBadAuth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for bad token, got %d", recBadAuth.Code)
	}

	// 3. Valid Bearer token -> 200 OK
	reqValidAuth := httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil)
	reqValidAuth.Header.Set("Authorization", "Bearer "+validToken)
	recValidAuth := httptest.NewRecorder()
	router.ServeHTTP(recValidAuth, reqValidAuth)

	if recValidAuth.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for valid token, got %d", recValidAuth.Code)
	}

	// 4. Codebase search with valid token
	searchPayload := map[string]any{"query": "mcp"}
	searchBody, _ := json.Marshal(searchPayload)
	reqSearch := httptest.NewRequest(http.MethodPost, "/api/v1/codebase/search", bytes.NewReader(searchBody))
	reqSearch.Header.Set("Authorization", "Bearer "+validToken)
	recSearch := httptest.NewRecorder()
	router.ServeHTTP(recSearch, reqSearch)

	if recSearch.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on search with valid token, got %d", recSearch.Code)
	}
}
