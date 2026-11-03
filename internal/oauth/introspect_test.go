package oauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntrospectorAndScopes(t *testing.T) {
	var calls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if r.URL.Path != "/admin/oauth2/introspect" {
			http.NotFound(w, r)
			return
		}
		_ = r.ParseForm()
		tok := r.FormValue("token")
		w.Header().Set("Content-Type", "application/json")
		switch tok {
		case "valid-agent-token":
			_ = json.NewEncoder(w).Encode(IntrospectionResult{
				Active:   true,
				ClientID: "iazio-agent-cli",
				Sub:      "user-1",
				Scope:    "openid offline_access harness:read harness:write",
				Exp:      time.Now().Add(10 * time.Minute).Unix(),
			})
		case "inactive-token":
			_ = json.NewEncoder(w).Encode(IntrospectionResult{Active: false})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	in := NewIntrospector(ts.URL)
	if !in.Enabled() {
		t.Fatal("expected introspector to be enabled")
	}

	ctx := context.Background()
	res, err := in.Introspect(ctx, "valid-agent-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Active || res.ClientID != "iazio-agent-cli" || res.AuthMethod != "oauth2" {
		t.Fatalf("unexpected result: %+v", res)
	}
	if !res.HasScope("harness:read") || !res.HasScope("harness:write") || res.HasScope("credentials:manage") {
		t.Fatalf("unexpected scope checks for %q", res.Scope)
	}

	// Second call should hit cache
	res2, err := in.Introspect(ctx, "valid-agent-token")
	if err != nil || !res2.Active {
		t.Fatalf("expected cached active result, got %+v, err=%v", res2, err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 HTTP call due to cache, got %d", got)
	}

	// Inactive token
	inactive, err := in.Introspect(ctx, "inactive-token")
	if err != nil || inactive.Active {
		t.Fatalf("expected inactive result, got %+v, err=%v", inactive, err)
	}

	// RequiredScopeForRequest checks
	if got := RequiredScopeForRequest(http.MethodGet, "/v1/hosts"); got != "harness:read" {
		t.Errorf("expected harness:read, got %q", got)
	}
	if got := RequiredScopeForRequest(http.MethodPost, "/v1/hosts/debian/heartbeat"); got != "harness:write" {
		t.Errorf("expected harness:write, got %q", got)
	}
	if got := RequiredScopeForRequest(http.MethodPost, "/v1/credentials"); got != "credentials:manage" {
		t.Errorf("expected credentials:manage, got %q", got)
	}
	if got := RequiredScopeForRequest(http.MethodGet, "/v1/auth/whoami"); got != "" {
		t.Errorf("expected empty scope for whoami, got %q", got)
	}

	// Principal context round-trip
	pCtx := WithPrincipal(ctx, res)
	if p, ok := PrincipalFromContext(pCtx); !ok || p.ClientID != "iazio-agent-cli" {
		t.Errorf("expected principal from context, got %+v, ok=%v", p, ok)
	}
}
