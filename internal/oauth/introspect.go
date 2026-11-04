// Package oauth provides Ory Hydra OAuth2 token introspection and scope authorization.
package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// IntrospectionResult represents the OAuth 2.0 RFC 7662 token introspection response.
type IntrospectionResult struct {
	Active     bool   `json:"active"`
	Scope      string `json:"scope,omitempty"`
	ClientID   string `json:"client_id,omitempty"`
	Sub        string `json:"sub,omitempty"`
	Exp        int64  `json:"exp,omitempty"`
	Iat        int64  `json:"iat,omitempty"`
	Iss        string `json:"iss,omitempty"`
	TokenType  string `json:"token_type,omitempty"`
	TokenUse   string `json:"token_use,omitempty"`
	AuthMethod string `json:"auth_method,omitempty"`
}

// HasScope reports whether the space-delimited Scope string contains required
// (or an equivalent Gemini/MCP scope grant for harness:read / harness:write).
func (r IntrospectionResult) HasScope(required string) bool {
	required = strings.TrimSpace(required)
	if required == "" {
		return true
	}
	for _, s := range strings.Fields(r.Scope) {
		if s == required {
			return true
		}
		switch required {
		case "harness:write":
			if s == "autopilot:write" || s == "iazio:mcp" {
				return true
			}
		case "harness:read":
			if s == "harness:write" || s == "workspace:read" || s == "autopilot:write" || s == "iazio:mcp" {
				return true
			}
		}
	}
	return false
}

type cachedEntry struct {
	result    IntrospectionResult
	expiresAt time.Time
}

// Introspector validates OAuth2 Bearer tokens against Ory Hydra's /admin/oauth2/introspect endpoint.
type Introspector struct {
	AdminURL   string
	HTTPClient *http.Client
	CacheTTL   time.Duration

	mu    sync.RWMutex
	cache map[string]cachedEntry
}

// NewIntrospector constructs an Introspector for the given Hydra Admin URL.
func NewIntrospector(adminURL string) *Introspector {
	return &Introspector{
		AdminURL:   strings.TrimRight(strings.TrimSpace(adminURL), "/"),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		CacheTTL:   60 * time.Second,
		cache:      make(map[string]cachedEntry),
	}
}

// Enabled reports whether a Hydra Admin URL is configured.
func (in *Introspector) Enabled() bool {
	return in != nil && strings.TrimSpace(in.AdminURL) != ""
}

// Introspect validates a bearer token using Hydra's RFC 7662 introspection endpoint.
func (in *Introspector) Introspect(ctx context.Context, token string) (IntrospectionResult, error) {
	if !in.Enabled() {
		return IntrospectionResult{}, errors.New("oauth introspector is not configured")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return IntrospectionResult{}, nil
	}

	cacheKey := hashToken(token)
	now := time.Now()

	in.mu.RLock()
	if entry, ok := in.cache[cacheKey]; ok && now.Before(entry.expiresAt) {
		in.mu.RUnlock()
		return entry.result, nil
	}
	in.mu.RUnlock()

	endpoint := strings.TrimRight(in.AdminURL, "/") + "/admin/oauth2/introspect"
	form := url.Values{}
	form.Set("token", token)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return IntrospectionResult{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	client := in.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return IntrospectionResult{}, fmt.Errorf("hydra introspect request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return IntrospectionResult{}, fmt.Errorf("hydra introspect read: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return IntrospectionResult{}, fmt.Errorf("hydra introspect status %d", resp.StatusCode)
	}

	var res IntrospectionResult
	if err := json.Unmarshal(body, &res); err != nil {
		return IntrospectionResult{}, fmt.Errorf("hydra introspect decode: %w", err)
	}
	if !res.Active {
		return IntrospectionResult{Active: false}, nil
	}
	res.AuthMethod = "oauth2"

	ttl := in.CacheTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	expiresAt := now.Add(ttl)
	if res.Exp > 0 {
		if tokenExp := time.Unix(res.Exp, 0); tokenExp.Before(expiresAt) {
			expiresAt = tokenExp
		}
	}
	if expiresAt.After(now) {
		in.mu.Lock()
		if in.cache == nil {
			in.cache = make(map[string]cachedEntry)
		}
		in.cache[cacheKey] = cachedEntry{result: res, expiresAt: expiresAt}
		in.mu.Unlock()
	}

	return res, nil
}

// RequiredScopeForRequest returns the OAuth2 scope required for the given HTTP method and path.
func RequiredScopeForRequest(method, path string) string {
	if strings.HasPrefix(path, "/v1/auth/whoami") || strings.HasPrefix(path, "/api/v1/auth/whoami") {
		return ""
	}
	if strings.HasPrefix(path, "/v1/credentials") || strings.HasPrefix(path, "/api/v1/credentials") {
		return "credentials:manage"
	}
	if isReadOnlyPostPath(path) {
		return "harness:read"
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return "harness:read"
	default:
		return "harness:write"
	}
}

func isReadOnlyPostPath(path string) bool {
	switch path {
	case "/v1/codebase/search", "/api/v1/codebase/search", "/v1/rag/search", "/api/v1/rag/search":
		return true
	}
	return strings.HasPrefix(path, "/v1/search/") || strings.HasPrefix(path, "/api/v1/search/")
}

type principalContextKey struct{}

// WithPrincipal attaches an authenticated IntrospectionResult to ctx.
func WithPrincipal(ctx context.Context, res IntrospectionResult) context.Context {
	return context.WithValue(ctx, principalContextKey{}, res)
}

// PrincipalFromContext retrieves the authenticated IntrospectionResult from ctx, if present.
func PrincipalFromContext(ctx context.Context) (IntrospectionResult, bool) {
	res, ok := ctx.Value(principalContextKey{}).(IntrospectionResult)
	return res, ok
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
