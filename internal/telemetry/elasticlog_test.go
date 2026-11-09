package telemetry

import (
	"context"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestElasticSink_SendCentralTelemetryGateway(t *testing.T) {
	var requestCount atomic.Int32
	var receivedAuth string
	var receivedBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		if r.URL.Path != "/v1/telemetry/logs" {
			t.Errorf("expected path /v1/telemetry/logs, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected json content type, got %s", r.Header.Get("Content-Type"))
		}
		receivedAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted","index":"iazio-logs"}`))
	}))
	defer ts.Close()

	sink := NewElasticSink(ElasticConfig{
		URL:           ts.URL,
		IsGateway:     true,
		AuthToken:     "harness-api-secret-token",
		ServiceName:   "iazio-harness-api",
		Version:       "0.1.0",
		HostName:      "docker1",
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		SpoolDir:      t.TempDir(),
	})

	sink.Log(LevelInfo, "control plane initialized", nil, "job-101", map[string]any{"phase": "boot"})
	time.Sleep(150 * time.Millisecond)
	_ = sink.Close()

	if requestCount.Load() == 0 {
		t.Fatalf("expected at least 1 request to mock gateway, got 0")
	}
	if receivedAuth != "Bearer harness-api-secret-token" {
		t.Fatalf("expected Authorization header Bearer harness-api-secret-token, got %s", receivedAuth)
	}
	if !strings.Contains(string(receivedBody), "control plane initialized") {
		t.Fatalf("expected body to contain message, got: %s", string(receivedBody))
	}
	if !strings.Contains(string(receivedBody), "iazio-harness-api") {
		t.Fatalf("expected body to contain service.name iazio-harness-api, got: %s", string(receivedBody))
	}
}

func TestSlogHandler_ForwardsToSinkWithActiveTrace(t *testing.T) {
	var requestCount atomic.Int32
	var receivedBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer ts.Close()

	sink := NewElasticSink(ElasticConfig{
		URL:           ts.URL,
		IsGateway:     true,
		ServiceName:   "iazio-harness-api",
		BatchSize:     1,
		FlushInterval: 20 * time.Millisecond,
		SpoolDir:      t.TempDir(),
	})

	ctx, span := StartSpan(context.Background(), "test.op")
	defer span.End()

	logger := slog.New(NewSlogHandler(sink, slog.NewJSONHandler(io.Discard, nil)))
	logger.InfoContext(ctx, "job leased to host", slog.String("job_id", "job-5211"), slog.String("story_id", "STORY-GOV-5211"))

	time.Sleep(100 * time.Millisecond)
	_ = sink.Close()

	if requestCount.Load() == 0 {
		t.Fatalf("expected log shipped to gateway, got 0 requests")
	}
	bodyStr := string(receivedBody)
	if !strings.Contains(bodyStr, "job leased to host") {
		t.Fatalf("expected body to contain log message, got %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "job-5211") {
		t.Fatalf("expected body to contain job.id job-5211, got %s", bodyStr)
	}
	if !strings.Contains(bodyStr, span.TraceID) {
		t.Fatalf("expected body to contain active span trace.id %s, got %s", span.TraceID, bodyStr)
	}
}

func TestResolveTelemetryDestination(t *testing.T) {
	u, isGw := ResolveTelemetryDestination("", func(k string) string {
		if k == "CENTRAL_TELEMETRY_GATEWAY_URL" {
			return "https://env-gateway.local/central-telemetry-gateway"
		}
		return ""
	})
	if u != "https://env-gateway.local/central-telemetry-gateway" || !isGw {
		t.Fatalf("expected env gateway url, got %q, isGw=%v", u, isGw)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/telemetry/config" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"central_telemetry_gateway_url":"https://api-discovered-harness.local","status":"healthy"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	u2, isGw2 := ResolveTelemetryDestination(ts.URL, func(string) string { return "" })
	if u2 != "https://api-discovered-harness.local" || !isGw2 {
		t.Fatalf("expected api discovered gateway url, got %q, isGw=%v", u2, isGw2)
	}

	tmpDir := t.TempDir()
	t.Setenv("USERPROFILE", tmpDir)
	t.Setenv("HOME", tmpDir)

	_ = os.MkdirAll(filepath.Join(tmpDir, ".iazio"), 0755)
	_ = os.WriteFile(filepath.Join(tmpDir, ".iazio", "harness-api.json"), []byte(`{"central_telemetry_gateway_url":"https://file-discovered-harness-api.local"}`), 0600)

	u3, isGw3 := ResolveTelemetryDestination("", func(string) string { return "" })
	if u3 != "https://file-discovered-harness-api.local" || !isGw3 {
		t.Fatalf("expected file discovered gateway url, got %q, isGw=%v", u3, isGw3)
	}

	_ = os.Remove(filepath.Join(tmpDir, ".iazio", "harness-api.json"))
	u4, isGw4 := ResolveTelemetryDestination("", func(k string) string {
		if k == "IAZIO_ELASTICSEARCH_URL" {
			return "http://direct.es.local:9200"
		}
		return ""
	})
	if u4 != "http://direct.es.local:9200" || isGw4 {
		t.Fatalf("expected direct ES url, got %q, isGw=%v", u4, isGw4)
	}
}

func TestNewSlogHandler_SetDefault_DoesNotDeadlockLogPrintf(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sink := NewElasticSink(ElasticConfig{
		URL:         ts.URL,
		IsGateway:   true,
		ServiceName: "iazio-harness-api",
		SpoolDir:    t.TempDir(),
	})
	defer sink.Close()

	origDefault := slog.Default()
	defer slog.SetDefault(origDefault)

	slog.SetDefault(slog.New(NewSlogHandler(sink, slog.Default().Handler())))

	done := make(chan struct{})
	go func() {
		log.Printf("[iazio-harness-api] Loaded control plane state")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("log.Printf deadlocked after slog.SetDefault(slog.New(NewSlogHandler(sink, slog.Default().Handler())))")
	}
}
