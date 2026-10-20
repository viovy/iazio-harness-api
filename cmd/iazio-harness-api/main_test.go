package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/pgstore"
)

func TestVersionFlag(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"iazio-harness-api", "version"}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := run(context.Background(), os.Args)

	_ = w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	out := buf.String()
	if !strings.Contains(out, "iazio-harness-api") {
		t.Fatalf("expected version output to contain 'iazio-harness-api', got %q", out)
	}
}

func TestSeedDefaults(t *testing.T) {
	e := control.NewEngine(nil)
	cbStore := codebase.NewMemStore()

	t.Setenv("IAZIO_KNOWN_HOSTS", "host-a, host-b")
	t.Setenv("IAZIO_DEFAULT_REPOS", `[{"HostID":"host-a","WorktreePath":"/repo/a","DefaultBranch":"main"}]`)

	seedDefaults(context.Background(), e, nil, cbStore)

	prompts := e.ListPrompts()
	if len(prompts) < 2 {
		t.Errorf("expected at least 2 seeded prompts, got %d", len(prompts))
	}

	if _, ok := e.GetHostDetail("host-a"); !ok {
		t.Error("expected host-a to be registered")
	}
	if _, ok := e.GetHostDetail("host-b"); !ok {
		t.Error("expected host-b to be registered")
	}

	if _, ok := e.GetRepo("host-a", "/repo/a"); !ok {
		t.Error("expected repo /repo/a to be registered")
	}

	repos, err := cbStore.ListRepositories(context.Background(), codebase.ListReposRequest{Limit: 10})
	if err != nil || repos.Total == 0 {
		t.Errorf("expected default fleet repos in cbStore, got err=%v total=%d", err, repos.Total)
	}
}

func getFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestRunServerGracefulShutdown(t *testing.T) {
	addr := getFreeAddr(t)
	t.Setenv("IAZIO_HARNESS_API_ADDR", addr)
	t.Setenv("IAZIO_AUTH_TOKEN", "test-token-xyz")
	t.Setenv("IAZIO_KNOWN_HOSTS", "test-h1")

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{"iazio-harness-api"})
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestRunServerTokenFallback(t *testing.T) {
	addr := getFreeAddr(t)
	t.Setenv("IAZIO_HARNESS_API_ADDR", addr)
	t.Setenv("IAZIO_AUTH_TOKEN", "")
	t.Setenv("IAZIO_HARNESS_API_TOKEN", "legacy-token-abc")

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{"iazio-harness-api"})
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestRunServerWithDatabase(t *testing.T) {
	dsn := "postgres://iazio:iazio@127.0.0.1:55432/iazio?sslmode=disable"
	// Check if DB is reachable
	testCtx, cancelTest := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelTest()
	tmpStore, err := pgstore.Open(testCtx, dsn)
	if err != nil {
		t.Skipf("skipping db test, postgres not reachable: %v", err)
	}
	tmpStore.Close()

	addr := getFreeAddr(t)
	t.Setenv("IAZIO_HARNESS_API_ADDR", addr)
	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("IAZIO_EXTRACT_WORKER", "/nonexistent/worker.py")

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{"iazio-harness-api"})
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}
