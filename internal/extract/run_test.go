package extract

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunEmptyWorkerAndShareURL(t *testing.T) {
	if _, _, err := Run(context.Background(), "", "https://example.com"); err == nil {
		t.Fatal("expected error when worker is empty, got nil")
	}
	if _, _, err := Run(context.Background(), "native", "   "); err == nil {
		t.Fatal("expected error when shareURL is empty, got nil")
	}
}

func TestValidateGeminiShareURL(t *testing.T) {
	if err := ValidateGeminiShareURL("https://gemini.google.com/share/abc123"); err != nil {
		t.Fatalf("expected valid Gemini share URL, got %v", err)
	}
	if err := ValidateGeminiShareURL("https://gemini.google.com/share/"); err == nil {
		t.Fatal("expected error for empty share ID")
	}
	if err := ValidateGeminiShareURL("https://example.com/share/abc"); err == nil {
		t.Fatal("expected error for non-Gemini host")
	}
}

func TestRunNativeHTTP_HTMLAndPlainText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/share/html":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<!doctype html>
<html>
<head><title>Gemini Share Title</title><script>console.log("ignore");</script></head>
<body>
  <main>
    <p>User prompt line</p>
    <div>Assistant response &amp; details</div>
  </main>
</body>
</html>`))
		case "/share/single":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("OnlyOneLine"))
		case "/share/empty":
			w.WriteHeader(http.StatusOK)
		case "/share/fail":
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	title, transcript, err := Run(context.Background(), "native", srv.URL+"/share/html")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if title != "Gemini Share Title" {
		t.Errorf("expected title 'Gemini Share Title', got %q", title)
	}
	want := "Gemini Share Title\nUser prompt line\nAssistant response & details"
	if transcript != want {
		t.Errorf("unexpected transcript:\ngot:  %q\nwant: %q", transcript, want)
	}

	titleSingle, transcriptSingle, err := Run(context.Background(), "http", srv.URL+"/share/single")
	if err != nil {
		t.Fatalf("unexpected error on single line: %v", err)
	}
	if titleSingle != "OnlyOneLine" || transcriptSingle != "OnlyOneLine" {
		t.Errorf("unexpected single line output: title=%q transcript=%q", titleSingle, transcriptSingle)
	}

	titleEmpty, transcriptEmpty, err := Run(context.Background(), "native", srv.URL+"/share/empty")
	if err != nil {
		t.Fatalf("unexpected error on empty response: %v", err)
	}
	if titleEmpty != "imported" || transcriptEmpty != "" {
		t.Errorf("unexpected empty response output: title=%q transcript=%q", titleEmpty, transcriptEmpty)
	}

	if _, _, err := Run(context.Background(), "native", srv.URL+"/share/fail"); err == nil {
		t.Fatal("expected HTTP error on 502 status")
	}

	if _, _, err := Run(context.Background(), "native", "://bad-url"); err == nil {
		t.Fatal("expected error on malformed URL")
	}
}

func TestRunCommandWorkerMultiLineAndSingleLine(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test skipped on windows")
	}
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "worker.sh")
	scriptContent := "#!/bin/sh\nprintf 'First Line Title\\nSecond Line Content\\nThird Line Content\\n'\n"
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0o755); err != nil {
		t.Fatal(err)
	}

	title, transcript, err := Run(context.Background(), scriptPath, "https://gemini.google.com/share/abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if title != "First Line Title" {
		t.Errorf("expected title 'First Line Title', got %q", title)
	}
	if transcript != "First Line Title\nSecond Line Content\nThird Line Content" {
		t.Errorf("unexpected transcript: %q", transcript)
	}

	singlePath := filepath.Join(tmpDir, "worker_single.sh")
	if err := os.WriteFile(singlePath, []byte("#!/bin/sh\nprintf 'OnlyOneLine\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	title2, transcript2, err := Run(context.Background(), singlePath, "https://gemini.google.com/share/abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if title2 != "OnlyOneLine" || transcript2 != "OnlyOneLine" {
		t.Errorf("unexpected single line output: title=%q transcript=%q", title2, transcript2)
	}
}

func TestRunCommandWorkerErrorAndTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test skipped on windows")
	}
	tmpDir := t.TempDir()
	errPath := filepath.Join(tmpDir, "worker_err.sh")
	if err := os.WriteFile(errPath, []byte("#!/bin/sh\necho 'Worker failed miserably' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, _, err := Run(context.Background(), errPath, "https://gemini.google.com/share/abc")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "Worker failed miserably" {
		t.Errorf("expected 'Worker failed miserably', got %q", err.Error())
	}

	sleepPath := filepath.Join(tmpDir, "worker_sleep.sh")
	if err := os.WriteFile(sleepPath, []byte("#!/bin/sh\nsleep 10\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, _, err = Run(ctx, sleepPath, "https://gemini.google.com/share/abc")
	if err == nil || !strings.Contains(err.Error(), "extract deadline") {
		t.Fatalf("expected extract deadline error, got %v", err)
	}
}

func TestRunHeadlessBrowserModeValidationAndMockBinary(t *testing.T) {
	if _, _, err := Run(context.Background(), "chromium", "https://example.com/not-gemini"); err == nil {
		t.Fatal("expected bad share url error for non-Gemini URL in chromium mode")
	}

	if runtime.GOOS == "windows" {
		t.Skip("shell script mock browser skipped on windows")
	}
	tmpDir := t.TempDir()
	mockChromium := filepath.Join(tmpDir, "chromium")
	script := "#!/bin/sh\nprintf '<html><head><title>Headless Title</title></head><body><p>Rendered Body</p></body></html>'\n"
	if err := os.WriteFile(mockChromium, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	title, transcript, err := Run(context.Background(), mockChromium, "https://gemini.google.com/share/12345")
	if err != nil {
		t.Fatalf("unexpected error from mock chromium: %v", err)
	}
	if title != "Headless Title" {
		t.Errorf("expected title 'Headless Title', got %q", title)
	}
	if transcript != "Headless Title\nRendered Body" {
		t.Errorf("unexpected transcript %q", transcript)
	}
}

func TestNoPythonWorkerScriptRemainsOnDisk(t *testing.T) {
	if _, err := os.Stat("gemini_worker.py"); !os.IsNotExist(err) {
		t.Fatalf("gemini_worker.py must be deleted (Go-native runtime hygiene), stat err=%v", err)
	}
}
