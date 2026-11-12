// Package extract runs the native Go share-page extractor.
package extract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/procutil"
)

// Deadline is the extractor's overall limit. Any external process group is killed when it elapses.
const Deadline = 45 * time.Second

// MaxResponseBytes bounds the maximum HTML body read during native HTTP extraction (4 MiB).
const MaxResponseBytes = 4 * 1024 * 1024

var (
	scriptStyleBlockRegex = regexp.MustCompile(`(?is)<(script|style|noscript|svg|head)\b[^>]*>.*?</(script|style|noscript|svg|head)>`)
	titleTagRegex         = regexp.MustCompile(`(?is)<title\b[^>]*>(.*?)</title>`)
	blockTagRegex         = regexp.MustCompile(`(?is)</?(p|div|section|article|main|header|footer|li|tr|h[1-6]|pre|blockquote|br|hr)\b[^>]*>`)
	anyTagRegex           = regexp.MustCompile(`(?is)<[^>]+>`)
	multiSpaceRegex       = regexp.MustCompile(`[^\S\n]+`)
	multiNewlineRegex     = regexp.MustCompile(`\n{3,}`)
)

// ValidateGeminiShareURL validates that shareURL is a well-formed Gemini share link
// when strict Gemini share validation is requested.
func ValidateGeminiShareURL(shareURL string) error {
	trimmed := strings.TrimSpace(shareURL)
	if !strings.HasPrefix(trimmed, "https://gemini.google.com/share/") || len(trimmed) <= len("https://gemini.google.com/share/") {
		return errors.New("bad share url")
	}
	return nil
}

// Run executes the share-page extractor for one share URL.
// worker specifies the extractor mode or command path (configured via IAZIO_EXTRACT_WORKER):
//   - "native" or "http": executes the built-in Go HTTP/HTML share-page extractor.
//   - "chromium" or "chrome": renders the share page via headless Chromium/Chrome CLI flags (--headless=new --dump-dom).
//   - path to an executable binary or script: executes the worker with SHARE_URL=<shareURL>.
//
// An empty worker string returns an error.
func Run(ctx context.Context, worker, shareURL string) (title, transcript string, err error) {
	worker = strings.TrimSpace(worker)
	if worker == "" {
		return "", "", fmt.Errorf("extract worker is not configured")
	}
	shareURL = strings.TrimSpace(shareURL)
	if shareURL == "" {
		return "", "", fmt.Errorf("share url is empty")
	}

	ctx, cancel := context.WithTimeout(ctx, Deadline)
	defer cancel()

	switch strings.ToLower(worker) {
	case "native", "http", "go":
		return RunNativeHTTP(ctx, shareURL)
	case "chromium", "chrome", "headless-chromium":
		return runHeadlessBrowser(ctx, "", shareURL)
	default:
		return runCommandWorker(ctx, worker, shareURL)
	}
}

// RunNativeHTTP fetches shareURL over HTTP/HTTPS in pure Go, extracts the visible
// document title and body text, and normalizes line breaks.
func RunNativeHTTP(ctx context.Context, shareURL string) (title, transcript string, err error) {
	parsed, err := url.Parse(strings.TrimSpace(shareURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", "", fmt.Errorf("bad share url")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", "iazio-harness-api-extractor/1.0")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8")

	client := &http.Client{
		Timeout: Deadline,
	}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", "", fmt.Errorf("extract deadline")
		}
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("extract http status %d", resp.StatusCode)
	}

	rawBytes, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes))
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", "", fmt.Errorf("extract deadline")
		}
		return "", "", err
	}

	return ExtractVisibleTranscriptFromHTML(string(rawBytes))
}

// ExtractVisibleTranscriptFromHTML parses an HTML or plain-text payload into a
// normalized (title, transcript) pair.
func ExtractVisibleTranscriptFromHTML(raw string) (title, transcript string, err error) {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "imported", "", nil
	}

	var extractedTitle string
	if m := titleTagRegex.FindStringSubmatch(trimmed); len(m) > 1 {
		extractedTitle = strings.TrimSpace(html.UnescapeString(anyTagRegex.ReplaceAllString(m[1], "")))
	}

	cleaned := scriptStyleBlockRegex.ReplaceAllString(trimmed, "\n")
	cleaned = blockTagRegex.ReplaceAllString(cleaned, "\n")
	cleaned = anyTagRegex.ReplaceAllString(cleaned, " ")
	cleaned = html.UnescapeString(cleaned)

	lines := strings.Split(cleaned, "\n")
	var normLines []string
	for _, line := range lines {
		line = strings.TrimSpace(multiSpaceRegex.ReplaceAllString(line, " "))
		if line != "" {
			normLines = append(normLines, line)
		}
	}

	if extractedTitle != "" && (len(normLines) == 0 || normLines[0] != extractedTitle) {
		normLines = append([]string{extractedTitle}, normLines...)
	}

	text := strings.TrimSpace(multiNewlineRegex.ReplaceAllString(strings.Join(normLines, "\n"), "\n\n"))
	return normalizeOutput(text)
}

func runHeadlessBrowser(ctx context.Context, browserBin, shareURL string) (string, string, error) {
	if err := ValidateGeminiShareURL(shareURL); err != nil {
		return "", "", err
	}
	if browserBin == "" {
		for _, candidate := range []string{"chromium", "chromium-browser", "google-chrome", "chrome"} {
			if path, err := exec.LookPath(candidate); err == nil && path != "" {
				browserBin = path
				break
			}
		}
	}
	if browserBin == "" {
		return "", "", fmt.Errorf("renderer unavailable")
	}

	cmd := procutil.CommandContext(ctx, browserBin,
		"--headless=new",
		"--no-sandbox",
		"--disable-dev-shm-usage",
		"--disable-gpu",
		"--dump-dom",
		shareURL,
	)
	out, err := executeBoundedCommand(ctx, cmd)
	if err != nil {
		return "", "", err
	}
	return ExtractVisibleTranscriptFromHTML(out)
}

func runCommandWorker(ctx context.Context, worker, shareURL string) (string, string, error) {
	base := strings.ToLower(worker)
	if strings.HasSuffix(base, "chromium") || strings.HasSuffix(base, "chromium-browser") || strings.HasSuffix(base, "google-chrome") || strings.HasSuffix(base, "chrome") {
		return runHeadlessBrowser(ctx, worker, shareURL)
	}

	var cmd *exec.Cmd
	if strings.HasSuffix(base, ".sh") {
		cmd = procutil.CommandContext(ctx, "sh", worker)
	} else {
		cmd = procutil.CommandContext(ctx, worker)
	}
	cmd.Env = append(os.Environ(), "SHARE_URL="+shareURL)

	out, err := executeBoundedCommand(ctx, cmd)
	if err != nil {
		return "", "", err
	}
	if strings.Contains(strings.ToLower(out), "<html") || strings.Contains(strings.ToLower(out), "<body") {
		return ExtractVisibleTranscriptFromHTML(out)
	}
	return normalizeOutput(out)
}

func executeBoundedCommand(ctx context.Context, cmd *exec.Cmd) (string, error) {
	setSysProcAttr(cmd)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var err error
	select {
	case err = <-wait:
	case <-ctx.Done():
		killProcessGroup(cmd)
		err = fmt.Errorf("extract deadline")
		<-wait
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("%s", msg)
	}
	return stdout.String(), nil
}

func normalizeOutput(raw string) (title, transcript string, err error) {
	text := strings.TrimSpace(raw)
	text = strings.ReplaceAll(text, "\r\n", "\n")
	title = "imported"
	if line, _, ok := strings.Cut(text, "\n"); ok && line != "" {
		title = line
	} else if text != "" {
		title = text
	}
	return title, text, nil
}
