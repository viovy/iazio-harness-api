package extract

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunEmptyScript(t *testing.T) {
	_, _, err := Run(context.Background(), "", "https://example.com")
	if err == nil {
		t.Fatalf("expected error when script is empty, got nil")
	}
}

func TestRunSuccessMultiLine(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "worker.py")
	scriptContent := `
import os
print("First Line Title")
print("Second Line Content")
print("Third Line Content")
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	title, transcript, err := Run(context.Background(), scriptPath, "https://example.com/share")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if title != "First Line Title" {
		t.Errorf("expected title 'First Line Title', got %q", title)
	}
	if transcript != "First Line Title\nSecond Line Content\nThird Line Content" {
		t.Errorf("unexpected transcript: %q", transcript)
	}
}

func TestRunSuccessSingleLine(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "worker_single.py")
	scriptContent := `print("OnlyOneLine")`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	title, transcript, err := Run(context.Background(), scriptPath, "https://example.com/share")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if title != "OnlyOneLine" {
		t.Errorf("expected title 'OnlyOneLine', got %q", title)
	}
	if transcript != "OnlyOneLine" {
		t.Errorf("unexpected transcript: %q", transcript)
	}
}

func TestRunScriptError(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "worker_err.py")
	scriptContent := `
import sys
sys.stderr.write("Worker failed miserably")
sys.exit(1)
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	_, _, err := Run(context.Background(), scriptPath, "https://example.com/share")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "Worker failed miserably" {
		t.Errorf("expected 'Worker failed miserably', got %q", err.Error())
	}
}

func TestRunScriptTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "worker_sleep.py")
	scriptContent := `
import time
time.sleep(10)
`
	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _, err := Run(ctx, scriptPath, "https://example.com/share")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}
