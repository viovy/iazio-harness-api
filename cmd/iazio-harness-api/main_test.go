package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"iazio-harness-api", "version"}

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	main()

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	out := buf.String()
	if !strings.Contains(out, "iazio-harness-api") {
		t.Fatalf("expected version output to contain 'iazio-harness-api', got %q", out)
	}
}
