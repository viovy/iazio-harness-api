// Package extract runs the headless share-page worker.
package extract

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Deadline is the worker's overall limit. The process group is killed when it elapses.
const Deadline = 45 * time.Second

// Run executes the Python worker for one share URL.
// script is the path in IAZIO_EXTRACT_WORKER. An empty script returns an error.
func Run(ctx context.Context, script, shareURL string) (title, transcript string, err error) {
	if script == "" {
		return "", "", fmt.Errorf("extract worker is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, Deadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", script)
	cmd.Env = append(os.Environ(), "SHARE_URL="+shareURL)
	setSysProcAttr(cmd)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		return "", "", err
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
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
		return "", "", fmt.Errorf("%s", msg)
	}
	text := strings.TrimSpace(stdout.String())
	title = "imported"
	if line, _, ok := strings.Cut(text, "\n"); ok && line != "" {
		title = line
	} else if text != "" {
		title = text
	}
	return title, text, nil
}
