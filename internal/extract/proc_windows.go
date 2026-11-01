//go:build windows

package extract

import (
	"os/exec"

	"github.com/viovy/iazio-harness-api/internal/procutil"
)

func setSysProcAttr(cmd *exec.Cmd) {
	procutil.HideWindow(cmd)
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
