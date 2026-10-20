//go:build windows

package extract

import (
	"os/exec"
)

func setSysProcAttr(cmd *exec.Cmd) {
	// Windows does not require specific process-group attributes for extract workers.
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
