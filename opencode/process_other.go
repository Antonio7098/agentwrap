//go:build !unix && !windows

package opencode

import (
	"os"
	"os/exec"
)

func configureProcessGroup(*exec.Cmd) {}

func signalProcessGroup(proc *os.Process, _ bool) error {
	if proc == nil {
		return os.ErrProcessDone
	}
	return proc.Kill()
}
