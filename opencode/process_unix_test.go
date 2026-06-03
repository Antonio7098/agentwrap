//go:build unix

package opencode

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureProcessGroupSetsParentDeathSignal(t *testing.T) {
	cmd := exec.Command("opencode")
	configureProcessGroup(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr = nil")
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid = false, want true")
	}
	if cmd.SysProcAttr.Pdeathsig != syscall.SIGTERM {
		t.Fatalf("Pdeathsig = %v, want SIGTERM", cmd.SysProcAttr.Pdeathsig)
	}
}
