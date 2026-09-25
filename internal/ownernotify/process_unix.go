//go:build unix

package ownernotify

import (
	"os/exec"
	"syscall"
)

// isolateProcessGroup starts the command as the leader of a new process group
// and makes cancellation kill that whole group, so a notification script that
// forked a child cannot leave it running past its timeout.
func isolateProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
}
