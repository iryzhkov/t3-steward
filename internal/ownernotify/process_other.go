//go:build !unix

package ownernotify

import "os/exec"

// isolateProcessGroup is a no-op where process groups are not available; the
// context still kills the program itself on timeout.
func isolateProcessGroup(*exec.Cmd) {}
