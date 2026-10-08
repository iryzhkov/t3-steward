//go:build !unix

package procgroup

import "os/exec"

// isolate is a no-op where process groups are not available; the context
// still kills the program itself and WaitDelay still bounds the wait.
func isolate(*exec.Cmd) {}
