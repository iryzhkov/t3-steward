//go:build linux

package testutil

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
)

// RerunWithUmasks checks matching tests under each supported fixture umask.
// The shell child sets the mask before exec; the parent process never changes it.
func RerunWithUmasks(t *testing.T, pattern string) {
	t.Helper()
	const marker = "T3_STEWARD_TEST_UMASK_CHILD"
	if os.Getenv(marker) != "" {
		t.Skip("already running in a umask child")
	}
	for _, mask := range []string{"0002", "0022", "0077"} {
		t.Run(mask, func(t *testing.T) {
			cmd := exec.Command("/bin/sh", "-c", "umask "+mask+" && exec \"$@\"", "umask-child",
				os.Args[0], "-test.v", "-test.count=1", "-test.run="+pattern)
			cmd.Env = append(os.Environ(), marker+"="+mask)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("umask %s: %v\n%s", mask, err, out)
			}
			if bytes.Contains(out, []byte("no tests to run")) || !bytes.Contains(out, []byte("--- PASS:")) {
				t.Fatalf("umask %s ran no passing tests:\n%s", mask, out)
			}
			t.Logf("umask %s child output:\n%s", mask, out)
		})
	}
}
