//go:build unix

package campaign

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// A campaign carries regular files. Anything else would either block the
// packer or produce an archive entry the coordinator forbids, so it is refused
// while it is still a directory.
func TestLoadRefusesSpecialFiles(t *testing.T) {
	root := t.TempDir()
	files := validTree()
	files["workflow.yaml"] = strings.Replace(validManifest, "  - inputs/plan.md\n", "  - inputs/pipe\n", 1)
	delete(files, "inputs/plan.md")
	writeTree(t, root, files, nil)
	if err := os.MkdirAll(filepath.Join(root, "inputs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "inputs", "pipe"), 0o600); err != nil {
		t.Skipf("this filesystem cannot create a named pipe: %v", err)
	}
	_, err := Load(root, DefaultLimits)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("named pipe error = %v", err)
	}
}
