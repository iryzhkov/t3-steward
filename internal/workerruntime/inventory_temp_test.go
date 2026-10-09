package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testutil"
)

// A symlinked TMPDIR is reported as a warning naming the link and its target,
// and it does not degrade the worker, because verification and gate commands
// get the resolved directory.
func TestInventorySnapshotWarnsAboutASymlinkedTMPDIR(t *testing.T) {
	root := testutil.RealTempDir(t)
	real := filepath.Join(root, "steward-tmp")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "cache-tmp")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", link)
	t.Setenv("GOTMPDIR", "")
	var warnings []string
	probe := HostInventoryProbe{DataDir: root, Warn: func(warning string) { warnings = append(warnings, warning) }}
	got, err := probe.Observe(context.Background(), domain.WorkerInventory{ID: "worker", AcceptBacklog: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"TMPDIR " + link + " is a symlink to " + real}; !slices.Equal(warnings, want) {
		t.Fatalf("warnings = %q, want %q", warnings, want)
	}
	if got.Health != domain.WorkerHealthReady || !got.AcceptBacklog {
		t.Fatalf("symlinked TMPDIR changed health: %+v", got)
	}

	warnings = nil
	t.Setenv("TMPDIR", real)
	if _, err := probe.Observe(context.Background(), domain.WorkerInventory{ID: "worker"}); err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings for a real TMPDIR = %q", warnings)
	}
}
