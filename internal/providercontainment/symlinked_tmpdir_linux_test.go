//go:build linux

package providercontainment

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

// Every test of the package passes again when TMPDIR is a symlink to a real
// directory.
func TestPackagePassesWithASymlinkedTMPDIR(t *testing.T) {
	testutil.RerunWithSymlinkedTempDir(t, ".")
}
