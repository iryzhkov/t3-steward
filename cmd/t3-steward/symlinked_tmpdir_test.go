package main

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

// The artifact download tests, whose output directory must be real, pass
// again when TMPDIR is a symlink to a real directory.
func TestArtifactTestsPassWithASymlinkedTMPDIR(t *testing.T) {
	testutil.RerunWithSymlinkedTempDir(t, "^Test(Artifact|SafeTerminal|OpenCheckedOutputDirectory).*")
}
