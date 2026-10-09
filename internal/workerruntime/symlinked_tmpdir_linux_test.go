//go:build linux

package workerruntime

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

// The tests that pass the temporary directory to code refusing symlinks pass
// again when TMPDIR is a symlink to a real directory.
func TestRefusingTestsPassWithASymlinkedTMPDIR(t *testing.T) {
	testutil.RerunWithSymlinkedTempDir(t, "^(TestAttentionStopObservationIsProducedByRuntimeAndConsumedBySQLite|TestContainedPreparationReusesPublishedWorkspaceAfterReadinessFailure|TestLiveCommands.*|TestScannedCommandLinesAreRedactedBeforeTheyAreShortened|TestSnapshotRunsNoCommandTheTaskConfigured|TestInventorySnapshotWarnsAboutASymlinkedTMPDIR)$")
}
