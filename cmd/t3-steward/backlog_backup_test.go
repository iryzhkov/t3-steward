package main

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// The snapshot bound must not be derived from the message limits again. A
// coordinator that has run a few hundred tasks holds more retained artifact
// files than any single message may carry, and deriving one from the other made
// the supported backup command refuse on precisely those coordinators.
func TestBackupBoundsAreNotMessageBounds(t *testing.T) {
	limits := backupSnapshotLimits()
	messages := config.Config{}.BacklogV2.MessageLimits
	messages.MaxFiles = 1000
	messages.MaxArtifactBytes = 16 << 20

	if limits.MaxFiles <= messages.MaxFiles*100 {
		t.Errorf("snapshot file bound %d is in the same range as a message's %d",
			limits.MaxFiles, messages.MaxFiles)
	}
	if limits.MaxBytes <= messages.MaxArtifactBytes*1000 {
		t.Errorf("snapshot byte bound %d is in the same range as a message's %d",
			limits.MaxBytes, messages.MaxArtifactBytes)
	}
}
