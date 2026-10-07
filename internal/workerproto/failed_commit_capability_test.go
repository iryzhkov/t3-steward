package workerproto

import (
	"slices"
	"testing"
)

func TestFailedCommitCapabilityIsAdvertisedAndOldWorkersRefuse(t *testing.T) {
	const capability = "campaign-failed-commit-v1"
	if !slices.Contains(SupportedPackageCapabilities(), capability) {
		t.Fatal("current worker does not advertise failed-commit support")
	}
	// A required unknown capability is refused, as rc.116 refuses the new one.
	if err := validatePackageCapabilities(ExecutionPackage{RequiredCapabilities: []string{"campaign-failed-commit-future-v99"}}); err == nil {
		t.Fatal("unsupported commit behavior accepted")
	}
	if err := validatePackageCapabilities(ExecutionPackage{RequiredCapabilities: []string{capability}}); err != nil {
		t.Fatal(err)
	}
}
