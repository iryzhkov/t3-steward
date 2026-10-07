package backlog

import (
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// rc.117 combination of main's placement capabilities (M16-0) and M16-3: the
// offer for a review-declared task requires workspace-head-v1, and the offer
// for a consumer of a review-declared producer's commit requires
// accepted-dependencies-v1. Placement must exclude a worker without them, as
// it does for the commit bundle and gate capabilities, rather than choose a
// worker whose offer is then withheld.
func TestPlacementRequiresTheReviewGateCapabilities(t *testing.T) {
	producer := ManifestTask{
		ReviewRequirements: &ManifestReviewRequirements{},
		Commits:            []ManifestCommit{{Name: "change"}},
	}
	manifest := Manifest{Tasks: map[string]ManifestTask{"producer": producer}}
	if got := placementCapabilities(manifest, producer); !slices.Contains(got, workerproto.PackageCapabilityWorkspaceHead) {
		t.Fatalf("review-declared task placement capabilities = %v, want %q", got, workerproto.PackageCapabilityWorkspaceHead)
	}
	consumer := ManifestTask{InputsFrom: map[string][]string{"producer": {"change"}}}
	got := placementCapabilities(manifest, consumer)
	if !slices.Contains(got, workerproto.PackageCapabilityAcceptedDependencies) || slices.Contains(got, workerproto.PackageCapabilityWorkspaceHead) {
		t.Fatalf("consumer placement capabilities = %v, want %q only for the dependency", got, workerproto.PackageCapabilityAcceptedDependencies)
	}
	plain := Manifest{Tasks: map[string]ManifestTask{"producer": {Commits: []ManifestCommit{{Name: "change"}}}}}
	if got := placementCapabilities(plain, consumer); slices.Contains(got, workerproto.PackageCapabilityAcceptedDependencies) {
		t.Fatalf("consumer of an unreviewed commit requires %q: %v", workerproto.PackageCapabilityAcceptedDependencies, got)
	}
}
