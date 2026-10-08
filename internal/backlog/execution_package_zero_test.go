package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCoordinatorOfferBuilderZeroByteArtifacts(t *testing.T) {
	for _, index := range []int{1, 2} {
		t.Run([]string{"static", "dependency"}[index-1], func(t *testing.T) {
			now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
			records, assignment := packageBuilderFixture(now)
			records.Artifacts[index].Size = 0
			records.Artifacts[index].SHA256 = fmt.Sprintf("%x", sha256.Sum256(nil))
			offer, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			object := offer.Package.Package.StaticInputs[0]
			if index == 2 {
				object = offer.Package.Package.Dependencies[0].Artifacts[0]
			}
			if object.Size != 0 || object.SHA256 != records.Artifacts[index].SHA256 {
				t.Fatalf("empty object changed: %+v", object)
			}
			if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCoordinatorOfferBuilderRejectsInvalidArtifactMetadata(t *testing.T) {
	cases := map[string]func(*domain.Artifact){
		"missing-id":         func(a *domain.Artifact) { a.ID = "" },
		"invalid-id":         func(a *domain.Artifact) { a.ID = "../invalid" },
		"missing-name":       func(a *domain.Artifact) { a.Name = "" },
		"missing-media-type": func(a *domain.Artifact) { a.MediaType = "" },
		"missing-sha":        func(a *domain.Artifact) { a.SHA256 = "" },
		"malformed-sha":      func(a *domain.Artifact) { a.SHA256 = "not-a-checksum" },
		"negative-size":      func(a *domain.Artifact) { a.Size = -1 },
		"excessive-size":     func(a *domain.Artifact) { a.Size = 3 << 20 },
		"traversal-path":     func(a *domain.Artifact) { a.Name = "../escape" },
		"absolute-path":      func(a *domain.Artifact) { a.Name = "/escape" },
		"unclean-path":       func(a *domain.Artifact) { a.Name = "safe/../escape" },
		"backslash-path":     func(a *domain.Artifact) { a.Name = "safe\\escape" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
			records, assignment := packageBuilderFixture(now)
			mutate(&records.Artifacts[1])
			// Keep the durable lookup pointed at the mutated object so the builder
			// exercises metadata validation rather than a missing-artifact lookup.
			records.Tasks[1].InputArtifactIDs = []string{records.Artifacts[1].ID}
			_, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
			if err == nil {
				t.Fatal("invalid artifact produced an offer")
			}
			if name == "negative-size" && !strings.Contains(err.Error(), "incomplete immutable metadata") {
				t.Fatalf("unexpected refusal: %v", err)
			}
		})
	}
}
