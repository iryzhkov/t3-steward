package campaign

import (
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFixChainSecondRunPreservesVerification(t *testing.T) {
	source, options := fixFixture()
	first, err := GenerateFixChain(source, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(writeFixTest(t, first), DefaultLimits); err != nil {
		t.Fatal(err)
	}
	producer := fixManifest(t, first).Tasks["fix2"]
	source.RunID = "run-fix1"
	source.Producer = domain.Task{
		Name: "fix2", Routes: source.Producer.Routes, Placement: source.Producer.Placement,
		ResourcePreset: producer.Resources.Preset, MaxTurns: producer.MaxTurns,
		Verification: producer.Verify, Gate: producer.Gate,
		Outputs: []domain.ArtifactDeclaration{{Name: "continuation.md"}, {Name: "handoff.md"}, {Name: "verification.log"}, {Name: "fix", Commit: &domain.CommitOutput{Revision: "HEAD"}}},
	}
	source.Review = domain.Task{
		Name: "review3", Routes: source.Review.Routes, MaxTurns: source.Review.MaxTurns,
		ReviewOutput: &domain.ReviewOutput{Verdict: "verdict.json"},
		Outputs:      []domain.ArtifactDeclaration{{Name: "continuation.md"}, {Name: "review.md"}, {Name: "verdict.json"}},
	}
	options.Lineage.RoundsUsedBefore = 2
	second, err := GenerateFixChain(source, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(writeFixTest(t, second), DefaultLimits); err != nil {
		t.Fatal(err)
	}
	manifest := fixManifest(t, second)
	for _, name := range []string{"fix1", "fix2"} {
		if !reflect.DeepEqual(manifest.Tasks[name].Verify, producer.Verify) {
			t.Fatalf("%s verification changed: got %v want %v", name, manifest.Tasks[name].Verify, producer.Verify)
		}
	}
	if !reflect.DeepEqual([]string(manifest.Tasks["fix1"].Needs), []string{"run-fix1/fix2", "run-fix1/review3"}) {
		t.Fatalf("second chain needs: %v", manifest.Tasks["fix1"].Needs)
	}
	if !reflect.DeepEqual(source.Producer.Verification, producer.Verify) {
		t.Fatal("source verification mutated")
	}
}

func TestFixChainExistingCleanTreeVerificationPreservesOrder(t *testing.T) {
	source, options := fixFixture()
	source.Producer.Verification = []string{"git diff --quiet && git diff --cached --quiet", "go test ./...", "go vet ./..."}
	before := append([]string(nil), source.Producer.Verification...)
	unit, err := GenerateFixChain(source, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(writeFixTest(t, unit), DefaultLimits); err != nil {
		t.Fatal(err)
	}
	for name, task := range fixManifest(t, unit).Tasks {
		if name == "fix1" || name == "fix2" {
			if !reflect.DeepEqual(task.Verify, before) {
				t.Fatalf("%s: got %v want %v", name, task.Verify, before)
			}
		}
	}
	if !reflect.DeepEqual(source.Producer.Verification, before) {
		t.Fatal("source verification mutated")
	}
}
