package backlog

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// commitBundleFixture extends the package fixture with a producer that declared
// a commit, ran on producerWorker and retained the commit's bundle, and a
// consumer that takes the commit as an input.
func commitBundleFixture(now time.Time, producerWorker string) (sqlite.CoordinatorRecords, domain.Assignment) {
	records, assignment := packageBuilderFixture(now)
	for index := range records.Tasks {
		switch records.Tasks[index].ID {
		case "task-producer":
			records.Tasks[index].Outputs = []domain.ArtifactDeclaration{
				{Name: "reports/result.txt", MediaType: "text/plain"},
				{Name: "repair", MediaType: "application/json", Commit: &domain.CommitOutput{}},
			}
		case "task-consumer":
			records.Tasks[index].DependencyInputs = map[string][]string{"producer": {"reports/result.txt", "repair"}}
		}
	}
	producerAttempt := domain.Attempt{
		ID: "attempt-producer", WorkflowRunID: "run-1", TaskID: "task-producer", Number: 1,
		Progress: domain.ProgressSucceeded, Control: domain.ControlUnassigned,
		Revision: 5, AssignmentID: "assignment-producer", UpdatedAt: now,
	}
	producerAssignment := assignment
	producerAssignment.ID, producerAssignment.AttemptID = "assignment-producer", producerAttempt.ID
	producerAssignment.WorkerID, producerAssignment.Route.WorkerID = producerWorker, producerWorker
	producerAssignment.DispatchToken, producerAssignment.ThreadID = "dispatch-producer", "thread-producer"
	producerAssignment.LeaseToken = "lease-producer"
	records.Attempts = append(records.Attempts, producerAttempt)
	records.Assignments = append(records.Assignments, producerAssignment)
	records.Artifacts = append(records.Artifacts,
		domain.Artifact{
			ID: "provenance-1", WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: producerAttempt.ID,
			Kind: domain.ArtifactOutput, Name: "repair", MediaType: "application/json", Size: 300,
			SHA256: strings.Repeat("b", 64), StoragePath: "objects/provenance-1", Producer: "producer", CreatedAt: now,
		},
		domain.Artifact{
			ID: "bundle-1", WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: producerAttempt.ID,
			Kind: domain.ArtifactGitState, Name: CommitBundleArtifactName("repair"), MediaType: CommitBundleMediaType,
			Size: 4096, SHA256: strings.Repeat("c", 64), StoragePath: "objects/bundle-1", Producer: "producer", CreatedAt: now,
		},
	)
	return records, assignment
}

func commitBundleBuilder(t *testing.T, records sqlite.CoordinatorRecords, capabilities map[string][]string) CoordinatorOfferBuilder {
	t.Helper()
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = capabilities
	return builder
}

// A consumer placed on another worker than its producer is sent the producer's
// bundle, outside its dependency view, and the package requires the capability
// that can import it.
func TestConsumerOnAnotherWorkerIsSentTheCommitBundle(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := commitBundleFixture(now, "omarchy-pc")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build consumer offer: %v", err)
	}
	pkg := offer.Package.Package
	if len(pkg.CommitBundles) != 1 {
		t.Fatalf("commit bundles = %+v", pkg.CommitBundles)
	}
	bundle := pkg.CommitBundles[0]
	if bundle.WorkflowRunID != "run-1" || bundle.TaskID != "task-producer" || bundle.Name != "repair" || bundle.Bundle == nil ||
		bundle.Bundle.ID != "bundle-1" || bundle.Bundle.Path != "commit-bundles/run-1/task-producer/repair.bundle" ||
		bundle.Bundle.SHA256 != strings.Repeat("c", 64) {
		t.Fatalf("commit bundle = %+v", bundle)
	}
	for _, dependency := range pkg.Dependencies {
		for _, object := range dependency.Artifacts {
			if object.ID == "bundle-1" {
				t.Fatalf("the bundle reached the task's dependency view: %+v", object)
			}
		}
	}
	if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("required capabilities = %v", pkg.RequiredCapabilities)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}

	// A package that carries bundles without the capability is refused by the
	// protocol itself, so no build can silently drop them.
	stripped := pkg
	stripped.RequiredCapabilities = nil
	if err := workerproto.ValidateExecutionPackage(stripped); err == nil || !strings.Contains(err.Error(), "commit bundle capability") {
		t.Fatalf("error = %v, want the capability required", err)
	}
}

// A consumer on the producer's own worker already holds the commit in its
// store, so nothing is delivered and nothing is required.
func TestConsumerOnTheProducersWorkerIsSentNoBundle(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := commitBundleFixture(now, "normandy")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": nil})
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build same-worker offer: %v", err)
	}
	if pkg := offer.Package.Package; len(pkg.CommitBundles) != 0 || slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("same-worker package carries bundles %+v, capabilities %v", pkg.CommitBundles, pkg.RequiredCapabilities)
	}
}

// A worker that cannot import a bundle is never sent one: the offer is refused
// with the capability named.
func TestConsumerWorkerWithoutTheCapabilityIsRefusedByName(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := commitBundleFixture(now, "omarchy-pc")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityPreflight}})
	_, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("error = %v, want the missing capability named", err)
	}
}

// A producer is told to retain a bundle only by a worker that advertises the
// capability, and is never refused for lacking it.
func TestProducerPackageOffersTheCommitBundleCapability(t *testing.T) {
	producer := workerproto.ExecutionPackage{
		WorkerID: "homelab",
		Outputs:  []domain.ArtifactDeclaration{{Name: "repair", MediaType: "application/json", Commit: &domain.CommitOutput{}}},
	}
	for name, test := range map[string]struct {
		capabilities map[string][]string
		want         bool
	}{
		"capable":   {map[string][]string{"homelab": {workerproto.PackageCapabilityCommitBundle}}, true},
		"old build": {map[string][]string{"homelab": {workerproto.PackageCapabilityPreflight}}, false},
		"unknown":   {map[string][]string{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			pkg := producer
			builder := CoordinatorOfferBuilder{WorkerCapabilities: test.capabilities}
			if err := builder.declarePackageCapabilities(context.Background(), &pkg, false); err != nil {
				t.Fatalf("declare capabilities: %v", err)
			}
			if got := slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle); got != test.want {
				t.Fatalf("capabilities = %v, want bundle capability %v", pkg.RequiredCapabilities, test.want)
			}
		})
	}
}

// A task that consumes a declared commit requires the capability to import it,
// so placement never puts it on a worker that would be refused the bundle. The
// producer and a task consuming only ordinary files require nothing new.
func TestCommitConsumerRequiresTheBundleCapabilityForPlacement(t *testing.T) {
	manifest := Manifest{Tasks: map[string]ManifestTask{
		"implement": {Outputs: []string{"notes.md"}, Commits: []ManifestCommit{{Name: "implementation"}}},
		"review": {
			InputsFrom: map[string][]string{"implement": {"implementation"}},
			Placement:  ManifestPlacement{Requires: []string{"git"}},
		},
		"summarize": {InputsFrom: map[string][]string{"implement": {"notes.md"}}},
	}}
	for name, want := range map[string][]string{
		"implement": {},
		"review":    {"git", workerproto.PackageCapabilityCommitBundle},
		"summarize": {},
	} {
		if got := placementCapabilities(manifest, manifest.Tasks[name]); !slices.Equal(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("%s placement capabilities = %v, want %v", name, got, want)
		}
	}
}

// The coordinator accepts the bundle of a declared commit as a result and
// nothing else under the git-state kind.
func TestResultImportAcceptsOnlyTheBundleOfADeclaredCommit(t *testing.T) {
	task := domain.Task{ID: "task-producer", Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID}
	manifest := workerproto.ArtifactTransferManifest{WorkerID: "omarchy-pc"}
	object := func(path, mediaType string) workerproto.ArtifactObject {
		return workerproto.ArtifactObject{
			ID: "artifact-1", Path: path, Kind: string(domain.ArtifactGitState),
			MediaType: mediaType, Size: 10, SHA256: strings.Repeat("d", 64),
		}
	}
	artifact, err := resultArtifact(object("results/"+CommitBundleArtifactName("repair"), CommitBundleMediaType), manifest, attempt, task, time.Now())
	if err != nil || artifact.Kind != domain.ArtifactGitState || artifact.Name != "git/campaign-commits/repair.bundle" {
		t.Fatalf("bundle import = %+v, %v", artifact, err)
	}
	for _, rejected := range []workerproto.ArtifactObject{
		object("results/"+CommitBundleArtifactName("other"), CommitBundleMediaType),
		object("results/git/diff.patch", "text/x-diff"),
		object("results/"+CommitBundleArtifactName("repair"), "text/plain"),
	} {
		if _, err := resultArtifact(rejected, manifest, attempt, task, time.Now()); err == nil {
			t.Fatalf("accepted %+v", rejected)
		}
	}
}
