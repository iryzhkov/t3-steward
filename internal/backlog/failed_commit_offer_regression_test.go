package backlog

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFailedCommitRetentionRequiresCoordinatorOffer(t *testing.T) {
	for _, bundles := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-coordinator", true: "bundle-only-coordinator"}[bundles], func(t *testing.T) {
			repository := newGitFixture(t)
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			task := domain.Task{ID: "implement", Name: "implement", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}, Verification: []string{"exit 7"}}
			result, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{Task: task, Attempt: domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: task.ID, Number: 1}, WorkspaceDir: repository, ExplicitSuccess: true, Repository: repository, BaseCommit: gitOutput(t, repository, "rev-parse", "HEAD"), CommitBundles: bundles})
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			for _, a := range result.Artifacts {
				if a.Kind == domain.ArtifactGitState || a.Kind == domain.ArtifactOutput {
					t.Fatalf("unoffered failed commit emitted: %+v", a)
				}
			}
			if records, err := refs.List("run"); err != nil || len(records) != 0 {
				t.Fatalf("unoffered retention: %+v %v", records, err)
			}
			if result.Completion.VerificationPassed || !strings.Contains(result.Completion.Failure, "verification command failed (7): exit 7") || !strings.Contains(result.Completion.Failure, "not published") {
				t.Fatalf("ordinary failure lost: %+v", result.Completion)
			}
		})
	}
}

func TestFailedCommitProducerCapabilityOfferMixedVersions(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps []string
		want bool
	}{
		{"new-worker", []string{workerproto.PackageCapabilityCommitBundle, workerproto.PackageCapabilityFailedCommit}, true},
		{"old-worker", []string{workerproto.PackageCapabilityCommitBundle}, false},
		{"unknown-worker", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg := workerproto.ExecutionPackage{WorkerID: "worker", Outputs: []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}}
			builder := CoordinatorOfferBuilder{WorkerCapabilities: map[string][]string{"worker": tc.caps}}
			if err := builder.declarePackageCapabilities(context.Background(), &pkg); err != nil {
				t.Fatal(err)
			}
			if got := slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityFailedCommit); got != tc.want {
				t.Fatalf("failed-commit offer=%v want=%v: %v", got, tc.want, pkg.RequiredCapabilities)
			}
		})
	}
}
