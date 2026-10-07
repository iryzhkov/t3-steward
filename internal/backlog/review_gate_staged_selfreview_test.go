package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A staged import whose push into the store failed leaves its staging record
// behind. A retry must import the commit again rather than take the record
// for the staging, or every consumer of that commit on the worker fails until
// the run is released.
func TestStagedImportRetriesAfterAFailedPush(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		name := "judge"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			repository := newGitFixture(t)
			storage := t.TempDir()
			produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
			consumer := newCommitWorker(t, storage)
			gitDir, err := consumer.refs.open(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			// A directory where the staged ref goes makes Git refuse to create it.
			stagedRef := filepath.Join(gitDir, filepath.FromSlash(StagedCampaignRef("run-1", "task-producer", "attempt-1", "repair")))
			if err := os.MkdirAll(stagedRef, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(stagedRef, "blocker"), []byte("not a ref\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			delivery := produced.delivery(t, nil)
			if _, err := consumeReviewed(t, consumer, produced, &delivery, "attempt-2", accepted); err == nil {
				t.Fatal("the blocked staged import succeeded")
			}
			if err := os.RemoveAll(stagedRef); err != nil {
				t.Fatal(err)
			}
			delivery = produced.delivery(t, nil)
			prepared, err := consumeReviewed(t, consumer, produced, &delivery, "attempt-3", accepted)
			if err != nil {
				t.Fatalf("retry after the push failure: %v", err)
			}
			ref := CampaignRef("run-1", "task-producer", "repair")
			if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", ref+"^{commit}"); got != produced.commit {
				t.Fatalf("consumer resolved %s, want %s", got, produced.commit)
			}
			if _, err := consumer.refs.Resolve("run-1", "task-producer", "repair"); (err == nil) != accepted {
				t.Fatalf("accepted=%v: published: %v", accepted, err)
			}
		})
	}
}

// Git refuses a ref component ending in .lock, so a staging attempt named that
// way is refused before any record is written for it.
func TestStagedRefRefusesALockComponent(t *testing.T) {
	if ref, err := bundleRef(CommitProvenance{WorkflowRunID: "run-1", TaskID: "task", Name: "repair", StagedAttempt: "attempt.lock"}); err == nil {
		t.Fatalf("bundleRef accepted %s", ref)
	}
	if err := validateGitRef("refs/campaigns/run.lock/task/name"); err == nil {
		t.Fatal("validateGitRef accepted a component ending in .lock")
	}
}

// forgedOutput stores provenance as an ordinary output of the producer named
// name, as its executor could write one.
func forgedOutput(t *testing.T, produced producedCommit, name string, provenance CommitProvenance) domain.Artifact {
	t.Helper()
	raw, err := MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}
	storagePath := filepath.ToSlash(filepath.Join("forged", name))
	writeTestFile(t, produced.storage, storagePath, string(raw))
	sum := sha256.Sum256(raw)
	record := produced.record
	record.ID = "forged-" + hex.EncodeToString(sum[:4])
	record.Name = name
	record.Size, record.SHA256, record.StoragePath = int64(len(raw)), hex.EncodeToString(sum[:]), storagePath
	return record
}

// An executor of a review-declared producer writes a copy of its commit
// record into an ordinary output, naming the base as a published commit,
// which needs no bundle. Only the file of the declared commit output is a
// commit reference: the copy publishes nothing on a judge's worker, and the
// accepted consumer's worker still publishes the accepted commit.
func TestForgedRecordInAnOrdinaryOutputPublishesNothing(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "judge"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			repository := newGitFixture(t)
			storage := t.TempDir()
			produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
			forged := produced.provenance
			forged.StagedAttempt, forged.Bundle, forged.BundleOmitted, forged.Commit = "", nil, "", forged.Base
			notes := forgedOutput(t, produced, "notes.json", forged)
			consumer := newCommitWorker(t, storage)
			producer := workspaceTask("task-producer", "producer")
			task := workspaceTask("task-consumer", "consumer")
			task.Needs = []string{"producer"}
			task.DependencyInputs = map[string][]string{"producer": {"repair", "notes.json"}}
			request := workspaceRequest(produced.repository, "main", task, "attempt-2")
			request.DependencyTasks = []domain.Task{producer, task}
			request.DependencyArtifacts = []domain.Artifact{produced.record, notes}
			request.CommitBundles = map[string]CommitBundleDelivery{CampaignRef("run-1", "task-producer", "repair"): produced.delivery(t, nil)}
			if accepted {
				request.AcceptedCommits = map[string][]string{"task-producer": {"repair"}}
			}
			prepared, err := consumer.preparer.Prepare(context.Background(), request)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			cleanupImmutable(t, prepared.RootDir)
			published, err := consumer.refs.Resolve("run-1", "task-producer", "repair")
			if accepted {
				if err != nil || published.Commit != produced.commit {
					t.Fatalf("the accepted commit was not published: %+v %v", published, err)
				}
			} else if err == nil {
				t.Fatalf("a judge's worker published %+v without acceptance", published)
			}
			if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")+"^{commit}"); got != produced.commit {
				t.Fatalf("consumer resolved %s, want %s", got, produced.commit)
			}
		})
	}
}

// A consumer that takes a review-declared producer's commit from another run
// carries its staged record, so placement requires what holding staged work
// needs, as it does for a consumer in the producer's own run.
func TestExternalConsumerOfAReviewedCommitNeedsAcceptedDependencies(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	store.records.Tasks[0].ReviewRequirements = &domain.TaskReviewRequirements{}
	store.records.Tasks[0].Outputs = []domain.ArtifactDeclaration{{Name: "implementation", MediaType: "application/json", Commit: &domain.CommitOutput{Revision: "HEAD"}}}
	source := &store.records.Artifacts[0]
	source.Name = "implementation"
	source.MediaType = "application/json"
	manifest.Tasks["consumer"].InputsFrom["source-run/producer"] = []string{"implementation"}
	target.Tasks[0].DependencyInputs["source-run/producer"] = []string{"implementation"}
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "retained-commit" }}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target, "target-run"); err != nil {
		t.Fatal(err)
	}
	got := target.Tasks[0].Placement.Capabilities
	for _, want := range []string{workerproto.PackageCapabilityCommitBundle, workerproto.PackageCapabilityAcceptedDependencies} {
		if !slices.Contains(got, want) {
			t.Fatalf("placement capabilities %v lack %q", got, want)
		}
	}
}
