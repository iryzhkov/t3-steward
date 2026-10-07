package backlog

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// consumeReviewed prepares a consumer of a review-gated producer's commit. An
// accepted consumer's package names the commit as an accepted dependency,
// which is what lets its worker publish the staging; a review judge's does
// not.
func consumeReviewed(t *testing.T, consumer commitWorker, produced producedCommit, delivery *CommitBundleDelivery, attemptID string, accepted bool) (PreparedWorkspace, error) {
	t.Helper()
	producer := workspaceTask("task-producer", "producer")
	task := workspaceTask("task-consumer", "consumer")
	task.Needs = []string{"producer"}
	task.DependencyInputs = map[string][]string{"producer": {"repair"}}
	request := workspaceRequest(produced.repository, "main", task, attemptID)
	request.DependencyTasks = []domain.Task{producer, task}
	request.DependencyArtifacts = []domain.Artifact{produced.record}
	if delivery != nil {
		request.CommitBundles = map[string]CommitBundleDelivery{
			CampaignRef("run-1", "task-producer", "repair"): *delivery,
		}
	}
	if accepted {
		request.AcceptedCommits = map[string][]string{"task-producer": {"repair"}}
	}
	prepared, err := consumer.preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
	}
	return prepared, err
}

// rc.117 integration of M16-3's staged commits with rc.116's commit bundles
// and F1 placement: a review-gated producer on one worker and its accepted
// consumer on another, both fully capable, is ordinary fleet placement. The
// producer retains a bundle of its staged commit; the consuming worker
// imports it as staged work, and only the accepted consumer's fetch publishes
// it there. A review judge on a third worker inspects it without publishing
// it, and the producer's own worker still holds it only as staged.
func TestReviewedCommitReachesAnAcceptedConsumerOnAnotherWorker(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer := newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{reviewGated: true})
	if produced.bundle == nil || produced.provenance.Bundle == nil || produced.provenance.BundleOmitted != "" {
		t.Fatalf("a review-gated producer retained no bundle: %+v", produced.provenance)
	}
	if _, err := producer.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatal("the producer published its staged commit")
	}
	ref := CampaignRef("run-1", "task-producer", "repair")

	// A review judge elsewhere inspects the staged commit.
	judge := newCommitWorker(t, storage)
	delivery := produced.delivery(t, nil)
	inspected, err := consumeReviewed(t, judge, produced, &delivery, "attempt-judge", false)
	if err != nil {
		t.Fatalf("prepare a review judge on another worker: %v", err)
	}
	if got := gitOutput(t, inspected.WorkspaceDir, "rev-parse", ref+"^{commit}"); got != produced.commit {
		t.Fatalf("judge resolved %s, want %s", got, produced.commit)
	}
	if provenance, err := judge.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatalf("a consumer without acceptance published the staged commit: %+v", provenance)
	}
	if campaignRefExists(t, judge.refs, ref) {
		t.Fatalf("a consumer without acceptance created %s", ref)
	}

	// The accepted consumer on another worker publishes it there.
	consumer := newCommitWorker(t, storage)
	delivery = produced.delivery(t, nil)
	prepared, err := consumeReviewed(t, consumer, produced, &delivery, "attempt-2", true)
	if err != nil {
		t.Fatalf("prepare an accepted consumer on another worker: %v", err)
	}
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", ref+"^{commit}"); got != produced.commit {
		t.Fatalf("consumer resolved %s, want %s", got, produced.commit)
	}
	published, err := consumer.refs.Resolve("run-1", "task-producer", "repair")
	if err != nil || published.Commit != produced.commit || published.StagedAttempt != "" {
		t.Fatalf("the accepted consumer's worker did not publish the commit: %+v %v", published, err)
	}

	// Both imports belong to the run and are released with it.
	for name, worker := range map[string]commitWorker{"judge": judge, "consumer": consumer} {
		runs, err := worker.refs.Runs()
		if err != nil || !reflect.DeepEqual(runs, []string{"run-1"}) {
			t.Fatalf("%s holds %v, %v", name, runs, err)
		}
		release := CampaignRefReleaseReconciler{Records: noRetainedCampaigns, Refs: worker.refs}
		if report := release.Tick(context.Background()); len(report.Errors) != 0 || !reflect.DeepEqual(report.Released, []string{"run-1"}) {
			t.Fatalf("%s release = %+v", name, report)
		}
		if runs, err := worker.refs.Runs(); err != nil || len(runs) != 0 {
			t.Fatalf("%s holds %v after release, %v", name, runs, err)
		}
	}
}

// A bundle of staged work is still bound to the attempt that staged it: a
// record naming another attempt, or naming none, does not import it as that
// attempt's staging or as published work.
func TestStagedBundleImportIsBoundToItsAttempt(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
	if produced.bundle == nil {
		t.Fatalf("a review-gated producer retained no bundle: %+v", produced.provenance)
	}
	for name, edit := range map[string]func(*CommitProvenance){
		"another attempt": func(p *CommitProvenance) { p.StagedAttempt = "attempt-9" },
		"published":       func(p *CommitProvenance) { p.StagedAttempt = "" },
		"unsafe attempt":  func(p *CommitProvenance) { p.StagedAttempt = "../attempt-1" },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := produced
			tampered.record = rebindProvenance(t, produced, edit)
			consumer := newCommitWorker(t, storage)
			delivery := produced.delivery(t, nil)
			if _, err := consumeReviewed(t, consumer, tampered, &delivery, "attempt-2", true); err == nil {
				t.Fatal("a staged bundle was imported under a record that does not name its attempt")
			}
			if _, err := consumer.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
				t.Fatal("the refused import published the commit")
			}
		})
	}
}

// The review's probe: a staged commit with no retained bundle, exported by the
// worker that staged it, which is the coordinator's fallback. It used to fail
// for the published provenance record a staging never writes.
func TestProducingWorkerExportsAStagedCommit(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "commit", "-am", "second")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	staged, err := refs.Stage(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "producer", Name: "change", Repository: repository,
		WorkspaceDir: repository, Base: base,
	}, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	path, err := refs.ExportPublishedBundle(ctx, staged, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatalf("export a staged commit on its worker: %v", err)
	}
	defer os.RemoveAll(filepath.Dir(path))
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	exported, err := ExportCommitBundle(ctx, staged, "review", source, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatalf("verify the exported staged commit: %v", err)
	}
	_ = exported.Close()
	if _, err := refs.Resolve("run-1", "producer", "change"); err == nil {
		t.Fatal("export published the staged commit")
	}
}

// H3's operator export of a review-gated leaf: the producer passed its review
// and no task consumes the commit, so nothing ever promotes the staging. The
// retained bundle exports through the coordinator, and without one the
// producing worker exports the staging itself, publishing nothing.
func TestReviewedLeafCommitIsExportable(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer := newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{reviewGated: true})
	if produced.bundle == nil {
		t.Fatalf("a review-gated leaf retained no bundle for export: %+v", produced.provenance)
	}
	retained, err := os.Open(filepath.Join(storage, filepath.FromSlash(produced.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	exported, err := ExportCommitBundle(ctx, produced.provenance, "review", retained, DefaultCommitBundleMaxBytes)
	_ = retained.Close()
	if err != nil {
		t.Fatalf("export the retained bundle of a reviewed leaf: %v", err)
	}
	if heads := gitOutput(t, repository, "bundle", "list-heads", exported.Path); heads != produced.commit+" refs/heads/review" {
		t.Fatalf("exported heads %q", heads)
	}
	_ = exported.Close()

	// The producing worker's export, which the coordinator falls back to.
	path, err := producer.refs.ExportPublishedBundle(ctx, produced.provenance, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatalf("the producing worker cannot export its reviewed leaf: %v", err)
	}
	defer os.RemoveAll(filepath.Dir(path))
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	unbound := produced.provenance
	unbound.Bundle = nil
	exported, err = ExportCommitBundle(ctx, unbound, "review", source, DefaultCommitBundleMaxBytes)
	_ = source.Close()
	if err != nil {
		t.Fatalf("export the producing worker's bundle of a reviewed leaf: %v", err)
	}
	_ = exported.Close()
	if _, err := producer.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatal("export published the staged commit")
	}

	// The worker exports only the staging the record names.
	other := produced.provenance
	other.StagedAttempt = "attempt-9"
	if path, err := producer.refs.ExportPublishedBundle(ctx, other, DefaultCommitBundleMaxBytes); err == nil {
		_ = os.RemoveAll(filepath.Dir(path))
		t.Fatal("exported the staging of an attempt that staged nothing")
	}
}
