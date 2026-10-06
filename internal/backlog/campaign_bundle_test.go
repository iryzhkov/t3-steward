package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// noRetainedCampaigns is a coordinator that no longer retains any campaign.
func noRetainedCampaigns(context.Context) (sqlite.CoordinatorRecords, error) {
	return sqlite.CoordinatorRecords{}, nil
}

// commitWorker is one worker's half of a campaign: its own runs root, and so
// its own repository cache, and its own campaign ref store. Two of them share
// nothing but the coordinator's artifact storage, which is exactly what two
// hosts share.
type commitWorker struct {
	refs     CampaignRefStore
	preparer WorkspacePreparer
}

func newCommitWorker(t *testing.T, storage string) commitWorker {
	t.Helper()
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	preparer := workspacePreparer(t.TempDir(), storage)
	preparer.CampaignRefs = refs
	return commitWorker{refs: refs, preparer: preparer}
}

// producedCommit is what a producing task left behind: the provenance record a
// consumer is handed, the bundle the coordinator retained, and the commit and
// base the record must name.
type producedCommit struct {
	repository string
	storage    string
	base       string
	commit     string
	record     domain.Artifact
	bundle     *domain.Artifact
	provenance CommitProvenance
}

type produceOptions struct {
	// ref is what the producer's workspace is pinned to; main by default.
	ref string
	// noChange declares a commit without making one, so the commit is the base.
	noChange bool
	// oldBuild finalizes as a build without the commit bundle capability.
	oldBuild bool
	// edit, when set, changes the producer's workspace before its commit, so a
	// test can make the commit modify files that already exist in the base.
	edit func(t *testing.T, workspace string)
}

func produceCommit(t *testing.T, producer commitWorker, repository, storage string, options produceOptions) producedCommit {
	t.Helper()
	ctx := context.Background()
	ref := options.ref
	if ref == "" {
		ref = "main"
	}
	task := workspaceTask("task-producer", "producer")
	request := workspaceRequest(repository, ref, task, "attempt-1")
	prepared, err := producer.preparer.Prepare(ctx, request)
	if err != nil {
		t.Fatalf("prepare producer workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if !options.noChange {
		gitRun(t, prepared.WorkspaceDir, "config", "user.name", "Test User")
		gitRun(t, prepared.WorkspaceDir, "config", "user.email", "test@example.test")
		writeGitFile(t, prepared.WorkspaceDir, "implementation.txt", "implemented\n")
		gitRun(t, prepared.WorkspaceDir, "add", "implementation.txt")
		if options.edit != nil {
			options.edit(t, prepared.WorkspaceDir)
		}
		gitRun(t, prepared.WorkspaceDir, "commit", "-m", "implementation")
	}
	task.Outputs = []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}
	finalizer := AttemptFinalizer{StorageRoot: storage, CampaignRefs: producer.refs, Processes: testProcessRunner{}}
	finalized, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: task, Attempt: request.Attempt, WorkspaceDir: prepared.WorkspaceDir,
		ExplicitSuccess: true, Repository: repository, BaseCommit: prepared.Commit,
		CommitBundles: !options.oldBuild,
	})
	if err != nil {
		t.Fatalf("finalize producer: %v", err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("producer failed: %+v", finalized.Completion)
	}
	produced := producedCommit{
		repository: repository, storage: storage, base: prepared.Commit,
		commit: gitOutput(t, prepared.WorkspaceDir, "rev-parse", "HEAD"),
	}
	for index, artifact := range finalized.Artifacts {
		switch {
		case artifact.Kind == domain.ArtifactOutput && artifact.Name == "repair":
			produced.record = artifact
		case artifact.Kind == domain.ArtifactGitState && artifact.Name == CommitBundleArtifactName("repair"):
			produced.bundle = &finalized.Artifacts[index]
		default:
			t.Fatalf("unexpected producer artifact %+v", artifact)
		}
	}
	if produced.record.ID == "" {
		t.Fatalf("producer published no provenance record: %+v", finalized.Artifacts)
	}
	provenance, err := ParseCommitProvenance([]byte(readTestFile(t, storage, produced.record.StoragePath)))
	if err != nil {
		t.Fatalf("parse provenance record: %v", err)
	}
	produced.provenance = provenance
	return produced
}

// delivery is the bundle as the execution package hands it to the consumer's
// worker: its digest, its size, and a way to read it that counts the reads.
func (p producedCommit) delivery(t *testing.T, opened *int) CommitBundleDelivery {
	t.Helper()
	if p.bundle == nil {
		t.Fatal("producer retained no commit bundle")
	}
	bundle := *p.bundle
	return CommitBundleDelivery{
		SHA256: bundle.SHA256, Size: bundle.Size,
		Open: func(context.Context) (io.ReadCloser, error) {
			if opened != nil {
				*opened++
			}
			return os.Open(filepath.Join(p.storage, filepath.FromSlash(bundle.StoragePath)))
		},
	}
}

func bytesDelivery(content []byte) CommitBundleDelivery {
	sum := sha256.Sum256(content)
	return CommitBundleDelivery{
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content)),
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		},
	}
}

// consume prepares a consumer of the producer's commit on the given worker.
func consume(t *testing.T, consumer commitWorker, produced producedCommit, record domain.Artifact, delivery *CommitBundleDelivery, attemptID string) (PreparedWorkspace, error) {
	t.Helper()
	producer := workspaceTask("task-producer", "producer")
	task := workspaceTask("task-consumer", "consumer")
	task.Needs = []string{"producer"}
	task.DependencyInputs = map[string][]string{"producer": {"repair"}}
	request := workspaceRequest(produced.repository, "main", task, attemptID)
	request.DependencyTasks = []domain.Task{producer, task}
	request.DependencyArtifacts = []domain.Artifact{record}
	if delivery != nil {
		request.CommitBundles = map[string]CommitBundleDelivery{
			CampaignRef("run-1", "task-producer", "repair"): *delivery,
		}
	}
	prepared, err := consumer.preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
	}
	return prepared, err
}

// rebindProvenance stores a provenance record with altered content, as a
// tampered or mistaken record would arrive.
func rebindProvenance(t *testing.T, produced producedCommit, edit func(*CommitProvenance)) domain.Artifact {
	t.Helper()
	provenance := produced.provenance
	if provenance.Bundle != nil {
		copied := *provenance.Bundle
		provenance.Bundle = &copied
	}
	edit(&provenance)
	raw, err := MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}
	storagePath := filepath.ToSlash(filepath.Join("tampered", hex.EncodeToString(raw[:8]), "repair"))
	writeTestFile(t, produced.storage, storagePath, string(raw))
	sum := sha256.Sum256(raw)
	record := produced.record
	record.ID = "tampered-" + hex.EncodeToString(sum[:4])
	record.Size, record.SHA256, record.StoragePath = int64(len(raw)), hex.EncodeToString(sum[:]), storagePath
	return record
}

func campaignRefExists(t *testing.T, refs CampaignRefStore, ref string) bool {
	t.Helper()
	return exec.Command("git", "--git-dir", filepath.Join(refs.Root, "campaigns.git"),
		"rev-parse", "--verify", "--quiet", ref).Run() == nil
}

// The incident: a producer on one worker and its consumer placed on another.
// The consumer's own store has never heard of the commit, so it imports the
// bundle the producer retained, checks it names exactly the declared commit on
// the declared base, and resolves the ref as if it had been published locally.
func TestConsumerOnAnotherWorkerImportsTheDeclaredCommit(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	if produced.bundle == nil || produced.provenance.Bundle == nil {
		t.Fatalf("producer retained no bundle: %+v", produced.provenance)
	}
	if produced.provenance.Bundle.SHA256 != produced.bundle.SHA256 || produced.provenance.Bundle.Size != produced.bundle.Size ||
		produced.provenance.Bundle.Artifact != produced.bundle.Name {
		t.Fatalf("provenance bundle %+v is not bound to artifact %+v", produced.provenance.Bundle, produced.bundle)
	}

	opened := 0
	delivery := produced.delivery(t, &opened)
	prepared, err := consume(t, consumer, produced, produced.record, &delivery, "attempt-2")
	if err != nil {
		t.Fatalf("prepare consumer on another worker: %v", err)
	}
	if opened != 1 {
		t.Fatalf("bundle opened %d times, want 1", opened)
	}
	ref := CampaignRef("run-1", "task-producer", "repair")
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", ref+"^{commit}"); got != produced.commit {
		t.Fatalf("consumer resolved %s, want %s", got, produced.commit)
	}
	if got := gitOutput(t, prepared.WorkspaceDir, "show", produced.commit+":implementation.txt"); got != "implemented" {
		t.Fatalf("consumer content = %q", got)
	}
	// The import is recorded in the consumer's own store, under the same ref,
	// so the next consumer on this worker finds it locally and the worker's
	// release of the run removes it.
	imported, err := consumer.refs.Resolve("run-1", "task-producer", "repair")
	if err != nil || imported.Commit != produced.commit || imported.Base != produced.base {
		t.Fatalf("consumer store record = %+v, %v", imported, err)
	}
	if !campaignRefExists(t, consumer.refs, ref) {
		t.Fatal("the imported ref is not in the consumer's campaign ref store")
	}

	// A second consumer on the same worker uses the imported ref and reads no
	// bundle at all.
	if _, err := consume(t, consumer, produced, produced.record, &delivery, "attempt-3"); err != nil {
		t.Fatalf("prepare second consumer: %v", err)
	}
	if opened != 1 {
		t.Fatalf("bundle opened %d times after a local import, want 1", opened)
	}
}

// A consumer on the producer's own worker behaves exactly as before: the ref is
// already in its store, so the bundle is never read.
func TestConsumerOnTheProducersWorkerReadsNoBundle(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	worker := newCommitWorker(t, storage)
	produced := produceCommit(t, worker, repository, storage, produceOptions{})
	delivery := CommitBundleDelivery{
		SHA256: produced.bundle.SHA256, Size: produced.bundle.Size,
		Open: func(context.Context) (io.ReadCloser, error) {
			t.Error("a same-worker consumer read the commit bundle")
			return nil, os.ErrNotExist
		},
	}
	for _, delivered := range []*CommitBundleDelivery{nil, &delivery} {
		prepared, err := consume(t, worker, produced, produced.record, delivered, "attempt-"+map[bool]string{true: "2", false: "3"}[delivered == nil])
		if err != nil {
			t.Fatalf("prepare same-worker consumer: %v", err)
		}
		if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")+"^{commit}"); got != produced.commit {
			t.Fatalf("consumer resolved %s, want %s", got, produced.commit)
		}
	}
}

// A bundle that is not the one the provenance record was bound to is refused
// before Git ever reads it, and one whose bytes match the binding but are not a
// bundle is refused by Git's own verification. Neither falls back to anything.
func TestConsumerRefusesACorruptBundle(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})

	content := []byte(readTestFile(t, storage, produced.bundle.StoragePath))
	corrupt := append([]byte(nil), content...)
	corrupt[len(corrupt)-30] ^= 0xff
	flipped := bytesDelivery(corrupt)
	flipped.SHA256, flipped.Size = produced.bundle.SHA256, produced.bundle.Size
	_, err := consume(t, consumer, produced, produced.record, &flipped, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), "corrupt") || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("error = %v, want a checksum refusal", err)
	}

	// The binding itself disagrees with what was delivered.
	unbound := bytesDelivery(corrupt)
	_, err = consume(t, consumer, produced, produced.record, &unbound, "attempt-3")
	if err == nil || !strings.Contains(err.Error(), "not the bundle its provenance record names") {
		t.Fatalf("error = %v, want a binding refusal", err)
	}

	// A record bound to bytes that are not a bundle at all.
	garbage := []byte("# v2 git bundle\nnot really a bundle\n")
	garbageDelivery := bytesDelivery(garbage)
	record := rebindProvenance(t, produced, func(p *CommitProvenance) {
		p.Bundle.SHA256, p.Bundle.Size = garbageDelivery.SHA256, garbageDelivery.Size
	})
	_, err = consume(t, consumer, produced, record, &garbageDelivery, "attempt-4")
	if err == nil || !strings.Contains(err.Error(), "commit bundle") || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("error = %v, want a bundle verification refusal", err)
	}
	if campaignRefExists(t, consumer.refs, CampaignRef("run-1", "task-producer", "repair")) {
		t.Fatal("a refused bundle left a ref in the consumer's store")
	}
}

// A bundle whose ref names a different commit from the provenance record is
// refused by name; the consumer never runs on a commit nobody declared.
func TestConsumerRefusesABundleForADifferentCommit(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	declared := strings.Repeat("1", len(produced.commit))
	record := rebindProvenance(t, produced, func(p *CommitProvenance) { p.Commit = declared })
	delivery := produced.delivery(t, nil)
	_, err := consume(t, consumer, produced, record, &delivery, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), produced.commit) || !strings.Contains(err.Error(), declared) {
		t.Fatalf("error = %v, want a commit mismatch naming both commits", err)
	}

	// The base is checked against the record as well. The record here names a
	// later commit on main as the base: the consumer holds it, the bundle's
	// prerequisite is its ancestor, and the declared commit does not descend
	// from it.
	writeGitFile(t, repository, "later.txt", "later\n")
	gitRun(t, repository, "add", "later.txt")
	gitRun(t, repository, "commit", "-m", "later")
	later := gitOutput(t, repository, "rev-parse", "HEAD")
	record = rebindProvenance(t, produced, func(p *CommitProvenance) { p.Base = later })
	_, err = consume(t, newCommitWorker(t, storage), produced, record, &delivery, "attempt-3")
	if err == nil || !strings.Contains(err.Error(), "does not descend from the base "+later) {
		t.Fatalf("error = %v, want a base mismatch", err)
	}
}

// The bundle carries only base..commit, so the consumer must already hold the
// base. When its repository cache does not, the error says so instead of Git's
// complaint about a missing ref.
func TestConsumerWithoutTheBaseNamesTheMissingPrerequisite(t *testing.T) {
	repository := newGitFixture(t)
	gitRun(t, repository, "checkout", "-b", "feature")
	writeGitFile(t, repository, "feature.txt", "feature\n")
	gitRun(t, repository, "add", "feature.txt")
	gitRun(t, repository, "commit", "-m", "feature")
	gitRun(t, repository, "checkout", "main")
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{ref: "feature"})
	// The branch the producer started from is gone before the consumer's
	// worker ever clones the repository.
	gitRun(t, repository, "branch", "-D", "feature")
	gitRun(t, repository, "reflog", "expire", "--expire=now", "--all")
	gitRun(t, repository, "gc", "--prune=now", "--quiet")

	delivery := produced.delivery(t, nil)
	_, err := consume(t, consumer, produced, produced.record, &delivery, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), "missing prerequisite") || !strings.Contains(err.Error(), produced.base) {
		t.Fatalf("error = %v, want a missing prerequisite naming base %s", err, produced.base)
	}
	if strings.Contains(err.Error(), "couldn't find remote ref") {
		t.Fatalf("error = %v still reads as a missing remote ref", err)
	}
}

// A bundle above the limit is not retained, the provenance record says why, and
// a consumer elsewhere is refused with that reason. A consumer whose own limit is
// lower than a delivered bundle refuses it too.
func TestCommitBundleSizeIsBounded(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	producer.refs.MaxBundleBytes = 64
	producer.preparer.CampaignRefs = producer.refs
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	if produced.bundle != nil || produced.provenance.Bundle != nil {
		t.Fatalf("an oversized bundle was retained: %+v", produced.provenance)
	}
	if produced.provenance.BundleOmitted != BundleOmittedSizeLimit {
		t.Fatalf("bundle omission = %q, want the fixed size-limit code", produced.provenance.BundleOmitted)
	}
	_, err := consume(t, consumer, produced, produced.record, nil, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), "storage.campaign_commit_bundle_max_bytes") {
		t.Fatalf("error = %v, want the size refusal", err)
	}

	generous := newCommitWorker(t, storage)
	retained := produceCommit(t, generous, repository, storage, produceOptions{})
	small := newCommitWorker(t, storage)
	small.refs.MaxBundleBytes = 16
	small.preparer.CampaignRefs = small.refs
	delivery := retained.delivery(t, nil)
	_, err = consume(t, small, retained, retained.record, &delivery, "attempt-3")
	if err == nil || !strings.Contains(err.Error(), "exceeds the limit of 16 bytes") {
		t.Fatalf("error = %v, want the consumer's size refusal", err)
	}
}

// The import lives in the consumer's store for exactly the campaign's lifetime:
// the worker's release of the run removes it, as it removes the producer's.
func TestImportedCommitIsReleasedOnBothWorkers(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	delivery := produced.delivery(t, nil)
	if _, err := consume(t, consumer, produced, produced.record, &delivery, "attempt-2"); err != nil {
		t.Fatalf("prepare consumer: %v", err)
	}
	ref := CampaignRef("run-1", "task-producer", "repair")
	for name, worker := range map[string]commitWorker{"producer": producer, "consumer": consumer} {
		runs, err := worker.refs.Runs()
		if err != nil || !reflect.DeepEqual(runs, []string{"run-1"}) {
			t.Fatalf("%s holds %v, %v", name, runs, err)
		}
		// The coordinator's keep list no longer names the run.
		release := CampaignRefReleaseReconciler{
			Records: noRetainedCampaigns,
			Refs:    worker.refs,
		}
		report := release.Tick(ctx)
		if len(report.Errors) != 0 || !reflect.DeepEqual(report.Released, []string{"run-1"}) {
			t.Fatalf("%s release = %+v", name, report)
		}
		if campaignRefExists(t, worker.refs, ref) {
			t.Fatalf("%s still holds %s after release", name, ref)
		}
		if runs, err := worker.refs.Runs(); err != nil || len(runs) != 0 {
			t.Fatalf("%s holds %v after release, %v", name, runs, err)
		}
	}
}

// A producer that ran on a build without the capability retained no bundle and
// recorded no reason. A consumer elsewhere is told which capability may be
// missing, never "couldn't find remote ref".
func TestConsumerOfAnOldProducerNamesTheMissingCapability(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{oldBuild: true})
	if produced.bundle != nil || produced.provenance.Bundle != nil {
		t.Fatalf("an old build retained a bundle: %+v", produced.provenance)
	}
	_, err := consume(t, consumer, produced, produced.record, nil, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("error = %v, want the missing capability named", err)
	}
	if strings.Contains(err.Error(), "couldn't find remote ref") {
		t.Fatalf("error = %v still reads as a missing remote ref", err)
	}

	// A record that names a bundle the package did not deliver is the
	// coordinator's omission and is reported as one.
	fresh := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	_, err = consume(t, newCommitWorker(t, storage), fresh, fresh.record, nil, "attempt-3")
	if err == nil || !strings.Contains(err.Error(), "was not delivered") || !strings.Contains(err.Error(), workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("error = %v, want the undelivered bundle named", err)
	}
}

// A producer that declares a commit and makes none declares its base. There is
// nothing to bundle, and the consumer elsewhere still gets a fetchable ref: the
// base is already in its repository cache.
func TestCommitEqualToItsBaseReachesAConsumerOnAnotherWorker(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{noChange: true})
	if produced.commit != produced.base || produced.provenance.Commit != produced.base {
		t.Fatalf("declared commit %s, base %s", produced.provenance.Commit, produced.base)
	}
	if produced.bundle != nil || produced.provenance.Bundle != nil || produced.provenance.BundleOmitted != "" {
		t.Fatalf("a commit equal to its base was bundled: %+v", produced.provenance)
	}
	prepared, err := consume(t, consumer, produced, produced.record, nil, "attempt-2")
	if err != nil {
		t.Fatalf("prepare consumer of an unchanged commit: %v", err)
	}
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", CampaignRef("run-1", "task-producer", "repair")+"^{commit}"); got != produced.base {
		t.Fatalf("consumer resolved %s, want base %s", got, produced.base)
	}
	if runs, err := consumer.refs.Runs(); err != nil || !reflect.DeepEqual(runs, []string{"run-1"}) {
		t.Fatalf("consumer holds %v, %v", runs, err)
	}
}
