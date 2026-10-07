package backlog

import (
	"context"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// twoCommitProducer is a finalized producer that declared two commits of the
// same change, with what a consumer of its first commit needs.
type twoCommitProducer struct {
	finalized FinalizedAttempt
	records   map[string]CommitProvenance
	// repair is the producer as a consumer of its first declared commit sees
	// it: the repository, the shared storage and that commit's record.
	repair producedCommit
}

// finalizeTwoCommits finalizes a producer that declares two commits of the
// same change, so each retains a bundle of the same, incompressible size. The
// result upload is bounded by limit, of which reserved bytes are already taken
// by what collection adds after finalization.
func finalizeTwoCommits(t *testing.T, limit, reserved int64) (FinalizedAttempt, map[string]CommitProvenance) {
	t.Helper()
	producer := finalizeTwoCommitsWith(t, 0, limit, reserved, true)
	return producer.finalized, producer.records
}

// uploadAdmission admits a result as an upload of its artifacts and of
// reserved further bytes would be admitted under per-artifact limit
// objectLimit and total limit totalLimit, by the shared upload validation. A
// zero objectLimit is totalLimit, and a zero totalLimit admits everything.
func uploadAdmission(objectLimit, totalLimit, reserved int64) func([]domain.Artifact) error {
	if totalLimit <= 0 {
		return nil
	}
	if objectLimit <= 0 {
		objectLimit = totalLimit
	}
	return func(artifacts []domain.Artifact) error {
		objects := make([]workerproto.ArtifactObject, 0, len(artifacts)+1)
		for _, artifact := range artifacts {
			objects = append(objects, workerproto.ArtifactObject{
				ID: artifact.ID, Path: "results/" + artifact.Name, Kind: string(artifact.Kind),
				MediaType: artifact.MediaType, Size: artifact.Size, SHA256: artifact.SHA256,
			})
		}
		objects = append(objects, workerproto.ArtifactObject{
			ID: "collection-reserved", Path: "results/thread.json", Kind: "log",
			MediaType: "application/json", Size: reserved, SHA256: strings.Repeat("0", 64),
		})
		_, err := workerproto.ValidateUploadObjects(objects, objectLimit, totalLimit)
		return err
	}
}

// finalizeTwoCommitsWith finalizes the two-commit producer with bundle
// generation enabled or disabled, admitted as an upload under objectLimit and
// limit (see uploadAdmission). The clock is fixed so that the provenance
// records of two producers have the same size. Git's commit dates are fixed for
// the same reason: git writes the current second into every commit, and a
// bundle's compressed size follows those digits, so two producers committed a
// second apart could differ by a byte and leave both bundles inside a limit
// computed from the first.
func finalizeTwoCommitsWith(t *testing.T, objectLimit, limit, reserved int64, bundles bool) twoCommitProducer {
	t.Helper()
	t.Setenv("GIT_AUTHOR_DATE", "2026-10-06T09:00:00Z")
	t.Setenv("GIT_COMMITTER_DATE", "2026-10-06T09:00:00Z")
	ctx := context.Background()
	repository := newGitFixture(t)
	storage := t.TempDir()
	worker := newCommitWorker(t, storage)
	task := workspaceTask("task-producer", "producer")
	request := workspaceRequest(repository, "main", task, "attempt-1")
	prepared, err := worker.preparer.Prepare(ctx, request)
	if err != nil {
		t.Fatalf("prepare producer workspace: %v", err)
	}
	cleanupImmutable(t, prepared.RootDir)
	noise := make([]byte, 4096)
	rand.New(rand.NewSource(16)).Read(noise)
	gitRun(t, prepared.WorkspaceDir, "config", "user.name", "Test User")
	gitRun(t, prepared.WorkspaceDir, "config", "user.email", "test@example.test")
	writeGitFile(t, prepared.WorkspaceDir, "noise.bin", string(noise))
	gitRun(t, prepared.WorkspaceDir, "add", "noise.bin")
	gitRun(t, prepared.WorkspaceDir, "commit", "-m", "noise")
	task.Outputs = []domain.ArtifactDeclaration{
		{Name: "repair", Commit: &domain.CommitOutput{}},
		{Name: "followup", Commit: &domain.CommitOutput{}},
	}
	finalizer := AttemptFinalizer{
		StorageRoot: storage, CampaignRefs: worker.refs, Processes: testProcessRunner{},
		Now: func() time.Time { return time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC) },
	}
	finalized, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: task, Attempt: request.Attempt, WorkspaceDir: prepared.WorkspaceDir,
		ExplicitSuccess: true, Repository: repository, BaseCommit: prepared.Commit,
		CommitBundles: bundles, AdmitResult: uploadAdmission(objectLimit, limit, reserved),
	})
	if err != nil {
		t.Fatalf("finalize producer: %v", err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("producer failed: %+v", finalized.Completion)
	}
	producer := twoCommitProducer{
		finalized: finalized, records: map[string]CommitProvenance{},
		repair: producedCommit{repository: repository, storage: storage, base: prepared.Commit},
	}
	for _, artifact := range finalized.Artifacts {
		if artifact.Kind != domain.ArtifactOutput {
			continue
		}
		provenance, err := ParseCommitProvenance([]byte(readTestFile(t, storage, artifact.StoragePath)))
		if err != nil {
			t.Fatalf("parse provenance record %s: %v", artifact.Name, err)
		}
		producer.records[artifact.Name] = provenance
		if artifact.Name == "repair" {
			producer.repair.record, producer.repair.provenance, producer.repair.commit = artifact, provenance, provenance.Commit
		}
	}
	return producer
}

func retainedBundles(finalized FinalizedAttempt) (names []string, bytes int64) {
	for _, artifact := range finalized.Artifacts {
		if artifact.Kind == domain.ArtifactGitState {
			names = append(names, artifact.Name)
			bytes += artifact.Size
		}
	}
	return names, bytes
}

func finalizedBytes(finalized FinalizedAttempt) int64 {
	var total int64
	for _, artifact := range finalized.Artifacts {
		total += artifact.Size
	}
	return total
}

// Bundles travel in the producer's one result upload together with every other
// result, the final message and the thread archive, and that upload has one
// aggregate limit. Two bundles that each fit the per-artifact limit must not
// together turn a producer that would otherwise collect into a permanent
// collection failure: a bundle that does not fit the upload is left out and the
// record says why, exactly as an oversized bundle is.
func TestCommitBundlesAreBudgetedWithTheWholeResultUpload(t *testing.T) {
	const reserved = 1500
	unbounded, _ := finalizeTwoCommits(t, 0, reserved)
	names, bundleBytes := retainedBundles(unbounded)
	if len(names) != 2 || bundleBytes < 8192 {
		t.Fatalf("unbounded producer retained %v (%d bytes), want two bundles", names, bundleBytes)
	}
	limit := finalizedBytes(unbounded) + reserved - 1

	budgeted, records := finalizeTwoCommits(t, limit, reserved)
	if total := finalizedBytes(budgeted) + reserved; total > limit {
		t.Fatalf("result upload is %d bytes, over its limit of %d", total, limit)
	}
	if names, _ := retainedBundles(budgeted); !slices.Equal(names, []string{CommitBundleArtifactName("repair")}) {
		t.Fatalf("retained bundles = %v, want only the first declared commit's", names)
	}
	if records["repair"].Bundle == nil || records["repair"].BundleOmitted != "" {
		t.Fatalf("repair record = %+v", records["repair"])
	}
	if records["followup"].Bundle != nil || records["followup"].BundleOmitted != BundleOmittedAggregateLimit {
		t.Fatalf("followup record = %+v, want the aggregate omission code", records["followup"])
	}
}

// The invariant of the result budget: bundle bookkeeping never makes a
// producer fail that would collect with bundle generation disabled. The
// ordinary result is measured with bundles disabled, and the same producer with
// bundles enabled is finalized at that size and just above it. Omission records
// are a fixed reason code counted in the budget; where even that does not fit,
// the record carries no bundle metadata at all, and a consumer on another
// worker is still refused by name rather than with a missing ref.
func TestOmissionMetadataNeverFailsAProducerThatFitsWithoutBundles(t *testing.T) {
	const reserved = 1500
	plainProducer := finalizeTwoCommitsWith(t, 0, 0, reserved, false)
	plain := finalizedBytes(plainProducer.finalized) + reserved
	for name, record := range plainProducer.records {
		if record.Bundle != nil || record.BundleOmitted != "" {
			t.Fatalf("%s record without bundles = %+v", name, record)
		}
	}
	stripped, err := MarshalCommitProvenance(plainProducer.records["repair"])
	if err != nil {
		t.Fatal(err)
	}
	coded := plainProducer.records["repair"]
	coded.BundleOmitted = BundleOmittedAggregateLimit
	withCode, err := MarshalCommitProvenance(coded)
	if err != nil {
		t.Fatal(err)
	}
	omission := int64(len(withCode) - len(stripped))
	if omission <= 0 || omission > 64 {
		t.Fatalf("an omission record adds %d bytes, want a small fixed record", omission)
	}

	ref := CampaignRef("run-1", "task-producer", "repair")
	// The review's reproduction left 64 spare bytes, room for one omission
	// record but not two.
	if 2*omission <= 64 {
		t.Fatalf("an omission record adds %d bytes; the 64-byte case expects room for exactly one", omission)
	}
	for _, test := range []struct {
		name             string
		limit            int64
		repair, followup string
	}{
		{name: "not even an omission record fits", limit: plain},
		{name: "one omission record fits", limit: plain + omission, repair: BundleOmittedAggregateLimit},
		{name: "only omission records fit", limit: plain + 2*omission,
			repair: BundleOmittedAggregateLimit, followup: BundleOmittedAggregateLimit},
		{name: "review's 64 spare bytes", limit: plain + 64, repair: BundleOmittedAggregateLimit},
	} {
		// Not subtests: the repository path is in every record, and a
		// subtest's temporary directory would change its length. The
		// per-artifact limit is ample, so the aggregate is what binds.
		producer := finalizeTwoCommitsWith(t, 1<<20, test.limit, reserved, true)
		if total := finalizedBytes(producer.finalized) + reserved; total > test.limit {
			t.Fatalf("%s: result upload is %d bytes, over its limit of %d; the same result without bundles is %d bytes",
				test.name, total, test.limit, plain)
		}
		if names, _ := retainedBundles(producer.finalized); len(names) != 0 {
			t.Fatalf("%s: retained bundles = %v under a limit with room for none", test.name, names)
		}
		for name, want := range map[string]string{"repair": test.repair, "followup": test.followup} {
			if record := producer.records[name]; record.Bundle != nil || record.BundleOmitted != want {
				t.Fatalf("%s: %s record = %+v, want omission %q", test.name, name, record, want)
			}
		}

		// A consumer on another worker is refused with the commit named and
		// the reason, or that no reason was recorded.
		consumer := newCommitWorker(t, producer.repair.storage)
		_, err := consume(t, consumer, producer.repair, producer.repair.record, nil, "attempt-2")
		if err == nil || !strings.Contains(err.Error(), ref) {
			t.Fatalf("%s: error = %v, want a refusal naming %s", test.name, err, ref)
		}
		if test.repair != "" && !strings.Contains(err.Error(), "total limit of its result upload") {
			t.Fatalf("%s: error = %v, want the aggregate omission explained", test.name, err)
		}
		if test.repair == "" && !strings.Contains(err.Error(), "recorded no reason") {
			t.Fatalf("%s: error = %v, want the missing reason explained", test.name, err)
		}
		if strings.Contains(err.Error(), "couldn't find remote ref") {
			t.Fatalf("%s: error = %v still reads as a missing remote ref", test.name, err)
		}
	}
}

// The same invariant at the per-artifact limit, which the review of round 3
// found the aggregate budget did not cover: with ample total room, a
// provenance record that fits the per-artifact limit without bundles must not
// be made to exceed it by bundle metadata. The plain records are measured with
// bundles disabled, and the producer is finalized with them enabled at a
// per-artifact limit of exactly the larger record, where no omission code fits
// it, and with room for a code in each record.
func TestOmissionMetadataNeverExceedsThePerArtifactLimit(t *testing.T) {
	const reserved, total = 100, 1 << 20
	plainProducer := finalizeTwoCommitsWith(t, 0, 0, reserved, false)
	var largest int64
	for _, artifact := range plainProducer.finalized.Artifacts {
		largest = max(largest, artifact.Size)
	}
	stripped, err := MarshalCommitProvenance(plainProducer.records["followup"])
	if err != nil {
		t.Fatal(err)
	}
	coded := plainProducer.records["followup"]
	coded.BundleOmitted = BundleOmittedSizeLimit
	withCode, err := MarshalCommitProvenance(coded)
	if err != nil {
		t.Fatal(err)
	}
	omission := int64(len(withCode) - len(stripped))

	ref := CampaignRef("run-1", "task-producer", "repair")
	for _, test := range []struct {
		name        string
		objectLimit int64
		coded       bool
	}{
		{name: "the largest ordinary object exactly", objectLimit: largest},
		{name: "room for a code in every record", objectLimit: largest + omission, coded: true},
	} {
		// Not subtests: the repository path is in every record, and a
		// subtest's temporary directory would change its length.
		producer := finalizeTwoCommitsWith(t, test.objectLimit, total, reserved, true)
		for _, artifact := range producer.finalized.Artifacts {
			if artifact.Size > test.objectLimit {
				t.Fatalf("%s: %s is %d bytes, over the per-artifact limit of %d; without bundles the largest is %d bytes",
					test.name, artifact.Name, artifact.Size, test.objectLimit, largest)
			}
		}
		if names, _ := retainedBundles(producer.finalized); len(names) != 0 {
			t.Fatalf("%s: retained bundles = %v over the per-artifact limit", test.name, names)
		}
		for name, record := range producer.records {
			if record.Bundle != nil || (record.BundleOmitted != "" && record.BundleOmitted != BundleOmittedSizeLimit) {
				t.Fatalf("%s: %s record = %+v", test.name, name, record)
			}
			if test.coded && record.BundleOmitted != BundleOmittedSizeLimit {
				t.Fatalf("%s: %s record = %+v, want the size omission code", test.name, name, record)
			}
		}
		if !test.coded && producer.records["followup"].BundleOmitted != "" {
			t.Fatalf("%s: followup record = %+v, want it bare, with no room for a code", test.name, producer.records["followup"])
		}

		consumer := newCommitWorker(t, producer.repair.storage)
		_, err := consume(t, consumer, producer.repair, producer.repair.record, nil, "attempt-2")
		if err == nil || !strings.Contains(err.Error(), ref) {
			t.Fatalf("%s: error = %v, want a refusal naming %s", test.name, err, ref)
		}
		if producer.repair.provenance.BundleOmitted != "" && !strings.Contains(err.Error(), "per-artifact limit") {
			t.Fatalf("%s: error = %v, want the per-artifact omission explained", test.name, err)
		}
		if producer.repair.provenance.BundleOmitted == "" && !strings.Contains(err.Error(), "recorded no reason") {
			t.Fatalf("%s: error = %v, want the missing reason explained", test.name, err)
		}
	}
}

// twoCommitBundleFixture is a producer on producerWorker that declared two
// commits whose retained bundles are each exactly the per-artifact limit.
func twoCommitBundleFixture(now time.Time, producerWorker string, bundleSize int64) (sqlite.CoordinatorRecords, domain.Assignment) {
	records, assignment := commitBundleFixture(now, producerWorker)
	for index := range records.Tasks {
		switch records.Tasks[index].ID {
		case "task-producer":
			records.Tasks[index].Outputs = append(records.Tasks[index].Outputs,
				domain.ArtifactDeclaration{Name: "followup", MediaType: "application/json", Commit: &domain.CommitOutput{}})
		case "task-consumer":
			records.Tasks[index].DependencyInputs = map[string][]string{"producer": {"reports/result.txt", "repair", "followup"}}
		}
	}
	for index := range records.Artifacts {
		if records.Artifacts[index].ID == "bundle-1" {
			records.Artifacts[index].Size = bundleSize
		}
	}
	records.Artifacts = append(records.Artifacts,
		domain.Artifact{
			ID: "provenance-2", WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: "attempt-producer",
			Kind: domain.ArtifactOutput, Name: "followup", MediaType: "application/json", Size: 300,
			SHA256: strings.Repeat("e", 64), StoragePath: "objects/provenance-2", Producer: "producer", CreatedAt: now,
		},
		domain.Artifact{
			ID: "bundle-2", WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: "attempt-producer",
			Kind: domain.ArtifactGitState, Name: CommitBundleArtifactName("followup"), MediaType: CommitBundleMediaType,
			Size: bundleSize, SHA256: strings.Repeat("f", 64), StoragePath: "objects/bundle-2", Producer: "producer", CreatedAt: now,
		},
	)
	return records, assignment
}

// Two bundles that each fit the per-artifact limit can still exceed the
// package's total. The offer is not withheld forever: it is built, carries the
// bundle that fits, and names the one that does not with the reason, so the
// consuming worker refuses it specifically if it needs it.
func TestCommitBundlesBeyondThePackageTotalAreNamedNotWithheld(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := twoCommitBundleFixture(now, "omarchy-pc", 1<<20)
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build consumer offer: %v", err)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 4<<20); err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if len(pkg.CommitBundles) != 2 {
		t.Fatalf("commit bundles = %+v", pkg.CommitBundles)
	}
	var delivered, omitted workerproto.CommitBundleInput
	for _, input := range pkg.CommitBundles {
		if input.Bundle != nil {
			delivered = input
		} else {
			omitted = input
		}
	}
	if delivered.Bundle == nil || delivered.Omitted != "" || omitted.Name == "" ||
		!strings.Contains(omitted.Omitted, "total byte limit") || !strings.Contains(omitted.Omitted, "2097152") {
		t.Fatalf("commit bundles = %+v, want one delivered and one named as over the total", pkg.CommitBundles)
	}
	if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("required capabilities = %v", pkg.RequiredCapabilities)
	}

	// The protocol accepts exactly one of a bundle and a reason.
	for name, edit := range map[string]func(*workerproto.CommitBundleInput){
		"both":    func(input *workerproto.CommitBundleInput) { input.Bundle = delivered.Bundle },
		"neither": func(input *workerproto.CommitBundleInput) { input.Omitted = "" },
	} {
		broken := pkg
		broken.CommitBundles = slices.Clone(pkg.CommitBundles)
		for index := range broken.CommitBundles {
			if broken.CommitBundles[index].Bundle == nil {
				edit(&broken.CommitBundles[index])
			}
		}
		if err := workerproto.ValidateExecutionPackage(broken); err == nil || !strings.Contains(err.Error(), "either delivered or omitted") {
			t.Fatalf("%s: error = %v, want the bundle shape refused", name, err)
		}
	}
}

// The consuming worker refuses a bundle the package could not carry with the
// coordinator's reason, but only when its own store does not already hold the
// commit.
func TestConsumerRefusesACommitWhoseBundleThePackageCouldNotCarry(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	reason := "its bundle is 1048576 bytes, and with this task's other inputs the execution package would exceed its total byte limit of 2097152 bytes"
	omitted := CommitBundleDelivery{Omitted: reason}
	_, err := consume(t, consumer, produced, produced.record, &omitted, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), reason) || !strings.Contains(err.Error(), CampaignRef("run-1", "task-producer", "repair")) {
		t.Fatalf("error = %v, want the package omission named", err)
	}
	if _, err := consume(t, producer, produced, produced.record, &omitted, "attempt-3"); err != nil {
		t.Fatalf("a worker that holds the commit needed no bundle: %v", err)
	}
}

// carriedCommitFixture is a consumer whose declared-commit input was carried
// from another run: a rerun's source or an external dependency. The source
// producer ran on producerWorker, and an older attempt of the same source task
// retained a different bundle that must never be selected.
func carriedCommitFixture(now time.Time, producerWorker string) (sqlite.CoordinatorRecords, domain.Assignment) {
	records, assignment := packageBuilderFixture(now)
	for index := range records.Tasks {
		if records.Tasks[index].ID == "task-consumer" {
			records.Tasks[index].Needs = nil
			records.Tasks[index].DependencyInputs = nil
			records.Tasks[index].CarriedInputs = []domain.CarriedInput{{
				Producer: "implement", ProducerNamespace: "external-implement", ProducerTaskID: "task-source",
				SourceRunID: "run-source", SourceAttemptID: "attempt-source", SourceArtifactID: "provenance-source",
				Name: "repair", ArtifactID: "reference-1",
			}}
		}
	}
	sourceAttempt := domain.Attempt{
		ID: "attempt-source", WorkflowRunID: "run-source", TaskID: "task-source", Number: 2,
		Progress: domain.ProgressSucceeded, Control: domain.ControlUnassigned,
		Revision: 5, AssignmentID: "assignment-source", UpdatedAt: now,
	}
	sourceAssignment := assignment
	sourceAssignment.ID, sourceAssignment.AttemptID = "assignment-source", sourceAttempt.ID
	sourceAssignment.WorkerID, sourceAssignment.Route.WorkerID = producerWorker, producerWorker
	sourceAssignment.DispatchToken, sourceAssignment.ThreadID = "dispatch-source", "thread-source"
	sourceAssignment.LeaseToken = "lease-source"
	records.Attempts = append(records.Attempts, sourceAttempt)
	records.Assignments = append(records.Assignments, sourceAssignment)
	artifact := func(id, runID, taskID, attemptID string, kind domain.ArtifactKind, name, media, digest string) domain.Artifact {
		return domain.Artifact{
			ID: id, WorkflowRunID: runID, TaskID: taskID, AttemptID: attemptID, Kind: kind, Name: name,
			MediaType: media, Size: 300, SHA256: strings.Repeat(digest, 64), StoragePath: "objects/" + id,
			Producer: "implement", CreatedAt: now,
		}
	}
	records.Artifacts = append(records.Artifacts,
		artifact("provenance-source", "run-source", "task-source", "attempt-source", domain.ArtifactOutput, "repair", "application/json", "b"),
		artifact("reference-1", "run-1", "task-consumer", "", domain.ArtifactInput, "repair", "application/json", "b"),
		artifact("bundle-source", "run-source", "task-source", "attempt-source", domain.ArtifactGitState,
			CommitBundleArtifactName("repair"), CommitBundleMediaType, "c"),
		artifact("bundle-stale", "run-source", "task-source", "attempt-stale", domain.ArtifactGitState,
			CommitBundleArtifactName("repair"), CommitBundleMediaType, "d"),
	)
	return records, assignment
}

// A consumer that carries a declared commit from another run is sent the
// bundle of exactly the source attempt it pinned, bound to the source run, when
// it runs on another worker than that attempt; on the source attempt's worker
// it is sent nothing; and a worker without the capability is refused by name.
func TestCarriedCommitConsumerIsSentTheSourceRunBundle(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := carriedCommitFixture(now, "omarchy-pc")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build carried consumer offer: %v", err)
	}
	pkg := offer.Package.Package
	want := []workerproto.CommitBundleInput{{
		WorkflowRunID: "run-source", TaskID: "task-source", Name: "repair",
		Bundle: &workerproto.ArtifactObject{
			ID: "bundle-source", Path: "commit-bundles/run-source/task-source/repair.bundle", Kind: "commit-bundle",
			MediaType: CommitBundleMediaType, Size: 300, SHA256: strings.Repeat("c", 64),
		},
	}}
	if !reflect.DeepEqual(pkg.CommitBundles, want) {
		t.Fatalf("commit bundles = %+v, want %+v", pkg.CommitBundles, want)
	}
	if len(pkg.Dependencies) != 1 || pkg.Dependencies[0].Provenance == nil || pkg.Dependencies[0].Provenance.RunID != "run-source" ||
		pkg.Dependencies[0].Provenance.TaskID != "task-source" {
		t.Fatalf("carried dependency = %+v", pkg.Dependencies)
	}
	if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("required capabilities = %v", pkg.RequiredCapabilities)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}

	same, sameAssignment := carriedCommitFixture(now, "normandy")
	offer, err = commitBundleBuilder(t, same, map[string][]string{"normandy": nil}).
		BuildAssignmentOffer(context.Background(), sameAssignment, now.Add(time.Minute))
	if err != nil || len(offer.Package.Package.CommitBundles) != 0 {
		t.Fatalf("same-worker carried consumer: bundles %+v, %v", offer.Package.Package.CommitBundles, err)
	}

	_, err = commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityPreflight}}).
		BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityCommitBundle) {
		t.Fatalf("error = %v, want the missing capability named", err)
	}
}

// consumeCarried prepares, in run-2, a consumer that carried the producer's
// commit from run-1 under the dependency directory namespace, as a worker
// receives it: the record is rebound to the consuming run, and the dependency's
// source binding is whatever sources says.
func consumeCarried(t *testing.T, consumer commitWorker, produced producedCommit, sources map[string]DependencySource, delivery *CommitBundleDelivery, attemptID string) (PreparedWorkspace, error) {
	t.Helper()
	const namespace = "external-producer"
	task := workspaceTask("task-consumer", "consumer")
	task.Needs = []string{namespace}
	task.DependencyInputs = map[string][]string{namespace: {"repair"}}
	request := workspaceRequest(produced.repository, "main", task, attemptID)
	request.WorkflowRunID, request.Attempt.WorkflowRunID = "run-2", "run-2"
	record := produced.record
	record.WorkflowRunID, record.TaskID, record.AttemptID = "run-2", namespace, ""
	request.DependencyTasks = []domain.Task{{ID: namespace, WorkflowID: "workflow-1", Name: namespace,
		Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}, task}
	request.DependencyArtifacts = []domain.Artifact{record}
	request.DependencySources = sources
	if delivery != nil {
		request.CommitBundles = map[string]CommitBundleDelivery{CampaignRef("run-1", "task-producer", "repair"): *delivery}
	}
	prepared, err := consumer.preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
	}
	return prepared, err
}

// A commit carried from run-1 into a consumer of run-2 on another worker is
// imported from the source run's bundle under the source run's ref, and the
// import is held for the source run, which is what the release reconciler
// keeps for as long as the carried record is retained. The source binding of
// the exact dependency the record arrived in is what lets a record of another
// run through; without it, or with one naming another producer, the record is
// refused as before.
func TestCarriedCommitIsImportedOnAnotherWorker(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	delivery := produced.delivery(t, nil)

	_, err := consumeCarried(t, consumer, produced, nil, &delivery, "attempt-2")
	if err == nil || !strings.Contains(err.Error(), `belongs to run "run-1", want "run-2"`) {
		t.Fatalf("error = %v, want an unbound record of another run refused", err)
	}
	_, err = consumeCarried(t, consumer, produced, map[string]DependencySource{
		"external-producer": {WorkflowRunID: "run-1", TaskID: "task-other"},
	}, &delivery, "attempt-3")
	if err == nil || !strings.Contains(err.Error(), "task-other") {
		t.Fatalf("error = %v, want a record of another producer refused", err)
	}
	if runs, err := consumer.refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("a refused carried commit left %v in the consumer's store, %v", runs, err)
	}

	sources := map[string]DependencySource{"external-producer": {WorkflowRunID: "run-1", TaskID: "task-producer"}}
	prepared, err := consumeCarried(t, consumer, produced, sources, &delivery, "attempt-4")
	if err != nil {
		t.Fatalf("prepare carried consumer on another worker: %v", err)
	}
	ref := CampaignRef("run-1", "task-producer", "repair")
	if got := gitOutput(t, prepared.WorkspaceDir, "rev-parse", ref+"^{commit}"); got != produced.commit {
		t.Fatalf("carried consumer resolved %s, want %s", got, produced.commit)
	}
	if runs, err := consumer.refs.Runs(); err != nil || !reflect.DeepEqual(runs, []string{"run-1"}) {
		t.Fatalf("consumer holds %v, %v; want the import held for the source run", runs, err)
	}
	if _, err := consumeCarried(t, producer, produced, sources, nil, "attempt-5"); err != nil {
		t.Fatalf("prepare carried consumer on the producer's worker: %v", err)
	}
}
