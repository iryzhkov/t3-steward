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

// judgeOfOrdinaryOutput prepares, on worker, a judge that consumes an
// attacker task's output named name, which holds forged as its content. The
// attacker declares the output as declared says.
func judgeOfOrdinaryOutput(t *testing.T, worker commitWorker, produced producedCommit, name string, declared domain.ArtifactDeclaration, forged CommitProvenance) error {
	t.Helper()
	output := forgedOutput(t, produced, name, forged)
	output.TaskID = "task-attacker"
	attacker := workspaceTask("task-attacker", "attacker")
	attacker.Outputs = []domain.ArtifactDeclaration{declared}
	judge := workspaceTask("judge", "judge")
	judge.Needs = []string{"attacker"}
	judge.DependencyInputs = map[string][]string{"attacker": {name}}
	request := workspaceRequest(produced.repository, "main", judge, "attempt-judge")
	request.DependencyTasks = []domain.Task{attacker, judge}
	request.DependencyArtifacts = []domain.Artifact{output}
	prepared, err := worker.preparer.Prepare(context.Background(), request)
	if err == nil {
		cleanupImmutable(t, prepared.RootDir)
	}
	return err
}

// Review round 2, finding 1: an ordinary output of one task, holding a record
// that names another producer's review-declared commit output at its base,
// passed the filename check. A judge's worker then published the victim's
// campaign ref without acceptance, and the real accepted consumer failed on
// it. The record is the content of an ordinary output, so it publishes
// nothing, and the accepted consumer publishes the accepted commit.
func TestAnOrdinaryOutputCannotPoisonAnotherProducersCommit(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
	forged := produced.provenance
	forged.StagedAttempt, forged.Bundle, forged.BundleOmitted, forged.Commit = "", nil, "", forged.Base
	worker := newCommitWorker(t, storage)
	if err := judgeOfOrdinaryOutput(t, worker, produced, "repair", domain.ArtifactDeclaration{Name: "repair"}, forged); err != nil {
		t.Fatalf("judge of an ordinary output: %v", err)
	}
	if provenance, err := worker.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatalf("an ordinary output of another task published the producer's campaign ref: %+v", provenance)
	}
	delivery := produced.delivery(t, nil)
	if _, err := consumeReviewed(t, worker, produced, &delivery, "attempt-real-consumer", true); err != nil {
		t.Fatalf("the accepted consumer was blocked: %v", err)
	}
	if provenance, err := worker.refs.Resolve("run-1", "task-producer", "repair"); err != nil || provenance.Commit != produced.commit {
		t.Fatalf("accepted consumer published %+v %v, want %s", provenance, err, produced.commit)
	}
}

// The declared commit output of one task vouches only for that task's own
// commit. A record there naming another producer is refused, and publishes
// nothing.
func TestADeclaredCommitOutputCannotNameAnotherProducer(t *testing.T) {
	repository := newGitFixture(t)
	storage := t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{reviewGated: true})
	forged := produced.provenance
	forged.StagedAttempt, forged.Bundle, forged.BundleOmitted, forged.Commit = "", nil, "", forged.Base
	worker := newCommitWorker(t, storage)
	err := judgeOfOrdinaryOutput(t, worker, produced, "repair", domain.ArtifactDeclaration{Name: "repair", Commit: &domain.CommitOutput{}}, forged)
	if err == nil || !strings.Contains(err.Error(), `but its record names task "task-producer"`) {
		t.Fatalf("a declared commit output naming another producer: %v", err)
	}
	if provenance, err := worker.refs.Resolve("run-1", "task-producer", "repair"); err == nil {
		t.Fatalf("published %+v", provenance)
	}
}

// A record in an ordinary output that names its own task and its own file,
// so that every name the old check compared matches, is still the executor's
// content: no campaign ref is published for an output that is not a declared
// commit, for a judge or for an accepted consumer.
func TestASelfNamedRecordInAnOrdinaryOutputPublishesNothing(t *testing.T) {
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
			forged.Name, forged.Ref = "notes.json", CampaignRef("run-1", "task-producer", "notes.json")
			notes := forgedOutput(t, produced, "notes.json", forged)
			consumer := newCommitWorker(t, storage)
			task := workspaceTask("task-consumer", "consumer")
			task.Needs = []string{"producer"}
			task.DependencyInputs = map[string][]string{"producer": {"repair", "notes.json"}}
			request := workspaceRequest(produced.repository, "main", task, "attempt-2")
			request.DependencyTasks = []domain.Task{commitProducerTask(), task}
			request.DependencyArtifacts = []domain.Artifact{produced.record, notes}
			delivery := produced.delivery(t, nil)
			request.CommitBundles = map[string]CommitBundleDelivery{CampaignRef("run-1", "task-producer", "repair"): delivery}
			if accepted {
				request.AcceptedCommits = map[string][]string{"task-producer": {"repair"}}
			}
			prepared, err := consumer.preparer.Prepare(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, prepared.RootDir)
			if provenance, err := consumer.refs.Resolve("run-1", "task-producer", "notes.json"); err == nil {
				t.Fatalf("an ordinary output published a campaign ref: %+v", provenance)
			}
			if refs := gitOutput(t, prepared.WorkspaceDir, "for-each-ref", "--format=%(refname)", "refs/campaigns/"); strings.Contains(refs, "notes.json") {
				t.Fatalf("consumer workspace received a ref for an ordinary output: %s", refs)
			}
		})
	}
}

// The coordinator names a dependency's declared commit outputs, and only
// them, to a worker it froze the continuation decision for; the decision is
// what a replay reads, so the package is the same when the inventory can no
// longer be read. A worker without that decision gets the package unmarked.
func TestPackagesMarkOnlyDeclaredCommitOutputs(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	// The producer ran on the consumer's worker, so no bundle is required and
	// a replay needs no inventory.
	records, assignment := commitBundleFixture(now, "normandy")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()})
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if len(pkg.Dependencies) != 1 || !slices.Equal(pkg.Dependencies[0].CommitOutputs, []string{"repair"}) {
		t.Fatalf("dependencies = %+v, want only repair marked", pkg.Dependencies)
	}
	if !pkg.MarksCommitOutputs() {
		t.Fatalf("required capabilities = %v", pkg.RequiredCapabilities)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}

	store := builder.Store.(packageRecordStore)
	store.snapshots = nil
	builder.WorkerCapabilities, builder.Store = nil, store
	replay, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Package.SHA256 != offer.Package.SHA256 {
		t.Fatalf("replayed package changed: %+v", replay.Package.Package.Dependencies)
	}

	plain, err := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}}).
		BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Package.Package.MarksCommitOutputs() || plain.Package.Package.Dependencies[0].CommitOutputs != nil {
		t.Fatalf("an older worker was sent marks: %+v %v", plain.Package.Package.Dependencies, plain.Package.Package.RequiredCapabilities)
	}
}

// An input carried from another run is marked by the declaration of the
// source task it was carried from, found in its source run.
func TestCarriedCommitOutputsFollowTheSourceDeclaration(t *testing.T) {
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-source", WorkflowID: "workflow-source"}},
		Tasks: []domain.Task{{ID: "task-source", WorkflowID: "workflow-source", Name: "implement", Outputs: []domain.ArtifactDeclaration{
			{Name: "repair", Commit: &domain.CommitOutput{}}, {Name: "notes.json"},
		}}},
	}
	carried := func(name, run string) domain.CarriedInput {
		return domain.CarriedInput{Producer: "implement", ProducerNamespace: "external-implement", ProducerTaskID: "task-source",
			SourceRunID: run, Name: name}
	}
	for _, test := range []struct {
		input domain.CarriedInput
		want  bool
	}{
		{carried("repair", "run-source"), true},
		{carried("notes.json", "run-source"), false},
		{carried("repair", "run-other"), false},
		{carried("repair", ""), false},
	} {
		if got := carriedCommitDeclared(records, test.input); got != test.want {
			t.Fatalf("carriedCommitDeclared(%+v) = %v, want %v", test.input, got, test.want)
		}
	}

	task := domain.Task{CarriedInputs: []domain.CarriedInput{carried("repair", "run-source"), carried("notes.json", "run-source")}}
	dependencies := []workerproto.DependencyInput{{TaskID: "external-implement", Artifacts: []workerproto.ArtifactObject{
		{Path: "dependencies/external-implement/notes.json"}, {Path: "dependencies/external-implement/repair"},
	}}}
	markDependencyCommitOutputs(dependencies, task, nil, func(input domain.CarriedInput) bool { return carriedCommitDeclared(records, input) })
	if !slices.Equal(dependencies[0].CommitOutputs, []string{"repair"}) {
		t.Fatalf("carried commit outputs = %v", dependencies[0].CommitOutputs)
	}
}
