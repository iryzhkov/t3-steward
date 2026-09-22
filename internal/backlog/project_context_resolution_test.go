package backlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func projectContextTemplate(now time.Time, refs ...domain.ProjectContextReference) *domain.ProjectContext {
	fresh := now.Add(time.Hour)
	required := make([]string, len(refs))
	for n := range refs {
		required[n] = refs[n].ID
	}
	return &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1",
		Objective: "execute from exact retained context", Budget: "one attempt",
		Outputs: []string{"out.txt"}, RequiredReferences: required, References: refs,
		Setup: []string{"true"}, Checks: []string{"true"},
		Freshness: domain.ProjectContextFreshness{ObservedAt: now.Add(-time.Minute), FreshThrough: &fresh},
	}
}

func TestResolvedProjectContextsBuildAssignmentOffers(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	git := domain.ProjectContextReference{
		ID: "git", Kind: domain.ContextReferenceGit, URI: "ssh://git/steward",
		Revision: strings.Repeat("b", 40), Artifact: "context.txt",
	}
	jocasta := domain.ProjectContextReference{
		ID: "jocasta", Kind: domain.ContextReferenceJocasta,
		URI: "jocasta:0123456789abcdef0123456789abcdef@7", Revision: "7", Artifact: "context.txt",
	}
	execution := domain.ProjectContextReference{
		ID: "execution", Kind: domain.ContextReferenceExecution,
		URI:      "execution:run-1/task-producer/attempt-producer/output-1",
		Revision: strings.Repeat("a", 64),
	}
	for name, refs := range map[string][]domain.ProjectContextReference{
		"git-only":     {git},
		"jocasta-only": {jocasta},
		"mixed":        {execution, git, jocasta},
	} {
		t.Run(name, func(t *testing.T) {
			records, assignment := packageBuilderFixture(now)
			records.Tasks[1].RunID = "run-1"
			records.Tasks[0].RunID = "run-1"
			records.Attempts = append(records.Attempts, domain.Attempt{
				ID: "attempt-producer", WorkflowRunID: "run-1", TaskID: "task-producer",
				Number: 1, Revision: 9, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
			})
			resolved, err := resolveAuthoredProjectContext(projectContextTemplate(now, refs...),
				map[string]domain.Artifact{"context.txt": records.Artifacts[1]}, now)
			if err != nil {
				t.Fatal(err)
			}
			if name == "mixed" {
				retained := records.Artifacts[2]
				retained.ID = "retained-execution"
				retained.TaskID = "task-consumer"
				retained.AttemptID = ""
				retained.Kind = domain.ArtifactInput
				records.Artifacts = append(records.Artifacts, retained)
				records.Tasks[1].DependencyInputs = nil
				records.Tasks[1].CarriedInputs = []domain.CarriedInput{{
					Producer: "producer", ProducerNamespace: "producer", ProducerTaskID: "task-producer",
					SourceRunID: "run-1", SourceAttemptID: "attempt-producer", SourceArtifactID: "output-1",
					Name: "reports/result.txt", ArtifactID: retained.ID,
				}}
				decision := domain.GateDecision{
					ID: "decision-1", RunID: "run-1", GateID: "gate-1",
					Actor:   domain.Actor{Kind: domain.ActorOverseer, Principal: "reviewer", ActivationEpoch: 3},
					Outcome: domain.GateDecisionAccept, Reason: "evidence accepted",
					Evidence: domain.EvidenceSnapshot{
						ID: "evidence-1", GraphRevision: 4,
						Producers: []domain.ProducerEvidence{{
							TaskID: "task-producer", AttemptID: "attempt-producer", ResultRevision: 9,
							ArtifactDigests:  []domain.ArtifactDigest{{ArtifactID: "output-1", Digest: "sha256:" + strings.Repeat("a", 64)}},
							CommitIdentities: []domain.CommitIdentity{{Repository: "ssh://git/steward", Commit: strings.Repeat("c", 40)}},
						}},
					},
				}
				acceptance := &resolvedProjectContextAcceptance{
					Receipt:  domain.ProjectContextAcceptance{GateID: "gate-1", DecisionID: decision.ID, EvidenceSnapshotID: decision.Evidence.ID},
					Decision: decision,
				}
				if err := bindExternalProjectContext(resolved, "run-1", "task-producer", "attempt-producer",
					records.Artifacts[2], retained, "producer", acceptance); err != nil {
					t.Fatal(err)
				}
			}
			if err := finalizeProjectContext(resolved); err != nil {
				t.Fatal(err)
			}
			records.Tasks[1].Context = resolved
			builder := packageBuilder(t, records)
			builder.WorkerCapabilities = map[string][]string{"normandy": {workerproto.PackageCapabilityProjectContext}}
			offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatalf("resolved context failed at BuildAssignmentOffer: %v", err)
			}
			raw, err := json.Marshal(offer.Package.Package.Context)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "unresolved") {
				t.Fatalf("package contains unresolved placeholder: %s", raw)
			}
			if name == "mixed" {
				got := offer.Package.Package.Context
				if len(got.Decisions) != 1 || got.Decisions[0].ID != "decision-1" ||
					!strings.Contains(got.Decisions[0].Authority, "reviewer") ||
					len(got.CheckpointDelta) != 1 ||
					!strings.Contains(got.CheckpointDelta[0], "resultRevision=9") ||
					!strings.Contains(got.CheckpointDelta[0], strings.Repeat("c", 40)) {
					t.Fatalf("accepted decision/checkpoint not projected: %+v", got)
				}
			}
		})
	}
}

func TestAuthoredProjectContextArtifactSelectorFailuresAreActionable(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	input := domain.Artifact{ID: "input-1", Name: "context.txt", SHA256: strings.Repeat("a", 64)}
	base := domain.ProjectContextReference{
		ID: "git", Kind: domain.ContextReferenceGit, URI: "ssh://git/steward",
		Revision: strings.Repeat("b", 40), Artifact: "context.txt",
	}
	tests := map[string]struct {
		mutate func(*domain.ProjectContextReference, *domain.ProjectContext)
		want   string
	}{
		"missing-selector": {func(ref *domain.ProjectContextReference, _ *domain.ProjectContext) { ref.Artifact = "" }, "artifact selector"},
		"wrong-selector":   {func(ref *domain.ProjectContextReference, _ *domain.ProjectContext) { ref.Artifact = "missing.txt" }, "missing retained input"},
		"stale": {func(_ *domain.ProjectContextReference, index *domain.ProjectContext) {
			stale := now.Add(-time.Second)
			index.Freshness.FreshThrough = &stale
		}, "stale"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ref := base
			index := projectContextTemplate(now, ref)
			test.mutate(&index.References[0], index)
			if err := validateAuthoredProjectContext(index); err != nil && name != "missing-selector" {
				t.Fatalf("authored shape unexpectedly failed before custody resolution: %v", err)
			}
			_, err := resolveAuthoredProjectContext(index, map[string]domain.Artifact{"context.txt": input}, now)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want actionable %q", err, test.want)
			}
		})
	}
}

func TestResolvedRunContextDoesNotContaminateSharedTemplate(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	ref := domain.ProjectContextReference{
		ID: "git", Kind: domain.ContextReferenceGit, URI: "ssh://git/steward",
		Revision: strings.Repeat("b", 40), Artifact: "context.txt",
	}
	authored := projectContextTemplate(now, ref)
	inputA := domain.Artifact{ID: "input-a", Name: "context.txt", SHA256: strings.Repeat("a", 64)}
	inputB := domain.Artifact{ID: "input-b", Name: "context.txt", SHA256: strings.Repeat("b", 64)}
	contextA, err := resolveAuthoredProjectContext(authored, map[string]domain.Artifact{"context.txt": inputA}, now)
	if err != nil {
		t.Fatal(err)
	}
	contextB, err := resolveAuthoredProjectContext(authored, map[string]domain.Artifact{"context.txt": inputB}, now)
	if err != nil {
		t.Fatal(err)
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{
			{ID: "run-a", WorkflowID: "workflow", Graph: &domain.GraphDefinition{RunID: "run-a", Revision: 1, Tasks: []domain.Task{{ID: "task-a", RunID: "run-a", WorkflowID: "workflow", Name: "task", Context: contextA}}}},
			{ID: "run-b", WorkflowID: "workflow", Graph: &domain.GraphDefinition{RunID: "run-b", Revision: 1, Tasks: []domain.Task{{ID: "task-b", RunID: "run-b", WorkflowID: "workflow", Name: "task", Context: contextB}}}},
		},
		Tasks: []domain.Task{{ID: "shared", WorkflowID: "workflow", Name: "task", Context: authored}},
	}
	if err := finalizeProjectContexts(&records, "run-a"); err != nil {
		t.Fatal(err)
	}
	if err := finalizeProjectContexts(&records, "run-b"); err != nil {
		t.Fatal(err)
	}
	if records.Tasks[0].Context.Status != "" || records.Tasks[0].Context.References[0].Binding != nil {
		t.Fatalf("shared template was contaminated: %+v", records.Tasks[0].Context)
	}
	a := records.WorkflowRuns[0].Graph.Tasks[0].Context.References[0].Binding
	b := records.WorkflowRuns[1].Graph.Tasks[0].Context.References[0].Binding
	if a.ArtifactID != "input-a" || b.ArtifactID != "input-b" || a.SHA256 == b.SHA256 {
		t.Fatalf("run bindings leaked: a=%+v b=%+v", a, b)
	}
}
