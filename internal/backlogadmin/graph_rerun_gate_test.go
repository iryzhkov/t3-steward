package backlogadmin

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// rerunGateFixture is a gated producer whose first attempt failed its gate and
// whose second attempt passed, with every artifact of both retained.
func rerunGateFixture(failedFirst bool) (rerunReferences, domain.Task) {
	artifact := func(id, attempt, name string, kind domain.ArtifactKind) domain.Artifact {
		return domain.Artifact{ID: id, WorkflowRunID: "run", TaskID: "producer", AttemptID: attempt, Kind: kind, Name: name}
	}
	failed := []domain.Artifact{
		artifact("failed-gate", "failed", "gate", domain.ArtifactGate),
		artifact("failed-log", "failed", "gate/log.txt", domain.ArtifactGate),
		artifact("failed-out", "failed", "out.txt", domain.ArtifactOutput),
	}
	passed := []domain.Artifact{
		artifact("passed-gate", "passed", "gate", domain.ArtifactGate),
		artifact("passed-log", "passed", "gate/log.txt", domain.ArtifactGate),
		artifact("passed-out", "passed", "out.txt", domain.ArtifactOutput),
	}
	artifacts := append(slices.Clone(passed), failed...)
	if failedFirst {
		artifacts = append(slices.Clone(failed), passed...)
	}
	byID := map[string]domain.Artifact{}
	for _, a := range artifacts {
		byID[a.ID] = a
	}
	records := sqlite.CoordinatorRecords{
		Tasks: []domain.Task{{ID: "producer", Name: "build", Gate: &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Second}}},
		Attempts: []domain.Attempt{
			{ID: "failed", TaskID: "producer", WorkflowRunID: "run", Number: 1, Progress: domain.ProgressFailed},
			{ID: "passed", TaskID: "producer", WorkflowRunID: "run", Number: 2, Progress: domain.ProgressSucceeded},
		},
		Artifacts: artifacts,
	}
	s := &Service{artifactOpen: func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return byID[id], io.NopCloser(strings.NewReader("{}")), nil
	}}
	return rerunReferences{service: s, source: domain.WorkflowRun{ID: "run"}, runID: "rerun", records: records}, records.Tasks[0]
}

func TestRerunCarriesGateEvidenceFromSuccessfulProducerAttempt(t *testing.T) {
	for _, failedFirst := range []bool{true, false} {
		b, producer := rerunGateFixture(failedFirst)
		task := domain.Task{ID: "consumer", Name: "review", Needs: []string{"build"},
			DependencyInputs: map[string][]string{"build": {"gate", "gate/log.txt", "out.txt"}}}
		if err := b.detach(context.Background(), &task, map[string]domain.Task{"build": producer}); err != nil {
			t.Fatal(err)
		}
		if len(task.CarriedInputs) != 3 {
			t.Fatalf("failedFirst=%v carried %+v", failedFirst, task.CarriedInputs)
		}
		for _, carried := range task.CarriedInputs {
			if carried.SourceAttemptID != "passed" || !strings.HasPrefix(carried.SourceArtifactID, "passed-") {
				t.Fatalf("failedFirst=%v rerun selected failed-attempt evidence: %+v", failedFirst, task.CarriedInputs)
			}
		}
	}
}

func TestRerunRefusesGateEvidenceWithoutAuthoritativeAttempt(t *testing.T) {
	for name, mutate := range map[string]func(*rerunReferences){
		"latest failed": func(b *rerunReferences) {
			b.records.Attempts = append(b.records.Attempts, domain.Attempt{ID: "third", TaskID: "producer", WorkflowRunID: "run", Number: 3, Progress: domain.ProgressFailed})
		},
		"ambiguous number": func(b *rerunReferences) {
			b.records.Attempts = append(b.records.Attempts, domain.Attempt{ID: "twin", TaskID: "producer", WorkflowRunID: "run", Number: 2, Progress: domain.ProgressSucceeded})
		},
		"no attempts": func(b *rerunReferences) { b.records.Attempts = nil },
		"duplicate evidence": func(b *rerunReferences) {
			b.records.Artifacts = append(b.records.Artifacts, domain.Artifact{ID: "extra", WorkflowRunID: "run", TaskID: "producer", AttemptID: "passed", Kind: domain.ArtifactGate, Name: "gate"})
		},
	} {
		b, producer := rerunGateFixture(true)
		mutate(&b)
		task := domain.Task{ID: "consumer", Name: "review", Needs: []string{"build"}, DependencyInputs: map[string][]string{"build": {"gate"}}}
		if err := b.detach(context.Background(), &task, map[string]domain.Task{"build": producer}); err == nil {
			t.Fatalf("%s: rerun carried %+v instead of refusing", name, task.CarriedInputs)
		}
	}
}
