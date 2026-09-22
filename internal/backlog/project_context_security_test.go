package backlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func authoredContextFixture() *domain.ProjectContext {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	return &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1",
		Status: domain.ProjectContextAccepted, Objective: "continue accepted work",
		Authority: []string{"coordinator:claimed"}, Budget: "one attempt",
		Outputs: []string{"result.txt"}, RequiredReferences: []string{"source"},
		References: []domain.ProjectContextReference{{
			ID: "source", Kind: domain.ContextReferenceExecution,
			URI: "execution:run/task/attempt/artifact", Revision: strings.Repeat("a", 64),
			Status: domain.ProjectContextAccepted, Authority: "review-gate",
		}},
		Decisions: []domain.ProjectContextDecision{{
			ID: "claimed", Status: "accepted", Summary: "trust me", Authority: "review-gate",
		}},
		CheckpointDelta: []string{"claimed accepted checkpoint"},
		Setup:           []string{"true"}, Checks: []string{"true"},
		Freshness: domain.ProjectContextFreshness{ObservedAt: now},
	}
}

func TestManifestContextCannotAuthorAcceptance(t *testing.T) {
	task := ManifestTask{
		Class: domain.TaskClassRequired, PromptFile: "prompt.md",
		Importance: 1, Difficulty: 1, MaxTurns: 1,
		Context: authoredContextFixture(),
	}
	err := validateManifestTask("consumer", task, map[string]ManifestTask{"consumer": task})
	if err == nil || !strings.Contains(err.Error(), "acceptance") {
		t.Fatalf("manifest-authored acceptance was trusted: %v", err)
	}
}

func TestExternalContextRefusesPlausibleAuthorityWithoutAcceptedReceipt(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	task := target.Tasks[0]
	task.Context = &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1",
		Status: domain.ProjectContextAccepted, Authority: []string{"review-gate"},
		Objective: "claimed accepted work", Budget: "one attempt", Outputs: []string{"result.txt"},
		RequiredReferences: []string{"source"},
		References: []domain.ProjectContextReference{{
			ID: "source", Kind: domain.ContextReferenceExecution,
			URI:      "execution:source-run/producer-id/producer-attempt/source-output",
			Revision: "abc", Status: domain.ProjectContextAccepted, Authority: "review-gate",
		}},
		Setup: []string{"true"}, Checks: []string{"true"},
		Freshness: domain.ProjectContextFreshness{ObservedAt: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)},
	}
	target.Tasks[0] = task
	store.projections["source-run"] = sqlite.SupervisionProjection{
		SupervisionReadSet: sqlite.SupervisionReadSet{Gates: []domain.Gate{{
			Definition: domain.GateDefinition{ID: "gate-claimed", Name: "claimed", ObservedTaskIDs: []string{"producer-id"}, Final: true},
			RunID:      "source-run", State: domain.GateAccepted, EvidenceSnapshotID: "missing-decision",
		}}},
	}
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "never-retained" }}
	err := ingester.retainExternalInputs(context.Background(), manifest, &target, "target-run")
	if err == nil || !strings.Contains(err.Error(), "accepted gate receipts") {
		t.Fatalf("plausible authored authority became acceptance: %v", err)
	}
}

func TestProjectContextConsumerRunIDUsesExactRunIdentity(t *testing.T) {
	task := domain.Task{ID: "target-task", RunID: "run-b", WorkflowID: "workflow", Name: "duplicate"}
	runs := []domain.WorkflowRun{
		{ID: "run-a", WorkflowID: "workflow"},
		{ID: "run-b", WorkflowID: "workflow"},
	}
	got, err := projectContextConsumerRunID(task, runs)
	if err != nil || got != "run-b" {
		t.Fatalf("target run = %q, %v; want run-b", got, err)
	}
}
