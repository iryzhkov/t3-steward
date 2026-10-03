package backlog

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"strings"
	"testing"
	"time"
)

func reviewJudgeTask(t *testing.T, needs ...string) domain.Task {
	t.Helper()
	task := testTask("judge", needs...)
	raw, _ := json.Marshal(task)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	fields["reviewJudge"] = true
	raw, _ = json.Marshal(fields)
	_ = json.Unmarshal(raw, &task)
	return task
}

func TestReviewJudgeInputsNameMissingLenses(t *testing.T) {
	now := time.Now().UTC()
	records, assignment := packageBuilderFixture(now)
	_ = json.Unmarshal([]byte(`{"reviewJudge":true}`), &records.Tasks[1])
	records.Tasks[1].DependencyInputs = map[string][]string{"producer": {"verdict.json"}}
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": {"preflight", "project-context-v1"}}
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if pkg.Context == nil || !strings.Contains(strings.Join(pkg.Context.CheckpointDelta, "\n"), "producer") {
		t.Fatal("missing lens not named in judge inputs")
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}
}

func TestReviewJudgeRunsAfterFailedTerminalLens(t *testing.T) {
	lens := testTask("swarm-security")
	other := testTask("swarm-tests")
	judge := reviewJudgeTask(t, lens.Name, other.Name)
	execution, err := NewDAGExecution(testDAGState(lens, other, judge))
	if err != nil {
		t.Fatal(err)
	}
	mustStart(t, execution, "swarm-security-1")
	mustComplete(t, execution, "swarm-security-1", CompletionResult{Failure: "lens crashed"})
	assertAttemptProgress(t, execution, "judge-1", domain.ProgressBlocked)
	succeedAttempt(t, execution, "swarm-tests-1")
	assertAttemptProgress(t, execution, "judge-1", domain.ProgressReady)
	// Ordinary tasks still require successful dependencies.
	ordinary := testTask("consumer", lens.Name)
	state := testDAGState(lens, ordinary)
	state.Attempts[0].Progress = domain.ProgressFailed
	normal, err := NewDAGExecution(state)
	if err != nil {
		t.Fatal(err)
	}
	assertAttemptProgress(t, normal, "consumer-1", domain.ProgressBlocked)
}

func TestReviewJudgePackagesAvailableVerdicts(t *testing.T) {
	lens := testTask("swarm-security")
	judge := reviewJudgeTask(t, lens.Name)
	judge.DependencyInputs = map[string][]string{lens.Name: {"verdict.json"}}
	dependencies, err := packageDependencies(judge, []domain.Task{lens, judge}, map[string]domain.Artifact{}, "run")
	if err != nil {
		t.Fatalf("missing optional lens blocks package: %v", err)
	}
	if len(dependencies) != 1 || len(dependencies[0].Artifacts) != 0 {
		t.Fatalf("dependencies: %+v", dependencies)
	}
	_, err = MaterializeDependencies(t.TempDir(), t.TempDir(), "run", judge, []domain.Task{lens, judge}, nil)
	if err != nil {
		t.Fatalf("missing optional lens blocks materialization: %v", err)
	}
	judgeRaw, _ := json.Marshal(judge)
	if !strings.Contains(string(judgeRaw), "reviewJudge") {
		t.Fatal("judge identity not retained")
	}
}
