package backlogadmin

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// rerunFixture builds a terminal three-task run shaped like the campaign the
// H5 record was written about: inspect succeeded and published an output,
// implement consumed it and failed, and qualify depends on implement and never
// started. Both the succeeded task and the failed one left artifacts behind.
func rerunFixture(t *testing.T) (*Service, *sqlite.Store, string) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	artifactRoot := filepath.Join(root, "artifacts")
	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{
		ID: "workflow", Name: "campaign", TaskIDs: []string{"inspect", "implement", "qualify"},
	}}}
	progress := map[string]domain.ProgressState{
		"inspect":   domain.ProgressSucceeded,
		"implement": domain.ProgressFailed,
		"qualify":   domain.ProgressSkipped,
	}
	needs := map[string][]string{"implement": {"inspect"}, "qualify": {"implement"}}
	outputs := map[string]string{
		"inspect": "findings.md", "implement": "result.txt",
	}
	for _, name := range []string{"inspect", "implement", "qualify"} {
		task := domain.Task{
			ID: name, WorkflowID: "workflow", Name: name, Class: domain.TaskClassSurplus,
			MaxTurns: 1, PromptArtifactID: "prompt-" + name, Needs: needs[name],
			Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}},
		}
		if declared := outputs[name]; declared != "" {
			task.Outputs = []domain.ArtifactDeclaration{{Name: declared, MediaType: "text/plain"}}
		}
		if len(needs[name]) == 1 {
			producer := needs[name][0]
			if declared := outputs[producer]; declared != "" {
				task.DependencyInputs = map[string][]string{producer: {declared}}
			}
		}
		prompt, err := backlog.PrepareGraphInput(artifactRoot, task.PromptArtifactID, "run", name, "prompt "+name, now)
		if err != nil {
			t.Fatal(err)
		}
		records.Tasks = append(records.Tasks, task)
		records.Artifacts = append(records.Artifacts, prompt)
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: "attempt-" + name, WorkflowRunID: "run", TaskID: name, Number: 1, Revision: 1,
			Progress: progress[name], Control: domain.ControlStopped, UpdatedAt: now,
		})
		if declared := outputs[name]; declared != "" {
			records.Artifacts = append(records.Artifacts, rerunOutput(t, artifactRoot, name, declared, now))
		}
	}
	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "run", WorkflowID: "workflow", GraphRevision: 1, Revision: 1,
		Progress: domain.ProgressFailed, CreatedAt: now, UpdatedAt: now,
	}, records.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	completed := now.Add(time.Hour)
	run.CompletedAt = &completed
	records.WorkflowRuns = append(records.WorkflowRuns, run)
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(2 * time.Hour) })
	service.SetGraphAmendmentSupport(artifactRoot, func(domain.Workflow, domain.Task) error { return nil })
	artifacts := backlog.CoordinatorArtifactStore{Root: artifactRoot, Catalog: store}
	service.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return artifacts.Open(ctx, id)
	})
	return service, store, artifactRoot
}

// rerunOutput lays down one retained output artifact of the source run.
func rerunOutput(t *testing.T, root, taskID, name string, now time.Time) domain.Artifact {
	t.Helper()
	artifact, err := backlog.PrepareGraphInput(root, "output-"+taskID, "run", taskID, "content of "+name, now)
	if err != nil {
		t.Fatal(err)
	}
	artifact.Kind = domain.ArtifactOutput
	artifact.Name = name
	artifact.MediaType = "text/plain"
	artifact.AttemptID = "attempt-" + taskID
	artifact.Producer = "worker:test"
	return artifact
}

func rerunRequest(id, from string) domain.GraphAmendment {
	return domain.GraphAmendment{
		ID: id, RunID: "run", Operation: "rerun", TaskID: from,
		ExpectedRevision: 1, Reason: "the clone failed on a stale ref",
	}
}

// sourceSnapshot is every fact about the source run a rerun must not change:
// its run, tasks, attempts and artifacts, and the audit events that name it.
func sourceSnapshot(t *testing.T, store *sqlite.Store) string {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var kept sqlite.CoordinatorRecords
	for _, run := range records.WorkflowRuns {
		if run.ID == "run" {
			kept.WorkflowRuns = append(kept.WorkflowRuns, run)
			kept.Tasks = domain.TasksForRun(run, records.Tasks)
		}
	}
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID == "run" {
			kept.Attempts = append(kept.Attempts, attempt)
		}
	}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == "run" {
			kept.Artifacts = append(kept.Artifacts, artifact)
		}
	}
	// The audit trail of the source run is part of what must not change: a
	// rerun that quietly wrote an event against the run it read would be
	// rewriting its history, and comparing only the records would not notice.
	events, err := store.LoadAuditEvents(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.WorkflowRunID == "run" || event.TargetID == "run" {
			kept.AuditEvents = append(kept.AuditEvents, event)
		}
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestRerunCreatesOneLinkedRunAndLeavesTheSourceAlone(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	before := sourceSnapshot(t, store)
	principal := Principal{ID: "operator"}
	result, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.ID == "run" || result.Run.WorkflowID != "workflow" || result.Run.GraphRevision != 1 {
		t.Fatalf("new run identity: %+v", result.Run)
	}
	provenance := result.Graph.RerunOf
	if provenance == nil {
		t.Fatal("the new run records no rerun provenance")
	}
	want := domain.RerunProvenance{
		SourceRunID: "run", SourceTaskID: "implement", SourceAttemptID: "attempt-implement",
		IdempotencyKey: "rerun-1", Reason: "the clone failed on a stale ref",
	}
	if *provenance != want {
		t.Fatalf("provenance = %+v, want %+v", *provenance, want)
	}
	if result.Graph.ClonedFrom == nil || result.Graph.ClonedFrom.RunID != "run" {
		t.Fatalf("the new graph does not name its source: %+v", result.Graph.ClonedFrom)
	}
	// Exactly one new run, and the source is byte-identical to what it was.
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 2 {
		t.Fatalf("runs after one rerun = %d, want 2", len(records.WorkflowRuns))
	}
	if after := sourceSnapshot(t, store); after != before {
		t.Fatalf("the rerun changed the source run:\nbefore %s\nafter  %s", before, after)
	}
}

// A first rerun of an original run. The rerun of a rerun, which is a different
// path because the source's own tasks already carry inputs, is
// TestARerunOfARerunCarriesTheInputsTheFirstRerunCarried.
func TestAFirstRerunRerunsDescendantsAndCarriesAncestorOutputsByReference(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	result, err := service.AmendGraph(ctx, Principal{ID: "operator"}, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]domain.Task{}
	for _, task := range result.Graph.Tasks {
		names[task.Name] = task
	}
	// implement and its descendant qualify are rerun; the succeeded ancestor
	// inspect is not a node of the new graph at all.
	if len(names) != 2 || names["implement"].ID == "" || names["qualify"].ID == "" {
		t.Fatalf("rerun scope = %v", result.Graph.Tasks)
	}
	implement := names["implement"]
	if len(implement.Needs) != 0 || len(implement.DependencyInputs) != 0 {
		t.Fatalf("the reused ancestor is still an edge: %+v", implement)
	}
	if len(implement.CarriedInputs) != 1 {
		t.Fatalf("carried inputs = %+v", implement.CarriedInputs)
	}
	carried := implement.CarriedInputs[0]
	if carried.Producer != "inspect" || carried.ProducerTaskID != "inspect" || carried.Name != "findings.md" {
		t.Fatalf("carried input = %+v", carried)
	}
	if !reflect.DeepEqual(names["qualify"].Needs, []string{"implement"}) {
		t.Fatalf("the descendant lost its in-graph edge: %+v", names["qualify"])
	}
	if len(names["qualify"].CarriedInputs) != 0 {
		t.Fatal("an artifact of the failed subtree was carried over")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var reference, source domain.Artifact
	for _, artifact := range records.Artifacts {
		switch {
		case artifact.ID == carried.ArtifactID:
			reference = artifact
		case artifact.ID == "output-inspect":
			source = artifact
		}
		// Nothing in the new run may point at what the failed subtree produced.
		if artifact.WorkflowRunID == result.Run.ID && artifact.SHA256 == "" {
			t.Fatalf("incomplete reference artifact %+v", artifact)
		}
	}
	if reference.ID == "" || source.ID == "" {
		t.Fatal("the carried artifact or its source is missing from the catalog")
	}
	if reference.SHA256 != source.SHA256 || reference.StoragePath != source.StoragePath {
		t.Fatalf("the carried artifact is a copy, not a reference: %+v vs %+v", reference, source)
	}
	if reference.WorkflowRunID != result.Run.ID || reference.TaskID != implement.ID {
		t.Fatalf("carried artifact custody = %+v", reference)
	}
	// The failed subtree's own output stays under the source run and is not an
	// input anywhere in the new run.
	failedOutput := ""
	for _, artifact := range records.Artifacts {
		if artifact.ID == "output-implement" {
			failedOutput = artifact.SHA256
			if artifact.WorkflowRunID != "run" {
				t.Fatalf("the failed subtree's artifact moved: %+v", artifact)
			}
		}
	}
	for _, task := range result.Graph.Tasks {
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			for _, artifact := range records.Artifacts {
				if artifact.ID == id && artifact.SHA256 == failedOutput {
					t.Fatalf("an artifact of the failed subtree is an input of %s", task.Name)
				}
			}
		}
	}
}

func TestRerunRefusesWhenAnAncestorArtifactIsUnretrievable(t *testing.T) {
	ctx := context.Background()
	service, store, artifactRoot := rerunFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := sourceSnapshot(t, store)
	for _, artifact := range records.Artifacts {
		if artifact.ID != "output-inspect" {
			continue
		}
		path := filepath.Join(artifactRoot, filepath.FromSlash(artifact.StoragePath))
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	_, err = service.AmendGraph(ctx, Principal{ID: "operator"}, rerunRequest("rerun-1", "implement"))
	if err == nil {
		t.Fatal("a rerun started a task whose declared input is gone")
	}
	if !strings.Contains(err.Error(), "no longer retrievable") {
		t.Fatalf("refusal does not name the cause: %v", err)
	}
	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.WorkflowRuns) != 1 {
		t.Fatalf("a refused rerun created %d runs", len(after.WorkflowRuns)-1)
	}
	if snapshot := sourceSnapshot(t, store); snapshot != before {
		t.Fatal("a refused rerun changed the source run")
	}
}

func TestRerunMayReplaceOnlyTheFailedRootPrompt(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	request := rerunRequest("rerun-corrected", "implement")
	request.Prompt = "corrected root instructions"
	result, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil {
		t.Fatal(err)
	}
	var root, descendant domain.Task
	for _, task := range result.Graph.Tasks {
		switch task.Name {
		case "implement":
			root = task
		case "qualify":
			descendant = task
		}
	}
	if root.PromptArtifactID != "input:rerun:rerun-corrected:prompt" {
		t.Fatalf("root prompt = %q", root.PromptArtifactID)
	}
	if descendant.PromptArtifactID == "" || descendant.PromptArtifactID == root.PromptArtifactID {
		t.Fatalf("descendant prompt was replaced: %+v", descendant)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range domain.TasksForRun(records.WorkflowRuns[0], records.Tasks) {
		if task.Name == "implement" && task.PromptArtifactID != "prompt-implement" {
			t.Fatal("source prompt changed")
		}
	}
	changed := request
	changed.Prompt = "different corrected instructions"
	if _, err = service.AmendGraph(ctx, Principal{ID: "operator"}, changed); err == nil {
		t.Fatal("idempotent replay accepted a changed corrected prompt")
	}
}

func TestRerunIdempotencyReturnsTheSameRunAndRefusesChangedContent(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	principal := Principal{ID: "operator"}
	first, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.Run.ID != first.Run.ID || replay.Graph.Digest != first.Graph.Digest {
		t.Fatalf("the same key with the same content did not return the same run: %+v", replay)
	}
	changed := rerunRequest("rerun-1", "qualify")
	if _, err = service.AmendGraph(ctx, principal, changed); err == nil {
		t.Fatal("the same key with different content was accepted")
	}
	reason := rerunRequest("rerun-1", "implement")
	reason.Reason = "something else entirely"
	if _, err = service.AmendGraph(ctx, principal, reason); err == nil {
		t.Fatal("the same key with a different reason was accepted")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 2 {
		t.Fatalf("replays created %d runs beyond the source", len(records.WorkflowRuns)-1)
	}
}

func TestRerunRefusesAnUnusableReuseSetAndALiveSourceRun(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	principal := Principal{ID: "operator"}
	// qualify is a descendant of the failed implement, so rerunning from it
	// alone would reuse a task that never succeeded.
	_, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-late", "qualify"))
	if err == nil {
		t.Fatal("a rerun reused a task that did not succeed")
	}
	if !strings.Contains(err.Error(), "implement") {
		t.Fatalf("the refusal does not name the unusable task: %v", err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index, run := range records.WorkflowRuns {
		if run.ID != "run" {
			continue
		}
		run.Progress = domain.ProgressActive
		run.CompletedAt = nil
		run.Revision++
		records.WorkflowRuns[index] = run
		if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = service.AmendGraph(ctx, principal, rerunRequest("rerun-live", "implement"))
	if err == nil {
		t.Fatal("a live run was reran from")
	}
	if !strings.Contains(err.Error(), "has not finished") {
		t.Fatalf("the refusal does not explain itself: %v", err)
	}
}
