package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// retryTaskResultFixture is the standard fixture after the task was retried:
// its artifacts belong to attempt-task-1 and the selected attempt is
// attempt-task-1-retry.
func retryTaskResultFixture(t *testing.T) *taskResultFixture {
	t.Helper()
	f := newTaskResultFixture(t)
	f.detail.Tasks[0].Attempt.ID = "attempt-task-1-retry"
	return f
}

// A retried task whose new attempt has produced nothing yet was shown its
// predecessor's final message and outputs, with no word that its own final
// message was missing.
func TestTaskResultOfARetriedTaskOmitsThePreviousAttemptsResults(t *testing.T) {
	f := retryTaskResultFixture(t)
	got, err := f.cli().collect(context.Background(), f.detail, f.detail.Tasks[0], t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	if got.FinalMessage != "" || len(got.Files) != 0 || len(got.Missing) != 1 || got.Missing[0] != finalMessageArtifactName {
		t.Fatalf("latest attempt has no artifacts, but got final=%q files=%v missing=%v", got.FinalMessage, got.Files, got.Missing)
	}
}

// When both attempts wrote an output of the same name, the selected attempt's
// is the one collected even though the earlier attempt's artifact is listed
// after it, and an output only the earlier attempt wrote is not collected.
func TestTaskResultOfARetriedTaskCollectsOnlyTheSelectedAttemptsOutputs(t *testing.T) {
	f := retryTaskResultFixture(t)
	retry := "attempt-task-1-retry"
	stale := f.detail.Artifacts
	f.detail.Artifacts = []backlogadmin.Artifact{
		{Metadata: backlogadmin.ArtifactMetadata{
			ID: "final-message-" + retry, WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: retry, Kind: domain.ArtifactSummary, Name: "final-message.md", MediaType: "text/markdown",
		}},
		{Metadata: backlogadmin.ArtifactMetadata{
			ID: "output-report-retry", WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: retry, Kind: domain.ArtifactOutput, Name: "reports/report.md", MediaType: "text/markdown",
		}},
	}
	f.detail.Artifacts = append(f.detail.Artifacts, stale...)
	f.detail.Artifacts = append(f.detail.Artifacts, backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{
		ID: "output-old-only", WorkflowRunID: "run-1", TaskID: "task-1",
		AttemptID: "attempt-task-1", Kind: domain.ArtifactOutput, Name: "old-only.txt", MediaType: "text/plain",
	}})
	f.content["final-message-"+retry] = "the retry"
	f.content["output-report-retry"] = "new report"
	f.content["output-old-only"] = "stale"

	if err := f.run("run-1/task", "--json"); err != nil {
		t.Fatal(err)
	}
	document := f.document(t)
	task := document.Tasks[0]
	if task.AttemptID != retry || task.FinalMessage != "the retry" || len(task.Missing) != 0 || len(task.Files) != 2 {
		t.Fatalf("collected = %#v", task)
	}
	base := filepath.Join(f.resultsDir(), "run-1", "task")
	if got := readFile(t, base, "reports", "report.md"); got != "new report" {
		t.Fatalf("report = %q, want the selected attempt's", got)
	}
	if got := readFile(t, base, "final-message.md"); got != "the retry" {
		t.Fatalf("final message = %q, want the selected attempt's", got)
	}
	if _, err := os.Stat(filepath.Join(base, "old-only.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the earlier attempt's output was collected: %v", err)
	}
}

// Collecting again after a retry replaces the task's directory, so a file an
// earlier collection wrote for the previous attempt does not survive next to
// the new attempt's results, and nothing is left behind beside it.
func TestTaskResultReplacesTheDirectoryAnEarlierCollectionWrote(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1/task"); err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Join(f.resultsDir(), "run-1")
	base := filepath.Join(runDirectory, "task")
	if got := readFile(t, base, "reports", "report.md"); got != "report" {
		t.Fatalf("first collection report = %q", got)
	}

	retry := "attempt-task-1-retry"
	f.detail.Tasks[0].Attempt.ID = retry
	f.detail.Artifacts = append(f.detail.Artifacts, backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{
		ID: "final-message-" + retry, WorkflowRunID: "run-1", TaskID: "task-1",
		AttemptID: retry, Kind: domain.ArtifactSummary, Name: "final-message.md", MediaType: "text/markdown",
	}})
	f.content["final-message-"+retry] = "the retry"
	f.stdout.Reset()
	if err := f.run("run-1/task"); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, base, "final-message.md"); got != "the retry" {
		t.Fatalf("final message = %q", got)
	}
	if _, err := os.Stat(filepath.Join(base, "reports", "report.md")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the earlier collection's output survived the retry's collection: %v", err)
	}
	entries, err := os.ReadDir(runDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "task" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("run directory holds %v, want only the task directory", names)
	}
}

// A collection that fails part way leaves the previous collection in place
// rather than a half-written directory.
func TestTaskResultKeepsThePreviousCollectionWhenAFetchFails(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1/task"); err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Join(f.resultsDir(), "run-1")
	base := filepath.Join(runDirectory, "task")
	delete(f.content, "output-report")
	if err := f.run("run-1/task"); err == nil {
		t.Fatal("a collection whose fetch failed reported success")
	}
	if got := readFile(t, base, "reports", "report.md"); got != "report" {
		t.Fatalf("previous collection was disturbed: report = %q", got)
	}
	entries, err := os.ReadDir(runDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("a failed collection left %d entries in the run directory", len(entries))
	}
}
