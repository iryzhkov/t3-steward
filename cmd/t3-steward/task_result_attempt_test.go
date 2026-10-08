package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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

// An earlier attempt's declared output cannot satisfy the selected attempt's
// completeness check, and collection leaves the durable artifact record intact.
func TestTaskResultReportsDeclaredOutputMissingFromSelectedAttempt(t *testing.T) {
	f := retryTaskResultFixture(t)
	f.detail.Tasks[0].Task.Outputs = []domain.ArtifactDeclaration{{Name: "reports/report.md"}}
	artifacts := len(f.detail.Artifacts)
	if err := f.run("run-1/task", "--json"); err != nil {
		t.Fatal(err)
	}
	got := f.document(t).Tasks[0]
	if len(got.Files) != 0 || len(got.MissingOutputs) != 1 || got.MissingOutputs[0] != "reports/report.md" {
		t.Fatalf("selected attempt result = %#v; want no earlier files and missing declared report", got)
	}
	if len(f.detail.Artifacts) != artifacts || f.content["output-report"] != "report" {
		t.Fatal("collection changed the earlier attempt's durable artifact record")
	}
	base := filepath.Join(f.resultsDir(), "run-1", "task")
	if _, err := os.Stat(filepath.Join(base, "reports", "report.md")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("earlier attempt's report was collected: %v", err)
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

// Two collections of the same task at once, as two agents waiting on one run
// do, each succeed, and exactly one complete directory is left, with nothing
// beside it. Moving the directory aside and renaming the new one in is not one
// step, so either could find the other's directory in the way.
func TestConcurrentCollectionsOfOneTaskEachSucceedAndLeaveOneDirectory(t *testing.T) {
	runDirectory := filepath.Join(t.TempDir(), "run-1")
	directory := filepath.Join(runDirectory, "task")
	const collectors, rounds = 4, 50
	errs := make(chan error, collectors*rounds)
	var wait sync.WaitGroup
	for collector := 0; collector < collectors; collector++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for round := 0; round < rounds; round++ {
				staged, err := stageResultDirectory(directory)
				if err != nil {
					errs <- err
					return
				}
				if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte("done"), 0o600); err != nil {
					errs <- err
					return
				}
				if left, err := replaceResultDirectory(staged, directory); err != nil || len(left) != 0 {
					errs <- errors.Join(err, errors.New("left behind: "+filepath.Join(left...)))
				}
				_ = os.RemoveAll(staged)
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := readFile(t, directory, "final-message.md"); got != "done" {
		t.Fatalf("final message = %q", got)
	}
	entries, err := os.ReadDir(runDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("run directory holds %d entries, want only the task directory", len(entries))
	}
}

// Once the new collection is in place, failing to delete the earlier one is
// cleanup that did not happen, not a failed collection: the new results are
// there, and the leftover is reported rather than the collection refused.
func TestAPreviousCollectionThatCannotBeRemovedDoesNotFailTheNewOne(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root removes read-only directories")
	}
	directory := filepath.Join(t.TempDir(), "run-1", "task")
	locked := filepath.Join(directory, "locked")
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "old.txt"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	var left []string
	t.Cleanup(func() {
		for _, path := range left {
			_ = os.Chmod(filepath.Join(path, "locked"), 0o700)
		}
	})
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	left, err = replaceResultDirectory(staged, directory)
	if err != nil {
		t.Fatalf("the collection failed after its results were in place: %v", err)
	}
	if got := readFile(t, directory, "final-message.md"); got != "new" {
		t.Fatalf("final message = %q", got)
	}
	if len(left) != 1 {
		t.Fatalf("leftovers = %v, want the previous collection reported", left)
	}
}

// The task's directory is replaced wholesale, so a task name that is not one
// path element is refused before anything is touched: ".." would otherwise
// replace the output directory itself, which --output . makes the caller's
// checkout.
func TestTaskResultRefusesATaskNameThatIsNotOneDirectory(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, ".task-result-1"} {
		t.Run(name, func(t *testing.T) {
			f := newTaskResultFixture(t)
			output := t.TempDir()
			precious := filepath.Join(output, "precious")
			if err := os.WriteFile(precious, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			task := f.detail.Tasks[0]
			task.Task.Name = name
			if _, err := f.cli().collect(context.Background(), f.detail, task, filepath.Join(output, "run-1"), true); err == nil {
				t.Fatalf("task name %q was accepted", name)
			}
			if got := readFile(t, precious); got != "keep" {
				t.Fatalf("precious = %q", got)
			}
		})
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
