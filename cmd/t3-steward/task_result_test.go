package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// taskResultFixture is one finished run as the coordinator reports it, with
// the artifact bytes it holds in custody.
type taskResultFixture struct {
	detail  backlogadmin.WorkflowDetail
	content map[string]string
	stdout  bytes.Buffer
	stderr  bytes.Buffer
	dir     string
}

func taskResultTaskDetail(name, id string, progress domain.ProgressState) backlogadmin.TaskDetail {
	return backlogadmin.TaskDetail{
		Task:    domain.Task{ID: id, Name: name},
		Attempt: &domain.Attempt{ID: "attempt-" + id, TaskID: id, Progress: progress},
	}
}

func newTaskResultFixture(t *testing.T) *taskResultFixture {
	t.Helper()
	return &taskResultFixture{
		dir: t.TempDir(),
		detail: backlogadmin.WorkflowDetail{
			Summary: backlogadmin.WorkflowSummary{
				Run: domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressSucceeded},
			},
			Tasks: []backlogadmin.TaskDetail{
				taskResultTaskDetail("task", "task-1", domain.ProgressSucceeded),
			},
			Artifacts: []backlogadmin.Artifact{
				{Metadata: backlogadmin.ArtifactMetadata{
					ID: "final-message-attempt-task-1", WorkflowRunID: "run-1", TaskID: "task-1",
					AttemptID: "attempt-task-1", Kind: domain.ArtifactSummary, Name: "final-message.md",
					MediaType: "text/markdown", Size: 7,
				}},
				{Metadata: backlogadmin.ArtifactMetadata{
					ID: "output-report", WorkflowRunID: "run-1", TaskID: "task-1",
					AttemptID: "attempt-task-1", Kind: domain.ArtifactOutput, Name: "reports/report.md",
					MediaType: "text/markdown", Size: 6,
				}},
				{Metadata: backlogadmin.ArtifactMetadata{
					ID: "thread-archive-attempt-task-1", WorkflowRunID: "run-1", TaskID: "task-1",
					AttemptID: "attempt-task-1", Kind: domain.ArtifactLog, Name: "thread.json",
					MediaType: "application/json", Size: 2,
				}},
			},
		},
		content: map[string]string{
			"final-message-attempt-task-1":  "the job",
			"output-report":                 "report",
			"thread-archive-attempt-task-1": "{}",
		},
	}
}

func (f *taskResultFixture) cli() taskResultCLI {
	return taskResultCLI{
		query: func(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			if query.Kind != backlogadmin.QueryWorkflow {
				return backlogadmin.Response{}, fmt.Errorf("unexpected query kind %q", query.Kind)
			}
			if query.WorkflowRunID != f.detail.Summary.Run.ID {
				return backlogadmin.Response{}, fmt.Errorf("unknown run %q", query.WorkflowRunID)
			}
			detail := f.detail
			return backlogadmin.Response{Version: backlogadmin.Version, Kind: query.Kind, Workflow: &detail}, nil
		},
		open: func(_ context.Context, id string) (backlogadmin.ArtifactContent, error) {
			body, ok := f.content[id]
			if !ok {
				return backlogadmin.ArtifactContent{}, fmt.Errorf("no artifact %q", id)
			}
			return backlogadmin.ArtifactContent{
				Metadata: backlogadmin.ArtifactMetadata{ID: id, Size: int64(len(body))},
				Content:  io.NopCloser(strings.NewReader(body)),
			}, nil
		},
		stdout: &f.stdout, stderr: &f.stderr, workdir: f.dir, results: f.resultsDir(),
	}
}

// resultsDir stands in for the user's state directory, which is where results
// go by default: outside every checkout, and so outside f.dir's role as one.
func (f *taskResultFixture) resultsDir() string {
	return filepath.Join(f.dir, "state", "results")
}

func (f *taskResultFixture) run(args ...string) error {
	return f.cli().run(context.Background(), args)
}

func (f *taskResultFixture) document(t *testing.T) taskResultDocument {
	t.Helper()
	var document taskResultDocument
	if err := json.Unmarshal(f.stdout.Bytes(), &document); err != nil {
		t.Fatalf("--json did not print one document: %v\n%s", err, f.stdout.String())
	}
	return document
}

func readFile(t *testing.T, parts ...string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// Collecting a finished task took an artifacts listing, a lookup of the final
// message id and one fetch per artifact. One call writes all of it.
func TestTaskResultWritesTheFinalMessageAndEveryOutput(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1"); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(f.resultsDir(), "run-1", "task")
	if got := readFile(t, base, "final-message.md"); got != "the job" {
		t.Fatalf("final message = %q", got)
	}
	if got := readFile(t, base, "reports", "report.md"); got != "report" {
		t.Fatalf("output = %q", got)
	}
	// The thread archive is a log, not a declared output: collecting it would
	// dump the whole session transcript into the caller's working directory.
	if _, err := os.Stat(filepath.Join(base, "thread.json")); !os.IsNotExist(err) {
		t.Fatalf("the thread archive was collected: %v", err)
	}
	if !strings.Contains(f.stdout.String(), base) {
		t.Fatalf("the text form does not say where the files went:\n%s", f.stdout.String())
	}
}

// --json inlines the final message, so an agent reads the answer without a
// second file read.
func TestTaskResultJSONInlinesTheFinalMessage(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1", "--json"); err != nil {
		t.Fatal(err)
	}
	document := f.document(t)
	if document.Run != "run-1" || document.Outcome != string(domain.ProgressSucceeded) {
		t.Fatalf("document = %+v", document)
	}
	if len(document.Tasks) != 1 || document.Tasks[0].FinalMessage != "the job" {
		t.Fatalf("tasks = %+v", document.Tasks)
	}
	if len(document.Tasks[0].Files) != 2 {
		t.Fatalf("files = %+v, want the final message and the one output", document.Tasks[0].Files)
	}
}

// The exit code is the task's own verdict, which is what a script branches on.
func TestTaskResultExitsWithTheTasksVerdict(t *testing.T) {
	t.Run("succeeded is 0", func(t *testing.T) {
		f := newTaskResultFixture(t)
		if err := f.run("run-1"); err != nil {
			t.Fatalf("error = %v, want none", err)
		}
	})
	t.Run("skipped is 0", func(t *testing.T) {
		f := newTaskResultFixture(t)
		f.detail.Tasks[0].Attempt.Progress = domain.ProgressSkipped
		if err := f.run("run-1"); err != nil {
			t.Fatalf("error = %v, want none: a skipped task is not a failure", err)
		}
	})
	t.Run("failed is 2 and still writes what exists", func(t *testing.T) {
		f := newTaskResultFixture(t)
		f.detail.Tasks[0].Attempt.Progress = domain.ProgressFailed
		f.detail.Summary.Run.Progress = domain.ProgressFailed
		err := f.run("run-1")
		if code := exitCodeFor(err); code != 2 {
			t.Fatalf("exit code = %d, want 2 (error %v)", code, err)
		}
		if got := readFile(t, f.resultsDir(), "run-1", "task", "final-message.md"); got != "the job" {
			t.Fatalf("a failed task's final message was not written: %q", got)
		}
	})
	t.Run("cancelled is 2", func(t *testing.T) {
		f := newTaskResultFixture(t)
		f.detail.Tasks[0].Attempt.Progress = domain.ProgressCancelled
		if code := exitCodeFor(f.run("run-1")); code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
	})
	t.Run("not terminal is 1 with the progress printed", func(t *testing.T) {
		f := newTaskResultFixture(t)
		f.detail.Tasks[0].Attempt.Progress = domain.ProgressActive
		f.detail.Summary.Run.Progress = domain.ProgressActive
		err := f.run("run-1")
		if code := exitCodeFor(err); code != 1 {
			t.Fatalf("exit code = %d, want 1 (error %v)", code, err)
		}
		if !strings.Contains(f.stdout.String()+err.Error(), string(domain.ProgressActive)) {
			t.Fatalf("the progress was not reported: %s / %v", f.stdout.String(), err)
		}
	})
}

// The exit code is what a script branches on, so the help text has to be the
// code and not a summary of it. A skipped task exits 0 beside a succeeded one,
// which is deliberate -- a task the graph skipped is not a failure to collect
// -- and the help said "every collected task succeeded". The outcomes that
// exit 0 are read from the verdict rather than listed here, so the two cannot
// drift apart again.
func TestTaskResultHelpNamesEveryOutcomeThatExitsZero(t *testing.T) {
	var zero []domain.ProgressState
	for _, state := range []domain.ProgressState{
		domain.ProgressSucceeded, domain.ProgressSkipped, domain.ProgressFailed,
		domain.ProgressCancelled, domain.ProgressActive,
	} {
		if taskResultVerdict(taskResultDocument{Run: "run-1", Outcome: string(state)}) == nil {
			zero = append(zero, state)
		}
	}
	line := ""
	for _, candidate := range strings.Split(taskResultUsage, "\n") {
		if strings.HasPrefix(candidate, "  0  ") {
			line = candidate
			break
		}
	}
	if line == "" {
		t.Fatal("the help text has no line for exit code 0")
	}
	for _, state := range zero {
		if !strings.Contains(line, string(state)) {
			t.Fatalf("%q exits 0 and the help line does not name it: %q", state, line)
		}
	}
}

// A run with several tasks collects all of them, and one task can be named.
func TestTaskResultCollectsEveryTaskOrTheOneNamed(t *testing.T) {
	f := newTaskResultFixture(t)
	f.detail.Tasks = append(f.detail.Tasks, taskResultTaskDetail("second", "task-2", domain.ProgressSucceeded))
	f.detail.Artifacts = append(f.detail.Artifacts, backlogadmin.Artifact{
		Metadata: backlogadmin.ArtifactMetadata{
			ID: "final-message-attempt-task-2", WorkflowRunID: "run-1", TaskID: "task-2",
			AttemptID: "attempt-task-2", Kind: domain.ArtifactSummary, Name: "final-message.md",
			MediaType: "text/markdown",
		},
	})
	f.content["final-message-attempt-task-2"] = "the other job"

	if err := f.run("run-1", "--json"); err != nil {
		t.Fatal(err)
	}
	if document := f.document(t); len(document.Tasks) != 2 {
		t.Fatalf("tasks = %+v, want both", document.Tasks)
	}

	one := newTaskResultFixture(t)
	one.detail = f.detail
	one.content = f.content
	if err := one.run("run-1/second", "--json"); err != nil {
		t.Fatal(err)
	}
	document := one.document(t)
	if len(document.Tasks) != 1 || document.Tasks[0].Task != "second" ||
		document.Tasks[0].FinalMessage != "the other job" {
		t.Fatalf("tasks = %+v", document.Tasks)
	}
}

// --output puts the files where the caller asked, which is how a wrapper keeps
// results out of the repository it is working in.
func TestTaskResultHonoursTheOutputDirectory(t *testing.T) {
	f := newTaskResultFixture(t)
	elsewhere := t.TempDir()
	if err := f.run("run-1", "--output", elsewhere); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, elsewhere, "run-1", "task", "final-message.md"); got != "the job" {
		t.Fatalf("final message = %q", got)
	}
}

// A task with no result yet is reported as having none, rather than writing an
// empty file that reads like an empty answer.
func TestTaskResultReportsAMissingFinalMessage(t *testing.T) {
	f := newTaskResultFixture(t)
	f.detail.Artifacts = nil
	err := f.run("run-1", "--json")
	if err != nil {
		t.Fatal(err)
	}
	document := f.document(t)
	if len(document.Tasks) != 1 || document.Tasks[0].FinalMessage != "" {
		t.Fatalf("tasks = %+v", document.Tasks)
	}
	if len(document.Tasks[0].Missing) == 0 {
		t.Fatalf("a task with no collected result did not say so: %+v", document.Tasks[0])
	}
}

func TestTaskResultRefusesAnUnknownSelector(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1/nope"); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("error = %v", err)
	}
	if err := f.run(); err == nil || !strings.Contains(err.Error(), "<run>") {
		t.Fatalf("error = %v", err)
	}
}
