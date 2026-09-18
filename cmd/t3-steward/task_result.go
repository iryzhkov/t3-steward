package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const taskResultUsage = `Usage: t3-steward task result <run>[/<task>] [--output DIR] [--json]

Collect a finished task: its final message and every output it declared, in one
call. Without a task, every task of the run is collected.

The files are written under DIR (default ./.t3/results/<run>/<task>/), each
under the name the task declared for it. The thread archive and other logs are
not collected: "t3-steward backlog artifacts <run>/<task>" lists everything the
coordinator holds, and "backlog artifact get" fetches one.

Exit codes are the task's own verdict, so a script branches on them:
  0  every collected task succeeded or was skipped; a task the graph skipped
     ran nothing and has nothing to collect, which is not a failure
  2  one failed or was cancelled; whatever exists is still written
  1  one is not terminal yet; its progress is printed and nothing is waited for

--json prints one document with the final message inlined, so reading the
answer needs no second file read.
` + coordinatorTransportHelp

// taskResultSchemaVersion versions the result document.
const taskResultSchemaVersion = 1

// finalMessageArtifactName is the name the worker gives the summary artifact
// every attempt produces, whether or not the task declared any output.
const finalMessageArtifactName = "final-message.md"

// taskResultDocument is what result prints.
type taskResultDocument struct {
	SchemaVersion int    `json:"schemaVersion"`
	Run           string `json:"run"`
	// Outcome is the worst verdict among the collected tasks, which is also
	// what the exit code reports.
	Outcome   string           `json:"outcome"`
	Directory string           `json:"directory"`
	Tasks     []taskResultTask `json:"tasks"`
}

// taskResultTask is one collected task.
type taskResultTask struct {
	Task      string `json:"task"`
	TaskID    string `json:"taskId,omitempty"`
	AttemptID string `json:"attemptId,omitempty"`
	Progress  string `json:"progress"`
	Directory string `json:"directory"`
	// FinalMessage is inlined with --json only, because the text form has
	// already written it to a file the caller can read.
	FinalMessage string           `json:"finalMessage,omitempty"`
	Files        []taskResultFile `json:"files"`
	// Missing names what was expected and is not there, such as a final
	// message a task that never ran cannot have produced.
	Missing []string `json:"missing,omitempty"`
}

// taskResultFile is one written file.
type taskResultFile struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Kind string `json:"kind"`
	Size int64  `json:"size"`
}

// exitCodeError carries an exit code a verb owns outright, which no transport
// class can express: "task result" reports the collected task's own verdict.
type exitCodeError struct {
	code int
	error
}

func (e exitCodeError) Unwrap() error { return e.error }

// exitCodeFor is the transport numbering, plus the explicit code an error
// carries when a verb owns its own verdict.
func exitCodeFor(err error) int {
	var coded exitCodeError
	if errors.As(err, &coded) {
		return coded.code
	}
	return backlogadmin.ExitCodeFor(err)
}

// taskResultCLI is one collection with the coordinator at arm's length: one
// query for the run and one open per artifact.
type taskResultCLI struct {
	query  func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
	open   func(context.Context, string) (backlogadmin.ArtifactContent, error)
	stdout io.Writer
	stderr io.Writer
	// workdir is the base of the default output directory. It is a field so
	// that a test never writes into the directory the test binary runs in.
	workdir string
}

func cmdTaskResult(g globalFlags, args []string) error {
	if len(args) > 0 && isHelp(args[0]) {
		fmt.Print(taskResultUsage)
		return nil
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	workdir, err := os.Getwd()
	if err != nil {
		return err
	}
	cli := taskResultCLI{stdout: os.Stdout, stderr: os.Stderr, workdir: workdir}
	cli.query = func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
		transport, err := newCoordinatorTransport(cfg)
		if err != nil {
			return backlogadmin.Response{}, err
		}
		query.Version = backlogadmin.Version
		query.Principal = transport.principal
		return transport.client.Query(ctx, query)
	}
	cli.open = func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
		return openTaskResultArtifact(ctx, cfg, id)
	}
	return cli.run(context.Background(), args)
}

func openTaskResultArtifact(ctx context.Context, cfg config.Config, id string) (backlogadmin.ArtifactContent, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return backlogadmin.ArtifactContent{}, err
	}
	return transport.client.OpenArtifact(ctx, transport.principal, id)
}

func (c taskResultCLI) run(ctx context.Context, args []string) error {
	selector, output, asJSON, err := parseTaskResultArgs(args)
	if err != nil {
		return err
	}
	runID, taskName, _ := strings.Cut(selector, "/")
	if c.query == nil {
		return errors.New("coordinator query transport is unavailable")
	}
	response, err := c.query(ctx, backlogadmin.Query{
		Kind: backlogadmin.QueryWorkflow, WorkflowRunID: runID,
	})
	if err != nil {
		return err
	}
	if response.Workflow == nil {
		return fmt.Errorf("run %q has no workflow detail", runID)
	}
	selected, err := selectResultTasks(*response.Workflow, taskName)
	if err != nil {
		return err
	}
	base := output
	if base == "" {
		base = filepath.Join(c.workdir, ".t3", "results")
	}
	document := taskResultDocument{
		SchemaVersion: taskResultSchemaVersion,
		Run:           runID,
		Directory:     filepath.Join(base, runID),
	}
	worst := domain.ProgressSucceeded
	for _, detail := range selected {
		collected, err := c.collect(ctx, *response.Workflow, detail, filepath.Join(base, runID), asJSON)
		if err != nil {
			return err
		}
		document.Tasks = append(document.Tasks, collected)
		worst = worseResultProgress(worst, domain.ProgressState(collected.Progress))
	}
	document.Outcome = string(worst)
	if asJSON {
		if err := encodeCampaignJSON(c.stdout, document); err != nil {
			return err
		}
	} else if err := renderTaskResult(c.stdout, document); err != nil {
		return err
	}
	return afterDocument(taskResultVerdict(document))
}

// parseTaskResultArgs takes the selector and the two flags.
func parseTaskResultArgs(args []string) (string, string, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return "", "", false, err
	}
	selector, output := "", ""
	for i := 0; i < len(clean); i++ {
		switch {
		case clean[i] == "--output":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return "", "", false, errors.New("--output needs a directory")
			}
			output = clean[i+1]
			i++
		case strings.HasPrefix(clean[i], "-"):
			return "", "", false, fmt.Errorf("unknown flag %q; usage: t3-steward task result <run>[/<task>] [--output DIR] [--json]", clean[i])
		case selector != "":
			return "", "", false, errors.New("task result takes one <run>[/<task>] selector")
		default:
			selector = clean[i]
		}
	}
	if selector == "" {
		return "", "", false, errors.New("task result needs a <run>[/<task>] selector, as \"task run\" printed it")
	}
	return selector, output, asJSON, nil
}

// selectResultTasks picks the tasks to collect. The sink is never one of them:
// it is the run's join point and produces no result of its own.
func selectResultTasks(detail backlogadmin.WorkflowDetail, name string) ([]backlogadmin.TaskDetail, error) {
	var selected []backlogadmin.TaskDetail
	var names []string
	for _, task := range detail.Tasks {
		if task.Sink != nil || task.Task.Name == domain.SinkTaskName {
			continue
		}
		names = append(names, task.Task.Name)
		if name == "" || task.Task.Name == name || task.Task.ID == name {
			selected = append(selected, task)
		}
	}
	if len(selected) == 0 {
		if name != "" {
			return nil, fmt.Errorf("run %s has no task %q; its tasks are %s",
				detail.Summary.Run.ID, name, strings.Join(names, ", "))
		}
		return nil, fmt.Errorf("run %s has no tasks", detail.Summary.Run.ID)
	}
	return selected, nil
}

// collect writes one task's final message and declared outputs.
func (c taskResultCLI) collect(ctx context.Context, detail backlogadmin.WorkflowDetail, task backlogadmin.TaskDetail, base string, inline bool) (taskResultTask, error) {
	collected := taskResultTask{
		Task:      task.Task.Name,
		TaskID:    task.Task.ID,
		Progress:  string(domain.ProgressQueued),
		Directory: filepath.Join(base, task.Task.Name),
		Files:     []taskResultFile{},
	}
	if task.Attempt != nil {
		collected.Progress = string(task.Attempt.Progress)
		collected.AttemptID = task.Attempt.ID
	}
	final := false
	for _, artifact := range detail.Artifacts {
		if artifact.Metadata.TaskID != task.Task.ID {
			continue
		}
		switch artifact.Metadata.Kind {
		case domain.ArtifactSummary:
			if artifact.Metadata.Name != finalMessageArtifactName {
				continue
			}
			final = true
		case domain.ArtifactOutput:
		default:
			// Logs and verification records are the coordinator's evidence, not
			// the task's result; "backlog artifacts" lists them.
			continue
		}
		body, err := c.fetch(ctx, artifact, collected.Directory)
		if err != nil {
			return taskResultTask{}, err
		}
		collected.Files = append(collected.Files, taskResultFile{
			Name: artifact.Metadata.Name,
			Path: filepath.Join(collected.Directory, filepath.FromSlash(artifact.Metadata.Name)),
			Kind: string(artifact.Metadata.Kind),
			Size: int64(len(body)),
		})
		if inline && artifact.Metadata.Kind == domain.ArtifactSummary {
			collected.FinalMessage = string(body)
		}
	}
	if !final {
		collected.Missing = append(collected.Missing, finalMessageArtifactName)
	}
	return collected, nil
}

// fetch writes one artifact under the task's directory and returns its bytes.
// The name is the one the task declared, so a relative path inside it is kept
// and anything that would climb out of the directory is refused.
func (c taskResultCLI) fetch(ctx context.Context, artifact backlogadmin.Artifact, directory string) ([]byte, error) {
	if c.open == nil {
		return nil, backlogadmin.ErrArtifactContentUnavailable
	}
	relative := filepath.FromSlash(artifact.Metadata.Name)
	if filepath.IsAbs(relative) || strings.HasPrefix(filepath.Clean(relative), "..") {
		return nil, fmt.Errorf("artifact %q has a name that would be written outside the result directory", artifact.Metadata.Name)
	}
	content, err := c.open(ctx, artifact.Metadata.ID)
	if err != nil {
		return nil, err
	}
	defer content.Content.Close()
	body, err := io.ReadAll(content.Content)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return nil, err
	}
	return body, nil
}

// worseResultProgress keeps the verdict of the worst task: not terminal beats
// failed, which beats succeeded, because a caller must not read "failed" as
// final while something is still running.
func worseResultProgress(current, candidate domain.ProgressState) domain.ProgressState {
	rank := func(state domain.ProgressState) int {
		switch state {
		case domain.ProgressSucceeded:
			return 0
		case domain.ProgressSkipped:
			return 1
		case domain.ProgressCancelled, domain.ProgressFailed:
			return 2
		default:
			return 3
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

// taskResultVerdict turns the collected outcome into the exit code, or nil for
// a clean success.
func taskResultVerdict(document taskResultDocument) error {
	switch domain.ProgressState(document.Outcome) {
	case domain.ProgressSucceeded, domain.ProgressSkipped:
		return nil
	case domain.ProgressFailed, domain.ProgressCancelled:
		return exitCodeError{code: 2, error: fmt.Errorf("run %s ended %s; what exists was written under %s",
			document.Run, document.Outcome, document.Directory)}
	default:
		return exitCodeError{code: 1, error: fmt.Errorf("run %s is %s and not terminal yet; nothing was waited for. "+
			"Park on it with \"t3-steward wait add --run %s\"", document.Run, document.Outcome, document.Run)}
	}
}

func renderTaskResult(out io.Writer, document taskResultDocument) error {
	fmt.Fprintf(out, "run %s: %s\n", document.Run, document.Outcome)
	for _, task := range document.Tasks {
		fmt.Fprintf(out, "\n%s (%s)\n  %s\n", task.Task, task.Progress, task.Directory)
		for _, file := range task.Files {
			fmt.Fprintf(out, "    %s\t%d bytes\n", file.Name, file.Size)
		}
		for _, missing := range task.Missing {
			fmt.Fprintf(out, "    %s is not there: this task produced no result\n", missing)
		}
	}
	return nil
}
