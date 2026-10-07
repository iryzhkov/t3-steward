package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// coordinatorWorkflowQuery is the one coordinator query "task result" and the
// wake summary both make: a fresh transport per call, built at use, so a
// credential that cannot be resolved now is retried on the next call.
func coordinatorWorkflowQuery(cfg config.Config) coordinatorQuery {
	return func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
		transport, err := newCoordinatorTransport(cfg)
		if err != nil {
			return backlogadmin.Response{}, err
		}
		query.Version = backlogadmin.Version
		query.Principal = transport.principal
		return transport.client.Query(ctx, query)
	}
}

// coordinatorSummarySource is the wake summary's view of the coordinator: the
// run's workflow detail and its retained artifacts, read exactly as "task
// result" reads them.
type coordinatorSummarySource struct {
	query func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
	open  func(context.Context, string) (backlogadmin.ArtifactContent, error)
}

var _ wait.NodeSummarySource = (*coordinatorSummarySource)(nil)

// newNodeSummarySource is the source the steward builds node wake summaries
// from, on whichever host delivers the wake.
func newNodeSummarySource(cfg config.Config) *coordinatorSummarySource {
	return &coordinatorSummarySource{
		query: coordinatorWorkflowQuery(cfg),
		open: func(ctx context.Context, id string) (backlogadmin.ArtifactContent, error) {
			return openTaskResultArtifact(ctx, cfg, id)
		},
	}
}

// summaryCategoryOf names a coordinator failure with the fixed word a wake
// prints. The error's own text, which may carry remote detail, never travels.
func summaryCategoryOf(err error) error {
	category := "query-failed"
	switch backlogadmin.ClassOf(err) {
	case backlogadmin.ClassClientConfiguration:
		category = "no-transport"
	case backlogadmin.ClassAuthentication:
		category = "authentication"
	case backlogadmin.ClassUnavailable:
		category = "unavailable"
	case backlogadmin.ClassTimeout:
		category = "timeout"
	case backlogadmin.ClassProtocol:
		category = "protocol"
	case backlogadmin.ClassRejected:
		category = "query-refused"
	}
	return wait.SummaryError{Category: category}
}

func (s *coordinatorSummarySource) SummaryRun(ctx context.Context, runID string) (wait.SummaryRun, error) {
	if s.query == nil {
		return wait.SummaryRun{}, wait.SummaryError{Category: "no-transport"}
	}
	response, err := s.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflow, WorkflowRunID: runID})
	if err != nil {
		return wait.SummaryRun{}, summaryCategoryOf(err)
	}
	if response.Workflow == nil {
		return wait.SummaryRun{}, wait.SummaryError{Category: "no-workflow"}
	}
	return summaryRunOf(*response.Workflow), nil
}

func (s *coordinatorSummarySource) OpenSummaryArtifact(ctx context.Context, id string) (io.ReadCloser, error) {
	if s.open == nil {
		return nil, backlogadmin.ErrArtifactContentUnavailable
	}
	content, err := s.open(ctx, id)
	if err != nil {
		return nil, err
	}
	return content.Content, nil
}

// summaryRunOf is the workflow detail as the summary builder reads it: tasks
// in the detail's order, each with its latest attempt and that attempt's
// retained outputs. A coordinator too old to list artifacts yields tasks with
// none, which the builder shows as unreadable rather than absent.
func summaryRunOf(detail backlogadmin.WorkflowDetail) wait.SummaryRun {
	run := wait.SummaryRun{ID: detail.Summary.Run.ID, Workflow: detail.Summary.Workflow.Name, Progress: detail.Summary.Run.Progress}
	for _, task := range detail.Tasks {
		row := wait.SummaryTask{ID: task.Task.ID, Name: task.Task.Name, Outputs: task.Task.Outputs}
		if task.Sink != nil {
			row.ID, row.Name, row.Sink, row.Progress = task.Sink.ID, domain.SinkTaskName, true, task.Sink.Progress
		}
		if task.Attempt != nil {
			row.Attempt, row.Progress, row.Failure = task.Attempt.ID, task.Attempt.Progress, task.Attempt.Failure
		}
		seen := map[string]bool{}
		for _, artifacts := range [][]backlogadmin.Artifact{detail.Artifacts, task.Artifacts} {
			for _, artifact := range artifacts {
				metadata := artifact.Metadata
				if metadata.TaskID != row.ID || metadata.Kind != domain.ArtifactOutput || seen[metadata.ID] {
					continue
				}
				// Only the latest attempt's outputs answer for the task.
				if metadata.AttemptID != "" && row.Attempt != "" && metadata.AttemptID != row.Attempt {
					continue
				}
				seen[metadata.ID] = true
				row.Artifacts = append(row.Artifacts, wait.SummaryArtifact{ID: metadata.ID, Name: metadata.Name, Kind: metadata.Kind, Size: metadata.Size})
			}
		}
		run.Tasks = append(run.Tasks, row)
	}
	return run
}

// waitSummarySources are where "wait summary" finds a wait and its summary.
type waitSummarySources struct {
	// local reads this host's waits; a GitHub wait's summary is stored on it.
	local func(context.Context) ([]wait.Wait, error)
	// nodes reads the coordinator's node waits, and is nil on a host with no
	// coordinator configured.
	nodes func(context.Context) ([]domain.NodeWait, error)
	// summary is what a node wait's summary is built from.
	summary wait.NodeSummarySource
}

func parseWaitSummaryArgs(args []string) (string, bool, error) {
	fs := flag.NewFlagSet("wait summary", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print the summary document as JSON")
	var id string
	// The id may come before or after --json.
	if len(args) != 0 && !strings.HasPrefix(args[0], "-") {
		id, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return "", false, err
	}
	rest := fs.Args()
	if id == "" && len(rest) != 0 {
		id, rest = rest[0], rest[1:]
	}
	if id == "" || len(rest) != 0 {
		return "", false, errors.New("wait summary takes exactly one wait id")
	}
	return id, *asJSON, nil
}

func cmdWaitSummary(ctx context.Context, cfg config.Config, args []string, out io.Writer) error {
	id, asJSON, err := parseWaitSummaryArgs(args)
	if err != nil {
		return err
	}
	sources := waitSummarySources{summary: newNodeSummarySource(cfg)}
	sources.local = func(ctx context.Context) ([]wait.Wait, error) {
		statePath, err := cfg.ResolveStatePath()
		if err != nil {
			return nil, err
		}
		store, err := sqlite.Open(statePath)
		if err != nil {
			return nil, err
		}
		defer store.Close()
		return store.ListWaits(ctx, "")
	}
	if cfg.BacklogV2.CoordinatorClient.Configured() || cfg.BacklogV2.Mode == "coordinator" {
		sources.nodes = func(ctx context.Context) ([]domain.NodeWait, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return nil, err
			}
			response, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
			return response.Waits, err
		}
	}
	return runWaitSummary(ctx, sources, id, asJSON, out)
}

// runWaitSummary prints one wait's summary: a node wait's built live with the
// same builder and bounds as its wake, a local GitHub wait's as stored when
// it settled.
func runWaitSummary(ctx context.Context, sources waitSummarySources, id string, asJSON bool, out io.Writer) error {
	var summary wait.WakeSummary
	switch {
	case strings.HasPrefix(id, "nw-"):
		if sources.nodes == nil {
			return fmt.Errorf("wait %s is a coordinator-held node wait and this host has no coordinator configured", id)
		}
		waits, err := sources.nodes(ctx)
		if err != nil {
			return err
		}
		var found *domain.NodeWait
		for i := range waits {
			if waits[i].Request.ID == id {
				found = &waits[i]
			}
		}
		if found == nil {
			return fmt.Errorf("the coordinator holds no node wait %s", id)
		}
		if !wait.NodeWaitHasSummary(*found) {
			return fmt.Errorf("wait %s has no summary: only a node wait settled on a terminal run or task has one", id)
		}
		summary = wait.BuildNodeSummary(ctx, sources.summary, *found)
	case strings.HasPrefix(id, "tw-"):
		return fmt.Errorf("wait %s is task-bound; its outcome is in the task's own wake, and it has no summary", id)
	default:
		if sources.local == nil {
			return errors.New("this host's waits cannot be read")
		}
		waits, err := sources.local(ctx)
		if err != nil {
			return err
		}
		var found *wait.Wait
		for i := range waits {
			if waits[i].ID == id {
				found = &waits[i]
			}
		}
		if found == nil {
			return fmt.Errorf("this host holds no wait %s", id)
		}
		if found.Summary == nil {
			return fmt.Errorf("wait %s has no stored summary: only a settled GitHub wait whose annotations were read has one", id)
		}
		summary = *found.Summary
	}
	if asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(summary); err != nil {
			return err
		}
	} else {
		fmt.Fprint(out, summary.Text())
	}
	if summary.Unavailable != "" {
		return afterDocument(fmt.Errorf("the summary of wait %s is unavailable (%s)", id, summary.Unavailable))
	}
	return nil
}
