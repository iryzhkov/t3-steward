package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const schedulesUsage = `Usage: t3-steward schedules <command> [args]

Definition administration:
  put <schedule> --name TEXT --workflow ID --cron "EXPR" --timezone IANA
      --reason TEXT [--after-failure next-cycle|hold] [--disabled]
      [--expected-revision N] [--request-id ID] [--json]

Read commands:
  list [--json]                List schedules.
  show <schedule> [--json]     Show a schedule definition.
  history <schedule> [--json]  Show its trigger history.

Revision-fenced controls:
  run|enable|disable <schedule> --reason TEXT [--command-id ID] [--json]
  delay-next <schedule> --until RFC3339 --reason TEXT [--command-id ID] [--json]
`

type localAdminAuthorizer struct{}

func (localAdminAuthorizer) Authorize(_ context.Context, principal backlogadmin.Principal, _ backlogadmin.Action) error {
	if principal.ID == "" {
		return errors.New("local admin principal is required")
	}
	for _, role := range principal.Roles {
		if role == "local-admin" {
			return nil
		}
	}
	return errors.New("local-admin role is required")
}

type adminQueryService interface {
	Query(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
}

type adminSubmissionService interface {
	SubmitArchive(context.Context, backlogadmin.LocalSubmissionRequest, io.Reader, int64) (backlogadmin.LocalSubmissionResponse, error)
}

type adminScheduleDefinitionService interface {
	PutSchedule(context.Context, backlogadmin.Principal, backlogadmin.LocalScheduleDefinitionRequest) (backlogadmin.LocalScheduleDefinitionResponse, error)
}

type adminRecoveryService interface {
	RecoverUnknown(context.Context, backlogadmin.Principal, backlogadmin.UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error)
}

type backlogAdminCLI struct {
	service             adminQueryService
	mutator             adminMutationService
	artifacts           adminArtifactService
	submissions         adminSubmissionService
	scheduleDefinitions adminScheduleDefinitionService
	recovery            adminRecoveryService
	principal           backlogadmin.Principal
	stdout              io.Writer
	newCommandID        func() (string, error)
}

func (c backlogAdminCLI) runBacklog(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("backlog admin command is required")
	}
	if args[0] == "submit" {
		return c.runSubmission(ctx, args[1:])
	}
	if args[0] == "recover" {
		return c.runUnknownRecovery(ctx, args)
	}
	if isBacklogMutation(args[0]) {
		return c.runBacklogMutation(ctx, args)
	}
	if len(args) >= 2 && args[0] == "artifact" && args[1] == "get" {
		return c.runArtifactGet(ctx, args[2:])
	}
	query, asJSON, err := parseBacklogAdminQuery(args)
	if err != nil {
		return err
	}
	return c.queryAndRender(ctx, query, asJSON, "")
}

func (c backlogAdminCLI) runSchedules(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Fprint(c.stdout, schedulesUsage)
		return nil
	}
	if args[0] == "put" {
		return c.runScheduleDefinition(ctx, args)
	}
	if isScheduleMutation(args[0]) {
		return c.runScheduleMutation(ctx, args)
	}
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return err
	}
	if len(clean) == 0 {
		return errors.New("schedule command is required")
	}
	switch clean[0] {
	case "list":
		if len(clean) != 1 {
			return errors.New("schedules list takes no arguments")
		}
		return c.queryAndRender(ctx, backlogadmin.Query{Kind: backlogadmin.QuerySchedules}, asJSON, "")
	case "show", "history":
		if len(clean) != 2 {
			return fmt.Errorf("schedules %s needs a schedule id", clean[0])
		}
		return c.queryAndRender(ctx, backlogadmin.Query{Kind: backlogadmin.QuerySchedules}, asJSON, clean[0]+":"+clean[1])
	default:
		return fmt.Errorf("unknown schedules command %q", clean[0])
	}
}

func (c backlogAdminCLI) queryAndRender(ctx context.Context, query backlogadmin.Query, asJSON bool, selector string) error {
	query.Version = backlogadmin.Version
	query.Principal = c.principal
	response, err := c.service.Query(ctx, query)
	if err != nil {
		return err
	}
	if selector != "" {
		response, err = selectSchedule(response, selector)
		if err != nil {
			return err
		}
	}
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response)
	}
	return renderAdminResponse(c.stdout, response, selector)
}

func parseBacklogAdminQuery(args []string) (backlogadmin.Query, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return backlogadmin.Query{}, false, err
	}
	if len(clean) == 0 {
		return backlogadmin.Query{}, false, errors.New("backlog admin command is required")
	}
	switch clean[0] {
	case "status":
		if len(clean) != 1 {
			return backlogadmin.Query{}, false, errors.New("backlog status takes no arguments")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryStatus}, asJSON, nil
	case "list":
		filter, err := parseWorkflowFilters(clean[1:])
		return backlogadmin.Query{Kind: backlogadmin.QueryWorkflows, Filter: filter}, asJSON, err
	case "show", "graph", "events":
		if len(clean) != 2 {
			return backlogadmin.Query{}, false, fmt.Errorf("%s needs a workflow-run id", clean[0])
		}
		kinds := map[string]backlogadmin.QueryKind{"show": backlogadmin.QueryWorkflow, "graph": backlogadmin.QueryGraph, "events": backlogadmin.QueryEvents}
		return backlogadmin.Query{Kind: kinds[clean[0]], WorkflowRunID: clean[1]}, asJSON, nil
	case "task":
		if len(clean) != 3 || clean[1] != "show" {
			return backlogadmin.Query{}, false, errors.New("task usage: backlog task show <workflow-run>/<task>")
		}
		runID, taskID, err := splitTaskTarget(clean[2])
		return backlogadmin.Query{Kind: backlogadmin.QueryTask, WorkflowRunID: runID, TaskID: taskID}, asJSON, err
	case "explain":
		if len(clean) != 2 {
			return backlogadmin.Query{}, false, errors.New("explain needs <workflow-run>/<task>")
		}
		runID, taskID, err := splitTaskTarget(clean[1])
		return backlogadmin.Query{Kind: backlogadmin.QueryExplanation, WorkflowRunID: runID, TaskID: taskID}, asJSON, err
	case "artifacts":
		if len(clean) > 2 {
			return backlogadmin.Query{}, false, errors.New("artifacts accepts at most <task> or <workflow-run>/<task>")
		}
		query := backlogadmin.Query{Kind: backlogadmin.QueryArtifacts}
		if len(clean) == 2 {
			runID, taskID := splitOptionalTaskTarget(clean[1])
			if taskID == "" {
				query.TaskID = runID
			} else {
				query.WorkflowRunID, query.TaskID = runID, taskID
			}
		}
		return query, asJSON, nil
	case "artifact":
		if len(clean) != 3 || clean[1] != "show" {
			return backlogadmin.Query{}, false, errors.New("artifact usage: backlog artifact show <artifact>")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryArtifact, ArtifactID: clean[2]}, asJSON, nil
	case "commands":
		if len(clean) > 2 {
			return backlogadmin.Query{}, false, errors.New("commands accepts at most <workflow-run>[/<task>]")
		}
		query := backlogadmin.Query{Kind: backlogadmin.QueryCommands}
		if len(clean) == 2 {
			query.WorkflowRunID, query.TaskID = splitOptionalTaskTarget(clean[1])
		}
		return query, asJSON, nil
	case "command":
		if len(clean) != 3 || clean[1] != "show" {
			return backlogadmin.Query{}, false, errors.New("command usage: backlog command show <command>")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryCommands, CommandID: clean[2]}, asJSON, nil
	default:
		return backlogadmin.Query{}, false, fmt.Errorf("unknown backlog admin command %q", clean[0])
	}
}

func takeJSONFlag(args []string) ([]string, bool, error) {
	clean := make([]string, 0, len(args))
	asJSON := false
	for _, arg := range args {
		if arg == "--json" {
			if asJSON {
				return nil, false, errors.New("--json may only be specified once")
			}
			asJSON = true
			continue
		}
		clean = append(clean, arg)
	}
	return clean, asJSON, nil
}

func parseWorkflowFilters(args []string) (backlogadmin.Filter, error) {
	var filter backlogadmin.Filter
	for len(args) > 0 {
		if len(args) < 2 {
			return filter, fmt.Errorf("%s needs a value", args[0])
		}
		flag, value := args[0], args[1]
		args = args[2:]
		switch flag {
		case "--project":
			filter.Project = value
		case "--schedule":
			filter.ScheduleID = value
		case "--progress":
			for _, item := range strings.Split(value, ",") {
				if !validProgress(item) {
					return filter, fmt.Errorf("invalid progress %q", item)
				}
				filter.Progress = append(filter.Progress, domain.ProgressState(item))
			}
		case "--class":
			if value != string(domain.TaskClassRequired) && value != string(domain.TaskClassSurplus) {
				return filter, fmt.Errorf("invalid class %q", value)
			}
			filter.Class = domain.TaskClass(value)
		case "--worker":
			filter.WorkerID = value
		case "--quota-pool":
			filter.QuotaPoolID = value
		default:
			return filter, fmt.Errorf("unknown backlog list flag %q", flag)
		}
	}
	return filter, nil
}

func validProgress(value string) bool {
	switch domain.ProgressState(value) {
	case domain.ProgressQueued, domain.ProgressBlocked, domain.ProgressReady, domain.ProgressActive,
		domain.ProgressNeedsInput, domain.ProgressVerifying, domain.ProgressSucceeded,
		domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped:
		return true
	default:
		return false
	}
}

func splitTaskTarget(target string) (string, string, error) {
	runID, taskID := splitOptionalTaskTarget(target)
	if runID == "" || taskID == "" {
		return "", "", fmt.Errorf("target %q must be <workflow-run>/<task>", target)
	}
	return runID, taskID, nil
}

func splitOptionalTaskTarget(target string) (string, string) {
	runID, taskID, found := strings.Cut(target, "/")
	if !found {
		return target, ""
	}
	return runID, taskID
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "--help" || arg == "-h"
}

func selectSchedule(response backlogadmin.Response, selector string) (backlogadmin.Response, error) {
	mode, id, _ := strings.Cut(selector, ":")
	for _, schedule := range response.Schedules {
		if schedule.Schedule.ID != id {
			continue
		}
		response.Schedules = []backlogadmin.Schedule{schedule}
		if mode == "show" {
			response.Schedules[0].Triggers = nil
		}
		return response, nil
	}
	return backlogadmin.Response{}, fmt.Errorf("%w: schedule %q", backlogadmin.ErrNotFound, id)
}

func renderAdminResponse(out io.Writer, response backlogadmin.Response, selector string) error {
	switch response.Kind {
	case backlogadmin.QueryStatus:
		renderStatus(out, response.Status)
	case backlogadmin.QueryWorkflows:
		renderWorkflows(out, response.Workflows)
	case backlogadmin.QueryWorkflow:
		renderWorkflow(out, response.Workflow)
	case backlogadmin.QueryGraph:
		renderGraph(out, response.Graph)
	case backlogadmin.QueryTask:
		renderTask(out, response.Task)
	case backlogadmin.QueryExplanation:
		renderExplanation(out, response.Explanation)
	case backlogadmin.QueryEvents:
		renderEvents(out, response.Events)
	case backlogadmin.QueryArtifacts:
		renderArtifacts(out, response.Artifacts)
	case backlogadmin.QueryArtifact:
		if response.Artifact != nil {
			renderArtifacts(out, []backlogadmin.Artifact{*response.Artifact})
		}
	case backlogadmin.QueryCommands:
		renderCommands(out, response.Commands)
	case backlogadmin.QuerySchedules:
		renderSchedules(out, response.Schedules, selector)
	default:
		return fmt.Errorf("no human renderer for admin response %q", response.Kind)
	}
	return nil
}

func renderStatus(out io.Writer, status *backlogadmin.Status) {
	if status == nil {
		return
	}
	fmt.Fprintln(out, "WORKFLOW RUNS")
	renderCounts(out, status.WorkflowRuns)
	fmt.Fprintln(out, "TASKS")
	renderCounts(out, status.Tasks)
	fmt.Fprintln(out, "WORKERS")
	renderCounts(out, status.Workers)
	fmt.Fprintln(out, "QUOTA POOLS")
	renderCounts(out, status.QuotaPools)
	fmt.Fprintf(out, "reservations: %d\nlocks: %d\n", status.Reservations, status.Locks)
}

func renderCounts[K ~string](out io.Writer, counts map[K]int) {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(out, "  %s: %d\n", key, counts[K(key)])
	}
}

func renderWorkflows(out io.Writer, workflows []backlogadmin.WorkflowSummary) {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "RUN\tWORKFLOW\tPROJECT\tCLASS\tSTATE\tTASKS")
	for _, item := range workflows {
		progress := item.Progress
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d/%d\n", item.Run.ID, item.Workflow.Name,
			item.Workflow.Project, item.Workflow.Class, item.Run.Progress, progress.Succeeded, progress.Total)
	}
	_ = table.Flush()
}

func renderWorkflow(out io.Writer, detail *backlogadmin.WorkflowDetail) {
	if detail == nil {
		return
	}
	summary := detail.Summary
	fmt.Fprintf(out, "run: %s\nworkflow: %s (%s)\nproject: %s\nclass: %s\nprogress: %s\nrevision: %d\n",
		summary.Run.ID, summary.Workflow.Name, summary.Workflow.ID, summary.Workflow.Project,
		summary.Workflow.Class, summary.Run.Progress, summary.Run.Revision)
	fmt.Fprintln(out, "tasks:")
	for _, task := range detail.Tasks {
		state, control, attempt := taskState(task)
		fmt.Fprintf(out, "  %s (%s): %s %s attempt=%s\n", task.Task.Name, task.Task.ID, state, control, attempt)
	}
	fmt.Fprintf(out, "artifacts: %d\nreservations: %d\nlocks: %d\n",
		len(detail.Artifacts), len(detail.Reservations), len(detail.ResourceLocks))
}

func renderGraph(out io.Writer, graph *backlogadmin.Graph) {
	if graph == nil {
		return
	}
	fmt.Fprintf(out, "workflow run: %s\n", graph.WorkflowRunID)
	for _, node := range graph.Nodes {
		fmt.Fprintf(out, "  %s [%s] %s", node.Name, node.Progress, node.TaskID)
		if node.AttemptID != "" {
			fmt.Fprintf(out, " attempt=%s", node.AttemptID)
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintln(out, "dependencies:")
	for _, edge := range graph.Edges {
		fmt.Fprintf(out, "  %s -> %s\n", edge.FromTaskID, edge.ToTaskID)
	}
}

func renderTask(out io.Writer, detail *backlogadmin.TaskDetail) {
	if detail == nil {
		return
	}
	state, control, attemptID := taskState(*detail)
	fmt.Fprintf(out, "task: %s (%s)\nworkflow: %s\nclass: %s\nprogress: %s\ncontrol: %s\nattempt: %s\n",
		detail.Task.Name, detail.Task.ID, detail.Task.WorkflowID, detail.Task.Class, state, control, attemptID)
	if detail.Attempt != nil {
		fmt.Fprintf(out, "attempt number: %d\nrevision: %d\n", detail.Attempt.Number, detail.Attempt.Revision)
		if detail.Attempt.Failure != "" {
			fmt.Fprintf(out, "failure: %s\n", detail.Attempt.Failure)
		}
	}
	if detail.Assignment != nil {
		fmt.Fprintf(out, "worker: %s\nprovider: %s/%s\n", detail.Assignment.WorkerID,
			detail.Assignment.Route.ProviderInstanceID, detail.Assignment.Route.Model)
	}
	if detail.ThreadURL != "" {
		fmt.Fprintf(out, "thread: %s\n", detail.ThreadURL)
	}
	fmt.Fprintf(out, "artifacts: %d\nlocks: %s\n", len(detail.Artifacts), strings.Join(detail.ResourceLocks, ", "))
}

func taskState(detail backlogadmin.TaskDetail) (domain.ProgressState, domain.ControlState, string) {
	if detail.Attempt == nil {
		return domain.ProgressQueued, "", "-"
	}
	return detail.Attempt.Progress, detail.Attempt.Control, detail.Attempt.ID
}

func renderExplanation(out io.Writer, explanation *backlogadmin.Explanation) {
	if explanation == nil {
		return
	}
	fmt.Fprintf(out, "%s/%s: %s\neligible: %t\n", explanation.WorkflowRunID, explanation.TaskID,
		explanation.Summary, explanation.Eligible)
	if explanation.EarliestAt != nil {
		fmt.Fprintf(out, "earliest: %s\n", formatTime(*explanation.EarliestAt))
	}
	for _, blocker := range explanation.Blockers {
		fmt.Fprintf(out, "  %s: %s\n", blocker.Code, blocker.Detail)
	}
}

func renderEvents(out io.Writer, events []backlogadmin.Event) {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "TIME\tKIND\tTASK\tATTEMPT\tDETAIL")
	for _, event := range events {
		detail := strings.TrimSpace(string(event.Detail))
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", formatTime(event.At), event.Kind, event.TaskID, event.AttemptID, detail)
	}
	_ = table.Flush()
}

func renderArtifacts(out io.Writer, artifacts []backlogadmin.Artifact) {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tKIND\tNAME\tTASK\tSIZE\tSHA256\tDOWNLOAD")
	for _, artifact := range artifacts {
		item := artifact.Metadata
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", item.ID, item.Kind, item.Name,
			item.TaskID, item.Size, item.SHA256, artifact.Download)
	}
	_ = table.Flush()
}

func renderCommands(out io.Writer, commands []backlogadmin.Command) {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tKIND\tTARGET\tSTATE\tREQUESTED BY\tREASON")
	for _, command := range commands {
		fmt.Fprintf(table, "%s\t%s\t%s/%s\t%s\t%s\t%s\n", command.ID, command.Kind,
			command.TargetType, command.TargetID, command.State, command.RequestedBy, command.Reason)
	}
	_ = table.Flush()
}

func renderSchedules(out io.Writer, schedules []backlogadmin.Schedule, selector string) {
	mode, _, _ := strings.Cut(selector, ":")
	if mode == "history" {
		if len(schedules) == 0 {
			return
		}
		table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NOMINAL\tSTATE\tRUN\tREASON")
		for _, trigger := range schedules[0].Triggers {
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", formatTime(trigger.NominalAt),
				trigger.State, trigger.WorkflowRunID, trigger.Reason)
		}
		_ = table.Flush()
		return
	}
	if mode == "show" && len(schedules) > 0 {
		item := schedules[0].Schedule
		fmt.Fprintf(out, "schedule: %s (%s)\nworkflow: %s\nexpression: %s\ntimezone: %s\nenabled: %t\nactive run: %s\nrevision: %d\n",
			item.Name, item.ID, item.WorkflowID, item.Expression, item.Timezone, item.Enabled, item.ActiveRunID, item.Revision)
		return
	}
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tNAME\tEXPRESSION\tTIMEZONE\tENABLED\tACTIVE RUN")
	for _, item := range schedules {
		schedule := item.Schedule
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%t\t%s\n", schedule.ID, schedule.Name,
			schedule.Expression, schedule.Timezone, schedule.Enabled, schedule.ActiveRunID)
	}
	_ = table.Flush()
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}
