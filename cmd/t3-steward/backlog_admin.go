package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
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

Example:
  t3-steward schedules put nightly-upkeep --name "nightly upkeep" \
    --workflow workflow-1 --cron "0 3 * * *" --timezone Europe/Amsterdam \
    --reason "restore the nightly pass" --request-id 2026-09-14-nightly --json
` + coordinatorTransportHelp

type localAdminAuthorizer struct{}

// remoteAdminCommandKinds is what a verified remote client may ask the
// coordinator to do. It is an allowlist rather than a list of exceptions,
// because the property that matters is "a remote client cannot rewrite the
// coordinator's own identity or epoch", and an allowlist keeps that true when
// a new command kind is added instead of depending on someone remembering to
// deny it.
func remoteAdminCommandKinds() map[domain.AdminCommandKind]bool {
	return map[domain.AdminCommandKind]bool{
		domain.AdminCommandStart:       true,
		domain.AdminCommandDelay:       true,
		domain.AdminCommandPause:       true,
		domain.AdminCommandResume:      true,
		domain.AdminCommandCancel:      true,
		domain.AdminCommandRetry:       true,
		domain.AdminCommandSkip:        true,
		domain.AdminCommandRewake:      true,
		domain.AdminCommandScheduleRun: true,
		domain.AdminCommandEnable:      true,
		domain.AdminCommandDisable:     true,
		domain.AdminCommandDelayNext:   true,
	}
}

// remoteAdminOperations is the same allowlist for the operations that are not
// revision-fenced commands. Worker enrollment is absent on purpose: it binds a
// worker to this coordinator's id, epoch and credential reference, so it stays
// an operation an operator performs on the coordinator host.
func remoteAdminOperations() map[backlogadmin.QueryKind]bool {
	return map[backlogadmin.QueryKind]bool{
		"node-wait":       true,
		"graph-amendment": true,
		// Releasing a quarantine tells intake to read a file again. It creates
		// nothing by itself, the coordinator still refuses content it cannot
		// accept, and the operator who fixed the configuration is usually not
		// sitting on the coordinator host.
		backlogadmin.QuarantineReleaseKind: true,
		// Supervision is an operator authority as much as an overseer one: an
		// operator inspects a supervised run, takes over, accepts with the same
		// evidence checks, escalates or resolves, and is usually not sitting on
		// the coordinator host. The narrower supervisor role is a separate
		// role, not a separate operation; see backlogadmin.SupervisorAuthorizer.
		backlogadmin.SupervisionShowKind:     true,
		backlogadmin.SupervisionDecisionKind: true,
	}
}

// Authorize admits the owner-only local peer to everything, and a verified
// remote client to reads plus an explicit allowlist of mutations.
//
// A mutation names its verb in CommandKind, not in Kind, so both are examined.
// Submission and schedule definition do not reach this authorizer at all; they
// are bounded by their own services, which is noted here so the next reader
// does not mistake this function for the whole authority boundary.
func (localAdminAuthorizer) Authorize(_ context.Context, principal backlogadmin.Principal, action backlogadmin.Action) error {
	if principal.ID == "" {
		return errors.New("local admin principal is required")
	}
	if action.Kind == backlogadmin.QueryKind("attention-decision") {
		if len(principal.Roles) == 1 && principal.Roles[0] == backlogadmin.ApproverRole {
			return nil
		}
		return errors.New("attention decisions require the separately authenticated approver role")
	}
	for _, role := range principal.Roles {
		switch role {
		case backlogadmin.LocalAdminRole:
			return nil
		case backlogadmin.RemoteAdminRole:
			return authorizeRemoteAdmin(action)
		case backlogadmin.ApproverRole:
			return errors.New("approver role is limited to attention decisions")
		}
	}
	return errors.New("local-admin or remote-admin role is required")
}

// A refusal says what the boundary is and stops there. It never tells the
// caller to go and run the command on the coordinator instead: an agent that
// takes that literally opens a shell on the coordinator's owner account, which
// is the authority story ADR-H1 exists to remove.
func authorizeRemoteAdmin(action backlogadmin.Action) error {
	if action.CommandKind != "" {
		if !remoteAdminCommandKinds()[action.CommandKind] {
			return fmt.Errorf("%q is reserved to the coordinator's own operator and is not available to the remote-admin role", action.CommandKind)
		}
		return nil
	}
	// Every declared query kind is a read view, so remote clients get all of
	// them. This is deliberately not a second hand-maintained list: the first
	// one fell behind and made the readiness check, which submit runs by
	// default, unreachable from the host that needs it most.
	if backlogadmin.IsQueryKind(action.Kind) || remoteAdminOperations()[action.Kind] {
		return nil
	}
	if action.Kind == backlogadmin.QueryKind("worker-enrollment") {
		return errors.New("worker enrollment binds a worker to this coordinator's identity and epoch, so it is an operator action on the coordinator itself and is not available to the remote-admin role")
	}
	return fmt.Errorf("%q is reserved to the coordinator's own operator and is not available to the remote-admin role", action.Kind)
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

type adminQuarantineService interface {
	ReleaseQuarantine(context.Context, backlogadmin.Principal, backlogadmin.QuarantineReleaseRequest) (domain.QuarantineRelease, error)
}

type backlogAdminCLI struct {
	service             adminQueryService
	mutator             adminMutationService
	artifacts           adminArtifactService
	submissions         adminSubmissionService
	scheduleDefinitions adminScheduleDefinitionService
	recovery            adminRecoveryService
	quarantine          adminQuarantineService
	principal           backlogadmin.Principal
	stdout              io.Writer
	newCommandID        func() (string, error)
}

func (c backlogAdminCLI) runBacklog(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("backlog admin command is required")
	}
	if args[0] == "edge" || args[0] == "run" || (args[0] == "task" && len(args) > 1 && (args[1] == "add" || args[1] == "set")) {
		return c.runGraphAmendment(ctx, args)
	}
	if args[0] == "submit" {
		return c.runSubmission(ctx, args[1:])
	}
	if args[0] == "recover" {
		return c.runUnknownRecovery(ctx, args)
	}
	if len(args) >= 2 && args[0] == "quarantine" && args[1] == "release" {
		return c.runQuarantineRelease(ctx, args[2:])
	}
	if isBacklogMutation(args[0]) {
		return c.runBacklogMutation(ctx, args)
	}
	if len(args) >= 2 && args[0] == "artifact" && args[1] == "get" {
		return c.runArtifactGet(ctx, args[2:])
	}
	if args[0] == "graph" {
		for _, arg := range args[1:] {
			if arg == "--dot" {
				return c.runGraphDOT(ctx, args)
			}
		}
	}
	query, display, err := parseBacklogAdminQuery(args)
	if err != nil {
		return err
	}
	return c.queryAndRender(ctx, query, display, "")
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
		return c.queryAndRender(ctx, backlogadmin.Query{Kind: backlogadmin.QuerySchedules}, commandDisplay{JSON: asJSON}, "")
	case "show", "history":
		if len(clean) != 2 {
			return fmt.Errorf("schedules %s needs a schedule id", clean[0])
		}
		return c.queryAndRender(ctx, backlogadmin.Query{Kind: backlogadmin.QuerySchedules}, commandDisplay{JSON: asJSON}, clean[0]+":"+clean[1])
	default:
		return fmt.Errorf("unknown schedules command %q", clean[0])
	}
}

// ask is one question to the coordinator, with the envelope every question
// carries. It is a function value so that a refusal can be explained by the
// shared explanation, which asks a question of its own.
func (c backlogAdminCLI) ask(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	query.Version = backlogadmin.Version
	query.Principal = c.principal
	return c.service.Query(ctx, query)
}

func (c backlogAdminCLI) queryAndRender(ctx context.Context, query backlogadmin.Query, display commandDisplay, selector string) error {
	response, err := c.ask(ctx, query)
	if err != nil {
		if query.Kind == backlogadmin.QueryProjects {
			// "backlog projects" is the catalog and nothing else, so a
			// coordinator that has no projects query refuses the whole verb.
			return explainRefusedProjectsQuery(ctx, c.ask, err, projectsWithoutTheCatalog)
		}
		return err
	}
	if selector != "" {
		response, err = selectSchedule(response, selector)
		if err != nil {
			return err
		}
	}
	response = adoptDeprecatedWaitKeys(response)
	response, matched := display.List.apply(response, display.JSON)
	if display.JSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		if document, summarised := summariseProjects(response, display.Verbose); summarised {
			return encoder.Encode(document)
		}
		return encoder.Encode(response)
	}
	if err := renderAdminResponse(c.stdout, response, selector, display); err != nil {
		return err
	}
	if response.Kind == backlogadmin.QueryWorkflows && matched > len(response.Workflows) {
		fmt.Fprintf(c.stdout, "showing the newest %d of %d runs; --limit N shows more, --limit 0 every one, --since DURATION only recent ones\n",
			len(response.Workflows), matched)
	}
	return nil
}

// adoptDeprecatedWaitKeys gives a document decoded from an older coordinator
// the content of its renamed wait keys, once, where the answer arrives.
//
// A coordinator of the previous release sends only the deprecated waits key,
// and this release reads taskWaits in a run document and nodeWaits in a
// diagnosis. Doing this in the renderers alone would fix the text and leave
// --json printing "taskWaits": null and "nodeWaits": null beside a populated
// "waits", because that form re-encodes the decoded response rather than
// passing the coordinator's bytes through -- a silent misread on the path the
// skills tell an agent to use. Normalising here means the renderer and the
// encoder see one document.
//
// The deprecated key is left populated: this release still emits it so that a
// client of the previous release can read this coordinator, and both go away
// together.
func adoptDeprecatedWaitKeys(response backlogadmin.Response) backlogadmin.Response {
	if detail := response.Workflow; detail != nil {
		detail.TaskWaits = workflowTaskWaits(detail)
	}
	if diagnosis := response.Diagnosis; diagnosis != nil {
		diagnosis.NodeWaits = diagnosisNodeWaits(diagnosis)
		diagnosis.Workflow.TaskWaits = workflowTaskWaits(&diagnosis.Workflow)
	}
	return response
}

// commandDisplay is what the command line asked of the rendering rather than
// of the coordinator: the shape of the answer, not its content. The query says
// what to read and the coordinator answers it; this says how much of that
// answer to print and is honoured here. The two are kept apart because the
// scope is always applied first: a summary is a summary of the scoped answer,
// never a truncation the scope is then looked for in.
type commandDisplay struct {
	JSON bool
	// Verbose asks a summarising renderer for its full form. Only backlog
	// projects summarises, so parseBacklogAdminQuery refuses the flag for every
	// other verb rather than accepting it and doing nothing with it.
	Verbose bool
	// List bounds the runs "backlog list" prints. Only list sets it.
	List listWindow
}

// listWindow is how many of the runs "backlog list" answered are printed and
// how far back they reach. The coordinator answers every run it holds, newest
// first, which on a fleet that has been running for months is hundreds of
// rows an agent has to page through to find the one it submitted.
//
// It is applied here, to the answer, rather than sent to the coordinator:
// the admin protocol decodes a query strictly, so a filter field a coordinator
// of the previous release does not know would have it refuse the whole
// question. Trimming the answer keeps every coordinator readable at the cost
// of carrying rows that are then dropped.
type listWindow struct {
	// Limit is the most runs printed; zero prints every run. Unset, it is
	// defaultListLimit for the text form and unbounded for --json, whose
	// readers are scripts that may count on the whole list.
	Limit    int
	LimitSet bool
	// Since keeps only runs created within this long of the answer's
	// generation time; zero keeps every run.
	Since time.Duration
}

// defaultListLimit is how many runs the text form of "backlog list" prints
// when --limit is not given.
const defaultListLimit = 50

// takeListWindowFlags removes --limit and --since from the arguments of
// "backlog list". It is a function of its own because it is the parser site
// the list help page derives the flags from, and "backlog usage" has a --limit
// of its own with another meaning.
func takeListWindowFlags(args []string) ([]string, listWindow, error) {
	clean := make([]string, 0, len(args))
	var window listWindow
	sinceSet := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--limit":
			if window.LimitSet {
				return nil, window, errors.New("--limit may only be specified once")
			}
			if i+1 >= len(args) {
				return nil, window, errors.New("--limit needs a value")
			}
			i++
			limit, err := strconv.Atoi(args[i])
			if err != nil || limit < 0 {
				return nil, window, fmt.Errorf("--limit %q is not a count of runs; 0 prints every run", args[i])
			}
			window.Limit, window.LimitSet = limit, true
		case "--since":
			if sinceSet {
				return nil, window, errors.New("--since may only be specified once")
			}
			if i+1 >= len(args) {
				return nil, window, errors.New("--since needs a value")
			}
			i++
			since, err := time.ParseDuration(args[i])
			if err != nil || since <= 0 {
				return nil, window, fmt.Errorf("--since %q is not a positive duration such as 24h", args[i])
			}
			window.Since, sinceSet = since, true
		default:
			clean = append(clean, args[i])
		}
	}
	return clean, window, nil
}

// apply trims a list answer to the window and returns how many runs matched
// before the limit, so the text form can say what it left out. The answer is
// already newest first, so the limit keeps the newest runs.
func (w listWindow) apply(response backlogadmin.Response, asJSON bool) (backlogadmin.Response, int) {
	if response.Kind != backlogadmin.QueryWorkflows {
		return response, 0
	}
	runs := response.Workflows
	if w.Since > 0 {
		now := response.GeneratedAt
		if now.IsZero() {
			now = time.Now()
		}
		cutoff := now.Add(-w.Since)
		kept := make([]backlogadmin.WorkflowSummary, 0, len(runs))
		for _, run := range runs {
			if !run.Run.CreatedAt.Before(cutoff) {
				kept = append(kept, run)
			}
		}
		runs = kept
	}
	matched := len(runs)
	limit := w.Limit
	if !w.LimitSet && !asJSON {
		limit = defaultListLimit
	}
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	response.Workflows = runs
	return response, matched
}

// takeVerboseFlag removes --verbose from the arguments. It is a function of
// its own because it is the parser site the backlog projects help page derives
// the flag from, and no other backlog verb accepts it.
func takeVerboseFlag(args []string) ([]string, bool, error) {
	clean := make([]string, 0, len(args))
	verbose := false
	for _, arg := range args {
		if arg == "--verbose" {
			if verbose {
				return nil, false, errors.New("--verbose may only be specified once")
			}
			verbose = true
			continue
		}
		clean = append(clean, arg)
	}
	return clean, verbose, nil
}

func parseBacklogAdminQuery(args []string) (backlogadmin.Query, commandDisplay, error) {
	args, verbose, err := takeVerboseFlag(args)
	if err != nil {
		return backlogadmin.Query{}, commandDisplay{}, err
	}
	var window listWindow
	if backlogQueryVerb(args) == "list" {
		if args, window, err = takeListWindowFlags(args); err != nil {
			return backlogadmin.Query{}, commandDisplay{Verbose: verbose}, err
		}
	}
	clean := make([]string, 0, len(args))
	include := false
	for _, arg := range args {
		if arg == "--include-sink" {
			if include {
				return backlogadmin.Query{}, commandDisplay{Verbose: verbose}, errors.New("--include-sink may only be specified once")
			}
			include = true
		} else {
			clean = append(clean, arg)
		}
	}
	query, asJSON, err := parseBacklogAdminQueryWithoutSink(clean)
	display := commandDisplay{JSON: asJSON, Verbose: verbose, List: window}
	if err != nil {
		return query, display, err
	}
	if include && query.Kind != backlogadmin.QueryStatus && query.Kind != backlogadmin.QueryWorkflows && query.Kind != backlogadmin.QueryWorkflow {
		return query, display, errors.New("--include-sink is supported by status, list, and show")
	}
	if verbose && query.Kind != backlogadmin.QueryProjects {
		return query, display, errors.New("--verbose is supported by backlog projects, where it spells out every project's eligible workers and routes")
	}
	query.IncludeSink = include
	return query, display, nil
}

// backlogQueryVerb is the verb of a backlog read: its first argument that is
// not one of the output flags every read accepts in any position.
func backlogQueryVerb(args []string) string {
	for _, arg := range args {
		if arg != "--json" && arg != "--include-sink" {
			return arg
		}
	}
	return ""
}

func parseBacklogAdminQueryWithoutSink(args []string) (backlogadmin.Query, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return backlogadmin.Query{}, false, err
	}
	if len(clean) == 0 {
		return backlogadmin.Query{}, false, errors.New("backlog admin command is required")
	}
	switch clean[0] {
	case "workers":
		if len(clean) != 1 {
			return backlogadmin.Query{}, false, errors.New("backlog workers takes no arguments")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryWorkers}, asJSON, nil
	case "projects":
		query := backlogadmin.Query{Kind: backlogadmin.QueryProjects}
		switch {
		case len(clean) == 1:
		case len(clean) == 3 && clean[1] == "--project" && strings.TrimSpace(clean[2]) != "":
			query.Filter.Project = clean[2]
		default:
			return backlogadmin.Query{}, false, errors.New("backlog projects usage: backlog projects [--project NAME] [--verbose] [--json]")
		}
		return query, asJSON, nil
	case "status":
		if len(clean) != 1 {
			return backlogadmin.Query{}, false, errors.New("backlog status takes no arguments")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryStatus}, asJSON, nil
	case "list":
		filter, err := parseWorkflowFilters(clean[1:])
		return backlogadmin.Query{Kind: backlogadmin.QueryWorkflows, Filter: filter}, asJSON, err
	case "show", "graph", "events", "diagnose":
		if len(clean) != 2 {
			return backlogadmin.Query{}, false, fmt.Errorf("%s needs a workflow-run id", clean[0])
		}
		kinds := map[string]backlogadmin.QueryKind{"show": backlogadmin.QueryWorkflow, "graph": backlogadmin.QueryGraph, "events": backlogadmin.QueryEvents, "diagnose": backlogadmin.QueryDiagnose}
		return backlogadmin.Query{Kind: kinds[clean[0]], WorkflowRunID: clean[1]}, asJSON, nil
	case "usage":
		if len(clean) < 2 || strings.HasPrefix(clean[1], "--") {
			return backlogadmin.Query{}, false, errors.New("backlog usage needs a workflow-run id")
		}
		query := backlogadmin.Query{Kind: backlogadmin.QueryUsage, WorkflowRunID: clean[1]}
		for i := 2; i < len(clean); i++ {
			switch clean[i] {
			case "--raw":
				if query.UsageRaw {
					return backlogadmin.Query{}, false, errors.New("--raw may only be specified once")
				}
				query.UsageRaw = true
			case "--limit":
				if i+1 >= len(clean) {
					return backlogadmin.Query{}, false, errors.New("--limit needs a value")
				}
				i++
				limit, parseErr := strconv.Atoi(clean[i])
				if parseErr != nil || limit < 1 || limit > 200 {
					return backlogadmin.Query{}, false, errors.New("--limit must be between 1 and 200")
				}
				query.UsageLimit = limit
			case "--cursor":
				if i+1 >= len(clean) || clean[i+1] == "" {
					return backlogadmin.Query{}, false, errors.New("--cursor needs a value")
				}
				i++
				query.UsageCursor = clean[i]
			default:
				return backlogadmin.Query{}, false, fmt.Errorf("unknown backlog usage flag %q", clean[i])
			}
		}
		if (query.UsageLimit != 0 || query.UsageCursor != "") && !query.UsageRaw {
			return backlogadmin.Query{}, false, errors.New("--limit and --cursor require --raw")
		}
		return query, asJSON, nil
	case "task":
		if len(clean) != 3 || clean[1] != "show" {
			return backlogadmin.Query{}, false, showOnlyUsage("task", "<workflow-run>/<task>", clean)
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
			return backlogadmin.Query{}, false, showOnlyUsage("artifact", "<artifact>", clean)
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryArtifact, ArtifactID: clean[2]}, asJSON, nil
	case "quarantine":
		if len(clean) != 1 {
			return backlogadmin.Query{}, false, errors.New("backlog quarantine takes no arguments")
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryQuarantine}, asJSON, nil
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
			return backlogadmin.Query{}, false, showOnlyUsage("command", "<command>", clean)
		}
		return backlogadmin.Query{Kind: backlogadmin.QueryCommands, CommandID: clean[2]}, asJSON, nil
	default:
		return backlogadmin.Query{}, false, fmt.Errorf("unknown backlog admin command %q", clean[0])
	}
}

// showOnlyUsage is the refusal of a verb that exists only in its show form.
// When the caller wrote the identifier without the show word, which is the
// natural mistake, the refusal spells out the command they meant.
func showOnlyUsage(verb, placeholder string, clean []string) error {
	usage := fmt.Sprintf("usage: backlog %s show %s", verb, placeholder)
	if len(clean) == 2 && clean[1] != "show" {
		return fmt.Errorf("%s; did you mean: backlog %s show %s?", usage, verb, clean[1])
	}
	return errors.New(usage)
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
					return filter, fmt.Errorf("invalid progress %q; valid values: %s", item, strings.Join(progressFilterValues(), ", "))
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

// progressFilterValues lists the progress states --progress accepts, in the
// order a task moves through them. The refusal of an unknown value prints
// this list, so it is the one place the accepted set is spelled.
func progressFilterValues() []string {
	states := []domain.ProgressState{
		domain.ProgressQueued, domain.ProgressBlocked, domain.ProgressReady, domain.ProgressActive,
		domain.ProgressNeedsInput, domain.ProgressWaitingExternal, domain.ProgressVerifying,
		domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped,
	}
	values := make([]string, 0, len(states))
	for _, state := range states {
		values = append(values, string(state))
	}
	return values
}

func validProgress(value string) bool {
	for _, valid := range progressFilterValues() {
		if value == valid {
			return true
		}
	}
	return false
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

func renderAdminResponse(out io.Writer, response backlogadmin.Response, selector string, display commandDisplay) error {
	switch response.Kind {
	case backlogadmin.QueryDiagnose:
		renderDiagnosis(out, response.Diagnosis)
	case backlogadmin.QueryWorkers:
		return renderWorkers(out, response.Workers)
	case backlogadmin.QueryProjects:
		return renderProjects(out, response.Projects, display.Verbose)
	case backlogadmin.QueryStatus:
		renderStatus(out, response.Status)
	case backlogadmin.QueryWorkflows:
		renderWorkflows(out, response.Workflows)
	case backlogadmin.QueryWorkflow:
		renderWorkflow(out, response.Workflow)
	case backlogadmin.QueryGraph:
		renderGraph(out, response.Graph)
	case backlogadmin.QueryTask:
		renderTask(out, response.Task, response.GeneratedAt)
	case backlogadmin.QueryExplanation:
		renderExplanation(out, response.Explanation)
	case backlogadmin.QueryEvents:
		renderEvents(out, response.Events)
	case backlogadmin.QueryUsage:
		return renderUsage(out, response.UsageReport, response.UsageSemantics)
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
	case backlogadmin.QueryQuarantine:
		renderQuarantine(out, response.Quarantine)
	default:
		return fmt.Errorf("no human renderer for admin response %q", response.Kind)
	}
	return nil
}

func renderUsage(out io.Writer, report *domain.UsageReport, semantics string) error {
	if report == nil {
		_, err := fmt.Fprintln(out, "usage report unavailable")
		return err
	}
	coverage := report.Coverage
	fmt.Fprintf(out, "Run: %s (%s)\n", report.WorkflowRunID, report.RunProgress)
	fmt.Fprintf(out, "Accepted outcomes: %d\n", report.AcceptedOutcomeCount)
	if report.MeasuredCostPerAcceptedOutcomeUSD != nil {
		fmt.Fprintf(out, "Measured provider cost per accepted outcome: %.6f\n", *report.MeasuredCostPerAcceptedOutcomeUSD)
	} else {
		fmt.Fprintln(out, "Measured provider cost per accepted outcome: unavailable (requires complete provider cost coverage)")
	}
	fmt.Fprintln(out, "Token totals are measured execution evidence, not subscription quota savings.")
	fmt.Fprintf(out, "Coverage: %s; raw=%d normalized=%d expected-sessions=%d missing-logs=%d overlap-excluded=%d overlap-ambiguous=%d diagnostics=%d dropped-diagnostics=%d unattributed=%d\n",
		coverage.State, coverage.RawSampleCount, coverage.NormalizedSampleCount,
		coverage.ExpectedSessionCount, coverage.MissingLogSessionCount,
		coverage.ExcludedOverlapCount, coverage.AmbiguousOverlapCount, coverage.DiagnosticCount,
		coverage.DiagnosticDroppedCount, coverage.UnattributedCount)
	if coverage.UnscopedUnattributedCount > 0 {
		// Context, not coverage: the fleet's unbound samples in the run's
		// window, of which only the unattributed count above could be the run's.
		fmt.Fprintf(out, "Fleet samples in this window without a dispatch binding: %d (not this run's unless counted as unattributed above)\n",
			coverage.UnscopedUnattributedCount)
	}
	if coverage.ObservedFrom != nil && coverage.ObservedThrough != nil {
		fmt.Fprintf(out, "Observed: %s through %s\n",
			coverage.ObservedFrom.UTC().Format(time.RFC3339), coverage.ObservedThrough.UTC().Format(time.RFC3339))
	}
	for _, reason := range coverage.Reasons {
		fmt.Fprintf(out, "Coverage reason: %s\n", reason)
	}
	if semantics != "" {
		fmt.Fprintf(out, "Semantics: %s\n", semantics)
	}
	fmt.Fprintln(out, "Totals:")
	if err := renderUsageAggregates(out, []domain.UsageAggregate{{Key: "workflow", Totals: report.Totals}}); err != nil {
		return err
	}
	for _, section := range []struct {
		name string
		rows []domain.UsageAggregate
	}{
		{"By task", report.ByTask}, {"By attempt", report.ByAttempt},
		{"By role", report.ByRole}, {"By model", report.ByModel},
	} {
		if len(section.rows) == 0 {
			continue
		}
		fmt.Fprintf(out, "%s:\n", section.name)
		if err := renderUsageAggregates(out, section.rows); err != nil {
			return err
		}
	}
	if len(report.Samples) > 0 {
		fmt.Fprintln(out, "Raw detail:")
		table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "EVENT\tPROVIDER\tTHREAD\tTASK\tATTEMPT\tROLE\tKIND\tMODEL")
		for _, sample := range report.Samples {
			a := sample.Attribution
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				sample.SourceEventID, sample.ProviderInstanceID, sample.ThreadID,
				a.TaskID, a.AttemptID, a.Role, sample.Kind, sample.Model)
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if report.NextCursor != "" {
		fmt.Fprintf(out, "Next cursor: %s\n", report.NextCursor)
	}
	return nil
}

func renderUsageAggregates(out io.Writer, rows []domain.UsageAggregate) error {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "KEY\tOUTCOME\tUNCACHED INPUT\tCACHE WRITE\tCACHE READ\tOUTPUT\tPROVIDER COST\tSAMPLES\tCALLS\tTURNS")
	for _, row := range rows {
		cost := string(row.Totals.ProviderCostCoverage)
		if row.Totals.ProviderCostReported {
			cost = strconv.FormatFloat(row.Totals.ProviderCostUSD, 'f', 6, 64) + " (" + cost + ")"
		}
		fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\n",
			row.Key, row.Outcome, row.Totals.UncachedInputTokens, row.Totals.CacheWriteTokens,
			row.Totals.CacheReadTokens, row.Totals.OutputTokens, cost,
			row.Totals.NormalizedSamples, row.Totals.Calls, row.Totals.Turns)
	}
	return table.Flush()
}

// renderWorkers prints the worker table and, under it, what each worker can
// actually take: the projects it advertises and the instance/model/pool routes
// it offers. Those two facts decide where work can run, and the text form used
// to drop them, so an operator had to read the JSON to answer "which worker
// could run this".
func renderWorkers(out io.Writer, workers []backlogadmin.Worker) error {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "WORKER\tSTATE\tHEALTH\tENROLLED\tSNAPSHOT AGE (s)\tCATALOG")
	for _, worker := range workers {
		fmt.Fprintf(table, "%s\t%s\t%s\t%t\t%.1f\t%s\n", worker.Snapshot.WorkerID, worker.State,
			worker.Health, worker.Enrolled, worker.SnapshotAgeSeconds, worker.Snapshot.Inventory.CatalogRevision)
	}
	if err := table.Flush(); err != nil {
		return err
	}
	for _, worker := range workers {
		inventory := worker.Snapshot.Inventory
		if len(inventory.Projects) == 0 && len(inventory.Providers) == 0 {
			continue
		}
		fmt.Fprintf(out, "\n%s offers:\n", worker.Snapshot.WorkerID)
		inner := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(inner, "  PROJECTS\tROUTES (instance/model@pool)")
		projects := make([]string, 0, len(inventory.Projects))
		for _, project := range inventory.Projects {
			label := project.Name
			if !project.Available {
				label += " (unavailable)"
			}
			projects = append(projects, label)
		}
		routes := make([]string, 0, len(inventory.Providers))
		for _, provider := range inventory.Providers {
			for _, model := range provider.Models {
				label := provider.InstanceID + "/" + model
				if provider.QuotaPoolID != "" {
					label += "@" + provider.QuotaPoolID
				}
				if !provider.Available {
					label += " (unavailable)"
				}
				routes = append(routes, label)
			}
		}
		sort.Strings(projects)
		sort.Strings(routes)
		fmt.Fprintf(inner, "  %s\t%s\n", campaignList(projects), campaignList(routes))
		if err := inner.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// renderProjects prints the catalog: one row per project and, when the answer
// is about one project or the caller asked for the whole thing, one table per
// project of the workers that could take its work with the routes each one
// advertises. It is the text form of what "run" derives a project and a route
// from, so an operator can see the same facts an agent acts on.
//
// The detail is what made this verb 28 KB of text and 90 KB of JSON on a fleet
// of thirteen projects: every eligible worker of every project with all of its
// advertised routes spelled out, in answer to "which projects are there". The
// summary states the counts and says how to get the rest, and the scope is
// applied first, by the coordinator: --project NAME is answered with one
// project, so the detailed form is the whole of a small answer rather than a
// window onto a large one.
func renderProjects(out io.Writer, projects []backlogadmin.Project, verbose bool) error {
	if len(projects) == 0 {
		_, err := fmt.Fprintln(out, "no project matched; this coordinator's catalog is backlog_v2.projects")
		return err
	}
	// One project in the answer is already the size of one project, whether the
	// caller scoped it with --project or this coordinator holds one.
	detailed := verbose || len(projects) == 1
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if detailed {
		fmt.Fprintln(table, "PROJECT\tREPOSITORY\tDEFAULT REF\tTYPE\tSETUP PROFILE\tELIGIBLE WORKERS")
	} else {
		fmt.Fprintln(table, "PROJECT\tREPOSITORY\tDEFAULT REF\tTYPE\tSETUP PROFILE\tWORKERS\tROUTES")
	}
	routes := 0
	for _, project := range projects {
		if !detailed {
			count := len(projectRouteLabels(project))
			routes += count
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n", project.Name, project.Repository, project.DefaultRef,
				firstNonEmptyText(project.Type, "git"), project.SetupProfile, len(project.Workers), count)
			continue
		}
		names := make([]string, 0, len(project.Workers))
		for _, worker := range project.Workers {
			names = append(names, worker.Worker)
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", project.Name, project.Repository, project.DefaultRef,
			firstNonEmptyText(project.Type, "git"), project.SetupProfile, campaignList(names))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if !detailed {
		fmt.Fprintf(out, "\nsummarised: %d projects, %d eligible worker rows and %d advertised routes are counted above and not spelled out.\n",
			len(projects), projectWorkerRows(projects), routes)
		_, err := fmt.Fprintln(out, projectsDetailAdvice)
		return err
	}
	for _, project := range projects {
		if len(project.Workers) == 0 {
			continue
		}
		fmt.Fprintf(out, "\n%s workers:\n", project.Name)
		workers := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(workers, "  WORKER\tSTATE\tCONFIGURED\tADVERTISES\tENROLLED\tREADY\tROUTES (instance/model@pool)")
		for _, worker := range project.Workers {
			fmt.Fprintf(workers, "  %s\t%s\t%t\t%t\t%t\t%t\t%s\n", worker.Worker, worker.State,
				worker.Configured, worker.Advertises, worker.Enrolled, worker.Ready, campaignList(workerRouteLabels(worker)))
		}
		if err := workers.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// workerRouteLabels is one eligible worker's advertised routes in the
// instance/model@pool form "task run --model" takes.
func workerRouteLabels(worker backlogadmin.ProjectWorker) []string {
	labels := make([]string, 0, len(worker.Routes))
	for _, route := range worker.Routes {
		label := route.Instance + "/" + route.Model
		if route.QuotaPool != "" {
			label += "@" + route.QuotaPool
		}
		labels = append(labels, label)
	}
	return labels
}

// projectRouteLabels is the distinct routes a project's eligible workers
// advertise between them. The summary counts these rather than the rows,
// because two workers offering the same route are one route to choose.
func projectRouteLabels(project backlogadmin.Project) []string {
	seen := map[string]bool{}
	var labels []string
	for _, worker := range project.Workers {
		for _, label := range workerRouteLabels(worker) {
			if seen[label] {
				continue
			}
			seen[label] = true
			labels = append(labels, label)
		}
	}
	sort.Strings(labels)
	return labels
}

// projectsDetailAdvice is the one sentence both summaries end with: the two
// commands that answer with the workers and the routes themselves. --json
// carries it in the document so that a reader of either form is told the same
// thing, and --verbose is named in it because --json alone no longer spells
// the catalog out.
const projectsDetailAdvice = "For one project's workers and routes: t3-steward backlog projects --project NAME. For every project's: --verbose, which --json honours too."

// projectWorkerRows is how many project-and-worker rows the detailed form
// would print, which is what the summary says it is not printing.
func projectWorkerRows(projects []backlogadmin.Project) int {
	rows := 0
	for _, project := range projects {
		rows += len(project.Workers)
	}
	return rows
}

// projectsSummaryDocument is what "backlog projects --json" answers with when
// it was not asked for the detail: the envelope of the whole document, one
// entry per project carrying its counts in place of its workers, and the
// totals those counts add up to.
//
// The whole document is 90 KB on a thirteen-project fleet for the reason the
// text form was 28 KB -- every eligible worker of every project with all of
// its advertised routes spelled out -- and an agent asking "which projects are
// there" reads all of it. So --json honours --verbose the way the text form
// does, and the two forms of the verb summarise under one rule: --verbose
// prints the whole document, and an answer that holds one project, named with
// --project or held alone by this coordinator, is printed in full.
//
// Nothing machine-parses this document. "task run" issues the projects query
// in process and consumes the []backlogadmin.Project it answers with, never
// this command's output, so the summary needs no schema version and no
// migration; version is on the document for a reader that wants one.
type projectsSummaryDocument struct {
	Version     string                 `json:"version"`
	Kind        backlogadmin.QueryKind `json:"kind"`
	GeneratedAt time.Time              `json:"generatedAt"`
	// Summarised says what this document is, so that a reader never has to know
	// which flags were in force to tell a project with no eligible workers from
	// a project whose workers this answer did not spell out.
	Summarised bool `json:"summarised"`
	// The totals are of the answer this document summarises, which is already
	// the scoped answer: the coordinator applied the filter before anything
	// here was counted.
	TotalProjects   int              `json:"totalProjects"`
	TotalWorkerRows int              `json:"totalWorkerRows"`
	TotalRoutes     int              `json:"totalRoutes"`
	Projects        []projectSummary `json:"projects"`
	// Detail names the two ways to the workers and the routes, in the same
	// words the text summary ends with.
	Detail string `json:"detail"`
}

// projectSummary is one project with the count of its eligible workers and of
// the distinct routes they advertise, in place of the workers and routes.
type projectSummary struct {
	Name         string `json:"name"`
	Repository   string `json:"repository,omitempty"`
	DefaultRef   string `json:"defaultRef,omitempty"`
	Type         string `json:"type,omitempty"`
	SetupProfile string `json:"setupProfile,omitempty"`
	WorkerCount  int    `json:"workerCount"`
	RouteCount   int    `json:"routeCount"`
}

// summariseProjects is the document --json prints in place of the catalog, and
// whether it applies at all. The rule is renderProjects' rule, expressed once
// here for the encoder, so that the text form and the JSON form of one command
// can never disagree about what counts as a summary.
func summariseProjects(response backlogadmin.Response, verbose bool) (projectsSummaryDocument, bool) {
	if response.Kind != backlogadmin.QueryProjects || verbose || len(response.Projects) <= 1 {
		return projectsSummaryDocument{}, false
	}
	document := projectsSummaryDocument{
		Version: response.Version, Kind: response.Kind, GeneratedAt: response.GeneratedAt,
		Summarised: true, TotalProjects: len(response.Projects),
		TotalWorkerRows: projectWorkerRows(response.Projects),
		Detail:          projectsDetailAdvice,
	}
	for _, project := range response.Projects {
		routes := len(projectRouteLabels(project))
		document.TotalRoutes += routes
		document.Projects = append(document.Projects, projectSummary{
			Name: project.Name, Repository: project.Repository, DefaultRef: project.DefaultRef,
			Type: firstNonEmptyText(project.Type, "git"), SetupProfile: project.SetupProfile,
			WorkerCount: len(project.Workers), RouteCount: routes,
		})
	}
	return document, true
}

// firstNonEmptyText returns value, or fallback when value is empty.
func firstNonEmptyText(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// renderQuarantine prints the intake the coordinator refuses and is silent
// about. The reason is printed in full on its own line rather than squeezed
// into a column, because it is the whole point of the view, and the retry rule
// is stated every time so that an operator never has to guess whether editing
// the file is enough.
func renderQuarantine(out io.Writer, quarantined []backlogadmin.QuarantinedIntake) {
	if len(quarantined) == 0 {
		fmt.Fprintln(out, "no quarantined intake: every submission source is being read.")
		return
	}
	for index, entry := range quarantined {
		if index > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "key:        %s\n", entry.Key)
		fmt.Fprintf(out, "record:     %s\n", entry.RecordKey)
		fmt.Fprintf(out, "digest:     %s\n", entry.Digest)
		fmt.Fprintf(out, "quarantined %s\n", entry.QuarantinedAt.UTC().Format(time.RFC3339))
		fmt.Fprintf(out, "reason:     %s\n", entry.Reason)
		fmt.Fprintf(out, "retry:      %s\n", entry.Retry)
	}
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

// workflowTaskWaits is the task-bound waits of a run document, from whichever
// key the coordinator that answered uses. This client reads taskWaits; a
// coordinator of the previous release sends only the deprecated waits key, and
// a mixed-version window is the normal state during a release, so without the
// fallback "campaign show" against an older coordinator would print a parked
// task with nothing to say about what it is parked on. Only
// adoptDeprecatedWaitKeys calls this, so that the text and the JSON forms
// cannot disagree; the fallback goes away with the deprecated key.
func workflowTaskWaits(detail *backlogadmin.WorkflowDetail) []backlogadmin.TaskWaitDetail {
	if len(detail.TaskWaits) != 0 {
		return detail.TaskWaits
	}
	return detail.Waits
}

func renderWorkflow(out io.Writer, detail *backlogadmin.WorkflowDetail) {
	if detail == nil {
		return
	}
	summary := detail.Summary
	fmt.Fprintf(out, "run: %s\nworkflow: %s (%s)\nproject: %s\nclass: %s\nprogress: %s\nrevision: %d\n",
		summary.Run.ID, summary.Workflow.Name, summary.Workflow.ID, summary.Workflow.Project,
		summary.Workflow.Class, summary.Run.Progress, summary.Run.Revision)
	taskNames := make(map[string]string, len(detail.Tasks))
	for _, task := range detail.Tasks {
		taskNames[task.Task.ID] = task.Task.Name
	}
	fmt.Fprintln(out, "tasks:")
	for _, task := range detail.Tasks {
		if task.Sink != nil {
			fmt.Fprintf(out, "  %s (%s): %s (coordinator sink)\n", task.Task.Name, task.Task.ID, task.Sink.Progress)
			continue
		}
		state, control, attempt := taskState(task)
		fmt.Fprintf(out, "  %s (%s): %s %s attempt=%s%s\n", task.Task.Name, task.Task.ID, state, control, attempt, evidenceMarker(task.Evidence))
		// A parked task says what it is parked on. The wait is the reason the
		// task is not moving, and its condition is what an operator can go and
		// satisfy or cancel.
		// A settled wait is listed too, with its outcome: it is what became
		// of the wait the previous answer reported as live.
		for _, wait := range detail.TaskWaits {
			if wait.TaskID != task.Task.ID {
				continue
			}
			if wait.Outcome == "" && wait.SettledAt == nil {
				fmt.Fprintf(out, "    wait %s %q: %s (deadline %s)\n", wait.ID, wait.Name, wait.Condition, formatTime(wait.Deadline))
				continue
			}
			fmt.Fprintf(out, "    wait %s %q: settled %s exit=%d at %s%s\n", wait.ID, wait.Name,
				wait.Outcome, wait.ExitCode, formatTime(settledWaitTime(wait)), waitReasonSuffix(wait.Reason))
		}
	}
	if len(detail.Gates) != 0 {
		fmt.Fprintln(out, "gates:")
		for _, gate := range detail.Gates {
			fmt.Fprintf(out, "  %s (%s): %s", gate.Name, gate.ID, gate.State)
			if gate.Final {
				fmt.Fprint(out, "; protects run settlement")
			} else if len(gate.ProtectedTaskIDs) != 0 {
				fmt.Fprintf(out, "; protects %s", strings.Join(taskLabels(gate.ProtectedTaskIDs, taskNames), ", "))
			}
			// A pending gate names the observed tasks that have not produced
			// evidence yet, with their progress, so "pending-evidence" says
			// which task the reviewer is waiting for.
			if len(gate.MissingEvidence) != 0 {
				gaps := make([]string, 0, len(gate.MissingEvidence))
				for _, gap := range gate.MissingEvidence {
					label := gap.TaskName
					if label == "" {
						label = gap.TaskID
					}
					gaps = append(gaps, fmt.Sprintf("%s (%s)", label, gap.Progress))
				}
				fmt.Fprintf(out, "; missing evidence: %s", strings.Join(gaps, ", "))
			}
			fmt.Fprintln(out)
		}
	}
	fmt.Fprintf(out, "artifacts: %d\nreservations: %d\nlocks: %d\n",
		len(detail.Artifacts), len(detail.Reservations), len(detail.ResourceLocks))
}

// taskLabels renders task ids by name where the run knows the name.
func taskLabels(ids []string, names map[string]string) []string {
	labels := make([]string, 0, len(ids))
	for _, id := range ids {
		if name := names[id]; name != "" {
			labels = append(labels, name)
			continue
		}
		labels = append(labels, id)
	}
	return labels
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

// renderTask prints one task: what it is, what state the coordinator holds it
// in, the attempt's own timeline, and what the worker last reported.
//
// now is when the answer was generated, which is what the elapsed and lease
// lines are measured against. It is a parameter rather than time.Now() so that
// the text and the JSON of one answer describe the same instant.
func renderTask(out io.Writer, detail *backlogadmin.TaskDetail, now time.Time) {
	if detail == nil {
		return
	}
	if detail.Sink != nil {
		fmt.Fprintf(out, "task: %s (%s)\nkind: coordinator sink\nprogress: %s\ngraph revision: %d\n", detail.Task.Name, detail.Task.ID, detail.Sink.Progress, detail.Sink.GraphRevision)
		if detail.Sink.Result != nil {
			fmt.Fprintf(out, "failed task IDs: %s\ncancelled task IDs: %s\nskipped task IDs: %s\n", strings.Join(detail.Sink.Result.FailedTaskIDs, ", "), strings.Join(detail.Sink.Result.CancelledTaskIDs, ", "), strings.Join(detail.Sink.Result.SkippedTaskIDs, ", "))
		}
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
	renderAttemptTimeline(out, "", detail, now)
	renderAttemptEvidence(out, detail)
	fmt.Fprintf(out, "artifacts: %d\nlocks: %s\n", len(detail.Artifacts), strings.Join(detail.ResourceLocks, ", "))
}

// renderAttemptEvidence prints what the worker last reported about the
// attempt: the thread, the worker, the observed session and any quota pause.
// Lines the assignment already printed (worker, thread URL) are not repeated.
func renderAttemptEvidence(out io.Writer, detail *backlogadmin.TaskDetail) {
	evidence := detail.Evidence
	if evidence == nil {
		return
	}
	if evidence.ThreadID != "" && detail.ThreadURL == "" {
		fmt.Fprintf(out, "thread: %s\n", evidence.ThreadID)
	}
	if evidence.WorkerID != "" && detail.Assignment == nil {
		fmt.Fprintf(out, "worker: %s\n", evidence.WorkerID)
	}
	if line, ok := providerSessionLine(detail); ok {
		fmt.Fprintln(out, line)
	}
	if evidence.PauseReason != "" {
		fmt.Fprintf(out, "paused: %s\n", evidence.PauseReason)
	}
	if evidence.Failure != "" && (detail.Attempt == nil || detail.Attempt.Failure != evidence.Failure) {
		fmt.Fprintf(out, "worker failure: %s\n", evidence.Failure)
	}
}

// renderAttemptTimeline prints the attempt's own clock: when the work started,
// how long it has been going, and, while it is not finished, when the lease
// that holds it expires. "It started an hour ago, why is it not finished" was
// answerable only from --json before this, because the text path printed the
// worker's observation time and no time of the attempt's own.
//
// now is when the answer was generated. A zero now means the caller had none,
// and the elapsed line then measures to the attempt's last update and says so.
// A timestamp missing from the record is printed as unknown, naming the field
// that was absent, because a zero time rendered as a date is worse than a gap.
//
// An attempt that has not started has no timeline and prints none, which is
// what attemptStarted decides for both callers: those unknown lines are for a
// record that lost a timestamp, not for work that has not begun.
func renderAttemptTimeline(out io.Writer, indent string, detail *backlogadmin.TaskDetail, now time.Time) {
	attempt := detail.Attempt
	if !attemptStarted(detail) {
		return
	}
	var started time.Time
	if detail.Assignment != nil {
		started = detail.Assignment.CreatedAt
	}
	switch {
	case !started.IsZero():
		fmt.Fprintf(out, "%sstarted: %s\n", indent, formatTime(started))
	case detail.Assignment == nil:
		fmt.Fprintf(out, "%sstarted: unknown (no assignment record, which is where assignment.createdAt lives)\n", indent)
	default:
		fmt.Fprintf(out, "%sstarted: unknown (assignment.createdAt is absent from the record)\n", indent)
	}
	terminal := attempt.Progress.Terminal()
	reference, measured := now, "as of the coordinator's answer"
	if terminal || reference.IsZero() {
		reference, measured = attempt.UpdatedAt, "to the attempt's last update"
	}
	switch {
	case started.IsZero():
		fmt.Fprintf(out, "%selapsed: unknown (no start time to measure from)\n", indent)
	case reference.IsZero():
		fmt.Fprintf(out, "%selapsed: unknown (attempt.updatedAt is absent from the record)\n", indent)
	default:
		fmt.Fprintf(out, "%selapsed: %s (%s %s)\n", indent, reference.Sub(started).Round(time.Second), measured, formatTime(reference))
	}
	if terminal {
		// A finished attempt holds no lease, so it claims none: this line is
		// about work something is still holding.
		return
	}
	switch {
	case detail.Assignment == nil:
		fmt.Fprintf(out, "%slease: none held (no assignment record; nothing holds this attempt)\n", indent)
	case detail.Assignment.LeaseExpiresAt.IsZero():
		fmt.Fprintf(out, "%slease: unknown (assignment.leaseExpiresAt is absent from the record)\n", indent)
	case reference.IsZero():
		fmt.Fprintf(out, "%slease: expires %s\n", indent, formatTime(detail.Assignment.LeaseExpiresAt))
	default:
		fmt.Fprintf(out, "%slease: expires %s (%s)\n", indent, formatTime(detail.Assignment.LeaseExpiresAt),
			leaseRemaining(detail.Assignment.LeaseExpiresAt, reference))
	}
}

// attemptStarted reports whether the attempt has begun, which is what decides
// whether there is a timeline to print at all.
//
// An attempt exists from the moment the run is planned and acquires its
// assignment at dispatch, so a queued, unassigned attempt has no clock of its
// own yet: it has a start time neither the coordinator nor anyone else knows,
// because there is none. Printing "started: unknown (no assignment record)"
// for it says a record is damaged when the record is intact, and it says it
// three times per task in the state diagnose is most often run in -- a freshly
// submitted campaign, a run held behind quota, a fleet with no eligible
// worker.
//
// An attempt whose assignment is present, or whose control state says a worker
// is acting on it, has started. That second half is what keeps the degraded
// case honest: a running attempt whose assignment record is missing still
// prints the timeline, with the absent field named, because there the record
// really has lost something.
func attemptStarted(detail *backlogadmin.TaskDetail) bool {
	attempt := detail.Attempt
	if attempt == nil {
		return false
	}
	if detail.Assignment != nil {
		return true
	}
	switch attempt.Control {
	case "", domain.ControlUnassigned:
		return false
	default:
		return true
	}
}

// leaseRemaining says how much of the lease was left at the reference time, or
// how long ago it lapsed, which is the difference between "a worker is holding
// this" and "the coordinator is about to recover it".
func leaseRemaining(expires, reference time.Time) string {
	if left := expires.Sub(reference); left >= 0 {
		return left.Round(time.Second).String() + " left"
	}
	return "expired " + reference.Sub(expires).Round(time.Second).String() + " ago"
}

// providerSessionLine is the worker's last observation of the provider session
// -- the T3 thread, its control state and the journal phase -- worded so that
// it cannot be read as the task's own state.
//
// The words are the provider's and mean "the provider thread was not
// generating at that instant". Concatenated bare they produced "session:
// thread stopped, control stopped, phase completed" four lines under the same
// attempt's "progress: active" and "control: running", and an agent diagnosing
// a slow task reads that as "the work stopped". So the line names the provider
// session, and when the words would contradict an attempt the coordinator
// holds as running it says what they mean and what the attempt's state is.
func providerSessionLine(detail *backlogadmin.TaskDetail) (string, bool) {
	evidence := detail.Evidence
	if evidence == nil {
		return "", false
	}
	var words []string
	if evidence.ThreadState != "" {
		words = append(words, "thread "+evidence.ThreadState)
	}
	if evidence.Control != "" {
		words = append(words, "control "+string(evidence.Control))
	}
	if evidence.Phase != "" {
		words = append(words, "phase "+evidence.Phase)
	}
	if !evidence.ObservedAt.IsZero() {
		words = append(words, "observed "+formatTime(evidence.ObservedAt))
	}
	if len(words) == 0 {
		return "", false
	}
	if !providerSessionContradictsAttempt(detail) {
		return "provider session: " + strings.Join(words, ", "), true
	}
	return fmt.Sprintf("provider session: idle when the worker last looked (%s); the attempt is progress %s, control %s, so that is the provider thread between turns and not the task stopping",
		strings.Join(words, ", "), detail.Attempt.Progress, detail.Attempt.Control), true
}

// providerSessionContradictsAttempt reports whether the provider's words say
// the session stopped or finished while the coordinator holds the attempt as
// executing. The attempt's state is the coordinator's and decides whether the
// work is over; the session's is one worker's observation of one thread.
func providerSessionContradictsAttempt(detail *backlogadmin.TaskDetail) bool {
	attempt, evidence := detail.Attempt, detail.Evidence
	if attempt == nil || evidence == nil || attempt.Progress.Terminal() {
		return false
	}
	switch attempt.Control {
	case domain.ControlPreparing, domain.ControlRunning, domain.ControlResuming:
	default:
		return false
	}
	return evidence.ThreadState == "stopped" || evidence.ThreadState == "missing" ||
		evidence.Control == domain.ControlStopped || evidence.Phase == "completed"
}

// evidenceMarker is the one word "campaign show" appends to a task line when
// the worker's evidence says the attempt is paused on a quota bucket or parked
// on a task-bound wait. It is empty for anything else.
func evidenceMarker(evidence *backlogadmin.AttemptEvidence) string {
	if evidence == nil {
		return ""
	}
	switch {
	case evidence.PauseReason != "", evidence.Control == domain.ControlPaused, evidence.Control == domain.ControlPausedUncheckpointed:
		return " paused"
	case evidence.Control == domain.ControlWaitingExternal:
		return " parked"
	}
	return ""
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
	for _, detail := range explanation.Details {
		fmt.Fprintf(out, "  detail: %s\n", detail)
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

// diagnosisNodeWaits is the interactive node waits of a diagnosis, from
// whichever key the coordinator that answered uses. In a diagnosis the
// deprecated waits key is the node waits, and a coordinator of the previous
// release sends only that one; taskWaits is not renamed here, so it needs no
// fallback. Only adoptDeprecatedWaitKeys calls this, so that the text and the
// JSON forms cannot disagree; the fallback goes away with the deprecated key.
func diagnosisNodeWaits(diagnosis *backlogadmin.Diagnosis) []domain.NodeWait {
	if len(diagnosis.NodeWaits) != 0 {
		return diagnosis.NodeWaits
	}
	return diagnosis.Waits
}

// renderDiagnosis prints the short form of "diagnose": the run and its
// revision, one line per task, the live task-bound waits, the node waits, the
// workers holding this run's assignments and what the coordinator could not
// read. --json carries the whole document.
func renderDiagnosis(out io.Writer, diagnosis *backlogadmin.Diagnosis) {
	if diagnosis == nil {
		return
	}
	summary := diagnosis.Workflow.Summary
	fmt.Fprintf(out, "run: %s\nworkflow: %s (%s)\nprogress: %s\nrevision: %d\ngenerated: %s\n",
		summary.Run.ID, summary.Workflow.Name, summary.Workflow.ID, summary.Run.Progress,
		diagnosis.GraphRevision, formatTime(diagnosis.GeneratedAt))
	fmt.Fprintln(out, "tasks:")
	for _, task := range diagnosis.Workflow.Tasks {
		if task.Sink != nil {
			fmt.Fprintf(out, "  %s (%s): %s (coordinator sink)\n", task.Task.Name, task.Task.ID, task.Sink.Progress)
			continue
		}
		state, control, attempt := taskState(task)
		fmt.Fprintf(out, "  %s (%s): %s %s attempt=%s%s\n", task.Task.Name, task.Task.ID, state, control, attempt, evidenceMarker(task.Evidence))
		// The timeline is printed for the attempts that are still going,
		// because "why is this one not finished" is the question diagnose is
		// run to answer and a finished attempt has already answered it. An
		// attempt that has not started prints none either: renderAttemptTimeline
		// applies attemptStarted for this caller and for backlog task show.
		if detail := task; detail.Attempt != nil && !detail.Attempt.Progress.Terminal() {
			renderAttemptTimeline(out, "    ", &detail, diagnosis.GeneratedAt)
		}
	}
	live := 0
	for _, wait := range diagnosis.TaskWaits {
		if wait.SettledAt != nil {
			continue
		}
		if live == 0 {
			fmt.Fprintln(out, "task waits:")
		}
		live++
		fmt.Fprintf(out, "  %s task=%s attempt=%s %q: %s (deadline %s)\n", wait.ID, wait.TaskID, wait.AttemptID, wait.Name, wait.Condition, formatTime(wait.Deadline))
	}
	if nodeWaits := diagnosis.NodeWaits; len(nodeWaits) != 0 {
		fmt.Fprintln(out, "node waits:")
		for _, wait := range nodeWaits {
			fmt.Fprintf(out, "  %s %q thread=%s host=%s delivery=%s (deadline %s)\n", wait.Request.ID, wait.Request.Name, wait.Request.ThreadID, wait.Host, wait.Delivery, formatTime(wait.Deadline))
		}
	}
	if len(diagnosis.Workers) != 0 {
		fmt.Fprintln(out, "workers:")
		for _, worker := range diagnosis.Workers {
			fmt.Fprintf(out, "  %s epoch=%s sequence=%d observed=%s\n", worker.WorkerID, worker.WorkerEpoch, worker.Sequence, formatTime(worker.ObservedAt))
			for _, assignment := range worker.Assignments {
				fmt.Fprintf(out, "    %s %s %s", assignment.AssignmentID, assignment.State, assignment.Control)
				if assignment.ThreadID != "" {
					fmt.Fprintf(out, " thread=%s", assignment.ThreadID)
				}
				if assignment.Journal != nil && assignment.Journal.PauseReason != "" {
					fmt.Fprintf(out, " paused=%q", assignment.Journal.PauseReason)
				}
				fmt.Fprintln(out)
			}
		}
	}
	if len(diagnosis.Unavailable) != 0 {
		fmt.Fprintln(out, "unavailable:")
		for _, item := range diagnosis.Unavailable {
			fmt.Fprintf(out, "  %s\n", item)
		}
	}
}

// settledWaitTime is the settlement time, or the zero time formatTime renders
// as "-" when the coordinator recorded an outcome without one.
func settledWaitTime(wait backlogadmin.TaskWaitDetail) time.Time {
	if wait.SettledAt == nil {
		return time.Time{}
	}
	return *wait.SettledAt
}

// waitReasonSuffix appends the settlement reason when there is one.
func waitReasonSuffix(reason string) string {
	if reason == "" {
		return ""
	}
	return ": " + reason
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}
