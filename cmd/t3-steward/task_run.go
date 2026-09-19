package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

const taskRunUsage = `Usage: t3-steward task run [flags] -- "<prompt>"

Start one task on the fleet from this checkout and be woken when it ends. It
composes the campaign path: the CLI derives, the coordinator validates, and the
coordinator never chooses a route.

Derived, each printed in the record:
  project  --project, else this checkout's origin remote matched against the
           coordinator's projects; exactly one match is required
  ref      --ref, else the current branch when it has an upstream and is not
           ahead of it; a detached HEAD or an unpushed branch is refused with
           "push first or pass --ref"; a dirty tree is a warning, because
           uncommitted changes are not sent
  route    --model INSTANCE/MODEL exactly, or --model MODEL when exactly one
           instance the project's eligible workers advertise offers it, else
           defaults.model from the client configuration; the quota pool is the
           one the instance advertises and is never invented; --worker pins the
           worker
  key      --idempotency-key, else run- plus sixteen hex characters of a digest
           over the derived inputs and the prompt; a repeat replays the same
           run and prints replayed: true. --worker and --name are not in it,
           and do change what is submitted, so a start that differs only in
           those is refused as the same key with different content
  name     --name, else the prompt's first line, as a manifest-legal slug

Flags:
  --project NAME        --ref REF              --fresh
  --model [INSTANCE/]MODEL                     --worker WORKER
  --name TEXT           --idempotency-key KEY
  --outputs a.md,b.md   --verify "CMD" (repeatable)
  --class surplus|required (default surplus)   --max-turns N (default 3)
  --prompt-file FILE    --fan-out GLOB         --json
  --notify-thread current|THREAD-ID (default current)        --no-notify
  --config PATH         the configuration file to read (default
                        $XDG_CONFIG_HOME/t3-steward/config.yaml); the dispatcher
                        takes it out of the arguments before the task family is
                        entered, so every task verb accepts it

The prompt is exactly one of: an inline argument after --, --prompt-file FILE,
--prompt-file - or stdin. --fan-out GLOB starts one run with one task per file,
named by the file stem, sharing every other derived value.

The calling thread is notified by default and the command is refused when no
thread resolves, so a run nobody will hear about is never started by accident;
--no-notify says that is intended, and --notify-thread THREAD-ID names another
thread, which is how a shell, an ssh session or a script on another host asks
for a wake. check reports ready or accepted_waiting and both are success: the
run exists either way. On wake, collect it with the printed "t3-steward task
result <run>" command.

A start whose key matches a run that already exists prints that run's progress
rather than the viability of the manifest it just composed, and it promises a
wake only when a wait exists that will fire for the calling thread.

  t3-steward models                 the routes this fleet can run now
  t3-steward backlog projects       the projects and their eligible workers

--project NAME with --model INSTANCE/MODEL decides what will run without the
coordinator's catalog, so the start works against a coordinator older than the
projects query. The catalog is still asked for one thing, the route's quota
pool, and a coordinator that refuses that query leaves the pool empty for the
coordinator to resolve from the worker's inventory. Both ways of naming a route
read the pool the same way, so both submit the same archive under the same key.
` + coordinatorTransportHelp

// taskRunSchemaVersion versions the record run prints.
const taskRunSchemaVersion = 1

// taskRunRecord is what one start reports, in JSON with --json and as text
// otherwise. Every derived value is in it, because the caller did not choose
// them and has to be able to see what was chosen for it.
type taskRunRecord struct {
	SchemaVersion  int                   `json:"schemaVersion"`
	Run            string                `json:"run"`
	Tasks          []string              `json:"tasks"`
	Project        string                `json:"project"`
	Ref            string                `json:"ref,omitempty"`
	Fresh          bool                  `json:"fresh,omitempty"`
	Route          taskRunRoute          `json:"route"`
	IdempotencyKey string                `json:"idempotencyKey"`
	Replayed       bool                  `json:"replayed"`
	Notify         *campaignNotification `json:"notify,omitempty"`
	// Check is the readiness outcome of the manifest this call composed: ready
	// or accepted_waiting, both success. It is empty on a replay, where the
	// manifest was not started and its viability says nothing about the run
	// that exists; Progress says what that run is doing instead.
	Check string `json:"check,omitempty"`
	// Progress is the replayed run's own progress, read from the coordinator.
	Progress string `json:"progress,omitempty"`
	// ProgressUnavailable says why a replayed run's progress could not be read.
	// A record that carries it promises no wake and names diagnose instead.
	ProgressUnavailable string `json:"progressUnavailable,omitempty"`
	// Result is the command that collects the outcome on wake.
	Result   string   `json:"result"`
	Warnings []string `json:"warnings,omitempty"`
}

// taskRunRoute is the route the run was started on. Worker is the worker the
// task will run on when that is already decided, either because --worker
// pinned it or because exactly one eligible worker advertises the route.
//
// Worker has no omitempty: contract 4 lists it among the route's four keys, and
// a key that disappears when the route is not pinned makes a reader of the JSON
// guess whether the run is unpinned or the field was never implemented. The
// printed record names it either way; see printedRoute.
type taskRunRoute struct {
	Worker    string `json:"worker"`
	Instance  string `json:"instance"`
	Model     string `json:"model"`
	QuotaPool string `json:"quotaPool,omitempty"`
}

// unpinnedWorker is what the route's worker is called when the run may go to
// any eligible worker. The text form has always said it; printedRoute makes
// the JSON form say the same word rather than leaving the key out.
const unpinnedWorker = "any"

// printedRoute is the route as the record prints it, in both forms: the worker
// is always named. The empty worker of the derived route means "not pinned",
// and it stays empty everywhere else -- the campaign manifest and the
// idempotency key are built from the derived route before this, so naming the
// unpinned worker here cannot pin anything or change a key.
func printedRoute(route taskRunRoute) taskRunRoute {
	if route.Worker == "" {
		route.Worker = unpinnedWorker
	}
	return route
}

// gitCheckout is what the current directory says about itself. It is a value
// rather than a set of calls so that every derivation rule can be tested
// without a repository on disk.
type gitCheckout struct {
	// Remote is the origin URL, empty when there is none.
	Remote string
	// Branch is the checked-out branch, empty on a detached HEAD.
	Branch string
	// Upstream is the branch's tracking ref, empty when it has none.
	Upstream string
	// Ahead is how many commits the branch has that its upstream does not.
	Ahead int
	// Dirty reports uncommitted changes in the working tree.
	Dirty bool
}

// taskRunCLI is one start with its outside world at arm's length: the campaign
// seams carry the check, the submission and the notification, query answers
// the projects view, and checkout answers for the working directory.
type taskRunCLI struct {
	campaign     campaignCLI
	query        func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
	checkout     func() (gitCheckout, error)
	defaultModel string
	stdin        io.Reader
	stdout       io.Writer
	stderr       io.Writer
}

// newTaskRunCLI wires the real transports. It reuses the campaign CLI whole,
// because "task run" is the campaign path with the authoring removed and must
// not become a second implementation of check, submit or notify.
func newTaskRunCLI(cfg config.Config) taskRunCLI {
	cli := taskRunCLI{
		campaign:     campaignCLIFor(cfg),
		defaultModel: strings.TrimSpace(cfg.BacklogV2.CoordinatorClient.Defaults.Model),
		checkout:     readGitCheckout,
		stdout:       os.Stdout,
		stderr:       os.Stderr,
		stdin:        promptStdin(),
	}
	cli.query = func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
		transport, err := newCoordinatorTransport(cfg)
		if err != nil {
			return backlogadmin.Response{}, err
		}
		query.Version = backlogadmin.Version
		query.Principal = transport.principal
		return transport.client.Query(ctx, query)
	}
	return cli
}

// promptStdin returns stdin when something is piped into it, and an empty
// reader when it is a terminal: a command that would otherwise block forever
// waiting for a prompt nobody is typing is worse than one that says a prompt
// is required.
func promptStdin() io.Reader {
	info, err := os.Stdin.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice != 0 {
		return strings.NewReader("")
	}
	return os.Stdin
}

// Help is not admitted here. The task family admits it once, in cmdTask,
// before this parser sees anything; a second guard per verb is what let
// eighteen of them drift apart in the first place.
func cmdTaskRun(g globalFlags, args []string) error {
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return newTaskRunCLI(cfg).run(context.Background(), args)
}

// taskRunArgs is one parsed command line. Nothing is derived here; parsing
// only reports what the caller said.
type taskRunArgs struct {
	project    string
	ref        string
	fresh      bool
	model      string
	worker     string
	name       string
	key        string
	outputs    []string
	verify     []string
	class      string
	maxTurns   int
	promptFile string
	fanOut     string
	inline     string
	hasInline  bool
	// notifyThread is "current", a T3 thread id, or empty when --no-notify
	// said nobody is woken. It is the same spelling "campaign submit" takes,
	// because one spelling that means one thing is the whole of the fix.
	notifyThread string
	noNotify     bool
	asJSON       bool
}

func parseTaskRunArgs(args []string) (taskRunArgs, error) {
	parsed := taskRunArgs{class: string(domain.TaskClassSurplus)}
	flags := args
	for i, arg := range args {
		if arg == "--" {
			flags = args[:i]
			parsed.hasInline = true
			parsed.inline = strings.Join(args[i+1:], " ")
			break
		}
	}
	value := func(i int, name string) (string, error) {
		if i+1 >= len(flags) || strings.TrimSpace(flags[i+1]) == "" {
			return "", fmt.Errorf("%s needs a value", name)
		}
		return flags[i+1], nil
	}
	for i := 0; i < len(flags); i++ {
		var err error
		switch flags[i] {
		case "--json":
			parsed.asJSON = true
			continue
		case "--fresh":
			parsed.fresh = true
			continue
		case "--no-notify":
			parsed.noNotify = true
			continue
		case "--project":
			parsed.project, err = value(i, "--project")
		case "--ref":
			parsed.ref, err = value(i, "--ref")
		case "--model":
			parsed.model, err = value(i, "--model")
		case "--worker":
			parsed.worker, err = value(i, "--worker")
		case "--name":
			parsed.name, err = value(i, "--name")
		case "--idempotency-key":
			parsed.key, err = value(i, "--idempotency-key")
		case "--notify-thread":
			parsed.notifyThread, err = value(i, "--notify-thread")
		case "--class":
			parsed.class, err = value(i, "--class")
		case "--fan-out":
			parsed.fanOut, err = value(i, "--fan-out")
		case "--prompt-file":
			if i+1 >= len(flags) {
				err = errors.New("--prompt-file needs a path, or - for stdin")
			} else {
				parsed.promptFile = flags[i+1]
			}
		case "--outputs":
			var raw string
			if raw, err = value(i, "--outputs"); err == nil {
				for _, output := range strings.Split(raw, ",") {
					if trimmed := strings.TrimSpace(output); trimmed != "" {
						parsed.outputs = append(parsed.outputs, trimmed)
					}
				}
			}
		case "--verify":
			var raw string
			if raw, err = value(i, "--verify"); err == nil {
				parsed.verify = append(parsed.verify, raw)
			}
		case "--max-turns":
			var raw string
			if raw, err = value(i, "--max-turns"); err == nil {
				parsed.maxTurns, err = strconv.Atoi(raw)
				if err != nil || parsed.maxTurns < 1 {
					err = fmt.Errorf("--max-turns takes a positive whole number (got %q)", raw)
				}
			}
		default:
			return taskRunArgs{}, fmt.Errorf("unknown flag %q; try \"t3-steward task run --help\"", flags[i])
		}
		if err != nil {
			return taskRunArgs{}, err
		}
		i++
	}
	if parsed.class != string(domain.TaskClassSurplus) && parsed.class != string(domain.TaskClassRequired) {
		return taskRunArgs{}, fmt.Errorf("--class takes surplus or required (got %q)", parsed.class)
	}
	if parsed.noNotify && parsed.notifyThread != "" {
		return taskRunArgs{}, errors.New("--no-notify and --notify-thread contradict each other: one says nobody is woken, the other names who is")
	}
	if !parsed.noNotify && parsed.notifyThread == "" {
		parsed.notifyThread = "current"
	}
	return parsed, nil
}

// taskRunPrompt is one task's prompt with the name it will carry.
type taskRunPrompt struct {
	name string
	body string
}

func (c taskRunCLI) run(ctx context.Context, args []string) error {
	parsed, err := parseTaskRunArgs(args)
	if err != nil {
		return err
	}
	prompts, err := c.prompts(parsed)
	if err != nil {
		return err
	}
	checkout, err := c.checkout()
	if err != nil {
		return err
	}
	// A start that names its project and its whole route derives nothing
	// about what will run from the catalog, so a coordinator that cannot
	// answer for the catalog cannot stop it: the projects query is the one
	// thing here a coordinator of the previous release does not have, and this
	// is what lets the single-task start work unchanged during a mixed-release
	// window. The query is still sent, a few lines below, for the route's
	// quota pool alone, and its refusal is swallowed there.
	project, route, explicit := explicitTaskRunRoute(parsed, c.defaultModel)
	if !explicit {
		projects, queryErr := c.projects(ctx)
		if queryErr != nil {
			return queryErr
		}
		if project, err = deriveTaskRunProject(parsed.project, checkout, projects); err != nil {
			return err
		}
	}
	var warnings []string
	ref := ""
	if !parsed.fresh {
		ref, warnings, err = deriveTaskRunRef(parsed.ref, checkout)
		if err != nil {
			return err
		}
	}
	if !explicit {
		if route, err = deriveTaskRunRoute(parsed.model, parsed.worker, c.defaultModel, project); err != nil {
			return err
		}
	} else {
		route = c.advertisedTaskRunRoute(ctx, parsed, route)
	}
	key := parsed.key
	if key == "" {
		key = taskRunIdempotencyKey(project.Name, ref, route, prompts, parsed)
	}
	name := parsed.name
	if name == "" {
		name = taskRunDerivedName(prompts)
	}
	// The thread is resolved before anything is built or sent. A run nobody
	// will hear about is worse than a run that was not started.
	thread := ""
	if !parsed.noNotify {
		thread, err = c.campaign.campaignNotifyThread("task run", parsed.notifyThread)
		if err != nil {
			return fmt.Errorf("%w\nOr pass --no-notify to start a task nobody is woken for", err)
		}
	}
	for _, warning := range warnings {
		fmt.Fprintln(c.stderr, "warning: "+warning)
	}

	directory, err := writeTaskRunCampaign(taskRunCampaign{
		name: name, project: project.Name, ref: ref, fresh: parsed.fresh,
		route: route, pinned: parsed.worker != "", prompts: prompts,
		outputs: parsed.outputs, verify: parsed.verify,
		class: domain.TaskClass(parsed.class), maxTurns: parsed.maxTurns,
	})
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)

	bundle, plan, err := c.campaign.prepare(directory)
	if err != nil {
		return err
	}
	matrix, err := c.campaign.checkViability(ctx, plan, bundle, "")
	if err != nil {
		return err
	}
	if matrix.Outcome == backlogadmin.ViabilityImpossible {
		return campaignImpossible(matrix)
	}
	if c.campaign.submissions == nil {
		return errors.New("coordinator submission transport is unavailable")
	}
	client, err := c.campaign.submissions()
	if err != nil {
		return err
	}
	response, err := client.SubmitArchive(ctx, backlogadmin.LocalSubmissionRequest{
		IdempotencyKey: key, Principal: c.campaign.submissionPrincipal(),
	}, bytes.NewReader(bundle.Archive), int64(len(bundle.Archive)))
	if err != nil {
		return explainSubmissionConflict(err)
	}
	record := taskRunRecord{
		SchemaVersion:  taskRunSchemaVersion,
		Run:            response.RunID,
		Project:        project.Name,
		Ref:            ref,
		Fresh:          parsed.fresh,
		Route:          printedRoute(route),
		IdempotencyKey: response.Key,
		Replayed:       response.Replay,
		Check:          string(matrix.Outcome),
		Result:         "t3-steward task result " + response.RunID,
		Warnings:       warnings,
	}
	for _, prompt := range prompts {
		record.Tasks = append(record.Tasks, prompt.name)
	}
	if response.Replay {
		// The key resolved to a run that already exists, so the manifest this
		// call composed was never started and its viability says nothing about
		// what the caller is being told. The existing run's own progress does.
		record.Check = ""
		progress, progressErr := c.replayedRunProgress(ctx, response.RunID)
		if progressErr != nil {
			record.ProgressUnavailable = progressErr.Error()
		} else {
			record.Progress = string(progress)
		}
	}
	if thread != "" && !record.runIsTerminal() {
		notification, err := c.campaign.registerCampaignNotification(ctx, key, response.RunID, thread)
		if err != nil {
			return err
		}
		if spentWake(notification, thread) {
			// The registration this key derives already exists and is spent:
			// delivered, cancelled, or held for another thread. Reporting it as
			// the wake for this call is exactly the lie A-1 recorded, so a fresh
			// registration is made before anything is promised. A fresh one that
			// is spent too, or that the coordinator refuses, leaves the record
			// saying plainly that no wake is attached.
			if fresh, freshErr := c.campaign.registerCampaignNotification(ctx, key+"-"+newWaitID(), response.RunID, thread); freshErr == nil {
				notification = fresh
			}
		}
		record.Notify = &notification
	}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, record)
	}
	return renderTaskRunRecord(c.stdout, record)
}

// replayedRunProgress reads the progress of the run an idempotency key
// resolved to. It is asked only on a replay, and its failure is reported as a
// failure rather than filled in with a guess: a record that cannot state the
// run's progress must not promise a wake for it.
func (c taskRunCLI) replayedRunProgress(ctx context.Context, runID string) (domain.ProgressState, error) {
	if c.query == nil {
		return "", errors.New("coordinator query transport is unavailable")
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflow, WorkflowRunID: runID})
	if err != nil {
		return "", err
	}
	if response.Workflow == nil {
		return "", fmt.Errorf("the coordinator returned no workflow detail for run %s", runID)
	}
	return response.Workflow.Summary.Run.Progress, nil
}

// runIsTerminal reports whether the record states a run that has already ended.
// Nothing is registered to wake a thread for one: the outcome is there to be
// collected now.
func (record taskRunRecord) runIsTerminal() bool {
	return record.Progress != "" && domain.ProgressState(record.Progress).Terminal()
}

// spentWake reports whether a registration that came back can no longer wake
// this caller: its wake was already delivered or cancelled, or the coordinator
// holds it for a different thread.
//
// An undeliverable wake is not spent. It is a live registration with a
// delivery-host fault, which has its own message and its own fix.
func spentWake(notification campaignNotification, thread string) bool {
	if notification.Undeliverable != "" {
		return false
	}
	if notification.WaitThreadID != "" && notification.WaitThreadID != thread {
		return true
	}
	return notification.Delivery == "delivered" || notification.Delivery == "cancelled"
}

// wakeWillFire reports whether a wait exists that will fire for the calling
// thread. The wake promise and the wait are the same fact: the "End this turn
// now" line is printed if and only if this answers true, so no state of the
// record can tell an agent to end its turn on a wake that will not arrive.
func (record taskRunRecord) wakeWillFire() bool {
	switch {
	case record.Notify == nil, record.Notify.WaitID == "":
		return false
	case record.Notify.Undeliverable != "":
		return false
	case spentWake(*record.Notify, record.Notify.ThreadID):
		return false
	case record.ProgressUnavailable != "":
		// The run's progress could not be read, so whether it is still running
		// to be woken from is unknown. An unknown is not a promise.
		return false
	case record.runIsTerminal():
		return false
	}
	return true
}

// explicitTaskRunRoute is the start that derives nothing it needs from the
// catalog: --project names the project and --model INSTANCE/MODEL names the
// whole route, optionally pinned to a worker. It does not settle the quota
// pool; advertisedTaskRunRoute does, the same way the catalog path does. The
// effective model is the flag or the configured default, because a qualified
// default names a route just as completely as the flag does.
func explicitTaskRunRoute(parsed taskRunArgs, defaultModel string) (backlogadmin.Project, taskRunRoute, bool) {
	model := strings.TrimSpace(parsed.model)
	if model == "" {
		model = strings.TrimSpace(defaultModel)
	}
	instance, name, qualified := strings.Cut(model, "/")
	if strings.TrimSpace(parsed.project) == "" || !qualified || instance == "" || name == "" {
		return backlogadmin.Project{}, taskRunRoute{}, false
	}
	return backlogadmin.Project{Name: parsed.project},
		taskRunRoute{Worker: parsed.worker, Instance: instance, Model: name}, true
}

// advertisedTaskRunRoute settles a route the caller named in full against the
// catalog, so that both paths derive the quota pool the same way.
//
// The pool is in the submitted manifest and deliberately not in the
// idempotency key, which covers what will run. A pool one path fills and the
// other leaves empty therefore gives two different archives one key: the
// second start is refused as the same key with different content, for a
// difference the caller never made and cannot see. Both paths read the pool
// from the instance the catalog advertises, and both leave it empty when the
// catalog cannot be read.
//
// Every refusal here is swallowed on purpose. A coordinator older than the
// projects query is exactly what this path exists for, and it cannot answer
// the catalog path at all, so the two cannot disagree there; a project or a
// route the catalog does not know is not refused here either, because this
// path validates nothing. A coordinator that cannot be reached at all fails
// the readiness check a moment later, with its own message.
func (c taskRunCLI) advertisedTaskRunRoute(ctx context.Context, parsed taskRunArgs, route taskRunRoute) taskRunRoute {
	if c.query == nil {
		return route
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryProjects})
	if err != nil {
		return route
	}
	for _, project := range response.Projects {
		if project.Name != strings.TrimSpace(parsed.project) {
			continue
		}
		advertised, err := deriveTaskRunRoute(route.Instance+"/"+route.Model, parsed.worker, "", project)
		if err != nil {
			return route
		}
		return advertised
	}
	return route
}

// projects asks the coordinator for its catalog. It is one query: the project
// and the route are both derived from it, so they cannot be derived from two
// different views of the fleet.
func (c taskRunCLI) projects(ctx context.Context) ([]backlogadmin.Project, error) {
	if c.query == nil {
		return nil, errors.New("coordinator query transport is unavailable")
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryProjects})
	if err != nil {
		return nil, explainRefusedProjectsQuery(ctx, c.query, err, taskRunWithoutTheCatalog)
	}
	return response.Projects, nil
}

// prompts reads the one prompt, or the fan-out's several. Exactly one source
// is accepted: a command that silently preferred one of two prompts would be
// the worst possible way to find out which.
func (c taskRunCLI) prompts(parsed taskRunArgs) ([]taskRunPrompt, error) {
	sources := 0
	for _, present := range []bool{parsed.hasInline, parsed.promptFile != "", parsed.fanOut != ""} {
		if present {
			sources++
		}
	}
	if sources > 1 {
		return nil, errors.New("a prompt comes from exactly one source: an argument after --, --prompt-file FILE, --fan-out GLOB, or stdin")
	}
	switch {
	case parsed.fanOut != "":
		return fanOutPrompts(parsed.fanOut)
	case parsed.hasInline:
		return onePrompt(parsed.inline)
	case parsed.promptFile != "" && parsed.promptFile != "-":
		raw, err := os.ReadFile(parsed.promptFile)
		if err != nil {
			return nil, fmt.Errorf("--prompt-file: %w", err)
		}
		return onePrompt(string(raw))
	default:
		if c.stdin == nil {
			return nil, errors.New("a prompt is required: pass it after --, with --prompt-file FILE, or on stdin")
		}
		raw, err := io.ReadAll(c.stdin)
		if err != nil {
			return nil, err
		}
		return onePrompt(string(raw))
	}
}

func onePrompt(body string) ([]taskRunPrompt, error) {
	if strings.TrimSpace(body) == "" {
		return nil, errors.New("a prompt is required: pass it after --, with --prompt-file FILE, or on stdin")
	}
	return []taskRunPrompt{{name: "task", body: body}}, nil
}

// fanOutPrompts turns a glob into one task per file, named by the file stem.
func fanOutPrompts(pattern string) ([]taskRunPrompt, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("--fan-out %q: %w", pattern, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("--fan-out %q matched no file", pattern)
	}
	sort.Strings(matches)
	prompts := make([]taskRunPrompt, 0, len(matches))
	seen := make(map[string]string, len(matches))
	for _, match := range matches {
		base := filepath.Base(match)
		name := manifestSlug(strings.TrimSuffix(base, filepath.Ext(base)))
		if name == "" {
			return nil, fmt.Errorf("--fan-out: %q has no name a task can carry", base)
		}
		if first, clash := seen[name]; clash {
			return nil, fmt.Errorf("--fan-out: %q and %q both name task %q", first, base, name)
		}
		seen[name] = base
		raw, err := os.ReadFile(match)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(string(raw)) == "" {
			return nil, fmt.Errorf("--fan-out: %q is empty", base)
		}
		prompts = append(prompts, taskRunPrompt{name: name, body: string(raw)})
	}
	return prompts, nil
}

// deriveTaskRunProject matches this checkout against the coordinator's
// catalog. Exactly one match is required: zero means the fleet does not know
// this repository, several mean the catalog is ambiguous, and guessing either
// way starts work in the wrong place.
func deriveTaskRunProject(explicit string, checkout gitCheckout, projects []backlogadmin.Project) (backlogadmin.Project, error) {
	if explicit != "" {
		for _, project := range projects {
			if project.Name == explicit {
				return project, nil
			}
		}
		return backlogadmin.Project{}, fmt.Errorf("project %q is not in this coordinator's catalog; %s",
			explicit, taskRunProjectList(projects))
	}
	if checkout.Remote == "" {
		return backlogadmin.Project{}, fmt.Errorf("this directory has no origin remote to match against a project; "+
			"pass --project NAME: %s", taskRunProjectList(projects))
	}
	wanted := normalizeRepository(checkout.Remote)
	var matched []backlogadmin.Project
	for _, project := range projects {
		if project.Repository != "" && normalizeRepository(project.Repository) == wanted {
			matched = append(matched, project)
		}
	}
	switch len(matched) {
	case 1:
		return matched[0], nil
	case 0:
		return backlogadmin.Project{}, fmt.Errorf("no project has repository %s; pass --project NAME: %s",
			checkout.Remote, taskRunProjectList(projects))
	default:
		names := make([]string, 0, len(matched))
		for _, project := range matched {
			names = append(names, project.Name)
		}
		return backlogadmin.Project{}, fmt.Errorf("repository %s matches %s; pass --project NAME to say which",
			checkout.Remote, strings.Join(names, " and "))
	}
}

func taskRunProjectList(projects []backlogadmin.Project) string {
	names := make([]string, 0, len(projects))
	for _, project := range projects {
		names = append(names, project.Name)
	}
	if len(names) == 0 {
		return "this coordinator has no projects (t3-steward backlog projects)"
	}
	sort.Strings(names)
	return "the projects are " + strings.Join(names, ", ")
}

// normalizeRepository reduces a remote URL to host and path so that the SSH
// and HTTPS spellings of one repository compare equal.
func normalizeRepository(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	value = strings.TrimSuffix(strings.TrimSuffix(value, "/"), ".git")
	if index := strings.Index(value, "://"); index >= 0 {
		value = value[index+3:]
	} else if at := strings.Index(value, "@"); at >= 0 && strings.Contains(value[at:], ":") {
		// scp-like: user@host:path
		value = strings.Replace(value[at+1:], ":", "/", 1)
	}
	if at := strings.Index(value, "@"); at >= 0 {
		value = value[at+1:]
	}
	host, path, found := strings.Cut(value, "/")
	if found {
		if colon := strings.Index(host, ":"); colon >= 0 {
			host = host[:colon]
		}
		value = host + "/" + path
	}
	return strings.TrimSuffix(strings.TrimSuffix(value, "/"), ".git")
}

// deriveTaskRunRef picks the ref the worker will fetch. Only a ref the fleet
// can actually fetch is accepted, because the alternative is a task that is
// accepted here and fails on the worker minutes later.
func deriveTaskRunRef(explicit string, checkout gitCheckout) (string, []string, error) {
	var warnings []string
	if checkout.Dirty {
		warnings = append(warnings, "this tree has uncommitted changes; they are not sent, the fleet fetches the ref")
	}
	if explicit != "" {
		return explicit, warnings, nil
	}
	switch {
	case checkout.Branch == "":
		return "", nil, errors.New("HEAD is detached, so there is no branch the fleet could fetch: push first or pass --ref")
	case checkout.Upstream == "":
		return "", nil, fmt.Errorf("branch %q has no upstream, so the fleet cannot fetch it: push first or pass --ref", checkout.Branch)
	case checkout.Ahead > 0:
		return "", nil, fmt.Errorf("branch %q is %d commit(s) ahead of %s, so the fleet would fetch older work: push first or pass --ref",
			checkout.Branch, checkout.Ahead, checkout.Upstream)
	}
	return checkout.Branch, warnings, nil
}

// deriveTaskRunRoute resolves --model against what the project's eligible
// workers advertise. The quota pool is always the advertised one: a pool this
// command invented would be refused by the coordinator or, worse, accepted
// against the wrong budget.
func deriveTaskRunRoute(model, worker, fallback string, project backlogadmin.Project) (taskRunRoute, error) {
	if strings.TrimSpace(model) == "" {
		model = fallback
	}
	if strings.TrimSpace(model) == "" {
		return taskRunRoute{}, fmt.Errorf("no --model and no defaults.model in the client configuration; "+
			"project %q can run %s (t3-steward models)", project.Name, taskRunRouteList(project, worker))
	}
	instance, wanted, qualified := strings.Cut(model, "/")
	if !qualified {
		instance, wanted = "", model
	}
	var matched []taskRunRoute
	seen := make(map[string]bool)
	for _, candidate := range project.Workers {
		if worker != "" && candidate.Worker != worker {
			continue
		}
		for _, route := range candidate.Routes {
			if route.Model != wanted || (instance != "" && route.Instance != instance) {
				continue
			}
			if seen[route.Instance] {
				// A second worker offering the same instance and model is the
				// same route, not a second one; only the worker is then unknown.
				for i := range matched {
					if matched[i].Instance == route.Instance {
						matched[i].Worker = ""
					}
				}
				continue
			}
			seen[route.Instance] = true
			matched = append(matched, taskRunRoute{
				Worker: candidate.Worker, Instance: route.Instance,
				Model: route.Model, QuotaPool: route.QuotaPool,
			})
		}
	}
	switch len(matched) {
	case 1:
		if worker != "" {
			matched[0].Worker = worker
		}
		return matched[0], nil
	case 0:
		scope := ""
		if worker != "" {
			scope = fmt.Sprintf(" on worker %s", worker)
		}
		return taskRunRoute{}, fmt.Errorf("no eligible worker of project %q advertises model %q%s; it can run %s (t3-steward models)",
			project.Name, model, scope, taskRunRouteList(project, worker))
	default:
		instances := make([]string, 0, len(matched))
		for _, route := range matched {
			instances = append(instances, route.Instance+"/"+route.Model)
		}
		sort.Strings(instances)
		return taskRunRoute{}, fmt.Errorf("model %q is offered by several instances (%s); pass --model INSTANCE/MODEL",
			wanted, strings.Join(instances, ", "))
	}
}

// taskRunRouteList names the routes a project could have been asked for.
func taskRunRouteList(project backlogadmin.Project, worker string) string {
	seen := make(map[string]bool)
	var pairs []string
	for _, candidate := range project.Workers {
		if worker != "" && candidate.Worker != worker {
			continue
		}
		for _, route := range candidate.Routes {
			pair := route.Instance + "/" + route.Model
			if seen[pair] {
				continue
			}
			seen[pair] = true
			pairs = append(pairs, pair)
		}
	}
	if len(pairs) == 0 {
		return "nothing: no eligible worker advertises a route for it"
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ", ")
}

// taskRunIdempotencyKey digests exactly the inputs that decide what will run,
// in a fixed order, so that the same command twice is the same run and any
// different input is a different one.
func taskRunIdempotencyKey(project, ref string, route taskRunRoute, prompts []taskRunPrompt, parsed taskRunArgs) string {
	digest := sha256.New()
	write := func(values ...string) {
		for _, value := range values {
			digest.Write([]byte(value))
			digest.Write([]byte{0})
		}
	}
	write(project, ref, route.Instance, route.Model)
	for _, prompt := range prompts {
		write(prompt.name, prompt.body)
	}
	write(parsed.outputs...)
	write(parsed.verify...)
	write(parsed.class, strconv.Itoa(parsed.maxTurns))
	return "run-" + hex.EncodeToString(digest.Sum(nil))[:16]
}

// explainSubmissionConflict names what can have changed the archive without
// changing the key. The key covers what will run, deliberately, so --worker
// and --name are outside it; both of them are inside the archive, so a start
// that differs from an earlier one only in those flags arrives with that key
// and different content.
//
// The route's quota pool is outside the key for the same reason and inside the
// archive in the same way, but the caller never chose it: advertisedTaskRunRoute
// and deriveTaskRunRoute both read it from the projects catalog and both leave
// it empty when the catalog cannot be read. An identical command therefore
// submits different content on either side of a coordinator upgraded to the
// release that has the query, a transient failure of that one query, or a
// principal without the projects read view. That cause is named here because
// nothing the caller can see distinguishes it from the two flags, and reading
// this refusal as being about flags it never passed is the wrong place to
// look. The coordinator's own refusal is true and names none of this, so it is
// kept and explained.
func explainSubmissionConflict(err error) error {
	if err == nil || !isSubmissionConflict(err) {
		return err
	}
	return fmt.Errorf("%w\nThe key covers the project, ref, route, prompts, outputs, "+
		"verify commands, class and max turns, and not --worker or --name, which do change "+
		"what is submitted.\nThe route's quota pool is outside the key as well: it is read "+
		"from the coordinator's projects catalog and left empty when that catalog cannot be "+
		"read, so the same command submits different content across a coordinator upgrade, a "+
		"transient failure of that query, or a principal without the projects read view.\n"+
		"Pass --idempotency-key KEY to start this as its own run, or repeat the earlier "+
		"--worker and --name against the same coordinator to replay that one", err)
}

// isSubmissionConflict recognises the conflict as a value where the transport
// carries one and by its message where it carries a string, which is what a
// remote coordinator's error is by the time it reaches here.
func isSubmissionConflict(err error) bool {
	return errors.Is(err, domain.ErrSubmissionConflict) ||
		strings.Contains(err.Error(), domain.ErrSubmissionConflict.Error())
}

// taskRunDerivedName is the prompt's first line, which is what an operator
// reading a run list wants to see.
func taskRunDerivedName(prompts []taskRunPrompt) string {
	if len(prompts) == 0 {
		return ""
	}
	first, _, _ := strings.Cut(strings.TrimSpace(prompts[0].body), "\n")
	if len(first) > 60 {
		first = first[:60]
	}
	return first
}

// manifestSlug turns free text into a name the manifest accepts: it must start
// with a lowercase letter and hold only lowercase letters, digits, hyphens and
// underscores.
func manifestSlug(text string) string {
	var builder strings.Builder
	lastSeparator := false
	for _, symbol := range strings.ToLower(strings.TrimSpace(text)) {
		switch {
		case symbol >= 'a' && symbol <= 'z':
			builder.WriteRune(symbol)
			lastSeparator = false
		case symbol >= '0' && symbol <= '9':
			if builder.Len() == 0 {
				continue
			}
			builder.WriteRune(symbol)
			lastSeparator = false
		default:
			if builder.Len() == 0 || lastSeparator {
				continue
			}
			builder.WriteRune('-')
			lastSeparator = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

// taskRunCampaign is the campaign directory this start will build.
type taskRunCampaign struct {
	name     string
	project  string
	ref      string
	fresh    bool
	route    taskRunRoute
	pinned   bool
	prompts  []taskRunPrompt
	outputs  []string
	verify   []string
	class    domain.TaskClass
	maxTurns int
}

// writeTaskRunCampaign writes the version 2 directory the campaign path
// expects. It is a temporary directory rather than a hidden format: what is
// submitted is exactly what "campaign submit" would submit, so a start that
// misbehaves can be reproduced by hand.
func writeTaskRunCampaign(spec taskRunCampaign) (string, error) {
	name := manifestSlug(spec.name)
	if name == "" {
		name = "task-run"
	}
	environmentType := backlog.EnvironmentGit
	ref := spec.ref
	if spec.fresh {
		environmentType, ref = backlog.EnvironmentFresh, ""
	}
	route := backlog.ManifestRoute{
		Instance: spec.route.Instance, Model: spec.route.Model, QuotaPool: spec.route.QuotaPool,
	}
	if spec.pinned {
		route.Host = spec.route.Worker
	}
	manifest := backlog.Manifest{
		Version: backlog.ManifestVersion,
		Name:    name,
		Class:   spec.class,
		Environment: backlog.ManifestEnvironment{
			Project: spec.project, Type: environmentType,
			Scope: backlog.EnvironmentScopeTask, Ref: ref,
		},
		Routes: []backlog.ManifestRoute{route},
		Tasks:  make(map[string]backlog.ManifestTask, len(spec.prompts)),
	}
	for _, prompt := range spec.prompts {
		manifest.Tasks[prompt.name] = backlog.ManifestTask{
			PromptFile: "prompts/" + prompt.name + ".md",
			Outputs:    append([]string(nil), spec.outputs...),
			Verify:     append([]string(nil), spec.verify...),
			MaxTurns:   spec.maxTurns,
		}
	}
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		return "", err
	}
	root, err := os.MkdirTemp("", "t3-steward-task-run-")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(root, "prompts"), 0o700); err != nil {
		os.RemoveAll(root)
		return "", err
	}
	if err := os.WriteFile(filepath.Join(root, campaign.ManifestFileName), raw, 0o600); err != nil {
		os.RemoveAll(root)
		return "", err
	}
	for _, prompt := range spec.prompts {
		body := prompt.body
		if !strings.HasSuffix(body, "\n") {
			body += "\n"
		}
		if err := os.WriteFile(filepath.Join(root, "prompts", prompt.name+".md"), []byte(body), 0o600); err != nil {
			os.RemoveAll(root)
			return "", err
		}
	}
	return root, nil
}

// renderTaskRunRecord prints the record as text. It ends with the two things
// the caller does next: end the turn, and collect the result on wake.
func renderTaskRunRecord(out io.Writer, record taskRunRecord) error {
	printed := printedRoute(record.Route)
	route := printed.Worker + " " + printed.Instance + "/" + printed.Model
	if printed.QuotaPool != "" {
		route += "@" + printed.QuotaPool
	}
	ref := record.Ref
	if record.Fresh {
		ref = "(fresh workspace)"
	}
	fmt.Fprintf(out, "run %s\n", record.Run)
	fmt.Fprintf(out, "tasks %s\n", strings.Join(record.Tasks, ", "))
	fmt.Fprintf(out, "project %s\n", record.Project)
	fmt.Fprintf(out, "ref %s\n", ref)
	fmt.Fprintf(out, "route %s\n", route)
	fmt.Fprintf(out, "idempotency-key %s (replayed: %t)\n", record.IdempotencyKey, record.Replayed)
	renderTaskRunState(out, record)
	renderTaskRunWake(out, record)
	_, err := fmt.Fprintf(out, "next:\n  %s\n", record.Result)
	return err
}

// renderTaskRunState prints what this start actually established: the
// viability of the manifest it composed, or, when the key replayed an existing
// run, that run's own progress. The two are never printed together, because a
// reader who sees "check accepted_waiting" on a replay reads it as the state of
// the run and it is not.
func renderTaskRunState(out io.Writer, record taskRunRecord) {
	switch {
	case record.ProgressUnavailable != "":
		fmt.Fprintf(out, "progress unknown: %s\n", record.ProgressUnavailable)
	case record.Progress != "":
		fmt.Fprintf(out, "progress %s (this run already exists; nothing new was started)\n", record.Progress)
	case record.Check != "":
		fmt.Fprintf(out, "check %s\n", record.Check)
	}
}

// renderTaskRunWake prints the wait, if there is one, and then exactly one
// instruction about the caller's turn. The "End this turn now" line is printed
// if and only if wakeWillFire, so the promise and the wait are the same fact.
func renderTaskRunWake(out io.Writer, record taskRunRecord) {
	if record.Notify != nil {
		line := fmt.Sprintf("notify thread %s (wait %s)", record.Notify.ThreadID, record.Notify.WaitID)
		if record.Notify.Delivery != "" {
			line += " delivery=" + record.Notify.Delivery
		}
		if record.Notify.Host != "" {
			// Host is printed because host is where D-4 hid: a wake recorded for
			// another host is sent into the T3 of that host.
			line += " host=" + record.Notify.Host
		}
		if record.Notify.WaitThreadID != "" && record.Notify.WaitThreadID != record.Notify.ThreadID {
			line += " held-for-thread=" + record.Notify.WaitThreadID
		}
		if record.Notify.Undeliverable != "" {
			line += " undeliverable: " + record.Notify.Undeliverable
		}
		fmt.Fprintln(out, line)
	}
	if record.wakeWillFire() {
		fmt.Fprint(out, "End this turn now; the steward wakes this thread when the run ends.\n")
		return
	}
	switch {
	case record.ProgressUnavailable != "":
		fmt.Fprintf(out, "This run's progress could not be read, so nothing is promised about a wake. "+
			"Read it with:\n  t3-steward diagnose %s\n", record.Run)
	case record.runIsTerminal():
		fmt.Fprintf(out, "This run already ended %s, so there is nothing to wait for and nothing will wake "+
			"this thread. Collect it now.\n", record.Progress)
	case record.Notify == nil:
		fmt.Fprint(out, "notify none: nothing will wake a thread when this run ends\n")
	case record.Notify.Undeliverable != "":
		// The run exists and the wait exists; what does not exist is a path from
		// one to this thread. Promising a wake here is worse than promising
		// nothing, because the agent would end its turn on it.
		fmt.Fprintf(out, "The run was started. Nothing will wake this thread, so do not end this turn "+
			"waiting for a wake.\nWatch it instead with:\n  t3-steward campaign show %s\n", record.Run)
	default:
		fmt.Fprintf(out, "No wake is attached to this run, so do not end this turn waiting for one. "+
			"Attach one with:\n  t3-steward wait add --run %s\n", record.Run)
	}
}

// readGitCheckout answers for the current directory. Every failure is the same
// answer as "not a checkout": the derivation rules refuse on the value, and a
// missing git binary must not produce a different message than a missing
// repository.
func readGitCheckout() (gitCheckout, error) {
	checkout := gitCheckout{}
	git := func(args ...string) (string, bool) {
		var out bytes.Buffer
		command := exec.Command("git", args...)
		command.Stdout = &out
		if err := command.Run(); err != nil {
			return "", false
		}
		return strings.TrimSpace(out.String()), true
	}
	if remote, ok := git("remote", "get-url", "origin"); ok {
		checkout.Remote = remote
	}
	branch, ok := git("symbolic-ref", "--quiet", "--short", "HEAD")
	if !ok {
		return checkout, nil
	}
	checkout.Branch = branch
	if upstream, ok := git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"); ok {
		checkout.Upstream = upstream
		if count, ok := git("rev-list", "--count", upstream+"..HEAD"); ok {
			checkout.Ahead, _ = strconv.Atoi(count)
		}
	}
	if status, ok := git("status", "--porcelain"); ok {
		checkout.Dirty = status != ""
	}
	return checkout, nil
}
