package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const backlogUsage = `Usage: t3-steward backlog <command> [args]

Coordinator submission command:
  submit <bundle.tar> [--idempotency-key KEY] [--json]

Stopped coordinator backup commands:
  backup create <snapshot-directory>
  backup verify <snapshot-directory>
  backup restore <snapshot-directory>

Coordinator read commands:
  status [--include-sink] [--json]
  projects [--project NAME] [--json]
       The configured projects (repository, default ref, setup profile) and,
       per project, the eligible workers with the instance/model routes they
       advertise. "t3-steward task run" derives its project and route from this.
  workers [--json]
  list [--project P] [--schedule S] [--progress STATES] [--class CLASS]
       [--worker W] [--quota-pool Q] [--include-sink] [--json]
  show <workflow-run> [--include-sink] [--json]
  graph <workflow-run> [--json|--dot]
  diagnose <workflow-run> [--json]
  task show <workflow-run>/<task> [--json]
  events <workflow-run> [--json]
  usage <workflow-run> [--raw] [--limit N] [--cursor C] [--json]   Bounded normalized usage.
  explain <workflow-run>/<task> [--json]
  artifacts [<task>|<workflow-run>/<task>] [--json]
  artifact show <artifact> [--json]
  artifact get <artifact> [--output PATH]
  commands [<workflow-run>[/<task>]] [--json]
  command show <command> [--json]
  quarantine [--json]   Intake the coordinator refused permanently and is now
                        silent about: key, digest, when and why. It names no
                        run, so "events" cannot show it.

Graph amendments (all require --expected-revision N --request-id ID --reason TEXT):
  task add <run>/<name> --provider INSTANCE --model MODEL --prompt TEXT --verify COMMAND
      [--needs NAME,OTHER-RUN/TASK] [--options JSON] [--timeout DURATION]
      [--class required|surplus] [--max-turns N]
  task set <run>/<task> [--model MODEL] [--provider INSTANCE]
      [--options JSON] [--timeout DURATION] [--verify COMMAND]
  --verify is repeatable; task set replaces the verification list.
  edge add|remove <run>/<task> --from <task|other-run/task>
  run clone --from <run>

Revision-fenced controls:
  start|resume|cancel|retry|skip <workflow-run>/<task> --reason TEXT [--command-id ID] [--json]
  delay <workflow-run>/<task> --until RFC3339 --reason TEXT [--command-id ID] [--json]
  pause <workflow-run>/<task> [--now] --reason TEXT [--command-id ID] [--json]
  rewake <workflow-run>/<task> --reason TEXT [--command-id ID] [--json]
      Wake an attempt left waiting-external after its task wait was cancelled
      or settled without reaching it; refused while a wait is still live.
  quarantine release <key> --reason TEXT [--json]
      Clear one intake quarantine after fixing what caused it. Editing the file
      clears it by itself; this is for a refusal the file cannot fix, such as a
      project no alias mapped.
  recover <assignment> --outcome stopped|failed --coordinator-epoch N
      --assignment-epoch N --attempt-revision N --evidence-id ID
      --evidence-sha256 HEX --reason TEXT [--recovery-id ID] [--json]

Legacy task-file helpers:
  new <id>           Create a task file from a template and print its path.
  path               Print the task directory.
  check <file|->     Validate a task: project, provider instance, model, options, host.
  receive <id>       Store a task sent by another host (used by forwarding).
  list --all         Show the legacy local task files and configured remote lists.

The runner is part of "run"; enable it with backlog.enabled: true.
The legacy task-file helpers above are offline: they touch no coordinator.

Example:
  t3-steward backlog submit ./bundle.tar --idempotency-key 2026-09-14-upkeeper --json
` + coordinatorTransportHelp

const taskTemplate = `---
project: %s
importance: 3
difficulty: 3
# model: claude-opus-5
# instance: claudeAgent
# not_before: %s
# deadline:
max_turns: 3
---
Describe the task for an agent that will get no input from you. Say what
"done" looks like, where the code or files are, and what to leave behind
(a branch, a summary file, a PR).
`

// newBacklogRunner builds the runner from the configuration.
func newBacklogRunner(cfg config.Config, store *sqlite.Store, control backlog.Control, logger *slog.Logger, dataDir string) (*backlog.Runner, error) {
	dir, err := cfg.ResolveBacklogDir()
	if err != nil {
		return nil, err
	}
	return backlog.New(backlog.Options{
		DisableQuotaChecks:       !cfg.QuotaChecksEnabled(),
		Dir:                      dir,
		Preamble:                 cfg.Backlog.Preamble,
		QuietFor:                 cfg.Backlog.QuietFor.D(),
		SafetyMargin:             cfg.Backlog.SafetyMargin,
		FallbackPerHour:          cfg.Backlog.FallbackPerHour,
		Quantile:                 cfg.Backlog.Quantile,
		MinSamples:               cfg.Backlog.MinSamples,
		LongWindowCap:            cfg.Backlog.LongWindowCap,
		HistoryDays:              cfg.Backlog.HistoryDays,
		DryRun:                   cfg.Policy.DryRun,
		MaxConcurrentPerProvider: cfg.Resume.MaxConcurrentPerProvider,
		Logger:                   logger,
		LocalHost:                localHostName(cfg),
		DefaultHost:              cfg.Backlog.DefaultHost,
		Forward:                  forwardTask,
		DataDir:                  dataDir,
	}, store, control), nil
}

// isCoordinatorAdmin decides which of cmdBacklog's dispatchers a command word
// belongs to. It has to agree with two other places -- the verbs backlogUsage
// documents and the verbs the admin parser implements -- and when it does not,
// a documented verb dies quietly in the legacy dispatcher as "unknown backlog
// command". The revision-fenced controls therefore ask isBacklogMutation, the
// same predicate the parser uses, rather than repeating its list; every other
// verb is named here and TestEveryDocumentedBacklogCommandReachesItsDispatcher
// drives the whole documented set through cmdBacklog to keep the three in
// agreement.
func isCoordinatorAdmin(args []string) bool {
	if len(args) == 0 {
		return false
	}
	if isBacklogMutation(args[0]) {
		return true
	}
	switch args[0] {
	case "submit", "status", "projects", "workers", "edge", "run", "diagnose", "graph", "task", "events", "usage", "explain", "artifacts", "artifact", "commands", "command", "show", "quarantine", "recover":
		return true
	case "list":
		return len(args) != 2 || args[1] != "--all"
	default:
		return false
	}
}

func runCoordinatorAdmin(cfg config.Config, args []string, schedules bool) error {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	client := transport.client
	cli := backlogAdminCLI{
		service:             client,
		mutator:             client,
		artifacts:           client,
		submissions:         client,
		scheduleDefinitions: client,
		recovery:            client,
		quarantine:          client,
		principal:           transport.principal,
		stdout:              os.Stdout,
	}
	// The --json error envelope is applied once, in run(), for every command.
	if schedules {
		return cli.runSchedules(context.Background(), args)
	}
	return cli.runBacklog(context.Background(), args)
}

func cmdSchedules(g globalFlags, args []string) error {
	if answered, err := admitFamilyHelp(os.Stdout, []string{"schedules"}, args); answered || err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCoordinatorAdmin(cfg, args, true)
}

func cmdBacklog(g globalFlags, args []string) error {
	if answered, err := admitFamilyHelp(os.Stdout, []string{"backlog"}, args); answered || err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	if isCoordinatorAdmin(args) {
		return backlogAdminRoute(cfg, args, false)
	}
	if args[0] == "backup" {
		return backlogBackupRoute(context.Background(), cfg, args[1:])
	}
	return backlogLegacyRoute(cfg, args)
}

// backlogAdminRoute, backlogBackupRoute and backlogLegacyRoute are the three
// dispatchers that live behind the single name "backlog". They are variables
// rather than direct calls so that a test can ask which one a documented verb
// reaches without a coordinator, a state database or a network: the routing is
// the thing that was wrong, and executing the dispatcher would hide it.
var (
	backlogAdminRoute  = runCoordinatorAdmin
	backlogBackupRoute = runBacklogBackup
	backlogLegacyRoute = runBacklogLegacy
)

// runBacklogLegacy serves the offline task-file helpers: they read and write
// this host's backlog directory and its state database and reach no
// coordinator. A verb that is not one of them is refused here, which is where
// a coordinator verb missing from isCoordinatorAdmin ends up.
func runBacklogLegacy(cfg config.Config, args []string) error {
	dir, err := cfg.ResolveBacklogDir()
	if err != nil {
		return err
	}
	ctx := context.Background()
	openStore := func() (*sqlite.Store, error) {
		statePath, err := cfg.ResolveStatePath()
		if err != nil {
			return nil, err
		}
		return sqlite.Open(statePath)
	}
	loadStates := func(store *sqlite.Store) (map[string]*backlog.State, error) {
		raw, err := store.LoadTaskStates(ctx)
		if err != nil {
			return nil, err
		}
		out := map[string]*backlog.State{}
		for id, js := range raw {
			var st backlog.State
			if json.Unmarshal(js, &st) == nil {
				out[id] = &st
			}
		}
		return out, nil
	}
	switch args[0] {
	case "path":
		fmt.Println(dir)
		return nil
	case "check":
		if len(args) != 2 {
			return errors.New("check needs a task file path, or - for stdin")
		}
		return cmdBacklogCheck(cfg, args[1])
	case "receive":
		if len(args) != 2 {
			return errors.New("receive needs a task id")
		}
		return cmdBacklogReceive(cfg, dir, args[1])
	case "new":
		if len(args) != 2 {
			return errors.New("new needs a task id")
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		path := filepath.Join(dir, args[1]+".md")
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists", path)
		}
		project := "<project title>"
		content := fmt.Sprintf(taskTemplate, project, time.Now().Add(time.Hour).Format(time.RFC3339))
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			return err
		}
		fmt.Println(path)
		if !cfg.Backlog.Enabled {
			fmt.Fprintln(os.Stderr, "note: backlog.enabled is false; the runner will not pick tasks up until it is true")
		}
		return nil
	case "list":
		tasks, errs := backlog.LoadDir(dir)
		all := len(args) > 1 && args[1] == "--all"
		if all {
			fmt.Printf("== %s (local)\n", localHostName(cfg))
		}
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, "warning:", e)
		}
		store, err := openStore()
		if err != nil {
			return err
		}
		defer store.Close()
		states, err := loadStates(store)
		if err != nil {
			return err
		}
		if len(tasks) == 0 && len(states) == 0 {
			fmt.Printf("No tasks in %s. Create one with: t3-steward backlog new <id>\n", dir)
			if !all {
				return nil
			}
			tasks = nil
		}
		backlog.Order(tasks, states, time.Now())
		fmt.Printf("%-24s %-12s %3s %3s %6s %6s  %s\n", "id", "status", "imp", "dif", "est%", "meas%", "detail")
		for _, t := range tasks {
			st := states[t.ID]
			status, est, meas, detail := "new", backlog.SeedCost(t.Difficulty), 0.0, "not seen by the runner yet"
			if st != nil {
				status, est, meas, detail = string(st.Status), st.EstimatedCost, st.MeasuredCost, st.Reason
				if st.ThreadID != "" {
					detail = strings.TrimSpace(detail + "  thread " + st.ThreadID)
				}
			}
			fmt.Printf("%-24s %-12s %3d %3d %6.0f %6.0f  %s\n", t.ID, status, t.Importance, t.Difficulty, est, meas, detail)
		}
		var gone []string
		for id, st := range states {
			found := false
			for _, t := range tasks {
				if t.ID == id {
					found = true
					break
				}
			}
			if !found && st.Status != backlog.StatusCancelled {
				gone = append(gone, id)
			}
		}
		sort.Strings(gone)
		for _, id := range gone {
			fmt.Printf("%-24s %-12s %3s %3s %6s %6s  %s\n", id, states[id].Status, "-", "-", "-", "-", "file removed")
		}
		if all {
			for _, host := range cfg.Report.Remotes {
				fmt.Println()
				if err := remoteBacklogList(ctx, host); err != nil {
					fmt.Fprintln(os.Stderr, "warning:", err)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown backlog command %q", args[0])
	}
}
