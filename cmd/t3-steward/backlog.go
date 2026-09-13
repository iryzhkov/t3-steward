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
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
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
  list [--project P] [--schedule S] [--progress STATES] [--class CLASS]
       [--worker W] [--quota-pool Q] [--include-sink] [--json]
  show <workflow-run> [--include-sink] [--json]
  graph <workflow-run> [--json|--dot]
  diagnose <workflow-run> [--json]
  task show <workflow-run>/<task> [--json]
  events <workflow-run> [--json]
  explain <workflow-run>/<task> [--json]
  artifacts [<task>|<workflow-run>/<task>] [--json]
  artifact show <artifact> [--json]
  artifact get <artifact> [--output PATH]
  commands [<workflow-run>[/<task>]] [--json]
  command show <command> [--json]

Graph amendments (all require --expected-revision N --request-id ID --reason TEXT):
  task add <run>/<name> --provider INSTANCE --model MODEL --prompt TEXT
      [--needs NAME,OTHER-RUN/TASK] [--options JSON] [--timeout DURATION]
      [--class required|surplus] [--max-turns N]
  task set <run>/<task> [--model MODEL] [--provider INSTANCE]
      [--options JSON] [--timeout DURATION]
  edge add|remove <run>/<task> --from <task|other-run/task>
  run clone --from <run>

Revision-fenced controls:
  start|resume|cancel|retry|skip <workflow-run>/<task> --reason TEXT [--command-id ID] [--json]
  delay <workflow-run>/<task> --until RFC3339 --reason TEXT [--command-id ID] [--json]
  pause <workflow-run>/<task> [--now] --reason TEXT [--command-id ID] [--json]
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
`

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

func isCoordinatorAdmin(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "submit", "status", "workers", "edge", "run", "diagnose", "graph", "task", "events", "explain", "artifacts", "artifact", "commands", "command", "show",
		"start", "delay", "pause", "resume", "cancel", "retry", "skip", "recover":
		return true
	case "list":
		return len(args) != 2 || args[1] != "--all"
	default:
		return false
	}
}

func runCoordinatorAdmin(cfg config.Config, args []string, schedules bool) error {
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	client := backlogadmin.LocalClient{
		Path:               socketPath,
		MaxResponseBytes:   int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes:   int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	cli := backlogAdminCLI{
		service:             client,
		mutator:             client,
		artifacts:           client,
		submissions:         client,
		scheduleDefinitions: client,
		recovery:            client,
		principal: backlogadmin.Principal{
			ID:    fmt.Sprintf("local:%d", os.Getuid()),
			Roles: []string{"local-admin"},
		},
		stdout: os.Stdout,
	}
	if schedules {
		return cli.runSchedules(context.Background(), args)
	}
	return cli.runBacklog(context.Background(), args)
}

func cmdSchedules(g globalFlags, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Print(schedulesUsage)
		return nil
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCoordinatorAdmin(cfg, args, true)
}

func cmdBacklog(g globalFlags, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Print(backlogUsage)
		return nil
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	if isCoordinatorAdmin(args) {
		return runCoordinatorAdmin(cfg, args, false)
	}
	if args[0] == "backup" {
		return runBacklogBackup(context.Background(), cfg, args[1:])
	}
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
