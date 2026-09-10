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

Coordinator read commands:
  status [--json]
  list [--project P] [--schedule S] [--progress STATES] [--class CLASS]
       [--worker W] [--quota-pool Q] [--json]
  show <workflow-run> [--json]
  graph <workflow-run> [--json]
  task show <workflow-run>/<task> [--json]
  events <workflow-run> [--json]
  explain <workflow-run>/<task> [--json]
  artifacts [<task>|<workflow-run>/<task>] [--json]
  artifact show <artifact> [--json]
  commands [<workflow-run>[/<task>]] [--json]
  command show <command> [--json]

Legacy task-file helpers:
  new <id>           Create a task file from a template and print its path.
  path               Print the task directory.
  check <file|->     Validate a task: project, provider instance, model, options, host.
  receive <id>       Store a task sent by another host (used by forwarding).
  list --all         Show the legacy local task files and configured remote lists.
  retry|cancel <id>  Legacy state controls; these will migrate to admin commands.

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
		Dir:             dir,
		Preamble:        cfg.Backlog.Preamble,
		QuietFor:        cfg.Backlog.QuietFor.D(),
		SafetyMargin:    cfg.Backlog.SafetyMargin,
		FallbackPerHour: cfg.Backlog.FallbackPerHour,
		Quantile:        cfg.Backlog.Quantile,
		MinSamples:      cfg.Backlog.MinSamples,
		LongWindowCap:   cfg.Backlog.LongWindowCap,
		HistoryDays:     cfg.Backlog.HistoryDays,
		DryRun:          cfg.Policy.DryRun,
		Logger:          logger,
		LocalHost:       localHostName(cfg),
		DefaultHost:     cfg.Backlog.DefaultHost,
		Forward:         forwardTask,
		DataDir:         dataDir,
	}, store, control), nil
}

func isCoordinatorRead(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "status", "graph", "task", "events", "explain", "artifacts", "artifact", "commands", "command", "show":
		return true
	case "list":
		return len(args) != 2 || args[1] != "--all"
	default:
		return false
	}
}

func runCoordinatorRead(cfg config.Config, args []string, schedules bool) error {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		return err
	}
	cli := backlogAdminCLI{
		service: service,
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
	return runCoordinatorRead(cfg, args, true)
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
	if isCoordinatorRead(args) {
		return runCoordinatorRead(cfg, args, false)
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
	case "retry", "cancel":
		if len(args) != 2 {
			return fmt.Errorf("%s needs a task id", args[0])
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
		st, ok := states[args[1]]
		if !ok {
			return fmt.Errorf("no state for task %q (has the runner seen it?)", args[1])
		}
		if args[0] == "retry" {
			st.Status, st.Reason, st.ThreadID, st.Turns = backlog.StatusPending, "re-queued manually", "", 0
		} else {
			if st.Status == backlog.StatusRunning {
				return errors.New("task is running; interrupt its thread in T3 first")
			}
			st.Status, st.Reason = backlog.StatusCancelled, "cancelled manually"
		}
		st.UpdatedAt = time.Now()
		if err := store.SaveTaskState(ctx, st.ID, string(st.Status), st); err != nil {
			return err
		}
		fmt.Printf("%s: %s\n", st.ID, st.Status)
		fmt.Fprintln(os.Stderr, "note: a running daemon picks the change up on its next poll; the change takes effect then")
		return nil
	default:
		return fmt.Errorf("unknown backlog command %q", args[0])
	}
}
