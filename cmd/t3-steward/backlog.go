package main

import (
	"context"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/config"
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
      Authenticated, reason-bound cleanup of one retained quarantine marker.
      Clearing it never retries a historical file or reenables intake.
  recover <assignment> --outcome stopped|failed --coordinator-epoch N
      --assignment-epoch N --attempt-revision N --evidence-id ID
      --evidence-sha256 HEX --reason TEXT [--recovery-id ID] [--json]

Retired legacy task-file commands (always refuse without file or SSH effects):
  new <id>
  path
  check <file|->
  receive <id>
  list --all

Markdown intake has been removed. Use "task run" or "campaign submit".
Both legacy enable flags accept only false/default; true is a configuration error.

Example:
  t3-steward backlog submit ./bundle.tar --idempotency-key 2026-09-14-upkeeper --json
` + coordinatorTransportHelp

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
		return !retiredBacklogCommand(args)
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
	if retiredBacklogCommand(args) {
		return backlogLegacyRoute(config.Config{}, args)
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

// retiredBacklogCommand refuses file verbs before configuration or transport access.
func retiredBacklogCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "new", "path", "check", "receive":
		return true
	case "list":
		for _, arg := range args[1:] {
			if arg == "--all" {
				return true
			}
		}
	}
	return false
}

// runBacklogLegacy retains actionable refusals for old command lines.
// It never resolves directories, reads stdin, opens state, or contacts a host.
func runBacklogLegacy(_ config.Config, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("backlog command required")
	}
	switch args[0] {
	case "new", "path", "check", "receive", "list":
		return fmt.Errorf("legacy Markdown file intake is retired (backlog %s); use t3-steward task run or campaign submit", args[0])
	default:
		return fmt.Errorf("unknown backlog command %q", args[0])
	}
}
