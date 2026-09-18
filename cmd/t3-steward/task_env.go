package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const taskUsage = `Usage: t3-steward task <command> [flags]

Commands:
  run                Start one task on the fleet from this checkout, with the
                     project, ref, route, idempotency key and wake derived, and
                     be notified in this thread when it ends.
                     t3-steward task run --help is the whole contract.
  result             Collect a finished task: its final message and every output
                     it declared, written under ./.t3/results/<run>/<task>/.
                     Exits 0 succeeded, 2 failed or cancelled, 1 not terminal.
  env                Print the identity of the task this shell runs inside, as
                     "export NAME=value" lines for the six T3_STEWARD_* variables,
                     read from .t3-steward/task.env in the prepared workspace
                     (found from the current directory upwards). The variables
                     are not in the environment unless something exported them;
                     eval "$(t3-steward task env)" does that.
  env --get NAME     Print one value: revision, attempt, task, run, assignment,
                     thread, or the full variable name.

Exit 1 with a message when no identity record is found: this shell is not
inside a t3-steward task.
`

// taskIdentityShortNames maps the short names "task env --get" accepts to the
// variable each stands for. The full variable name is accepted as well.
var taskIdentityShortNames = map[string]string{
	"run":        domain.TaskWaitEnvWorkflowRunID,
	"task":       domain.TaskWaitEnvTaskID,
	"attempt":    domain.TaskWaitEnvAttemptID,
	"revision":   domain.TaskWaitEnvAttemptRevision,
	"assignment": domain.TaskWaitEnvAssignmentID,
	"thread":     domain.TaskWaitEnvThreadID,
}

// cmdTask dispatches the task family. "env" reads only the workspace identity
// record and needs no configuration; "run" and "result" reach the coordinator
// and take the shared global flags, which is why they are dispatched with them.
func cmdTask(g globalFlags, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		fmt.Print(taskUsage)
		return nil
	}
	switch args[0] {
	case "env":
		return runTaskEnv(args[1:], os.Getenv, os.Stdout)
	case "run":
		return cmdTaskRun(g, args[1:])
	case "result":
		return cmdTaskResult(g, args[1:])
	}
	return fmt.Errorf("%w %q; the commands are run, result and env (try task --help)", errUnknownTaskCommand, args[0])
}

// runTaskEnv prints the task identity the way wait resolves it: the injected
// environment when it is complete, otherwise the record the worker wrote into
// the workspace.
func runTaskEnv(args []string, getenv func(string) string, out io.Writer) error {
	get := ""
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--get" && i+1 < len(args):
			get = args[i+1]
			i++
		case strings.HasPrefix(args[i], "--get="):
			get = strings.TrimPrefix(args[i], "--get=")
		default:
			return fmt.Errorf("task env takes only --get NAME, not %q", args[i])
		}
	}
	values, err := taskIdentityValues(getenv)
	if err != nil {
		if errors.Is(err, errNotInsideTask) {
			return fmt.Errorf("no task identity: %s was not found in the current directory or any parent, and the T3_STEWARD_* variables are not set; this shell is not inside a t3-steward task", domain.TaskIdentityFile)
		}
		return err
	}
	if get != "" {
		name, ok := taskIdentityShortNames[get]
		if !ok {
			if _, known := values[get]; !known {
				return fmt.Errorf("task env --get: %q is not an identity name; use revision, attempt, task, run, assignment, thread or a T3_STEWARD_* variable name", get)
			}
			name = get
		}
		_, err := fmt.Fprintln(out, values[name])
		return err
	}
	for _, name := range domain.TaskWaitEnvironmentNames() {
		if _, err := fmt.Fprintf(out, "export %s=%s\n", name, shellWord(values[name])); err != nil {
			return err
		}
	}
	return nil
}

// shellWord quotes a value for a POSIX shell unless it is a plain word.
func shellWord(value string) string {
	plain := value != "" && strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_.:/@", r))
	}) < 0
	if plain {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
