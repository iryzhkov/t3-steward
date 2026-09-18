package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// taskIdentity is the execution identity injected into a task process. It is
// the only way a running task can name itself: the full identity otherwise
// stops at the worker.
type taskIdentity struct {
	WorkflowRunID   string
	TaskID          string
	AttemptID       string
	AttemptRevision int64
	AssignmentID    string
	ThreadID        string
}

// errNotInsideTask is what `--task current` says when it is used from an
// ordinary interactive session. It names the difference rather than silently
// registering an interactive wait, because the two kinds do different things:
// one parks a workflow attempt, the other only wakes a thread.
var errNotInsideTask = errors.New(
	"--task current is only valid inside a t3-steward task: no injected execution identity " +
		"(T3_STEWARD_ATTEMPT_ID and friends). From an interactive session, register an ordinary " +
		"wait with `t3-steward wait add -- <command>`, or name a task explicitly with --task <run>/<task>")

// resolveTaskIdentity finds the execution identity of the task this process is
// executing: from the injected environment when a contained execution provided
// it, otherwise from the record the worker wrote into the prepared workspace.
//
// The environment wins when both are present. It is the more specific of the
// two: a sandbox sets it for exactly one execution, while a workspace can in
// principle be reached from a shell that belongs to another.
func resolveTaskIdentity(getenv func(string) string) (taskIdentity, error) {
	values, err := taskIdentityValues(getenv)
	if err != nil {
		return taskIdentity{}, err
	}
	revision, err := strconv.ParseInt(values[domain.TaskWaitEnvAttemptRevision], 10, 64)
	if err != nil {
		return taskIdentity{}, fmt.Errorf("%s is not a revision number: %w", domain.TaskWaitEnvAttemptRevision, err)
	}
	return taskIdentity{
		WorkflowRunID:   values[domain.TaskWaitEnvWorkflowRunID],
		TaskID:          values[domain.TaskWaitEnvTaskID],
		AttemptID:       values[domain.TaskWaitEnvAttemptID],
		AttemptRevision: revision,
		AssignmentID:    values[domain.TaskWaitEnvAssignmentID],
		ThreadID:        values[domain.TaskWaitEnvThreadID],
	}, nil
}

func taskIdentityValues(getenv func(string) string) (map[string]string, error) {
	values := make(map[string]string, 6)
	complete := true
	for _, name := range domain.TaskWaitEnvironmentNames() {
		value := strings.TrimSpace(getenv(name))
		if value == "" {
			complete = false
			continue
		}
		values[name] = value
	}
	if complete {
		return values, nil
	}
	fileValues, err := readTaskIdentityFile()
	if err != nil {
		return nil, err
	}
	return fileValues, nil
}

// readTaskIdentityFile reads the worker-written identity record from the
// current directory or an ancestor, the way a tool finds the repository it is
// inside.
//
// The file is accepted only as a private regular file owned by this user. It
// decides which attempt a command speaks for, so a copy anyone could have
// written, or a symlink pointing somewhere else, is refused rather than read.
func readTaskIdentityFile() (map[string]string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return nil, errNotInsideTask
	}
	for {
		path := filepath.Join(directory, filepath.FromSlash(domain.TaskIdentityFile))
		info, statErr := os.Lstat(path)
		switch {
		case statErr != nil:
		case !info.Mode().IsRegular():
			return nil, fmt.Errorf("%s is not a regular file; refusing to read a task identity from it", path)
		case info.Mode().Perm() != 0o600:
			return nil, fmt.Errorf("%s has mode %04o, want 0600; refusing to read a task identity that is not private", path, info.Mode().Perm())
		default:
			if err := taskIdentityFileIsOwned(info); err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil, readErr
			}
			values, parseErr := domain.ParseTaskIdentityFile(string(content))
			if parseErr != nil {
				return nil, fmt.Errorf("%s: %w", path, parseErr)
			}
			return values, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return nil, errNotInsideTask
		}
		directory = parent
	}
}

// taskWaitRequestID settles the registration id of a task-bound wait. With no
// --request-id it is derived from the resolved identity, "park-<attempt>-
// <revision>", which is stable for a retry of the same park and different for
// every later park of the same task; the random id remains only for an
// identity with no attempt, which registration refuses anyway. An explicit id
// that ends in "-" is almost always a shell variable that was empty when the
// command line was built, so it is warned about, but still used: a repeated
// registration must keep returning the same wait.
func taskWaitRequestID(explicit string, identity taskIdentity, warnings io.Writer) string {
	if explicit == "" {
		if identity.AttemptID == "" {
			return strings.TrimPrefix(newWaitID(), "w-")
		}
		return fmt.Sprintf("park-%s-%d", identity.AttemptID, identity.AttemptRevision)
	}
	if strings.HasSuffix(explicit, "-") {
		fmt.Fprintf(warnings, "warning: --request-id %q ends in \"-\", which usually means an empty shell variable was interpolated into it; "+
			"the identity is in .t3-steward/task.env, not the environment (unless t3.send_thread_environment is on), so use $(t3-steward task env --get revision) or omit --request-id to derive park-%s-%d\n",
			explicit, identity.AttemptID, identity.AttemptRevision)
	}
	return explicit
}

func cmdTaskWaitAdd(ctx context.Context, cfg config.Config, args []string) error {
	now := time.Now()
	spec, err := parseLocalWaitSpec(args, now)
	if err != nil {
		return err
	}
	if spec.Thread != "" || spec.Group != "" {
		return errors.New("--thread and --group do not apply to --task current: the wait is bound to this task's own thread, and --wake all is scoped to the attempt")
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		return err
	}
	if spec.Dir == "" {
		spec.Dir, _ = os.Getwd()
	}
	spec.RequestID = taskWaitRequestID(spec.RequestID, identity, os.Stderr)

	local := wait.Wait{
		ID: newWaitID(), ThreadID: identity.ThreadID, Name: spec.Name, Kind: spec.Kind, Command: spec.Command, Dir: spec.Dir,
		At: spec.At, OrTimeout: spec.OrTimeout,
		Every: spec.Every, MaxEvery: spec.MaxEvery, Timeout: spec.Timeout, RunTimeout: spec.RunTimeout,
		Wake: wait.WakeMode(spec.WakeMode), Status: wait.StatusWaiting, CreatedAt: now,
	}
	// The check is proven to run before the attempt is parked. A condition that
	// cannot run, gives up at once, or is already true would otherwise park a
	// task for a wake that is either immediate or never coming. A time wait
	// was already checked to lie in the future.
	code, firstLine, out := 1, "", ""
	if spec.Kind == domain.WaitKindShell {
		var probeErr error
		out, code, probeErr = runCheck(ctx, local)
		if err := refuseFirstRun(os.Stderr, out, code, probeErr, "so there is nothing to park for"); err != nil {
			return err
		}
		firstLine = firstOutputLine(out)
	}

	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	client := transport.client
	// The coordinator record is written first. It is what parks the attempt, so
	// a failure here must leave no local poll behind that nothing is waiting on.
	// The reverse order would let the attempt keep running while a check quietly
	// polled for it.
	response, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{
		Action: "register-task",
		Task: &domain.TaskWaitRegistration{
			RequestID: spec.RequestID, WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID,
			AttemptID: identity.AttemptID, IssuedRevision: identity.AttemptRevision,
			ThreadID: identity.ThreadID, Wake: domain.WakeMode(spec.WakeMode), MaxDuration: spec.Timeout,
			Name: spec.Name, Condition: spec.Condition, Kind: spec.Kind, OrTimeout: spec.OrTimeout,
		},
	})
	if err != nil {
		return err
	}
	if len(response.TaskWaits) != 1 {
		return errors.New("the coordinator did not return exactly one task-bound wait")
	}
	registered := response.TaskWaits[0]
	if !registered.Live() {
		// The message below tells the agent to end its turn. It must never be
		// printed for a wait that is not holding the attempt, or the turn ends,
		// the worker collects, and verification runs against outputs that were
		// never written.
		return fmt.Errorf("task-bound wait %s is already settled, so this task is not parked; register a new wait with a different --request-id", registered.ID)
	}

	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	local.TaskWaitID = registered.ID
	// A registration retry must never create a second poll or reset a settled one.
	local.ID = "w-" + registered.ID
	if spec.Kind == domain.WaitKindShell {
		ran := time.Now()
		local.LastRunAt = &ran
		local.Runs = 1
		local.LastExit = code
		local.LastOutput = out
	}
	if err := saveRegisteredTaskCheck(ctx, store, local); err != nil {
		// The attempt is parked and the coordinator owns its maximum duration,
		// so an unpolled wait expires with a structured timeout rather than
		// stranding the task. Report the failure honestly instead of implying
		// the condition will be checked.
		return fmt.Errorf("task-bound wait %s is registered and this attempt is parked, but the local check could not be saved, so the wait will only settle when it times out after %s: %w",
			registered.ID, registered.MaxDuration, err)
	}
	if spec.Kind == domain.WaitKindShell {
		warnUnconventionalFirstExit(os.Stderr, code, firstLine)
	}
	if spec.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(struct {
			domain.TaskWait
			FirstExit       int    `json:"firstExit"`
			FirstOutputLine string `json:"firstOutputLine"`
		}{registered, code, firstLine}); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "This task is now parked. End this turn now: nothing is collected and nothing is verified")
		fmt.Fprintln(os.Stderr, "until the steward resumes this same thread with the outcome.")
		return nil
	}
	fmt.Printf("task-bound wait %s (%s) registered for attempt %s on thread %s: %s.\n",
		registered.ID, spec.Kind, registered.AttemptID, registered.ThreadID, spec.registrationSummary(code, firstLine))
	fmt.Println("This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Println("until the steward resumes this same thread with the outcome.")
	return nil
}
