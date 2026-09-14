package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
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

func resolveTaskIdentity(getenv func(string) string) (taskIdentity, error) {
	var identity taskIdentity
	values := make(map[string]string, 6)
	for _, name := range domain.TaskWaitEnvironmentNames() {
		value := strings.TrimSpace(getenv(name))
		if value == "" {
			return identity, errNotInsideTask
		}
		values[name] = value
	}
	revision, err := strconv.ParseInt(values[domain.TaskWaitEnvAttemptRevision], 10, 64)
	if err != nil {
		return identity, fmt.Errorf("%s is not a revision number: %w", domain.TaskWaitEnvAttemptRevision, err)
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

// cmdTaskWaitAdd registers a task-bound wait: the coordinator parks this
// attempt, the worker collects nothing, and the steward resumes the same thread
// and the same attempt when the condition settles.
func cmdTaskWaitAdd(ctx context.Context, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("wait add --task current", flag.ContinueOnError)
	task := fs.String("task", "", "current")
	name := fs.String("name", "", "what is being waited for")
	every := fs.Duration("every", 30*time.Second, "first poll interval")
	maxEvery := fs.Duration("max-every", 10*time.Minute, "backoff ceiling")
	timeout := fs.Duration("timeout", 24*time.Hour, "maximum duration of the wait")
	runTimeout := fs.Duration("run-timeout", time.Minute, "bound one run of the check")
	dir := fs.String("dir", "", "working directory for the check")
	wakeMode := fs.String("wake", "each", "each or all")
	requestID := fs.String("request-id", "", "stable registration ID for safe retries")
	asJSON := fs.Bool("json", false, "print the registered wait as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *task != "current" {
		return errors.New("cmdTaskWaitAdd requires --task current")
	}
	command := fs.Args()
	if len(command) > 0 && command[0] == "--" {
		command = command[1:]
	}
	if len(command) == 0 {
		return errors.New("--task current needs the condition to poll after --, for example: -- gh run view 123 --json status --jq '.status==\"completed\"'")
	}
	if *wakeMode != "each" && *wakeMode != "all" {
		return errors.New("--wake must be each or all")
	}
	if *every < 30*time.Second {
		return errors.New("--every must be at least 30s")
	}
	if *maxEvery < *every {
		return errors.New("--max-every must not be shorter than --every")
	}
	if *timeout <= *every {
		return errors.New("--timeout must be longer than --every")
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		return err
	}
	if *dir == "" {
		*dir, _ = os.Getwd()
	}
	if *name == "" {
		*name = strings.Join(command, " ")
		if len(*name) > 60 {
			*name = (*name)[:60]
		}
	}
	if *requestID == "" {
		*requestID = strings.TrimPrefix(newWaitID(), "w-")
	}

	local := wait.Wait{
		ID: newWaitID(), ThreadID: identity.ThreadID, Name: *name, Command: command, Dir: *dir,
		Every: *every, MaxEvery: *maxEvery, Timeout: *timeout, RunTimeout: *runTimeout,
		Wake: wait.WakeMode(*wakeMode), Status: wait.StatusWaiting, CreatedAt: time.Now(),
	}
	// The check is proven to run before the attempt is parked. A condition that
	// cannot run, gives up at once, or is already true would otherwise park a
	// task for a wake that is either immediate or never coming.
	out, code, err := runCheck(ctx, local)
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "%s", out)
		return fmt.Errorf("check cannot run: %v", err)
	case code == 0:
		fmt.Fprintf(os.Stderr, "%s", out)
		return errors.New("the check already exits 0: the condition is met, so there is nothing to park for")
	case code == 2:
		fmt.Fprintf(os.Stderr, "%s", out)
		return errors.New("the check exits 2 (give up) right away; fix it before registering")
	}

	path, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	client := backlogadmin.LocalClient{
		Path: path, MaxResponseBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxArtifactBytes: cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	// The coordinator record is written first. It is what parks the attempt, so
	// a failure here must leave no local poll behind that nothing is waiting on.
	// The reverse order would let the attempt keep running while a check quietly
	// polled for it.
	response, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{
		Action: "register-task",
		Task: &domain.TaskWaitRegistration{
			RequestID: *requestID, WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID,
			AttemptID: identity.AttemptID, ExpectedRevision: identity.AttemptRevision,
			ThreadID: identity.ThreadID, Wake: domain.WakeMode(*wakeMode), MaxDuration: *timeout,
			Name: *name, Condition: strings.Join(command, " "),
		},
	})
	if err != nil {
		return err
	}
	if len(response.TaskWaits) != 1 {
		return errors.New("the coordinator did not return exactly one task-bound wait")
	}
	registered := response.TaskWaits[0]

	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now()
	local.TaskWaitID = registered.ID
	local.LastRunAt = &now
	local.Runs = 1
	local.LastExit = code
	local.LastOutput = out
	if err := store.SaveWait(ctx, local); err != nil {
		// The attempt is parked and the coordinator owns its maximum duration,
		// so an unpolled wait expires with a structured timeout rather than
		// stranding the task. Report the failure honestly instead of implying
		// the condition will be checked.
		return fmt.Errorf("task-bound wait %s is registered and this attempt is parked, but the local check could not be saved, so the wait will only settle when it times out after %s: %w",
			registered.ID, registered.MaxDuration, err)
	}
	if *asJSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(registered)
	}
	fmt.Printf("task-bound wait %s registered for attempt %s on thread %s: first check exited %d; polling every %s, backing off to %s, giving up after %s.\n",
		registered.ID, registered.AttemptID, registered.ThreadID, code, *every, *maxEvery, *timeout)
	fmt.Println("This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Println("until the steward resumes this same thread with the outcome.")
	return nil
}
