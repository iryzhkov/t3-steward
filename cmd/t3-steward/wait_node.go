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
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// currentTaskWaitArgs reports whether these are the arguments of a task-bound
// wait. --task current names the task this process is executing, which is a
// different thing from --task <run>/<task>, an observation of another node.
func currentTaskWaitArgs(args []string) bool {
	for index, arg := range args {
		if arg == "--task=current" || arg == "-task=current" {
			return true
		}
		if (arg == "--task" || arg == "-task") && index+1 < len(args) && args[index+1] == "current" {
			return true
		}
	}
	return false
}

func nativeWaitArgs(args []string) bool {
	if len(args) != 0 && args[0] == "answer" {
		return true
	}
	for _, arg := range args {
		if arg == "--native" || arg == "--task" || strings.HasPrefix(arg, "--task=") || arg == "--run" || strings.HasPrefix(arg, "--run=") || arg == "--node" || strings.HasPrefix(arg, "--node=") || arg == "--quota" || strings.HasPrefix(arg, "--quota=") || strings.HasPrefix(arg, "nw-") || strings.HasPrefix(arg, "tw-") {
			return true
		}
	}
	return false
}
func cmdNodeWait(ctx context.Context, cfg config.Config, args []string) error {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	client := transport.client
	op := backlogadmin.NodeWaitOperation{Action: args[0]}
	// Text is the default here as it is everywhere else. These verbs used to
	// print a JSON document unconditionally, with the registration and request
	// blocks duplicated and every duration in nanoseconds, which is a document
	// an agent has to parse to learn one sentence.
	scope := waitListOptions{all: true}
	// A node wake is sent by the steward of the host the coordinator recorded on
	// the wait, so the caller's own host and the coordinator's release decide
	// both what is sent and what may honestly be printed afterwards.
	caller := localWakeHost(nil)
	release := ""
	switch args[0] {
	case "add":
		if coordinatorWaitArgs(args[1:]) {
			return cmdCoordinatorWaitAdd(ctx, cfg, client, coordinatorWaitRelease(transport), args[1:])
		}
		fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
		task := fs.String("task", "", "run/task")
		run := fs.String("run", "", "run sink")
		node := fs.String("node", "", "<run>[/<task>]")
		state := fs.String("state", "", "node state")
		thread := fs.String("thread", "", "thread")
		name := fs.String("name", "", "name")
		id := fs.String("request-id", "nw-"+strings.TrimPrefix(newWaitID(), "w-"), "stable registration ID")
		timeout := fs.Duration("timeout", 24*time.Hour, "deadline")
		asJSON := fs.Bool("json", false, "print the registration as JSON")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		scope.asJSON = *asJSON
		if *task == "current" {
			return errors.New("--task current is a task-bound wait and is routed before this point")
		}
		named := 0
		for _, value := range []string{*task, *run, *node} {
			if value != "" {
				named++
			}
		}
		if fs.NArg() != 0 || named != 1 {
			return errors.New("a node wait names exactly one of --node <run>[/<task>], --task <run>/<task> or --run <run>, and takes no shell command")
		}
		var ref domain.NodeRef
		switch {
		case *run != "":
			ref = domain.NodeRef{RunID: *run, TaskID: domain.SinkTaskName}
		case *node != "":
			ref, err = parseNodeTarget(*node)
			if err != nil {
				return err
			}
		default:
			ref, err = domain.ParseNodeRef(*task)
			if err != nil {
				return err
			}
		}
		nodeState, err := domain.ParseNodeWaitState(*state)
		if err != nil {
			return err
		}
		threadID, err := resolveThread(cfg, *thread)
		if err != nil {
			return refuseWaitThread("wait add", "<the rest of this call>", err)
		}
		logger := newLogger("error")
		t3, _, err := connectForCallerThread(cfg, logger)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
		defer cancel()
		target, err := t3control.New(t3, logger, true).GetThread(cctx, threadID)
		if err != nil {
			return err
		}
		if target == nil || target.ArchivedAt != nil {
			return errors.New("wait thread is unavailable on this host")
		}
		if *name == "" {
			*name = ref.String()
		}
		op.Action = "register"
		op.Request = domain.NodeWaitRequest{ID: *id, ThreadID: threadID, Name: *name, Target: ref, Timeout: *timeout}
		if nodeState != domain.NodeStateTerminal {
			// The default is left empty so a registration from an older client
			// and one from this client replay as the same request.
			op.Request.State = nodeState
		}
		release = coordinatorWaitRelease(transport)(ctx)
		op.Host = statedWakeHost(release, caller)
	case "list":
		op.Action = "list"
		// The raw inventory measured 99,630 bytes and 62 waits on one host,
		// covering every thread and every host including long-delivered
		// ones, and it refused to be scoped. It is still the raw inventory;
		// it can now be asked a narrower question.
		fs := flag.NewFlagSet("wait list --native", flag.ContinueOnError)
		fs.Bool("native", false, "read the coordinator-held waits")
		nativeThread := fs.String("thread", "", "thread id")
		nativeHost := fs.String("host", "", "delivery host")
		asJSON := fs.Bool("json", false, "print the inventory as JSON")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("wait list --native takes no arguments (got %q)", strings.Join(fs.Args(), " "))
		}
		scope.thread, scope.host, scope.asJSON = *nativeThread, *nativeHost, *asJSON
	case "answer":
		fs := flag.NewFlagSet("wait answer", flag.ContinueOnError)
		decisionID := fs.String("decision-id", "decision-"+strings.TrimPrefix(newWaitID(), "w-"), "stable decision ID")
		waitID := fs.String("wait", "", "attention wait ID")
		requestID := fs.String("request-id", "", "attention request ID")
		runID := fs.String("run", "", "workflow run ID")
		taskID := fs.String("task-id", "", "task ID")
		attemptID := fs.String("attempt", "", "attempt ID")
		assignmentID := fs.String("assignment", "", "assignment ID")
		threadID := fs.String("thread", "", "thread ID")
		revisionText := fs.String("revision", "", "registered revision")
		kind := fs.String("decision", "", "approve, resume, hold, stop or change")
		reason := fs.String("reason", "", "decision reason")
		change := fs.String("change", "", "proposed change")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("wait answer takes flags only")
		}
		revision, err := strconv.ParseInt(*revisionText, 10, 64)
		if err != nil {
			return fmt.Errorf("--revision must be an integer: %w", err)
		}
		decision := domain.AttentionDecision{
			ID: *decisionID, WaitID: *waitID, RequestID: *requestID, WorkflowRunID: *runID,
			TaskID: *taskID, AttemptID: *attemptID, AssignmentID: *assignmentID, ThreadID: *threadID,
			RegisteredRevision: revision, Kind: domain.AttentionDecisionKind(*kind), Reason: *reason, Change: *change,
		}
		if err := decision.Validate(); err != nil {
			return err
		}
		op.Action, op.Decision = "decide-attention", &decision
	case "cancel", "run-now":
		for _, arg := range args[1:] {
			if arg == "--json" {
				scope.asJSON = true
				continue
			}
			if arg != "--native" {
				if op.ID != "" {
					return errors.New("one native wait ID required")
				}
				op.ID = arg
			}
		}
		if op.ID == "" {
			return errors.New("native wait ID required")
		}
		if strings.HasPrefix(op.ID, "tw-") {
			if op.Action == "run-now" {
				return errors.New("task-bound checks run on their owning worker; run-now is not supported for a task-bound wait")
			}
			op.Action = "cancel-task"
		}
	default:
		return errors.New("unknown native wait command")
	}
	result, err := client.NodeWait(ctx, op)
	if err != nil {
		return err
	}
	if op.Action == "decide-attention" {
		if result.AttentionReceipt == nil {
			return errors.New("the coordinator returned no attention receipt")
		}
		return json.NewEncoder(os.Stdout).Encode(result.AttentionReceipt)
	}
	if op.Action == "cancel-task" && len(result.TaskWaits) == 1 {
		// The coordinator has settled the wait; the local check on this host,
		// if there is one, is now polling for nothing. Failing to mark it is
		// reported, not fatal: the coordinator's outcome already stands and a
		// later local settlement cannot overwrite it.
		settled := result.TaskWaits[0]
		localID, err := cancelLocalTaskCheck(ctx, cfg, settled)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: the local check bound to %s could not be marked cancelled: %v\n", settled.ID, err)
		}
		// This command prints its result as JSON, so the human report goes to
		// stderr.
		reportTaskWaitCancelled(os.Stderr, localID, settled)
	}
	if op.Action == "list" {
		tasks, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"})
		if err != nil {
			return err
		}
		result.TaskWaits = tasks.TaskWaits
	}
	if err := renderNativeWaitResult(os.Stdout, result, scope); err != nil {
		return err
	}
	if op.Action == "register" && len(result.Waits) == 1 {
		reportNodeWaitRegistration(os.Stderr, result.Waits[0], caller, release, nodeWakeDeliveryFor(cfg), time.Now())
	}
	return nil
}
