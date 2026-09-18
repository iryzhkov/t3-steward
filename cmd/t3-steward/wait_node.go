package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
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
	for _, arg := range args {
		if arg == "--native" || arg == "--task" || strings.HasPrefix(arg, "--task=") || arg == "--run" || strings.HasPrefix(arg, "--run=") || arg == "--node" || strings.HasPrefix(arg, "--node=") || strings.HasPrefix(arg, "nw-") || strings.HasPrefix(arg, "tw-") {
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
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
		task := fs.String("task", "", "run/task")
		run := fs.String("run", "", "run sink")
		node := fs.String("node", "", "<run>[/<task>]")
		state := fs.String("state", "", "node state")
		thread := fs.String("thread", "", "thread")
		name := fs.String("name", "", "name")
		id := fs.String("request-id", "nw-"+strings.TrimPrefix(newWaitID(), "w-"), "stable registration ID")
		timeout := fs.Duration("timeout", 24*time.Hour, "deadline")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
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
			return err
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
	case "list":
		op.Action = "list"
		for _, arg := range args[1:] {
			if arg != "--native" && arg != "--json" {
				return fmt.Errorf("unknown native wait list flag %q", arg)
			}
		}
	case "cancel", "run-now":
		for _, arg := range args[1:] {
			if arg != "--native" && arg != "--json" {
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
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if op.Action == "register" && len(result.Waits) == 1 && result.Waits[0].Delivery != "delivered" && result.Waits[0].Delivery != "cancelled" {
		// This command always prints its result as JSON, so the instruction to
		// the agent goes to stderr. On stdout it would leave a document no
		// strict reader can parse.
		fmt.Fprintln(os.Stderr, "End this turn now; the coordinator has registered the node wait.")
	}
	return nil
}
