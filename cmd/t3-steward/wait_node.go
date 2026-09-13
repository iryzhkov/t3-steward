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

func nativeWaitArgs(args []string) bool {
	for _, arg := range args {
		if arg == "--native" || arg == "--task" || strings.HasPrefix(arg, "--task=") || arg == "--run" || strings.HasPrefix(arg, "--run=") || strings.HasPrefix(arg, "nw-") {
			return true
		}
	}
	return false
}
func cmdNodeWait(ctx context.Context, cfg config.Config, args []string) error {
	path, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	client := backlogadmin.LocalClient{
		Path: path, MaxResponseBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxArtifactBytes: cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	op := backlogadmin.NodeWaitOperation{Action: args[0]}
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
		task := fs.String("task", "", "run/task")
		run := fs.String("run", "", "run sink")
		thread := fs.String("thread", "", "thread")
		name := fs.String("name", "", "name")
		id := fs.String("request-id", "nw-"+strings.TrimPrefix(newWaitID(), "w-"), "stable registration ID")
		timeout := fs.Duration("timeout", 24*time.Hour, "deadline")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 || (*task == "") == (*run == "") {
			return errors.New("native wait requires exactly one --task or --run and no shell command")
		}
		var ref domain.NodeRef
		if *run != "" {
			ref = domain.NodeRef{RunID: *run, TaskID: domain.SinkTaskName}
		} else {
			ref, err = domain.ParseNodeRef(*task)
			if err != nil {
				return err
			}
		}
		threadID, err := resolveThread(cfg, *thread)
		if err != nil {
			return err
		}
		logger := newLogger("error")
		t3, _, err := connect(cfg, logger)
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
	case "list":
		op.Action = "list"
		for _, arg := range args[1:] {
			if arg != "--native" && arg != "--json" {
				return fmt.Errorf("unknown native wait list flag %q", arg)
			}
		}
	case "cancel", "run-now":
		for _, arg := range args[1:] {
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
	default:
		return errors.New("unknown native wait command")
	}
	result, err := client.NodeWait(ctx, op)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if op.Action == "register" && len(result.Waits) == 1 && result.Waits[0].Delivery != "delivered" && result.Waits[0].Delivery != "cancelled" {
		fmt.Println("End this turn now; the coordinator has registered the node wait.")
	}
	return nil
}
