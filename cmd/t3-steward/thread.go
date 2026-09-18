package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const threadUsage = `Usage: t3-steward thread <command> [flags]

Operate on one T3 thread on this host through the same control client the
watchdog uses. There is no coordinator involved: this is the operator's hand
on the local T3 server, for a session nothing else owns any more.

Commands:
  stop <thread-id> [--session]   Dispatch thread.turn.interrupt for the thread's
                                 running turn; with --session also dispatch
                                 thread.session.stop so the provider process ends.

A thread that is not running is reported as such and, without --session, left
alone. The dry-run setting of the configuration applies: with policy.dry_run
the commands are logged, not sent.
`

// threadStopper is the part of the T3 control client thread stop needs.
type threadStopper interface {
	GetThread(context.Context, string) (*domain.Thread, error)
	StopThread(context.Context, domain.Thread, t3control.StopMode) error
}

func cmdThread(g globalFlags, args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Print(threadUsage)
		return nil
	}
	if args[0] != "stop" {
		return fmt.Errorf("unknown thread command %q; see t3-steward thread --help", args[0])
	}
	threadID, session, err := parseThreadStop(args[1:])
	if err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	client, _, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	control := t3control.New(client, logger, cfg.Policy.DryRun)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.T3.RequestTimeout.D())
	defer cancel()
	return stopThread(ctx, control, threadID, session, os.Stdout)
}

func parseThreadStop(args []string) (string, bool, error) {
	var threadID string
	session := false
	for _, arg := range args {
		switch {
		case arg == "--session":
			session = true
		case strings.HasPrefix(arg, "-"):
			return "", false, fmt.Errorf("unknown thread stop flag %q", arg)
		case threadID != "":
			return "", false, errors.New("thread stop takes exactly one thread id")
		default:
			threadID = arg
		}
	}
	if strings.TrimSpace(threadID) == "" || strings.TrimSpace(threadID) != threadID {
		return "", false, errors.New("thread stop usage: thread stop <thread-id> [--session]")
	}
	return threadID, session, nil
}

// stopThread interrupts the thread's running turn and, when asked, stops its
// session, printing each dispatch so the operator sees what was sent.
func stopThread(ctx context.Context, control threadStopper, threadID string, session bool, out io.Writer) error {
	thread, err := control.GetThread(ctx, threadID)
	if err != nil {
		return fmt.Errorf("read thread %s: %w", threadID, err)
	}
	if thread == nil {
		return fmt.Errorf("thread %s not found on this host's T3 server", threadID)
	}
	fmt.Fprintf(out, "thread: %s\ntitle: %s\nrunning: %t\nturn: %s (%s)\n", thread.ID, thread.Title, thread.Running, thread.TurnID, thread.TurnState)
	if thread.Running {
		if err := control.StopThread(ctx, *thread, t3control.StopInterrupt); err != nil {
			return err
		}
		fmt.Fprintln(out, "dispatched: thread.turn.interrupt")
	} else {
		fmt.Fprintln(out, "no running turn; thread.turn.interrupt not sent")
	}
	if session {
		if err := control.StopThread(ctx, *thread, t3control.StopSession); err != nil {
			return err
		}
		fmt.Fprintln(out, "dispatched: thread.session.stop")
	}
	return nil
}
