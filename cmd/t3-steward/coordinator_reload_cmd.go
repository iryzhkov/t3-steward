package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

const coordinatorReloadUsage = `Usage: t3-steward coordinator reload [--json] [--wait DURATION]

Send SIGHUP to the coordinator running on this host and print the receipt it
writes for that signal. The coordinator re-reads its configuration file and
decides: accepted (the new backlog_v2 catalog and policy are active),
unchanged (the file's digest equals the effective one; nothing was replaced)
or rejected (the effective configuration and its digest are exactly what they
were, and the receipt says why: the error and, for a catalog change on a
worker with retained work, every blocking assignment with its attempt and the
command that unblocks it).

The verb runs on the coordinator host only: it finds the process through the
pid file the coordinator writes beside its receipt, under the state directory
(<state dir>/coordinator/coordinator.pid and reload-receipt.json). From any
other host, read the same receipt with "t3-steward coordinator identity --json"
(field lastReload).

Flags:
  --json          Print the receipt as one JSON document.
  --wait DURATION How long to wait for a receipt whose requestedAt is at or
                  after the signal (default 10s).

Exit codes:
  0  accepted or unchanged.
  8  rejected (class rejected); the receipt is printed first.
  6  no receipt newer than the signal within --wait (class timeout); the
     coordinator may still be activating, or it predates the receipt.
  5  no coordinator pid file, or the process is gone (class unavailable).

Idempotency: safe to repeat; each signal produces its own receipt.
`

// coordinatorReloadDocument is what "coordinator reload --json" prints: the
// receipt, with the version, kind and signal time around it.
type coordinatorReloadDocument struct {
	Version     string    `json:"version"`
	Kind        string    `json:"kind"`
	PID         int       `json:"pid"`
	SignalledAt time.Time `json:"signalledAt"`
	backlogadmin.ReloadReceipt
}

// reloadSignaller delivers SIGHUP to one process. Tests replace it with a
// fake that writes a receipt, or nothing.
type reloadSignaller interface {
	Signal(pid int) error
}

type processReloadSignaller struct{}

func (processReloadSignaller) Signal(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return process.Signal(syscall.SIGHUP)
}

func cmdCoordinatorReload(g globalFlags, args []string) error {
	if len(args) != 0 && isHelp(args[0]) {
		fmt.Print(coordinatorReloadUsage)
		return nil
	}
	fs := flag.NewFlagSet("coordinator reload", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print the receipt as JSON")
	wait := fs.Duration("wait", 10*time.Second, "how long to wait for the receipt")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("coordinator reload: %w", err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("coordinator reload takes no arguments (got %q)", fs.Args())
	}
	if *wait <= 0 {
		return errors.New("coordinator reload: --wait must be positive")
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCoordinatorReload(context.Background(), cfg, os.Stdout, *asJSON, *wait, processReloadSignaller{}, time.Now)
}

// readCoordinatorPID reads the pid file the coordinator writes at start.
func readCoordinatorPID(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("pid file %s does not hold a pid: %q", path, strings.TrimSpace(string(raw)))
	}
	return pid, nil
}

// runCoordinatorReload signals the local coordinator, waits for a receipt
// requested at or after the signal, prints it and returns the verdict as an
// exit class: nil for accepted and unchanged, rejected for rejected, timeout
// when no receipt arrives in time.
func runCoordinatorReload(ctx context.Context, cfg config.Config, out io.Writer, asJSON bool, wait time.Duration, signaller reloadSignaller, now func() time.Time) error {
	const operation = "coordinator reload"
	pidPath, err := coordinatorPIDPath(cfg)
	if err != nil {
		return err
	}
	receiptPath, err := coordinatorReloadReceiptPath(cfg)
	if err != nil {
		return err
	}
	pid, err := readCoordinatorPID(pidPath)
	if err != nil {
		return &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Operation: operation,
			Err: fmt.Errorf("no coordinator pid file at %s; is the coordinator running on this host? (%v)", pidPath, err)}
	}
	// The signal time is taken just before the signal, so a receipt requested
	// at or after it can only be the answer to this signal or a later one.
	signalledAt := now().UTC()
	if err := signaller.Signal(pid); err != nil {
		return &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Operation: operation,
			Err: fmt.Errorf("coordinator process %d named by %s cannot be signalled: %v", pid, pidPath, err)}
	}
	receipt, found, err := awaitReloadReceipt(ctx, receiptPath, signalledAt, wait)
	if err != nil {
		return err
	}
	if !found {
		return &backlogadmin.TransportError{Class: backlogadmin.ClassTimeout, Operation: operation,
			Err: fmt.Errorf("no reload receipt requested at or after %s appeared at %s within %s; the coordinator (pid %d) may still be activating, or it predates the receipt; re-run, or read the journal with journalctl --user -u t3-steward.service",
				signalledAt.Format(time.RFC3339Nano), receiptPath, wait, pid)}
	}
	if asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(coordinatorReloadDocument{
			Version: backlogadmin.Version, Kind: "coordinator-reload", PID: pid, SignalledAt: signalledAt, ReloadReceipt: receipt,
		}); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(out, "coordinator  pid %d, signalled at %s\n", pid, signalledAt.Format(time.RFC3339))
		writeReloadReceiptLines(out, "reload       ", "             ", receipt)
		fmt.Fprintf(out, "receipt      %s\n", receiptPath)
	}
	if receipt.Outcome == backlogadmin.ReloadRejected {
		message := receipt.Error
		if n := len(receipt.Blockers); n == 1 {
			message += "; 1 blocking assignment, see the receipt"
		} else if n > 1 {
			message += fmt.Sprintf("; %d blocking assignments, see the receipt", n)
		}
		return afterDocument(&backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: operation, Err: errors.New(message)})
	}
	return nil
}

// awaitReloadReceipt polls the receipt file until one requested at or after
// since appears, or wait elapses. A receipt older than the signal is the
// previous verdict and is ignored.
func awaitReloadReceipt(ctx context.Context, path string, since time.Time, wait time.Duration) (backlogadmin.ReloadReceipt, bool, error) {
	deadline := time.Now().Add(wait)
	interval := 100 * time.Millisecond
	if wait < interval {
		interval = wait / 4
		if interval <= 0 {
			interval = time.Millisecond
		}
	}
	for {
		receipt, err := readReloadReceipt(path)
		if err == nil && !receipt.RequestedAt.Before(since) {
			return receipt, true, nil
		}
		if !time.Now().Before(deadline) {
			return backlogadmin.ReloadReceipt{}, false, nil
		}
		select {
		case <-ctx.Done():
			return backlogadmin.ReloadReceipt{}, false, ctx.Err()
		case <-time.After(interval):
		}
	}
}
