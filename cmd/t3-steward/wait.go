package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

const waitUsage = `Usage: t3-steward wait <command> [flags]

Park a T3 thread until an external condition holds. The agent registers a
check, ends its turn, and the steward runs the check periodically; when it
settles the steward wakes the thread with a message that starts the next
turn, carrying the outcome and the check's last output.

Commands:
  add --task <run>/<task> [--thread ID] [--name TEXT] [--timeout 24h] [--request-id ID]
  add --run <run> [flags]      Native wait on the run sink; no shell command.
  list --native [--json]       Native waits, outcomes and delivery state.
  cancel|run-now <nw-id>       Control a native wait through the admin socket.
  add [flags] -- <command...>   Register a shell check (run once first; see below).
  list [--thread ID] [--all]    Waits of this thread, or of every thread.
  cancel <id>                   Cancel a wait.
  run-now <id>                  Run a wait's check immediately.

add flags:
  --name TEXT        What is being waited for (shown in the wake message).
  --every DURATION   First poll interval (default 30s, minimum 30s); doubles
                     after every "not yet" up to --max-every (default 10m).
  --timeout DURATION Give up after this long (default 24h).
  --run-timeout DUR  Bound one run of the check (default 1m).
  --thread ID        T3 thread to wake (default: resolved from
                     CLAUDE_CODE_SESSION_ID of the calling agent).
  --dir PATH         Working directory for the check (default: current).
  --group NAME       Group with other waits of the same thread.
  --wake each|all    Wake per wait (default) or once the whole group settled.

Check protocol: exit 0 = condition met, wake. Exit 2 = give up, wake with
failure. Any other exit = not yet, keep polling. Timeout = wake with
"timed out". The check is run once at registration: a command that cannot
run, exits 2, or already exits 0 is not registered.
`

func cmdWait(g globalFlags, args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Print(waitUsage)
		return nil
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	if nativeWaitArgs(args) {
		return cmdNodeWait(context.Background(), cfg, args)
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
	ctx := context.Background()
	switch args[0] {
	case "add":
		return cmdWaitAdd(ctx, cfg, store, args[1:])
	case "list":
		fs := flag.NewFlagSet("wait list", flag.ContinueOnError)
		thread := fs.String("thread", "", "thread id")
		all := fs.Bool("all", false, "every thread")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *thread == "" && !*all {
			*thread, _ = resolveThread(cfg, "")
		}
		waits, err := store.ListWaits(ctx, *thread)
		if err != nil {
			return err
		}
		if len(waits) == 0 {
			fmt.Println("No waits.")
			return nil
		}
		fmt.Printf("%-10s %-36s %-10s %-6s %-8s %s\n", "id", "thread", "status", "runs", "exit", "name / command")
		for _, w := range waits {
			fmt.Printf("%-10s %-36s %-10s %-6d %-8d %s: %s\n", w.ID, w.ThreadID, w.Status, w.Runs, w.LastExit, w.Name, strings.Join(w.Command, " "))
			if w.Reason != "" {
				fmt.Printf("%-10s %s\n", "", w.Reason)
			}
		}
		return nil
	case "cancel", "run-now":
		if len(args) != 2 {
			return fmt.Errorf("%s needs a wait id", args[0])
		}
		waits, err := store.ListWaits(ctx, "")
		if err != nil {
			return err
		}
		for _, w := range waits {
			if w.ID != args[1] {
				continue
			}
			if args[0] == "cancel" {
				w.Status, w.Reason = wait.StatusCancelled, "cancelled manually"
				return store.SaveWait(ctx, w)
			}
			out, code, err := runCheck(ctx, w)
			fmt.Printf("exit %d\n%s", code, out)
			return err
		}
		return fmt.Errorf("no wait %q", args[1])
	default:
		return fmt.Errorf("unknown wait command %q", args[0])
	}
}

func runCheck(ctx context.Context, w wait.Wait) (string, int, error) {
	r := wait.New(nil, nil, nil)
	return r.Exec(ctx, w)
}

func cmdWaitAdd(ctx context.Context, cfg config.Config, store *sqlite.Store, args []string) error {
	fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
	name := fs.String("name", "", "what is being waited for")
	every := fs.Duration("every", 30*time.Second, "first poll interval; doubles after each not-yet up to --max-every")
	maxEvery := fs.Duration("max-every", 10*time.Minute, "backoff ceiling")
	timeout := fs.Duration("timeout", 24*time.Hour, "give up after")
	runTimeout := fs.Duration("run-timeout", time.Minute, "bound one run")
	thread := fs.String("thread", "", "thread id")
	dir := fs.String("dir", "", "working directory")
	group := fs.String("group", "", "group name")
	wakeMode := fs.String("wake", "each", "each or all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	command := fs.Args()
	if len(command) > 0 && command[0] == "--" {
		command = command[1:]
	}
	if len(command) == 0 {
		return errors.New("add needs a command after --")
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
	if *wakeMode != "each" && *wakeMode != "all" {
		return errors.New("--wake must be each or all")
	}
	if *wakeMode == "all" && *group == "" {
		return errors.New("--wake all needs --group")
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
	threadID, err := resolveThread(cfg, *thread)
	if err != nil {
		return err
	}
	// The thread must exist on this host's T3.
	logger := newLogger("error")
	client, _, err := connect(cfg, logger)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, cfg.T3.RequestTimeout.D())
	defer cancel()
	t, err := t3control.New(client, logger, true).GetThread(cctx, threadID)
	if err != nil {
		return fmt.Errorf("T3: %w", err)
	}
	if t == nil {
		return fmt.Errorf("thread %s is not known to T3 on this host", threadID)
	}
	w := wait.Wait{
		ID: newWaitID(), ThreadID: threadID, Name: *name, Command: command, Dir: *dir,
		Every: *every, MaxEvery: *maxEvery, Timeout: *timeout, RunTimeout: *runTimeout, Group: *group,
		Wake: wait.WakeMode(*wakeMode), Status: wait.StatusWaiting, CreatedAt: time.Now(),
	}
	// Verify the check runs before accepting the wait.
	out, code, err := runCheck(ctx, w)
	switch {
	case err != nil:
		fmt.Fprintf(os.Stderr, "%s", out)
		return fmt.Errorf("check cannot run: %v", err)
	case code == 0:
		fmt.Fprintf(os.Stderr, "%s", out)
		return errors.New("the check already exits 0: the condition is met, nothing to wait for")
	case code == 2:
		fmt.Fprintf(os.Stderr, "%s", out)
		return errors.New("the check exits 2 (give up) right away; fix it before registering")
	}
	now := time.Now()
	w.LastRunAt = &now
	w.Runs = 1
	w.LastExit = code
	w.LastOutput = out
	if err := store.SaveWait(ctx, w); err != nil {
		return err
	}
	fmt.Printf("wait %s registered for thread %s (%s): first check exited %d; polling every %s, backing off to %s, up to %s.\n",
		w.ID, threadID, t.Title, code, *every, *maxEvery, *timeout)
	fmt.Println("End this turn now; the steward wakes the thread with the outcome.")
	return nil
}

// resolveThread returns the explicit id, or the thread of the calling
// agent found through its provider session id.
func resolveThread(cfg config.Config, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	session := os.Getenv("CLAUDE_CODE_SESSION_ID")
	if session == "" {
		session = os.Getenv("CODEX_THREAD_ID")
	}
	if session == "" {
		return "", errors.New("no --thread given and CLAUDE_CODE_SESSION_ID is not set; pass --thread with the T3 thread id")
	}
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		return "", err
	}
	return wait.ResolveThread(t3api.ProviderLogDir(dataDir), session)
}

func newWaitID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "w-" + hex.EncodeToString(b[:])
}
