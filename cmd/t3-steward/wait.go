package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Native waits reach the coordinator, so the wait help carries the short
// transport note; shell checks stay entirely local to this host.
const waitUsage = waitCommandUsage + coordinatorTransportSummary

const waitCommandUsage = `Usage: t3-steward wait <command> [flags]

Wait instead of polling in a loop: register a check, end the turn, and the
steward wakes you when the condition settles.

There are two kinds of wait. They differ in what they wake and in what they are
allowed to change, and choosing the wrong one is the difference between a task
that parks safely and a task that is verified against work it has not done.

  TASK-BOUND WAIT  --task current
      For a thread that is executing a backlog or campaign task. It is
      MUTATING: it parks the attempt in waiting-external. While it is live the
      worker collects no outputs, the coordinator verifies nothing, no
      dependent task is released and the run sink cannot settle. The executor
      slot, the CPU, memory and scratch reservation, the provider slot and the
      quota tally are released; the attempt, thread, workspace, artifacts,
      dependency mounts, assignment ownership, resource locks and directory
      bindings are held. On settlement the same thread and the same attempt
      resume with the outcome in the initial context, and verification runs
      once, at the end of the turn that ends with no live wait.
      Only valid inside a task: outside one it is an error that says so.

  INTERACTIVE WAIT  (no --task, or --task <run>/<task>, or --run <run>)
      For an ordinary session. It is NON-MUTATING with respect to workflow
      state: it wakes the selected thread and creates or alters nothing else.
      No task is parked, nothing is held, nothing is released.

Commands:
  add --task current [flags] -- <command...>
                                Park this task until the check settles.
  add [flags] -- <command...>    Interactive shell check on this thread.
  add --task <run>/<task> [--thread ID] [--name TEXT] [--timeout 24h] [--request-id ID]
                                Interactive wait on another task's outcome.
  add --run <run> [flags]       Interactive wait on a run sink; no shell command.
  list [--thread ID] [--all]    Waits of this thread, or of every thread.
  list --native [--json]        Native waits, outcomes and delivery state.
  cancel <id> | run-now <id>    Control an interactive wait.
  cancel|run-now <nw-id>        Control a native wait through the admin socket.

add flags:
  --name TEXT        What is being waited for (shown in the wake message).
  --every DURATION   First poll interval (default 30s, minimum 30s); doubles
                     after every "not yet" up to --max-every (default 10m).
  --timeout DURATION Give up after this long (default 24h). For --task current
                     this is the wait's maximum duration, enforced by the
                     coordinator, because a parked task holds its directory
                     bindings and those have no deadline of their own.
  --run-timeout DUR  Bound one run of the check (default 1m).
  --thread ID        T3 thread to wake. Default: the canonical thread of the
                     task this process is executing, taken from the injected
                     environment or from .t3-steward/task.env in the prepared
                     workspace; otherwise resolved from CLAUDE_CODE_SESSION_ID,
                     CODEX_THREAD_ID or OPENCODE_SESSION_ID. A provider session
                     ID is an input to that resolution and never a thread ID; if
                     it is ambiguous the candidates are named and --thread is
                     required.
  --dir PATH         Working directory for the check (default: current).
  --group NAME       Group with other waits of the same thread (interactive).
  --wake each|all    Wake on the first settlement (default) or once every wait
                     has settled.
  --request-id ID    Stable registration ID, for retrying one registration
                     safely. Repeating it while that wait is still live and
                     holding this attempt returns the same wait. Repeating it
                     after the wait settled is refused: the ID names one park,
                     not a standing permission to park again. Include
                     $T3_STEWARD_ATTEMPT_REVISION so each park gets its own ID.
                     A repeated ID with different contents is also refused.
  --json             Print the registered wait as JSON (--task current).

Check protocol: exit 0 = condition met, wake. Exit 2 = give up, wake with the
failure. Any other exit = not yet, keep polling. The check is run once at
registration: a command that cannot run, exits 2, or already exits 0 is not
registered and nothing is parked.

On failure or timeout the wait still wakes you, with structured evidence:
which wait, which condition, which exit status and how long it ran. A
task-bound wait that times out releases its attempt to finish or fail
honestly; it never leaves the task parked. Silence is not an outcome.

Registering a task-bound wait is refused if the attempt is already terminal
("attempt is terminal (<progress>); task-bound waits are refused") or if the
attempt has moved on since this process was started. Both are ordinary command
errors: report them, do not retry blindly.

Exit codes: 0 registered or listed, 1 refused or failed.

Complete example, inside a task, waiting for CI on a pushed commit:

  t3-steward wait add --task current --name "CI on $(git rev-parse HEAD)" \\
    --every 60s --max-every 10m --timeout 2h --wake all \\
    --request-id ci-$T3_STEWARD_ATTEMPT_REVISION-$(git rev-parse --short HEAD) -- \\
    sh -c 'gh run view --json status --jq \'.status == "completed"\' | grep -q true'

Then end the turn. Nothing is collected or verified until the steward resumes
this same thread with the outcome.
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
	if args[0] == "add" && currentTaskWaitArgs(args[1:]) {
		return cmdTaskWaitAdd(context.Background(), cfg, args[1:])
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

// providerSessionKeys are the provider-local session identifiers this command
// knows how to resolve, in the order it reports them.
var providerSessionKeys = []string{"CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "OPENCODE_SESSION_ID"}

// resolveThread returns the explicit id, the canonical T3 thread injected into
// a task process, or the thread of the calling agent resolved from its
// provider-local session id.
//
// A provider session id is an input to resolution and never a thread id. They
// look alike, both being opaque strings, and treating one as the other wakes
// whatever thread happens to share the spelling, or nothing at all.
func resolveThread(cfg config.Config, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	// Inside a task the canonical thread is known exactly, from the injected
	// environment or from the identity record in the prepared workspace. A
	// failure to read either is not fatal here: an interactive session in some
	// unrelated directory still resolves through its provider.
	if identity, err := resolveTaskIdentity(os.Getenv); err == nil {
		return identity.ThreadID, nil
	}
	sessions, err := callerSessions(os.Getenv)
	if err != nil {
		return "", err
	}
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		return "", err
	}
	logDir := t3api.ProviderLogDir(dataDir)
	resolved := map[string][]string{}
	var failures []string
	for _, session := range sessions {
		thread, err := wait.ResolveThread(logDir, session.value)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s=%s: %v", session.key, session.value, err))
			continue
		}
		resolved[thread] = append(resolved[thread], session.key)
	}
	if len(resolved) == 1 {
		for thread := range resolved {
			return thread, nil
		}
	}
	if len(resolved) == 0 {
		return "", fmt.Errorf("no T3 thread could be resolved from the caller's provider session (%s); pass --thread with the T3 thread id",
			strings.Join(failures, "; "))
	}
	candidates := make([]string, 0, len(resolved))
	for thread, keys := range resolved {
		candidates = append(candidates, fmt.Sprintf("%s (from %s)", thread, strings.Join(keys, ", ")))
	}
	sort.Strings(candidates)
	return "", fmt.Errorf("the caller's provider sessions resolve to several T3 threads: %s; pass --thread with the intended one",
		strings.Join(candidates, "; "))
}

type providerSession struct {
	key   string
	value string
}

// callerSessions collects every provider-local session id set in the
// environment. Each is an independent input to resolution, so an inherited
// context from another provider is reported by name rather than silently
// deciding which session the caller meant.
func callerSessions(getenv func(string) string) ([]providerSession, error) {
	var sessions []providerSession
	seen := map[string]bool{}
	for _, key := range providerSessionKeys {
		value := strings.TrimSpace(getenv(key))
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		sessions = append(sessions, providerSession{key: key, value: value})
	}
	if len(sessions) == 0 {
		return nil, errors.New("no caller session found: set CLAUDE_CODE_SESSION_ID, CODEX_THREAD_ID or OPENCODE_SESSION_ID, or pass --thread with the T3 thread id")
	}
	return sessions, nil
}

// callerSession reports the one provider session id of the caller, and names
// the conflicting candidates when several providers are in the environment.
func callerSession(getenv func(string) string) (string, error) {
	sessions, err := callerSessions(getenv)
	if err != nil {
		return "", err
	}
	if len(sessions) > 1 {
		named := make([]string, 0, len(sessions))
		for _, session := range sessions {
			named = append(named, session.key+"="+session.value)
		}
		return "", fmt.Errorf("several provider session IDs are set (%s); pass --thread with the intended T3 thread id",
			strings.Join(named, ", "))
	}
	return sessions[0].value, nil
}

func newWaitID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "w-" + hex.EncodeToString(b[:])
}
