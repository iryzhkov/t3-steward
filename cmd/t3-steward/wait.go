package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Native waits reach the coordinator, so the wait help carries the short
// transport note; shell checks stay entirely local to this host.
const waitUsage = waitCommandUsage + coordinatorTransportSummary

const waitCommandUsage = `Usage: t3-steward wait <command> [flags]

Wait instead of polling in a loop: register a wait, end the turn, and the
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
      once, at the end of the turn that ends with nothing parking the task:
      no wait still undecided, and no settled wait whose outcome has yet to
      reach the thread.
      Only valid inside a task: outside one it is an error that says so.

  INTERACTIVE WAIT  (no --task current)
      For an ordinary session. It is NON-MUTATING with respect to workflow
      state: it wakes the selected thread and creates or alters nothing else.
      No task is parked, nothing is held, nothing is released.

Every wait has one of five conditions, its KIND. Both forms take every kind.

  LOCAL KINDS  settled by this host's wait runner, with a local check row
    shell    -- <command...>          exit 0 met, exit 2 gave up, else not yet
    time     --at RFC3339 | --for DURATION
                                      met when the instant passes; the poll
                                      interval follows the remaining time
    github   --github run <id> | pr <n> [--state STATE] [--repo owner/name]
                                      reads the target with gh and fixed
                                      arguments; run: completed (met on
                                      conclusion success, failed otherwise);
                                      pr: merged (default), reviewed,
                                      checks-passed; three gh errors in a row
                                      give up, a target that is gone gives up
                                      at once

  COORDINATOR KINDS  settled by the coordinator from its own records, no
  local check on any host
    node     --node <run>[/<task>] [--state terminal|succeeded|paused|waiting-external|active]
                                      terminal (default) is met when the node
                                      reaches any terminal progress except
                                      cancelled; succeeded is failed on a
                                      failed run; paused, waiting-external and
                                      active read the latest attempt. A run
                                      alone names its sink.
    quota    --quota <pool> --below N | --phase normal | --reset
                                      settled from the merged bucket
                                      observations of the pool: usage under N
                                      percent, every bucket normal, or the
                                      window current at registration reset

Outcomes: met, failed, gave-up, cancelled, timed-out. --or-timeout makes the
deadline a normal outcome for every kind: the wake still says timed-out, with
or-timeout=true, and nothing calls it a failure.

The wake. The first line of every wake message, every kind, interactive and
task-bound, is one parseable line:

  t3-steward-wait kind=<kind> outcome=<outcome> wait=<id> <key>=<value>...

plus kind-specific pairs: shell exit=; time at=; github target=run:<id>|pr:<n>
state= conclusion= url=; node run= task= attempt= revision= progress=
[control= pauseReason=] and, for a terminal run, failed=<comma list> and
result="t3-steward task result <run>"; quota pool= phase= percent=. Values with a
space are quoted, unknown keys are to be ignored, key order is not promised. A
blank line and the prose follow.

Commands:
  add --task current [flags] <condition>
                                Park this task until the condition settles.
  add [flags] <condition>       Interactive wait on this thread.
  list [--thread ID] [--all] [--json]
                                Local checks of this thread, or of every
                                thread, with their kind and outcome. A check
                                bound to a task-bound wait names it.
  list --native [--json]        Coordinator-held waits (node, quota and every
                                task-bound wait), outcomes and delivery state.
  cancel <id> | run-now <id>    Control an interactive wait or a local check.
  cancel <w-tw-id> | cancel <tw-id>
                                Cancel a task-bound wait, by its local check or
                                by its coordinator id. The coordinator settles
                                the wait as cancelled first, and only then is
                                the local check marked; the attempt resumes
                                with the cancellation as its wait outcome. If
                                the coordinator cannot be reached nothing is
                                changed and the command fails with the
                                transport exit code.
  cancel|run-now <nw-id>        Control a coordinator-held wait through the
                                admin socket.

add flags:
  --name TEXT        What is being waited for (shown in the wake message).
  --every DURATION   First poll interval of a shell or github wait (default
                     30s, minimum 30s); doubles after every "not yet" up to
                     --max-every (default 10m). A time wait polls from the
                     remaining time instead, never under 30s.
  --timeout DURATION Give up after this long (default 24h; a time wait's
                     default covers its instant). For --task current this is
                     the wait's maximum duration, enforced by the coordinator,
                     because a parked task holds its directory bindings and
                     those have no deadline of their own.
  --or-timeout       Treat the deadline as a normal outcome (timed-out, exit
                     0) rather than a failure.
  --run-timeout DUR  Bound one run of a shell check (default 1m).
  --state STATE      The state waited for: a github or node state, see above.
  --repo owner/name  The repository of a github target.
  --thread ID        T3 thread to wake. Default: the canonical thread of the
                     task this process is executing, taken from the injected
                     environment or from .t3-steward/task.env in the prepared
                     workspace; otherwise resolved from CLAUDE_CODE_SESSION_ID,
                     CODEX_THREAD_ID or OPENCODE_SESSION_ID. A provider session
                     ID is an input to that resolution and never a thread ID; if
                     it is ambiguous the candidates are named and --thread is
                     required.
  --dir PATH         Working directory for a shell check (default: current).
  --group NAME       Group with other waits of the same thread (interactive).
  --wake each|all    Wake on the first settlement (default) or once every wait
                     has settled. A group, and a task's --wake all set, is all
                     local kinds or all coordinator kinds; mixing the two is
                     refused, naming both members.
  --request-id ID    Stable registration ID, for retrying one registration
                     safely. Default for --task current: park-<attempt>-<revision>
                     from this task's identity, which is stable for a retry and
                     different for every later park. Repeating an ID while that
                     wait is still live and holding this attempt returns the
                     same wait. Repeating it after the wait settled is refused:
                     the ID names one park, not a standing permission to park
                     again. A custom ID that must differ per park can include
                     $(t3-steward task env --get revision); the T3_STEWARD_*
                     variables are not in the environment unless
                     t3.send_thread_environment is on (off by default). A
                     repeated ID with different contents is also refused.
  --json             Print the registered wait as JSON, with firstExit and
                     firstOutputLine from the registration probe.

Check protocol (shell kind): exit 0 = condition met, wake. Exit 2 = give up,
wake with the failure. Any other exit = not yet, keep polling. The check is run
once at registration: a command that cannot run, exits 2, or already exits 0 is
not registered and nothing is parked. The first run's exit code and first
output line are reported in every mode. An exit other than 1 is registered as
"not yet" and warned about on stderr, because the protocol cannot tell a
not-yet from a command that will fail the same way forever. A github target is
read once at registration the same way: unreadable, already met or already
failed is refused. A time wait is refused when its instant has passed. A node
or quota condition is checked by the coordinator: an unknown target or pool,
or a condition that already holds, is refused and nothing is parked.

Verifying the caller's thread for an interactive wait needs the T3 API token:
t3.token or t3.token_file in the configuration, T3_STEWARD_T3_TOKEN in the
environment, or the t3 CLI, in that order.

On failure or timeout the wait still wakes you, with structured evidence:
which wait, which condition, which exit status and how long it ran. A
task-bound wait that times out releases its attempt to finish or fail
honestly; it never leaves the task parked. Cancelling the task settles its
live wait as cancelled. Silence is not an outcome.

Registering a task-bound wait is refused if the attempt is already terminal
("attempt is terminal (<progress>); task-bound waits are refused") or if the
attempt has moved on since this process was started. Both are ordinary command
errors: report them, do not retry blindly.

Exit codes: 0 registered or listed, 1 refused or failed.

Examples, inside a task:

  t3-steward wait add --task current --github run $(gh run list --limit 1 --json databaseId --jq '.[0].databaseId') --timeout 2h
  t3-steward wait add --task current --for 30m --or-timeout
  t3-steward wait add --task current --node <run>/<task> --state succeeded
  t3-steward wait add --task current --quota claude --phase normal
  t3-steward wait add --task current --name "deploy finished" -- ./scripts/deployed.sh

Then end the turn. Nothing is collected or verified until the steward resumes
this same thread with the outcome.
`

func cmdWait(g globalFlags, args []string) error {
	if answered, err := admitFamilyHelp(os.Stdout, []string{"wait"}, args); answered || err != nil {
		return err
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
		return cmdWaitList(ctx, cfg, store, args[1:], os.Stdout)
	case "cancel", "run-now":
		if len(args) != 2 {
			return fmt.Errorf("%s needs a wait id", args[0])
		}
		return cmdWaitControl(ctx, cfg, store, args[0], args[1])
	default:
		return fmt.Errorf("unknown wait command %q", args[0])
	}
}

// cmdWaitList answers what a thread is waiting for, from both sources: this
// host's local checks and the waits the coordinator holds. Reading only the
// first is what made "No waits." appear while a node wait registered seconds
// earlier was pending.
func cmdWaitList(ctx context.Context, cfg config.Config, store *sqlite.Store, args []string, out io.Writer) error {
	options, err := parseWaitListArgs(args)
	if err != nil {
		return err
	}
	if options.thread == "" && !options.all {
		options.thread, options.threadResolutionErr = resolveThread(cfg, "")
	}
	sources := waitListSources{
		host: localWakeHost(nil),
		local: func(ctx context.Context, thread string) ([]wait.Wait, error) {
			return store.ListWaits(ctx, thread)
		},
		coordinator: coordinatorWaitSource(cfg),
	}
	return runWaitList(ctx, sources, options, out)
}

// coordinatorWaitSource reads the node waits and task-bound waits the
// coordinator holds, or is nil on a host with no coordinator configured, which
// is a source that does not exist rather than one that failed.
func coordinatorWaitSource(cfg config.Config) func(context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
	if !cfg.BacklogV2.CoordinatorClient.Configured() {
		return nil
	}
	return func(ctx context.Context) ([]domain.NodeWait, []domain.TaskWait, error) {
		transport, err := newCoordinatorTransport(cfg)
		if err != nil {
			return nil, nil, err
		}
		nodes, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
		if err != nil {
			return nil, nil, err
		}
		tasks, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"})
		if err != nil {
			return nil, nil, err
		}
		return nodes.Waits, tasks.TaskWaits, nil
	}
}

// cmdWaitControl cancels or re-runs one local check.
//
// A check bound to a task-bound wait is half of a park; the coordinator's
// record is the half that holds the attempt. Cancelling the local half alone
// left the attempt parked on a wait nothing would settle until its deadline,
// so the coordinator is told first, and the local row changes only once the
// coordinator has settled the wait. A coordinator that cannot be reached
// changes nothing: the failure keeps its transport class so the operator
// retries instead of believing the wait is gone.
func cmdWaitControl(ctx context.Context, cfg config.Config, store *sqlite.Store, action, id string) error {
	waits, err := store.ListWaits(ctx, "")
	if err != nil {
		return err
	}
	for _, w := range waits {
		if w.ID != id {
			continue
		}
		if action == "run-now" {
			out, code, err := runCheck(ctx, w)
			fmt.Printf("exit %d\n%s", code, out)
			return err
		}
		if w.Status != wait.StatusWaiting {
			// A settled or already cancelled row keeps its outcome: overwriting a
			// met check as cancelled would misreport what the runner observed.
			return fmt.Errorf("wait %s is already %s; nothing to cancel", w.ID, w.Status)
		}
		if w.TaskWaitID == "" {
			w.Status, w.Reason = wait.StatusCancelled, "cancelled manually"
			if err := store.SaveWait(ctx, w); err != nil {
				return err
			}
			fmt.Printf("wait %s cancelled.\n", w.ID)
			return nil
		}
		settled, err := cancelTaskWait(ctx, cfg, w.TaskWaitID)
		if err != nil {
			return err
		}
		w.Status = wait.StatusCancelled
		w.Reason = fmt.Sprintf("cancelled manually; coordinator wait %s settled as %s", settled.ID, taskWaitOutcome(settled))
		if err := store.SaveWait(ctx, w); err != nil {
			return fmt.Errorf("task-bound wait %s is settled on the coordinator, but the local check %s could not be marked: %w", settled.ID, w.ID, err)
		}
		reportTaskWaitCancelled(os.Stdout, w.ID, settled)
		return nil
	}
	return fmt.Errorf("no wait %q", id)
}

// cancelTaskWait settles a coordinator task-bound wait as cancelled through
// the configured transport and returns the record as the coordinator holds it.
// A wait that already has an outcome keeps it: cancellation never retracts
// evidence, and the returned record says which outcome stands.
func cancelTaskWait(ctx context.Context, cfg config.Config, id string) (domain.TaskWait, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return domain.TaskWait{}, err
	}
	response, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "cancel-task", ID: id})
	if err != nil {
		return domain.TaskWait{}, err
	}
	if len(response.TaskWaits) != 1 {
		return domain.TaskWait{}, errors.New("the coordinator did not return exactly one task-bound wait")
	}
	return response.TaskWaits[0], nil
}

func taskWaitOutcome(w domain.TaskWait) string {
	if w.Result == nil {
		return "live"
	}
	return string(w.Result.Outcome)
}

// reportTaskWaitCancelled says what was cancelled and what happens next. The
// attempt is resumed by the coordinator's own tick, not by this command, and
// the message says so rather than implying the thread is already running.
func reportTaskWaitCancelled(out io.Writer, localID string, settled domain.TaskWait) {
	outcome := taskWaitOutcome(settled)
	fmt.Fprintf(out, "task-bound wait %s settled as %s on the coordinator", settled.ID, outcome)
	if localID != "" {
		fmt.Fprintf(out, "; local check %s cancelled", localID)
	}
	fmt.Fprintln(out, ".")
	if outcome == string(domain.TaskWaitCancelled) {
		fmt.Fprintf(out, "Attempt %s resumes on the coordinator's next tick with the cancellation as its wait outcome.\n", settled.AttemptID)
		return
	}
	fmt.Fprintf(out, "The wait had already settled as %s before the cancel; attempt %s resumes with that outcome.\n", outcome, settled.AttemptID)
}

// cancelLocalTaskCheck marks the local check bound to a coordinator wait as
// cancelled, once the coordinator has settled that wait. A host that holds
// no such check, or whose check has already settled, is left as it is.
func cancelLocalTaskCheck(ctx context.Context, cfg config.Config, settled domain.TaskWait) (string, error) {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return "", err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return "", err
	}
	defer store.Close()
	waits, err := store.ListWaits(ctx, "")
	if err != nil {
		return "", err
	}
	for _, w := range waits {
		if w.TaskWaitID != settled.ID || w.Status != wait.StatusWaiting {
			continue
		}
		w.Status = wait.StatusCancelled
		w.Reason = fmt.Sprintf("cancelled manually; coordinator wait %s settled as %s", settled.ID, taskWaitOutcome(settled))
		return w.ID, store.SaveWait(ctx, w)
	}
	return "", nil
}

func runCheck(ctx context.Context, w wait.Wait) (string, int, error) {
	r := wait.New(nil, nil, nil)
	return r.Exec(ctx, w)
}

func cmdWaitAdd(ctx context.Context, cfg config.Config, store *sqlite.Store, args []string) error {
	now := time.Now()
	spec, err := parseLocalWaitSpec(args, now)
	if err != nil {
		return err
	}
	if spec.RequestID != "" {
		return errors.New("--request-id applies to --task current and node waits; an interactive local wait has no registration to retry")
	}
	if spec.WakeMode == "all" && spec.Group == "" {
		return errors.New("--wake all needs --group")
	}
	if spec.Dir == "" {
		spec.Dir, _ = os.Getwd()
	}
	threadID, err := resolveThread(cfg, spec.Thread)
	if err != nil {
		return refuseWaitThread("wait add", "<the rest of this call>", err)
	}
	// The thread must exist on this host's T3.
	logger := newLogger("error")
	client, _, err := connectForCallerThread(cfg, logger)
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
	if spec.Group != "" && cfg.BacklogV2.CoordinatorClient.Configured() {
		// The coordinator's waits of this thread are the other side of a mixed
		// group. A coordinator that cannot be reached does not block a local
		// wait: the check is advisory here and enforced when the coordinator
		// kind registers.
		if transport, err := newCoordinatorTransport(cfg); err == nil {
			if listed, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list"}); err == nil {
				if err := refuseMixedGroup(threadID, spec.Group, spec.Kind, nil, listed.Waits); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(os.Stderr, "warning: the coordinator's waits could not be listed to check --group %q for mixed kinds: %v\n", spec.Group, err)
			}
		}
	}
	w := wait.Wait{
		ID: newWaitID(), ThreadID: threadID, Name: spec.Name, Kind: spec.Kind, Command: spec.Command, Dir: spec.Dir,
		At: spec.At, GitHub: spec.GitHub, OrTimeout: spec.OrTimeout,
		Every: spec.Every, MaxEvery: spec.MaxEvery, Timeout: spec.Timeout, RunTimeout: spec.RunTimeout, Group: spec.Group,
		Wake: wait.WakeMode(spec.WakeMode), Status: wait.StatusWaiting, CreatedAt: now,
	}
	// Verify the condition can be observed before accepting the wait. A time
	// wait has nothing to prove; its first poll is the runner's next tick.
	code, firstLine, err := probeLocalWait(ctx, spec, &w, "nothing to wait for")
	if err != nil {
		return err
	}
	if err := store.SaveWait(ctx, w); err != nil {
		return err
	}
	if spec.Kind == domain.WaitKindShell {
		warnUnconventionalFirstExit(os.Stderr, code, firstLine)
	}
	if spec.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(struct {
			wait.Wait
			FirstExit       int    `json:"firstExit"`
			FirstOutputLine string `json:"firstOutputLine"`
		}{w, code, firstLine}); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "End this turn now; the steward wakes the thread with the outcome.")
		return nil
	}
	fmt.Printf("wait %s (%s) registered for thread %s (%s): %s.\n",
		w.ID, spec.Kind, threadID, t.Title, spec.registrationSummary(code, firstLine))
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
		return "", unresolvedThread("no T3 thread could be resolved from the caller's provider session (%s)",
			strings.Join(failures, "; "))
	}
	candidates := make([]string, 0, len(resolved))
	for thread, keys := range resolved {
		candidates = append(candidates, fmt.Sprintf("%s (from %s)", thread, strings.Join(keys, ", ")))
	}
	sort.Strings(candidates)
	return "", unresolvedThread("the caller's provider sessions resolve to several T3 threads: %s",
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
		return nil, unresolvedThread("no caller session is set in the environment")
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
		return "", unresolvedThread("several provider session IDs are set (%s)",
			strings.Join(named, ", "))
	}
	return sessions[0].value, nil
}

func newWaitID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "w-" + hex.EncodeToString(b[:])
}
