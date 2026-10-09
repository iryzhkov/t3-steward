package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
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
	return resolveTaskIdentityFrom(getenv, "")
}

// resolveTaskIdentityFrom is resolveTaskIdentity looking for the workspace
// record from directory rather than the current directory, when it is set.
func resolveTaskIdentityFrom(getenv func(string) string, directory string) (taskIdentity, error) {
	values, err := taskIdentityValuesFrom(getenv, directory)
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
	return taskIdentityValuesFrom(getenv, "")
}

func taskIdentityValuesFrom(getenv func(string) string, directory string) (map[string]string, error) {
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
	fileValues, err := readTaskIdentityFileFrom(directory)
	if err != nil {
		return nil, err
	}
	return fileValues, nil
}

// readTaskIdentityFileFrom reads the worker-written identity record from the
// directory, or the current directory when it is empty, or an ancestor, the
// way a tool finds the repository it is inside.
//
// The file is accepted only as a private regular file owned by this user. It
// decides which attempt a command speaks for, so a copy anyone could have
// written, or a symlink pointing somewhere else, is refused rather than read.
func readTaskIdentityFileFrom(directory string) (map[string]string, error) {
	if directory == "" {
		var err error
		if directory, err = os.Getwd(); err != nil {
			return nil, errNotInsideTask
		}
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
// --request-id it is derived from the resolved identity and the command's own
// arguments, "park-<attempt>-<revision>-<digest>". The identity part is stable
// for a retry of the same park and different for every later park of the same
// task; the digest part is stable for a retry of the same command and different
// for every other condition registered in the same park. Without it the second
// condition of a park reused the first one's id and was answered with the first
// one's wait, so a task waiting for runs A and B woke after A alone. The random
// id remains only for an identity with no attempt, which registration refuses
// anyway. An explicit id that ends in "-" is almost always a shell variable
// that was empty when the command line was built, so it is warned about, but
// still used: a repeated registration must keep returning the same wait.
//
// dir is the working directory a shell check runs in, and empty for every
// other kind: the same command in another directory is another condition, and
// it is usually not on the command line.
func taskWaitRequestID(explicit string, identity taskIdentity, args []string, dir string, warnings io.Writer) string {
	if explicit == "" {
		if identity.AttemptID == "" {
			return strings.TrimPrefix(newWaitID(), "w-")
		}
		return fmt.Sprintf("park-%s-%d-%s", identity.AttemptID, identity.AttemptRevision, taskWaitArgsDigest(args, dir))
	}
	if strings.HasSuffix(explicit, "-") {
		fmt.Fprintf(warnings, "warning: --request-id %q ends in \"-\", which usually means an empty shell variable was interpolated into it; "+
			"the identity is in .t3-steward/task.env, not the environment (unless t3.send_thread_environment is on), so use $(t3-steward task env --get revision) or omit --request-id to derive park-%s-%d-<digest>\n",
			explicit, identity.AttemptID, identity.AttemptRevision)
	}
	return explicit
}

// taskWaitArgsDigest is the condition part of a derived request id: the first
// twelve hex digits of a hash of the `wait add` arguments. --json is left out
// because it changes only how the answer is printed, so a retry that adds it
// still replays the same wait. The arguments are hashed rather than the parsed
// condition because a relative condition such as --for 30m resolves to a
// different instant on every run, and a retry must not become a new wait. A
// shell check's directory is hashed first, made absolute, when there is one.
func taskWaitArgsDigest(args []string, dir string) string {
	hash := sha256.New()
	if dir != "" {
		if absolute, err := filepath.Abs(dir); err == nil {
			dir = absolute
		}
		hash.Write([]byte("\x01dir=" + dir + "\x00"))
	}
	for index, arg := range args {
		if arg == "--" {
			for _, rest := range args[index:] {
				hash.Write([]byte(rest))
				hash.Write([]byte{0})
			}
			break
		}
		switch arg {
		case "--json", "-json", "--json=true", "-json=true", "--json=false", "-json=false":
			continue
		}
		hash.Write([]byte(arg))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))[:12]
}

// taskWakeModeFlags settles the wake mode of a `wait add` from --wake, --all
// and --any. A task-bound wait takes all unless told otherwise: the several
// conditions of one park are a set the task wants complete, and a single
// condition wakes the same way under either mode. An interactive wait keeps
// each, and composes with --group NAME --wake all as before.
func taskWakeModeFlags(fs *flag.FlagSet, wake string, all, any, taskBound bool) (string, error) {
	wakeGiven := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "wake" {
			wakeGiven = true
		}
	})
	given := 0
	for _, set := range []bool{wakeGiven, all, any} {
		if set {
			given++
		}
	}
	switch {
	case given > 1:
		return "", errors.New("--all, --any and --wake are three spellings of one choice; give one")
	case (all || any) && !taskBound:
		return "", errors.New("--all and --any apply to --task current; an interactive wait chooses with --group NAME --wake each|all")
	case all:
		return "all", nil
	case any:
		return "each", nil
	case wakeGiven:
		if wake != "each" && wake != "all" {
			return "", errors.New("--wake must be each or all")
		}
		return wake, nil
	case taskBound:
		return "all", nil
	default:
		return "each", nil
	}
}

// parkWakeSentence tells the agent how the conditions of its park combine, so
// a second registration is never a guess about whether it was kept.
func parkWakeSentence(mode string) string {
	if mode == "each" {
		return "Wake: any. This task resumes as soon as this condition settles, whatever else this park holds."
	}
	return "Wake: all. This task resumes once every --all condition of this park has settled, each reported with its own outcome. " +
		"To add another condition, run `t3-steward wait add --task current` again before ending the turn."
}

// localTaskWaitRegistration records complete shell identity at the coordinator
// boundary. Older strict coordinators refuse the new shell field rather than
// accepting a registration whose replay identity they cannot protect.
func localTaskWaitRegistration(spec localWaitSpec, identity taskIdentity) domain.TaskWaitRegistration {
	kind := spec.Kind
	if kind == domain.WaitKindShell {
		kind = ""
	}
	var shell *domain.ShellWaitCondition
	if spec.Kind == domain.WaitKindShell {
		shell = &domain.ShellWaitCondition{Dir: spec.Dir, Command: spec.Command}
	}
	condition := spec.Condition
	if spec.Kind == domain.WaitKindGitHub && spec.GitHub != nil && spec.GitHub.Repo != "" {
		// Bind the resolved repository even when the display name was supplied
		// explicitly. The coordinator must protect identity without a local save.
		condition = "github " + spec.GitHub.Ref() + " " + spec.GitHub.State + " in " + spec.GitHub.Repo
	}
	return domain.TaskWaitRegistration{
		Shell:     shell,
		RequestID: spec.RequestID, WorkflowRunID: identity.WorkflowRunID, TaskID: identity.TaskID,
		AttemptID: identity.AttemptID, IssuedRevision: identity.AttemptRevision,
		ThreadID: identity.ThreadID, Wake: domain.WakeMode(spec.WakeMode), MaxDuration: spec.Timeout,
		Name: spec.Name, Condition: condition, Kind: kind, OrTimeout: spec.OrTimeout,
	}
}

func cmdTaskWaitAdd(ctx context.Context, cfg config.Config, args []string) error {
	if coordinatorWaitArgs(args) {
		return cmdTaskCoordinatorWaitAdd(ctx, cfg, args)
	}
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
		spec.Dir = "."
	}
	spec.Dir, err = filepath.Abs(spec.Dir)
	if err != nil {
		return fmt.Errorf("resolve wait directory: %w", err)
	}
	checkDir := ""
	if spec.Kind == domain.WaitKindShell {
		checkDir = spec.Dir
	}
	if spec.Kind == domain.WaitKindGitHub {
		if spec.GitHub.Repo == "" {
			runner := wait.New(nil, nil, nil)
			runner.GitHub = gitHubCommand
			spec.GitHub.Repo = gitHubRepository(ctx, runner, spec.Dir)
			if spec.GitHub.Repo == "" {
				return errors.New("cannot resolve the GitHub repository for this task-bound wait; give --repo owner/name so its condition can be recorded before parking")
			}
		}
		// Identical arguments in different checkouts must name distinct waits.
		checkDir = spec.GitHub.Repo
	}
	spec.RequestID = taskWaitRequestID(spec.RequestID, identity, args, checkDir, os.Stderr)

	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	checks, err := store.ListWaits(ctx, "")
	if err != nil {
		return err
	}
	saved, retry := registeredTaskCheck(checks, sqlite.TaskWaitID(spec.RequestID))
	if retry && spec.Kind == domain.WaitKindTime && spec.For > 0 && saved.For == spec.For && saved.At != nil {
		// A retry of --for DURATION keeps the instant its first registration
		// resolved. Resolving it again names a later instant, which is another
		// condition, and the retry would be refused for it.
		spec.setTimeInstant(*saved.At)
		if spec.ExplicitName != "" {
			spec.Name = spec.ExplicitName
		}
	}

	local := wait.Wait{
		ID: newWaitID(), ThreadID: identity.ThreadID, Name: spec.Name, Kind: spec.Kind, Command: spec.Command, Dir: spec.Dir,
		At: spec.At, For: spec.For, GitHub: spec.GitHub, OrTimeout: spec.OrTimeout,
		Every: spec.Every, MaxEvery: spec.MaxEvery, Timeout: spec.Timeout, RunTimeout: spec.RunTimeout,
		Wake: wait.WakeMode(spec.WakeMode), Status: wait.StatusWaiting, CreatedAt: now,
	}
	// The condition is proven observable before the attempt is parked. A check
	// that cannot run, gives up at once, or is already true would otherwise
	// park a task for a wake that is either immediate or never coming. A time
	// wait was already checked to lie in the future.
	code, firstLine, err := probeLocalWait(ctx, spec, &local, "so there is nothing to park for")
	if err != nil {
		return err
	}
	// A retry is refused before it reaches the coordinator when it changes
	// part of the condition the coordinator record does not hold, such as a
	// shell check's directory. Answering it with the saved wait would report
	// the new condition registered while only the old one is watched.
	if retry {
		local.TaskWaitID = saved.TaskWaitID
		if err := refuseChangedTaskCheck(spec.RequestID, saved, local); err != nil {
			return err
		}
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
	registration := localTaskWaitRegistration(spec, identity)
	response, err := client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "register-task", Task: &registration})
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

	local.TaskWaitID = registered.ID
	// A registration retry must never create a second poll or reset a settled one.
	local.ID = "w-" + registered.ID
	if err := saveRegisteredTaskCheck(ctx, store, spec.RequestID, local); errors.Is(err, errTaskCheckConditionChanged) {
		return err
	} else if err != nil {
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
	fmt.Println(parkWakeSentence(string(registered.Wake)))
	fmt.Println("This task is now parked. End this turn now: nothing is collected and nothing is verified")
	fmt.Println("until the steward resumes this same thread with the outcome.")
	return nil
}
