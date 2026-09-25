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

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// gitHubCommand runs gh for the registration probe of a github wait. It is a
// variable so tests never reach GitHub.
var gitHubCommand wait.GitHubRunner = wait.ExecGitHub

// gitHubRepository is the owner/name gh resolves in dir, or the empty string
// when it resolves none.
//
// It is asked at registration so that the wait records the repository the
// caller meant, in the one place where a directory means what the caller thinks
// it means. The steward daemon that polls the wait later has no such directory
// of its own; see wait.GitHubRunner.
func gitHubRepository(ctx context.Context, runner *wait.Runner, dir string) string {
	run := runner.GitHub
	if run == nil {
		run = wait.ExecGitHub
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := run(cctx, dir, []string{"repo", "view", "--json", "nameWithOwner"})
	if err != nil {
		return ""
	}
	var view struct {
		NameWithOwner string `json:"nameWithOwner"`
	}
	if err := json.Unmarshal([]byte(out), &view); err != nil {
		return ""
	}
	repo := strings.TrimSpace(view.NameWithOwner)
	if strings.Count(repo, "/") != 1 || strings.HasPrefix(repo, "/") || strings.HasSuffix(repo, "/") {
		return ""
	}
	return repo
}

// normalizeKindArgs rewrites the two-token form `--github run 123` into the
// one-token form `--github=run:123` that the flag package can parse, since it
// stops at the first positional argument.
func normalizeKindArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			out = append(out, args[i:]...)
			break
		}
		if (arg == "--github" || arg == "-github") && i+2 < len(args) && (args[i+1] == "run" || args[i+1] == "pr") && !strings.HasPrefix(args[i+2], "-") {
			out = append(out, "--github="+args[i+1]+":"+args[i+2])
			i += 2
			continue
		}
		if (arg == "--github" || arg == "-github") && i+1 < len(args) && (args[i+1] == "run" || args[i+1] == "pr") {
			// `--github run` with no id: keep the kind so the error names it.
			out = append(out, "--github="+args[i+1]+":")
			i++
			continue
		}
		out = append(out, arg)
	}
	return out
}

// gitHubTargetForms names every form --github accepts. A refusal of a target
// that does not parse quotes it, because the forms an agent reaches for first
// -- owner/name#<n> and a pull request URL copied from gh or a browser -- used
// to pass straight through to gh, which read owner/name#<n> as a branch name
// and answered "no pull requests found for branch", a message that says
// nothing about the form being wrong.
const gitHubTargetForms = "run <id>, run owner/name#<id>, run https://github.com/owner/name/actions/runs/<id>, " +
	"pr <n>, pr owner/name#<n> or pr https://github.com/owner/name/pull/<n>"

// parseGitHubTarget reads run:<id>, pr:<n>, run/<id> or pr/<n>, where the id
// may be qualified by its repository as owner/name#<id> or given as the
// target's github.com URL. A URL alone, without a kind, names its own kind. A
// repository named by the target is recorded as if it had been given with
// --repo, and one that disagrees with --repo is refused rather than guessed
// between.
func parseGitHubTarget(spec, state, repo string) (wait.GitHubTarget, error) {
	spec = strings.TrimSpace(spec)
	kind, id, ok := gitHubURLKind(spec)
	if !ok {
		if kind, id, ok = strings.Cut(spec, ":"); !ok {
			kind, id, _ = strings.Cut(spec, "/")
		}
	}
	id = strings.TrimSpace(id)
	if kind != "run" && kind != "pr" {
		return wait.GitHubTarget{}, fmt.Errorf("--github %q is not a target; --github takes %s", spec, gitHubTargetForms)
	}
	if id != "" {
		number, named, err := splitGitHubTargetID(kind, id)
		if err != nil {
			return wait.GitHubTarget{}, err
		}
		if named != "" && repo != "" && !strings.EqualFold(named, repo) {
			return wait.GitHubTarget{}, fmt.Errorf("--github %s names %s but --repo names %s; give one repository", id, named, repo)
		}
		if named != "" {
			repo = named
		}
		id = number
	}
	target := wait.GitHubTarget{Kind: kind, ID: id, State: state, Repo: repo}
	if target.State == "" {
		if states := wait.GitHubStates(kind); len(states) != 0 {
			target.State = states[0]
		}
	}
	if err := target.Validate(); err != nil {
		return target, err
	}
	return target, nil
}

// gitHubURLKind recognises a --github value that is a whole github.com URL,
// without a kind in front of it, and returns the kind the URL names with the
// URL itself as the id for splitGitHubTargetID to read.
func gitHubURLKind(spec string) (kind, id string, ok bool) {
	kind, _, _, ok = parseGitHubURL(spec)
	return kind, spec, ok
}

// parseGitHubURL reads https://github.com/owner/name/pull/<n> as a pull
// request and https://github.com/owner/name/actions/runs/<id> as a workflow
// run. Anything after the number, such as /files, /job/<id> or a query, is a
// view of the same target and is ignored.
func parseGitHubURL(raw string) (kind, repo, id string, ok bool) {
	rest := raw
	for _, scheme := range []string{"https://", "http://"} {
		rest = strings.TrimPrefix(rest, scheme)
	}
	rest = strings.TrimPrefix(rest, "www.")
	rest, found := strings.CutPrefix(rest, "github.com/")
	if !found {
		return "", "", "", false
	}
	if cut := strings.IndexAny(rest, "?#"); cut >= 0 {
		rest = rest[:cut]
	}
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) >= 4 && parts[2] == "pull":
		kind, id = "pr", parts[3]
	case len(parts) >= 5 && parts[2] == "actions" && parts[3] == "runs":
		kind, id = "run", parts[4]
	default:
		return "", "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", "", false
	}
	return kind, parts[0] + "/" + parts[1], id, true
}

// splitGitHubTargetID reads the id of a run or pr target: a bare number,
// owner/name#<number>, or the target's github.com URL. It returns the number
// and the repository the id named, if any. Anything else is refused with the
// accepted forms, before gh is asked, since gh reads a pr id that is not a
// number as a branch name.
func splitGitHubTargetID(kind, raw string) (id, repo string, err error) {
	refuse := fmt.Errorf("--github %s %q is not a target; --github takes %s", kind, raw, gitHubTargetForms)
	switch {
	case strings.Contains(raw, "github.com/"):
		urlKind, named, number, ok := parseGitHubURL(raw)
		if !ok {
			return "", "", refuse
		}
		if urlKind != kind {
			return "", "", fmt.Errorf("--github %s was given the URL of a %s: %s; --github takes %s", kind, urlKind, raw, gitHubTargetForms)
		}
		id, repo = number, named
	case strings.Contains(raw, "#"):
		named, number, _ := strings.Cut(raw, "#")
		owner, name, ok := strings.Cut(named, "/")
		if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
			return "", "", refuse
		}
		id, repo = number, named
	default:
		id = raw
	}
	if id == "" || strings.Trim(id, "0123456789") != "" {
		return "", "", refuse
	}
	return id, repo, nil
}

// localWaitSpec is a parsed `wait add` for a local kind: shell, time or
// github. One parser serves the interactive form and `--task current`, so the
// two cannot drift apart in which kinds and flags they accept; each command
// then refuses the flags that do not apply to it.
type localWaitSpec struct {
	Kind    domain.WaitKind
	Command []string
	// At is the instant of a time wait.
	At *time.Time
	// GitHub is the target of a github wait.
	GitHub *wait.GitHubTarget
	// Name is the name to use when --name was not given.
	Name string
	// Condition is the text the coordinator records for a task-bound wait.
	Condition string

	Every, MaxEvery, Timeout, RunTimeout time.Duration
	// TimeoutExplicit reports that --timeout was on the command line.
	TimeoutExplicit bool
	OrTimeout       bool
	Dir, WakeMode   string
	ExplicitName    string

	// Interactive-only flags.
	Thread, Group string
	// Task-bound-only flags.
	RequestID string
	JSON      bool
}

// timeWaitTimeoutMargin is how much later than the instant a time wait's
// default deadline lands: enough for the last poll and the wake.
const timeWaitTimeoutMargin = 10 * time.Minute

// parseLocalWaitSpec parses the flags of `wait add` for the local kinds and
// validates what is common to both forms. now is the clock the time kind is
// checked against.
func parseLocalWaitSpec(args []string, now time.Time) (localWaitSpec, error) {
	var spec localWaitSpec
	fs := flag.NewFlagSet("wait add", flag.ContinueOnError)
	fs.SetOutput(discardWriter{})
	task := fs.String("task", "", "current")
	fs.StringVar(&spec.ExplicitName, "name", "", "what is being waited for")
	fs.DurationVar(&spec.Every, "every", 30*time.Second, "first poll interval; doubles after each not-yet up to --max-every")
	fs.DurationVar(&spec.MaxEvery, "max-every", 10*time.Minute, "backoff ceiling")
	fs.DurationVar(&spec.Timeout, "timeout", 24*time.Hour, "give up after")
	fs.DurationVar(&spec.RunTimeout, "run-timeout", time.Minute, "bound one run")
	fs.StringVar(&spec.Thread, "thread", "", "thread id")
	fs.StringVar(&spec.Dir, "dir", "", "working directory")
	fs.StringVar(&spec.Group, "group", "", "group name")
	fs.StringVar(&spec.WakeMode, "wake", "each", "each or all")
	fs.StringVar(&spec.RequestID, "request-id", "", "stable registration ID")
	fs.BoolVar(&spec.JSON, "json", false, "print the registered wait as JSON")
	fs.BoolVar(&spec.OrTimeout, "or-timeout", false, "treat the deadline as a normal outcome")
	at := fs.String("at", "", "RFC 3339 instant of a time wait")
	after := fs.String("for", "", "duration of a time wait")
	github := fs.String("github", "", "run <id> or pr <number>")
	state := fs.String("state", "", "state waited for")
	repo := fs.String("repo", "", "owner/name for --github")
	if err := fs.Parse(normalizeKindArgs(args)); err != nil {
		return spec, err
	}
	if *task != "" && *task != "current" {
		return spec, fmt.Errorf("--task %q is a node wait, not a local one", *task)
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "timeout" {
			spec.TimeoutExplicit = true
		}
	})
	spec.Command = fs.Args()
	if len(spec.Command) > 0 && spec.Command[0] == "--" {
		spec.Command = spec.Command[1:]
	}

	var kinds []string
	if *at != "" || *after != "" {
		kinds = append(kinds, "--at/--for")
	}
	if *github != "" {
		kinds = append(kinds, "--github")
	}
	if len(spec.Command) > 0 {
		kinds = append(kinds, "a command after --")
	}
	switch {
	case len(kinds) > 1:
		return spec, fmt.Errorf("one wait has one kind; %s were given", strings.Join(kinds, " and "))
	case len(kinds) == 0:
		return spec, errors.New("add needs a condition: a command after --, --at RFC3339 / --for DURATION, or --github run <id> | pr <n>")
	}
	if *github == "" && (*state != "" || *repo != "") {
		return spec, errors.New("--state and --repo belong to --github")
	}

	switch {
	case *github != "":
		target, err := parseGitHubTarget(*github, *state, *repo)
		if err != nil {
			return spec, err
		}
		spec.Kind = domain.WaitKindGitHub
		spec.GitHub = &target
		spec.Condition = "github " + target.Ref() + " " + target.State
		if target.Repo != "" {
			spec.Condition += " in " + target.Repo
		}
		spec.Name = spec.Condition
	case *at != "" && *after != "":
		return spec, errors.New("--at and --for are two ways to name one instant; give one")
	case *at != "" || *after != "":
		instant, err := parseTimeWaitInstant(*at, *after, now)
		if err != nil {
			return spec, err
		}
		spec.Kind = domain.WaitKindTime
		spec.At = &instant
		spec.Condition = "time at " + instant.UTC().Format(time.RFC3339)
		spec.Name = spec.Condition
	default:
		spec.Kind = domain.WaitKindShell
		spec.Condition = strings.Join(spec.Command, " ")
		spec.Name = spec.Condition
		if len(spec.Name) > 60 {
			spec.Name = spec.Name[:60]
		}
	}
	if spec.ExplicitName != "" {
		spec.Name = spec.ExplicitName
	}

	if spec.Every < 30*time.Second {
		return spec, errors.New("--every must be at least 30s")
	}
	if spec.MaxEvery < spec.Every {
		return spec, errors.New("--max-every must not be shorter than --every")
	}
	if spec.WakeMode != "each" && spec.WakeMode != "all" {
		return spec, errors.New("--wake must be each or all")
	}
	if spec.At != nil {
		remaining := spec.At.Sub(now)
		switch {
		case spec.TimeoutExplicit && spec.Timeout <= remaining:
			return spec, fmt.Errorf("--timeout %s ends before the instant %s (%s away); raise --timeout or leave it out to have it set from the instant",
				spec.Timeout, spec.At.UTC().Format(time.RFC3339), remaining.Round(time.Second))
		case !spec.TimeoutExplicit && spec.Timeout <= remaining+timeWaitTimeoutMargin:
			spec.Timeout = remaining + timeWaitTimeoutMargin
		}
		if spec.Timeout > domain.MaxTaskWaitDuration {
			return spec, fmt.Errorf("the instant is more than %s away, which is longer than any wait may last", domain.MaxTaskWaitDuration)
		}
	}
	if spec.Timeout <= spec.Every {
		return spec, errors.New("--timeout must be longer than --every")
	}
	return spec, nil
}

// parseTimeWaitInstant reads --at or --for into an instant in the future.
func parseTimeWaitInstant(at, duration string, now time.Time) (time.Time, error) {
	var instant time.Time
	if at != "" {
		parsed, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return instant, fmt.Errorf("--at %q is not an RFC 3339 instant such as %s: %w", at, now.UTC().Add(time.Hour).Format(time.RFC3339), err)
		}
		instant = parsed
	} else {
		d, err := time.ParseDuration(duration)
		if err != nil {
			return instant, fmt.Errorf("--for %q is not a duration such as 2h30m: %w", duration, err)
		}
		if d <= 0 {
			return instant, fmt.Errorf("--for %s is not in the future", duration)
		}
		instant = now.Add(d)
	}
	if !instant.After(now) {
		return instant, fmt.Errorf("the instant %s has already passed (now %s); nothing to wait for", instant.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	return instant.UTC(), nil
}

// discardWriter keeps the flag package's own usage text off stderr; the
// command prints the wait help itself.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// registrationSummary is the human sentence about how a local wait polls.
func (spec localWaitSpec) registrationSummary(code int, firstLine string) string {
	switch spec.Kind {
	case domain.WaitKindTime:
		return fmt.Sprintf("met at %s; the poll interval follows the remaining time (at most %s), giving up after %s",
			spec.At.UTC().Format(time.RFC3339), spec.MaxEvery, spec.Timeout)
	case domain.WaitKindGitHub:
		return fmt.Sprintf("gh reports %s; polling every %s, backing off to %s, up to %s", firstLine, spec.Every, spec.MaxEvery, spec.Timeout)
	}
	return fmt.Sprintf("%s; polling every %s, backing off to %s, up to %s", firstRunSummary(code, firstLine), spec.Every, spec.MaxEvery, spec.Timeout)
}

// probeLocalWait proves a wait can be registered: a shell check is run once
// and refused when it cannot run, already exits 0 or gives up; a github
// target is read once and refused when gh cannot read it or the condition has
// already settled; a time wait was checked to lie in the future by the parser.
// It returns the first exit code and first output line the registration
// reports, and leaves the probe's evidence on the wait.
func probeLocalWait(ctx context.Context, spec localWaitSpec, w *wait.Wait, parked string) (int, string, error) {
	now := time.Now()
	switch spec.Kind {
	case domain.WaitKindShell:
		out, code, err := runCheck(ctx, *w)
		if err := refuseFirstRun(os.Stderr, out, code, err, parked); err != nil {
			return code, "", err
		}
		w.LastRunAt = &now
		w.Runs = 1
		w.LastExit = code
		w.LastOutput = out
		return code, firstOutputLine(out), nil
	case domain.WaitKindGitHub:
		runner := wait.New(nil, nil, nil)
		runner.GitHub = gitHubCommand
		// Name the repository on the wait itself when the caller did not, so the
		// stored wait says which repository it is about and the daemon's poll
		// does not depend on this directory still being a checkout of it. It is
		// best effort: the poll runs in the wait's directory either way, and a
		// repository that cannot be read here is gh's answer to report at the
		// probe below rather than a reason to refuse.
		if w.GitHub != nil && w.GitHub.Repo == "" {
			if repo := gitHubRepository(ctx, runner, w.Dir); repo != "" {
				w.GitHub.Repo = repo
			}
		}
		reading, err := runner.ReadGitHub(ctx, *w.GitHub, w.Dir)
		if err != nil {
			return 1, "", fmt.Errorf("%w: %v", wait.ErrGitHubProbe, err)
		}
		switch reading.Status {
		case wait.StatusMet:
			return 0, reading.Reason, fmt.Errorf("the condition already holds (%s), %s", reading.Reason, parked)
		case wait.StatusFailed:
			return 2, reading.Reason, fmt.Errorf("the condition has already settled against you (%s), %s", reading.Reason, parked)
		}
		w.LastRunAt = &now
		w.Runs = 1
		w.LastExit = 1
		w.LastOutput = reading.Reason
		return 1, reading.Reason, nil
	}
	return 1, "", nil
}
