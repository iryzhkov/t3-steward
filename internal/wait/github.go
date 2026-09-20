package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// GitHubTarget is what a github wait observes: one workflow run or one pull
// request, and the state it waits for.
type GitHubTarget struct {
	// Kind is run or pr.
	Kind string `json:"kind"`
	// ID is the run id or the pull request number.
	ID string `json:"id"`
	// State is completed for a run; merged, reviewed or checks-passed for a
	// pull request.
	State string `json:"state"`
	// Repo is owner/name, passed to gh as --repo when set.
	Repo string `json:"repo,omitempty"`
}

// GitHub states by target kind, the first being the default.
var gitHubStates = map[string][]string{
	"run": {"completed"},
	"pr":  {"merged", "reviewed", "checks-passed"},
}

// GitHubStates lists the states a target kind accepts, the default first.
func GitHubStates(kind string) []string { return gitHubStates[kind] }

// Validate checks the target names a kind, an id and a state of that kind.
func (t GitHubTarget) Validate() error {
	states, ok := gitHubStates[t.Kind]
	if !ok {
		return fmt.Errorf("--github takes run <id> or pr <number>, not %q", t.Kind)
	}
	if strings.TrimSpace(t.ID) == "" {
		return fmt.Errorf("--github %s needs the %s id", t.Kind, t.Kind)
	}
	valid := false
	for _, state := range states {
		if t.State == state {
			valid = true
		}
	}
	if !valid {
		return fmt.Errorf("--state %q is not a state of a %s; a %s reaches %s", t.State, t.Kind, t.Kind, strings.Join(states, ", "))
	}
	if t.Repo != "" && (strings.Count(t.Repo, "/") != 1 || strings.HasPrefix(t.Repo, "/") || strings.HasSuffix(t.Repo, "/")) {
		return fmt.Errorf("--repo %q is not owner/name", t.Repo)
	}
	return nil
}

// Ref is the trailer's target value: run:<id> or pr:<n>.
func (t GitHubTarget) Ref() string { return t.Kind + ":" + t.ID }

// String describes the target and the state waited for.
func (t GitHubTarget) String() string {
	s := t.Ref() + " " + t.State
	if t.Repo != "" {
		s += " in " + t.Repo
	}
	return s
}

// Args are the fixed gh arguments that read the target. They never vary with
// user input beyond the id and the repository.
func (t GitHubTarget) Args() []string {
	var args []string
	switch t.Kind {
	case "run":
		args = []string{"run", "view", t.ID, "--json", "status,conclusion,url"}
	case "pr":
		args = []string{"pr", "view", t.ID, "--json", "state,mergedAt,reviewDecision,statusCheckRollup,url"}
	}
	if t.Repo != "" {
		args = append(args, "--repo", t.Repo)
	}
	return args
}

// GitHubRunner runs gh with the given arguments in a directory and returns its
// standard output. It is a seam so tests never reach GitHub.
//
// The directory is part of the call because gh resolves the repository from the
// working directory when no --repo is given, and the runner of a registered
// wait is the steward daemon, whose working directory is not the one the wait
// was registered in -- as a user service it is the filesystem root. A github
// wait registered without --repo therefore polled a gh that answered "not a git
// repository" every time and gave up after three of them, while the same
// command at registration time, run in the caller's checkout, had just read the
// target successfully.
type GitHubRunner func(ctx context.Context, dir string, args []string) (string, error)

// ExecGitHub runs the real gh on PATH, in dir when one is given.
func ExecGitHub(ctx context.Context, dir string, args []string) (string, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("gh %s: %s", strings.Join(args[:2], " "), message)
	}
	return stdout.String(), nil
}

// GitHubReading is one evaluation of a target against gh's answer.
type GitHubReading struct {
	// Status is StatusMet, StatusFailed or StatusWaiting.
	Status Status
	Reason string
	// Fields are the trailer pairs: target, state, conclusion, url.
	Fields map[string]string
}

// gitHubGiveUpAfter is how many consecutive gh errors end the wait.
const gitHubGiveUpAfter = 3

// gitHubGonePattern recognises a target that no longer exists, which gives up
// at once rather than after three identical answers.
var gitHubGonePattern = regexp.MustCompile(`(?i)could not find|not found|could not resolve|HTTP 404`)

// EvaluateGitHub applies the contract's mapping to gh's JSON.
func EvaluateGitHub(target GitHubTarget, output []byte) (GitHubReading, error) {
	reading := GitHubReading{Status: StatusWaiting, Fields: map[string]string{"target": target.Ref()}}
	switch target.Kind {
	case "run":
		var run struct {
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			URL        string `json:"url"`
		}
		if err := json.Unmarshal(output, &run); err != nil {
			return reading, fmt.Errorf("gh run view returned something other than the requested JSON: %w", err)
		}
		reading.Fields["state"] = strings.ToLower(run.Status)
		reading.Fields["conclusion"] = strings.ToLower(run.Conclusion)
		reading.Fields["url"] = run.URL
		switch {
		case strings.EqualFold(run.Status, "completed") && strings.EqualFold(run.Conclusion, "success"):
			reading.Status, reading.Reason = StatusMet, "run completed with conclusion success"
		case strings.EqualFold(run.Status, "completed"):
			reading.Status, reading.Reason = StatusFailed, "run completed with conclusion "+strings.ToLower(run.Conclusion)
		default:
			reading.Reason = "run is " + strings.ToLower(run.Status)
		}
	case "pr":
		var pr struct {
			State             string  `json:"state"`
			MergedAt          *string `json:"mergedAt"`
			ReviewDecision    string  `json:"reviewDecision"`
			StatusCheckRollup []struct {
				Conclusion string `json:"conclusion"`
				Status     string `json:"status"`
				State      string `json:"state"`
			} `json:"statusCheckRollup"`
			URL string `json:"url"`
		}
		if err := json.Unmarshal(output, &pr); err != nil {
			return reading, fmt.Errorf("gh pr view returned something other than the requested JSON: %w", err)
		}
		state := strings.ToLower(pr.State)
		merged := strings.EqualFold(pr.State, "MERGED") || (pr.MergedAt != nil && *pr.MergedAt != "")
		closed := strings.EqualFold(pr.State, "CLOSED") && !merged
		reading.Fields["state"] = state
		reading.Fields["url"] = pr.URL
		switch target.State {
		case "merged":
			switch {
			case merged:
				reading.Fields["state"] = "merged"
				reading.Status, reading.Reason = StatusMet, "pull request merged"
			case closed:
				reading.Status, reading.Reason = StatusFailed, "pull request closed without merging"
			default:
				reading.Reason = "pull request is " + state
			}
		case "reviewed":
			decision := strings.ToLower(pr.ReviewDecision)
			switch {
			case decision == "approved" || decision == "changes_requested":
				reading.Fields["state"] = decision
				reading.Status, reading.Reason = StatusMet, "pull request reviewed: "+decision
			case closed:
				reading.Status, reading.Reason = StatusFailed, "pull request closed before any review"
			case merged:
				reading.Fields["state"] = "merged"
				reading.Status, reading.Reason = StatusMet, "pull request merged"
			default:
				reading.Reason = "no review yet"
			}
		case "checks-passed":
			pending, failed := len(pr.StatusCheckRollup) == 0, ""
			for _, check := range pr.StatusCheckRollup {
				conclusion := strings.ToUpper(check.Conclusion)
				if conclusion == "" {
					conclusion = strings.ToUpper(check.State)
				}
				switch conclusion {
				case "SUCCESS", "SKIPPED", "NEUTRAL":
				case "FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE":
					failed = strings.ToLower(conclusion)
				default:
					pending = true
				}
			}
			switch {
			case failed != "":
				reading.Fields["conclusion"] = failed
				reading.Status, reading.Reason = StatusFailed, "a check concluded "+failed
			case closed:
				reading.Status, reading.Reason = StatusFailed, "pull request closed without merging"
			case pending:
				reading.Reason = "checks still running"
			default:
				reading.Fields["conclusion"] = "success"
				reading.Status, reading.Reason = StatusMet, "every check succeeded"
			}
		}
	default:
		return reading, fmt.Errorf("github target kind %q is not run or pr", target.Kind)
	}
	return reading, nil
}

func init() {
	kindRunners[domain.WaitKindGitHub] = (*Runner).runGitHubOnce
	kindConditions[domain.WaitKindGitHub] = func(w Wait) string {
		if w.GitHub == nil {
			return "GitHub: (no target)"
		}
		return "GitHub: " + w.GitHub.String()
	}
}

// ReadGitHub runs gh once for the target, in dir, and evaluates the answer. A
// gh error is returned as such; the caller decides whether it is one of three.
func (r *Runner) ReadGitHub(ctx context.Context, target GitHubTarget, dir string) (GitHubReading, error) {
	run := r.GitHub
	if run == nil {
		run = ExecGitHub
	}
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	output, err := run(cctx, dir, target.Args())
	if err != nil {
		return GitHubReading{}, err
	}
	return EvaluateGitHub(target, []byte(output))
}

// runGitHubOnce is one poll of a github wait. Three consecutive gh errors give
// up with the last error; a target that is gone gives up at once. runOnce
// records the poll and saves the row.
func (r *Runner) runGitHubOnce(ctx context.Context, w *Wait, now time.Time) {
	if w.GitHub == nil {
		w.LastExit = 2
		w.settle(StatusFailed, "the github wait has no target", now, nil)
		return
	}
	// The wait's own directory, which is the one it was registered in: see
	// GitHubRunner. A wait that named a repository does not need it, and one
	// whose directory has since been removed fails with gh's own reason.
	reading, err := r.ReadGitHub(ctx, *w.GitHub, w.Dir)
	if err != nil {
		w.Errors++
		w.LastExit = 1
		w.LastOutput = tail(err.Error(), 4000)
		gone := gitHubGonePattern.MatchString(err.Error())
		if gone || w.Errors >= gitHubGiveUpAfter {
			w.LastExit = 2
			reason := fmt.Sprintf("gh failed %d times in a row; the last error: %v", w.Errors, err)
			if gone {
				reason = "the target no longer exists: " + err.Error()
			}
			w.settle(StatusGaveUp, reason, now, map[string]string{"target": w.GitHub.Ref()})
			return
		}
		r.backOff(w)
		return
	}
	w.Errors = 0
	w.LastOutput = reading.Reason
	switch reading.Status {
	case StatusMet:
		w.LastExit = 0
		w.settle(StatusMet, reading.Reason, now, reading.Fields)
	case StatusFailed:
		w.LastExit = 2
		w.settle(StatusFailed, reading.Reason, now, reading.Fields)
	default:
		w.LastExit = 1
		r.backOff(w)
	}
}

// backOff doubles the interval up to the ceiling, as a shell not-yet does.
func (*Runner) backOff(w *Wait) {
	next := w.currentInterval() * 2
	if max := w.maxInterval(); next > max {
		next = max
	}
	w.Interval = next
}

// ErrGitHubProbe wraps a registration-time gh failure.
var ErrGitHubProbe = errors.New("gh cannot read the target")
