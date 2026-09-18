package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// localWaitSpec is a parsed `wait add` for a local kind: shell, time or
// github. One parser serves the interactive form and `--task current`, so the
// two cannot drift apart in which kinds and flags they accept; each command
// then refuses the flags that do not apply to it.
type localWaitSpec struct {
	Kind    domain.WaitKind
	Command []string
	// At is the instant of a time wait.
	At *time.Time
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
	if err := fs.Parse(args); err != nil {
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
	if len(spec.Command) > 0 {
		kinds = append(kinds, "a command after --")
	}
	switch {
	case len(kinds) > 1:
		return spec, fmt.Errorf("one wait has one kind; %s were given", strings.Join(kinds, " and "))
	case len(kinds) == 0:
		return spec, errors.New("add needs a condition: a command after --, or --at RFC3339 / --for DURATION")
	}

	switch {
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
	}
	return fmt.Sprintf("%s; polling every %s, backing off to %s, up to %s", firstRunSummary(code, firstLine), spec.Every, spec.MaxEvery, spec.Timeout)
}
