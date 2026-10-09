package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// errTaskCheckConditionChanged reports a registration whose request ID this
// host already checks for a different local condition. The coordinator record
// does not carry everything a local check runs with, such as a shell check's
// directory, so this host is where such a change is caught.
var errTaskCheckConditionChanged = errors.New("the request ID already registered a different condition")

// registeredTaskCheck is this host's local check for the task-bound wait
// taskWaitID, if one was saved by an earlier registration.
func registeredTaskCheck(checks []wait.Wait, taskWaitID string) (wait.Wait, bool) {
	for _, check := range checks {
		if check.TaskWaitID == taskWaitID {
			return check, true
		}
	}
	return wait.Wait{}, false
}

// taskCheckConditionDifference names the first part of the condition that a
// registration's local check changes from the check already saved for its
// request ID, or returns "" when they watch the same thing. Polling settings
// and the name are not the condition; the kind, command, directory, instant,
// GitHub target and deadline treatment are.
func taskCheckConditionDifference(saved, local wait.Wait) string {
	switch {
	case saved.Kind.OrShell() != local.Kind.OrShell():
		return fmt.Sprintf("kind %s, not %s", local.Kind.OrShell(), saved.Kind.OrShell())
	case !slices.Equal(saved.Command, local.Command):
		return fmt.Sprintf("command %q, not %q", local.Command, saved.Command)
	case saved.Dir != local.Dir:
		return fmt.Sprintf("directory %s, not %s", local.Dir, saved.Dir)
	case !sameInstant(saved.At, local.At):
		return fmt.Sprintf("instant %s, not %s", formatInstant(local.At), formatInstant(saved.At))
	case !sameGitHubTarget(saved.GitHub, local.GitHub):
		return fmt.Sprintf("GitHub target %s, not %s", formatGitHubTarget(local.GitHub), formatGitHubTarget(saved.GitHub))
	case saved.OrTimeout != local.OrTimeout:
		return fmt.Sprintf("--or-timeout %t, not %t", local.OrTimeout, saved.OrTimeout)
	}
	return ""
}

// refuseChangedTaskCheck refuses a registration that would answer a
// different local condition with the wait already saved for its request ID.
func refuseChangedTaskCheck(requestID string, saved, local wait.Wait) error {
	difference := taskCheckConditionDifference(saved, local)
	if difference == "" {
		return nil
	}
	return fmt.Errorf("%w: request ID %q is task-bound wait %s, and this registration has %s, so it was not registered and that wait is unchanged; register it without --request-id, or with a different one, to add it to this park",
		errTaskCheckConditionChanged, requestID, saved.TaskWaitID, difference)
}

func sameInstant(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func formatInstant(at *time.Time) string {
	if at == nil {
		return "none"
	}
	return at.UTC().Format(time.RFC3339Nano)
}

func sameGitHubTarget(a, b *wait.GitHubTarget) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func formatGitHubTarget(target *wait.GitHubTarget) string {
	if target == nil {
		return "none"
	}
	return strings.TrimSpace(target.Ref() + " " + target.State + " " + target.Repo)
}

// saveRegisteredTaskCheck saves the local check of a registered task-bound
// wait. A retry finds the check its first registration saved and leaves it
// as it is, unless the retry describes another condition, which is refused.
func saveRegisteredTaskCheck(ctx context.Context, store *sqlite.Store, requestID string, local wait.Wait) error {
	checks, err := store.ListWaits(ctx, "")
	if err != nil {
		return err
	}
	if saved, ok := registeredTaskCheck(checks, local.TaskWaitID); ok {
		return refuseChangedTaskCheck(requestID, saved, local)
	}
	return store.InsertTaskWaitCheck(ctx, local)
}
