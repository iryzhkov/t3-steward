package main

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// firstOutputLineLimit bounds what registration quotes from the first run of a
// check, so a chatty command does not turn one warning into a transcript.
const firstOutputLineLimit = 200

// firstOutputLine is the first non-blank line of a check's output, trimmed and
// bounded. It is what registration quotes back so that an author can tell a
// "not yet" from a broken command without re-running it.
func firstOutputLine(out string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if len(line) > firstOutputLineLimit {
			return line[:firstOutputLineLimit] + "..."
		}
		return line
	}
	return ""
}

// refuseFirstRun is the registration refusal for a first run the protocol
// cannot accept: a command that cannot run, one that already exits 0, and one
// that gives up at once. It returns nil for every other exit.
func refuseFirstRun(stderr io.Writer, out string, code int, err error, parked string) error {
	switch {
	case err != nil:
		fmt.Fprintf(stderr, "%s", out)
		return fmt.Errorf("check cannot run: %v", err)
	case code == 0:
		fmt.Fprintf(stderr, "%s", out)
		return errors.New("the check already exits 0: the condition is met, " + parked)
	case code == 2:
		fmt.Fprintf(stderr, "%s", out)
		return errors.New("the check exits 2 (give up) right away; fix it before registering")
	}
	return nil
}

// warnUnconventionalFirstExit says on stderr what registration cannot decide.
// The protocol reads every exit other than 0 and 2 as "not yet", so a check
// whose first run exits 7 is registered exactly like one that exits 1. That is
// deliberate, because an exit 7 can be a legitimate not-yet, and it is also
// how a check that can never succeed polls quietly until its deadline. The
// warning quotes the exit and the first output line so the author can tell
// which of the two this is.
func warnUnconventionalFirstExit(stderr io.Writer, code int, firstLine string) {
	if code == 1 {
		return
	}
	fmt.Fprintf(stderr, "warning: the first run of the check exited %d (first output line: %q). "+
		"The wait protocol treats every exit other than 0 and 2 as \"not yet\", so the check is registered and keeps polling; "+
		"if that exit is an error rather than a not-yet, the check may never succeed and the wait ends only at its timeout.\n",
		code, firstLine)
}

// firstRunSummary is the human form of what the first run reported.
func firstRunSummary(code int, firstLine string) string {
	if firstLine == "" {
		return fmt.Sprintf("first check exited %d", code)
	}
	return fmt.Sprintf("first check exited %d (%s)", code, firstLine)
}

// tokenSourceError is the failure of an operation that needed the T3 API
// token when the only source of one was the t3 CLI and the CLI is gone. It
// names the operation and every alternative, and prints none of the lookup's
// own wording; the cause stays reachable through errors.Is.
type tokenSourceError struct {
	operation string
	cause     error
}

func (e *tokenSourceError) Error() string {
	return e.operation + " needs the T3 API token: set t3.token or t3.token_file in the configuration, " +
		"or T3_STEWARD_T3_TOKEN in the environment, or restore the t3 CLI that mints one (" +
		t3api.ErrT3CLINotFound.Error() + ")"
}

func (e *tokenSourceError) Unwrap() error { return e.cause }

// connectForCallerThread builds the T3 client that verifies the caller's
// thread before a wait is registered. The token is resolved from the
// configuration and the environment before the t3 CLI is consulted, and when
// nothing supplies one the error says which operation needed it.
func connectForCallerThread(cfg config.Config, logger *slog.Logger) (*t3api.Client, string, error) {
	client, dataDir, err := connect(cfg, logger)
	if errors.Is(err, t3api.ErrT3CLINotFound) {
		return nil, dataDir, &tokenSourceError{operation: "resolving the caller's thread", cause: err}
	}
	return client, dataDir, err
}
