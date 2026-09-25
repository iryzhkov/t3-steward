package ownernotify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"time"
)

// CommandSinkName is the command sink's identity in the outbox.
const CommandSinkName = "command"

// CommandPayloadVersion is the schema version of the JSON a command sink
// receives. A receiver reads it first.
const CommandPayloadVersion = 1

// Command runs a local program for every notification, with the event as one
// JSON document on its standard input. It is how an owner reaches a channel
// this package does not know: a Matrix bot, ntfy, mail, a phone.
//
// The program is run from an argv with no shell, so nothing in an event can
// become part of a command line. Exit status 0 is delivery; anything else is
// a failure retried with the same backoff as a webhook, and the program must
// therefore tolerate receiving the same id more than once.
//
// The program gets a minimal environment -- PATH, HOME and LANG, plus the
// variables the operator names -- because the coordinator's own environment
// can hold credentials that a notification script has no business reading.
// It runs in a process group of its own, so a timeout kills everything it
// started and not only the program itself.
type Command struct {
	argv      []string
	env       []string
	selection Selection
}

// commandBaseEnvironment is what every command receives from the
// coordinator's environment, when set.
var commandBaseEnvironment = []string{"PATH", "HOME", "LANG"}

// NewCommand returns a command sink. env names further variables to pass
// through from the coordinator's environment.
func NewCommand(argv, env []string, selection Selection) *Command {
	selection.Events = append([]Event(nil), selection.Events...)
	return &Command{argv: append([]string(nil), argv...), env: append([]string(nil), env...), selection: selection}
}

// environment is the program's whole environment.
func (c *Command) environment() []string {
	var environment []string
	seen := map[string]bool{}
	for _, name := range append(append([]string(nil), commandBaseEnvironment...), c.env...) {
		if seen[name] {
			continue
		}
		seen[name] = true
		if value, ok := os.LookupEnv(name); ok {
			environment = append(environment, name+"="+value)
		}
	}
	return environment
}

// Name implements Sink.
func (c *Command) Name() string { return CommandSinkName }

// Selection implements Sink.
func (c *Command) Selection() Selection { return c.selection }

// CommandPayload is what a command sink receives on standard input.
type CommandPayload struct {
	SchemaVersion int `json:"schemaVersion"`
	Notification
	// Commands are the next commands the owner would type.
	Commands map[string]string `json:"commands,omitempty"`
	// Message is the same short line a Discord sink would post.
	Message string `json:"message"`
}

// NewCommandPayload builds the standard-input document for a notification.
func NewCommandPayload(notification Notification) CommandPayload {
	return CommandPayload{
		SchemaVersion: CommandPayloadVersion,
		Notification:  notification,
		Commands:      notification.Commands(),
		Message:       RenderDiscord(notification),
	}
}

// Deliver implements Sink. The notifier's context bounds the run; a program
// still running when it ends is killed and the send counts as failed.
func (c *Command) Deliver(ctx context.Context, notification Notification) error {
	if len(c.argv) == 0 {
		return &DeliveryError{Reason: "command sink has no argv", Permanent: true}
	}
	payload, err := json.Marshal(NewCommandPayload(notification))
	if err != nil {
		return &DeliveryError{Reason: "encode command payload: " + err.Error(), Permanent: true}
	}
	command := exec.CommandContext(ctx, c.argv[0], c.argv[1:]...)
	command.Stdin = bytes.NewReader(payload)
	// A non-nil empty slice, not nil: nil would inherit everything.
	command.Env = append([]string{}, c.environment()...)
	isolateProcessGroup(command)
	var stderr limitedBuffer
	stderr.limit = 512
	command.Stderr = &stderr
	// A program that forks a child holding stderr open would otherwise keep
	// Wait blocked after the context killed the program itself.
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil {
		reason := "command failed: " + err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			reason = "command exited with status " + strings.TrimPrefix(exitErr.ProcessState.String(), "exit status ")
		}
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			reason += ": " + truncate(inline(detail), 200)
		}
		return &DeliveryError{Reason: reason}
	}
	return nil
}

// limitedBuffer keeps the first limit bytes written to it and discards the
// rest, so a noisy program cannot grow the coordinator's memory.
type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.Len(); room > 0 {
		b.Buffer.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}
