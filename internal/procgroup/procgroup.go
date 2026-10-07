// Package procgroup runs a short-lived local command so that cancelling it
// ends everything it started and returns within a bounded time. The owner
// notification command sink and command waits share it.
package procgroup

import (
	"context"
	"os/exec"
	"time"
	"unicode/utf8"
)

// CommandContext is exec.CommandContext for a program that runs in a process
// group of its own, where cancellation kills the whole group and not only the
// program. waitDelay bounds how long Wait then waits for output pipes that a
// process outside the group still holds open; zero leaves it unbounded.
func CommandContext(ctx context.Context, waitDelay time.Duration, name string, args ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, name, args...)
	isolate(command)
	command.WaitDelay = waitDelay
	return command
}

// TailBuffer keeps the last Limit bytes written to it and discards the rest as
// it arrives, so a noisy program cannot grow its caller's memory. It always
// accepts the whole write: refusing one would kill the program with a broken
// pipe only because its caller has seen enough. A Limit of zero or less keeps
// everything. It is not safe for concurrent use; exec.Cmd serialises writes
// when Stdout and Stderr are the same writer.
type TailBuffer struct {
	Limit     int
	buf       []byte
	truncated bool
}

func (b *TailBuffer) Write(p []byte) (int, error) {
	if b.Limit <= 0 {
		b.buf = append(b.buf, p...)
		return len(p), nil
	}
	if len(p) >= b.Limit {
		b.truncated = b.truncated || len(p) > b.Limit || len(b.buf) > 0
		b.buf = append(b.buf[:0], p[len(p)-b.Limit:]...)
		return len(p), nil
	}
	// Compact only when the retained bytes reach twice the limit, so each
	// byte is copied a bounded number of times.
	if len(b.buf)+len(p) > 2*b.Limit {
		keep := b.Limit - len(p)
		b.truncated = true
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-keep:]...)
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// Truncated reports whether any written byte was discarded.
func (b *TailBuffer) Truncated() bool {
	return b.truncated || (b.Limit > 0 && len(b.buf) > b.Limit)
}

// String returns the last Limit bytes, starting at a rune boundary when the
// front was cut.
func (b *TailBuffer) String() string {
	value := b.buf
	if b.Limit > 0 && len(value) > b.Limit {
		value = value[len(value)-b.Limit:]
	}
	if b.Truncated() {
		for len(value) > 0 && !utf8.RuneStart(value[0]) {
			value = value[1:]
		}
	}
	return string(value)
}
