package wait

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A command that times out returns at its run timeout even when a
// descendant it started still holds the command's output open.
func TestCommandWaitTimeoutDoesNotWaitForDescendant(t *testing.T) {
	started := time.Now()
	_, code, err := execCommand(context.Background(), Wait{
		Command:    []string{"/bin/sh", "-c", "sleep 2; true"},
		Dir:        t.TempDir(),
		RunTimeout: 20 * time.Millisecond,
	})
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "run timeout") || code != -1 {
		t.Fatalf("timed-out command = (%d, %v), want run timeout", code, err)
	}
	if elapsed > time.Second {
		t.Fatalf("20ms run timeout returned after %s: a descendant held the command open", elapsed.Round(time.Millisecond))
	}
}

// A noisy command keeps only a bounded tail of its output while it runs, and
// the tail is the end of what it wrote.
func TestCommandWaitKeepsABoundedOutputTail(t *testing.T) {
	out, code, err := execCommand(context.Background(), Wait{
		Command:    []string{"/bin/sh", "-c", "yes noise | head -c 4000000; echo; echo LAST-LINE; exit 1"},
		Dir:        t.TempDir(),
		RunTimeout: 30 * time.Second,
	})
	if err != nil || code != 1 {
		t.Fatalf("noisy command = (%d, %v), want exit 1", code, err)
	}
	if len(out) > commandOutputLimit {
		t.Fatalf("kept %d bytes of output, want at most %d", len(out), commandOutputLimit)
	}
	if !strings.HasSuffix(out, "LAST-LINE\n") {
		t.Fatalf("output tail does not end with the last line: %q", out[max(0, len(out)-64):])
	}
}

// A command that succeeds but leaves a background child holding its output
// is still met: the bounded wait for its output is not a failure.
func TestCommandWaitSucceedsDespiteABackgroundChildHoldingOutput(t *testing.T) {
	started := time.Now()
	out, code, err := execCommand(context.Background(), Wait{
		Command:    []string{"/bin/sh", "-c", "echo met; sleep 3 & exit 0"},
		Dir:        t.TempDir(),
		RunTimeout: 30 * time.Second,
	})
	if err != nil || code != 0 || out != "met\n" {
		t.Fatalf("successful command = (%q, %d, %v), want met with exit 0", out, code, err)
	}
	if elapsed := time.Since(started); elapsed > 2500*time.Millisecond {
		t.Fatalf("successful command waited %s for its background child", elapsed.Round(time.Millisecond))
	}
}

// Exit-code semantics are unchanged: 0 met, 2 give up, anything else not yet.
func TestCommandWaitReportsExitCodes(t *testing.T) {
	for _, want := range []int{0, 1, 2, 7} {
		out, code, err := execCommand(context.Background(), Wait{
			Command: []string{"/bin/sh", "-c", "echo checked; exit \"$0\"", strconv.Itoa(want)},
			Dir:     t.TempDir(),
		})
		if err != nil || code != want || out != "checked\n" {
			t.Fatalf("exit %d = (%q, %d, %v)", want, out, code, err)
		}
	}
}
