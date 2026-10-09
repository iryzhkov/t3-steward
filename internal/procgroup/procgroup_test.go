package procgroup

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

func TestCommandCancellationReturnsDespiteADescendantHoldingOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// The descendant sleeps far longer than the scaled bound below, so a Run
	// that waited for it fails the bound.
	command := CommandContext(ctx, time.Second, "/bin/sh", "-c", "sleep 60; true")
	var out TailBuffer
	out.Limit = 64
	command.Stdout = &out
	command.Stderr = &out
	started := time.Now()
	if err := command.Run(); err == nil {
		t.Fatal("cancelled command reported success")
	}
	if elapsed := time.Since(started); elapsed > testtiming.Bound(2*time.Second) {
		t.Fatalf("cancelled command returned after %s", elapsed.Round(time.Millisecond))
	}
}

func TestCommandSucceedsWhenItFinishesBeforeItsContext(t *testing.T) {
	command := CommandContext(context.Background(), time.Second, "/bin/sh", "-c", "echo ok")
	var out TailBuffer
	out.Limit = 64
	command.Stdout = &out
	if err := command.Run(); err != nil || out.String() != "ok\n" {
		t.Fatalf("command = (%q, %v)", out.String(), err)
	}
}

func TestTailBufferKeepsTheLastBytes(t *testing.T) {
	var out TailBuffer
	out.Limit = 10
	for i := 0; i < 1000; i++ {
		if n, err := out.Write([]byte("0123456789abc")); n != 13 || err != nil {
			t.Fatalf("write = (%d, %v), want the whole slice accepted", n, err)
		}
	}
	if _, err := out.Write([]byte("END")); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "6789abcEND" {
		t.Fatalf("tail = %q, want the last 10 bytes", got)
	}
	if !out.Truncated() {
		t.Fatal("tail buffer did not report discarded bytes")
	}
	if cap(out.buf) > 4*out.Limit+16 {
		t.Fatalf("tail buffer grew to %d bytes for a %d-byte limit", cap(out.buf), out.Limit)
	}
}

func TestTailBufferDoesNotStartMidRune(t *testing.T) {
	var out TailBuffer
	out.Limit = 4
	_, _ = out.Write([]byte(strings.Repeat("é", 10)))
	if got := out.String(); !utf8.ValidString(got) || got != "éé" {
		t.Fatalf("tail = %q, want whole runes", got)
	}
	var small TailBuffer
	small.Limit = 4
	_, _ = small.Write([]byte("ab"))
	if small.String() != "ab" || small.Truncated() {
		t.Fatalf("short tail = %q truncated=%v", small.String(), small.Truncated())
	}
}
