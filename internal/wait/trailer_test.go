package wait

import (
	"strings"
	"testing"
)

// The first line of every wake is the trailer: kind, outcome and wait id first,
// then the kind-specific pairs. Readers parse it, so it has to round-trip.
func TestWakeTrailerRendersAndParsesEveryKind(t *testing.T) {
	cases := []struct {
		kind, outcome, id string
		fields            []Field
	}{
		{"shell", "met", "w-1", []Field{F("exit", "0")}},
		{"time", "met", "w-2", []Field{F("at", "2030-01-01T00:00:00Z")}},
		{"github", "failed", "w-3", []Field{F("target", "run:123"), F("state", "completed"), F("conclusion", "failure"), F("url", "https://github.com/o/r/actions/runs/123")}},
		{"node", "met", "nw-4", []Field{F("run", "run-1"), F("task", "sink:run-1"), F("attempt", ""), F("revision", "7"), F("progress", "succeeded"), F("result", "t3-steward result run-1")}},
		{"quota", "met", "tw-5", []Field{F("pool", "claude"), F("phase", "normal"), F("percent", "42")}},
	}
	for _, c := range cases {
		line := WakeTrailer(c.kind, c.outcome, c.id, c.fields...)
		if strings.Contains(line, "\n") {
			t.Fatalf("%s: the trailer is more than one line: %q", c.kind, line)
		}
		prefix := "t3-steward-wait kind=" + c.kind + " outcome=" + c.outcome + " wait=" + c.id
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("%s: trailer %q does not start with %q", c.kind, line, prefix)
		}
		parsed, ok := ParseWakeTrailer(line + "\n\nsome prose\n")
		if !ok {
			t.Fatalf("%s: the trailer did not parse: %q", c.kind, line)
		}
		if parsed["kind"] != c.kind || parsed["outcome"] != c.outcome || parsed["wait"] != c.id {
			t.Fatalf("%s: parsed %v from %q", c.kind, parsed, line)
		}
		for _, f := range c.fields {
			if f.Value == "" {
				if _, present := parsed[f.Key]; present {
					t.Fatalf("%s: an empty %s was rendered: %q", c.kind, f.Key, line)
				}
				continue
			}
			if parsed[f.Key] != f.Value {
				t.Fatalf("%s: %s parsed as %q, want %q, from %q", c.kind, f.Key, parsed[f.Key], f.Value, line)
			}
		}
	}
}

// A value with a space is quoted, and a reader that ignores unknown keys still
// gets the known ones.
func TestWakeTrailerQuotesValuesWithSpacesAndIgnoresUnknownKeys(t *testing.T) {
	line := WakeTrailer("node", "met", "nw-1", F("result", "t3-steward result run-1"), F("failed", "a,b"))
	if !strings.Contains(line, `result="t3-steward result run-1"`) {
		t.Fatalf("a value with a space is not quoted: %q", line)
	}
	if !strings.Contains(line, " failed=a,b") {
		t.Fatalf("a value without a space is quoted: %q", line)
	}
	parsed, ok := ParseWakeTrailer(line + " future=\"some new key\" other=1")
	if !ok || parsed["result"] != "t3-steward result run-1" || parsed["failed"] != "a,b" || parsed["future"] != "some new key" {
		t.Fatalf("parsed %v ok=%v", parsed, ok)
	}
	if _, ok := ParseWakeTrailer("Wait finished (T3 steward): something"); ok {
		t.Fatal("a prose first line parsed as a trailer")
	}
}
