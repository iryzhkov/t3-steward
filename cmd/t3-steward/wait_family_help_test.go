package main

import (
	"regexp"
	"strings"
	"testing"
)

// M-3. "wait list --help" was rewritten and "wait --help" was not, so the
// shipped binary printed the old contract on its family page: local checks
// only, no --host, and a --native form that took neither --thread nor --host.
// Shipping a binary whose own help describes the superseded behaviour is the
// finding this campaign started from, so the two pages are tied together here
// rather than left to agree by hand.

var helpFlagPattern = regexp.MustCompile(`--[a-z][a-z-]*`)

// waitFamilyListBlock is the "list" part of the family page: every line from
// the first list synopsis up to the next command's synopsis.
func waitFamilyListBlock(t *testing.T) string {
	t.Helper()
	lines := strings.Split(waitCommandUsage, "\n")
	start := -1
	for i, line := range lines {
		switch {
		case start < 0 && strings.HasPrefix(line, "  list "):
			start = i
		case start >= 0 && strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   ") &&
			!strings.HasPrefix(line, "  list "):
			return strings.Join(lines[start:i], "\n")
		}
	}
	if start < 0 {
		t.Fatal("the wait family page has no list synopsis")
	}
	return strings.Join(lines[start:], "\n")
}

func TestTheWaitFamilyPageDescribesTheListVerbItShips(t *testing.T) {
	block := waitFamilyListBlock(t)
	page, found := helpPageFor("wait list")
	if !found {
		t.Fatal("there is no \"wait list\" help page to agree with")
	}
	// Every flag the verb's own page puts in a synopsis is a flag the family
	// page has to show too, or an agent reading the family page believes the
	// verb rejects it.
	for _, usage := range page.Usage {
		for _, flag := range helpFlagPattern.FindAllString(usage, -1) {
			if !strings.Contains(block, flag) {
				t.Fatalf("the wait family page's list entries do not offer %s:\n%s", flag, block)
			}
		}
	}
	// The joined answer and its JSON document, which is what changed.
	for _, want := range []string{"coordinator", "waits, sources, unavailable, hidden"} {
		if !strings.Contains(block, want) {
			t.Fatalf("the wait family page's list entries do not mention %q:\n%s", want, block)
		}
	}
	if strings.Contains(block, "Local checks of this thread") {
		t.Fatalf("the wait family page still describes list as local-only:\n%s", block)
	}
}
