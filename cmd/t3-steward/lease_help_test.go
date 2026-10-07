package main

import (
	"strings"
	"testing"
)

// Review round 1 (P3): "lease --help" reflowed the option list into one
// run-on paragraph and its exit line omitted the check verdicts 10 and 11.
func TestLeaseShortHelpKeepsUsageOptionsAndVerdictExits(t *testing.T) {
	page, ok := helpPageFor("lease")
	if !ok {
		t.Fatal("no lease help page")
	}
	short := page.renderShort()
	if !strings.Contains(short, "Usage:\n  t3-steward lease <acquire|renew|release|check|show|list> [NAME] [options]\n") {
		t.Fatalf("usage line not on its own:\n%s", short)
	}
	for _, runOn := range []string{"Client configuration. --owner-thread", "case is preserved. --config", "refusal. Acquire"} {
		if strings.Contains(strings.Join(strings.Fields(short), " "), runOn) {
			t.Fatalf("option descriptions reflowed into prose (%q):\n%s", runOn, short)
		}
	}
	if !strings.Contains(short, "Options:\n  --config; --owner-thread; --ttl") {
		t.Fatalf("options list missing:\n%s", short)
	}
	if !strings.Contains(short, "Exit: 0 held by caller or done; 10 conflict, fencing or authority refusal;") || strings.Contains(short, "Exit: 0 accepted/done; nonzero refused or failed.") {
		t.Fatalf("exit line omits lease verdicts:\n%s", short)
	}
	// Self-review: authority refusals also answer 10, and an error answer is
	// replayed for the same request id, so a retry after one needs a new id.
	if !strings.Contains(short, "10 conflict, fencing or authority refusal") {
		t.Fatalf("exit line omits authority refusals:\n%s", short)
	}
	if !strings.Contains(page.Body, "after an error answer, retry with a new --request-id") {
		t.Fatalf("full reference omits request-id retry rule:\n%s", page.Body)
	}
	for _, line := range []string{
		"  --owner-thread ID    Holder thread, default current; resolves the calling T3 thread.\n",
		"  --json               Print one JSON response, including a refusal.\n",
	} {
		if !strings.Contains(page.Body, line) {
			t.Fatalf("full reference lost option line %q", line)
		}
	}
}
