package main

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// usageListsCommand reports whether the top-level overview has a command-list entry for
// this verb. The entry is matched as a whole word so that "worker-exchange" does not
// answer for "worker", which is the confusion the missing entry created.
func usageListsCommand(text, verb string) bool {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "  "+verb+" ") {
			return true
		}
	}
	return false
}

// B-9: dispatch routes "worker" and "ui-archive", and the overview named neither. It
// named worker-exchange, the restricted SSH endpoint, so breadth help said the worker
// verb did not exist while several refusals told the reader to run it. Enrolment is the
// action that unblocks a stale worker, and it is under "worker".
func TestUsageListsTheRoutedWorkerVerbs(t *testing.T) {
	for _, verb := range []string{"worker", "ui-archive"} {
		if !usageListsCommand(usage, verb) {
			t.Errorf("the command list does not name %q, which dispatch routes", verb)
		}
	}
	// The restricted endpoints are different verbs and stay named.
	for _, verb := range []string{"worker-exchange", "coordinator-exchange", "archive"} {
		if !usageListsCommand(usage, verb) {
			t.Errorf("the command list no longer names %q", verb)
		}
	}
	if !strings.Contains(usage, "enroll") {
		t.Error("the worker entry does not name enroll, the action several refusals ask for")
	}
}

// C-7: task run's help is scrupulous about defaults everywhere else and omitted these
// two. The class default is asserted against the parser here; the turn default is
// applied in internal/backlog, applyManifestDefaults, where task.MaxTurns == 0 becomes 3.
func TestTaskRunUsageStatesTheClassAndTurnDefaults(t *testing.T) {
	flat := strings.Join(strings.Fields(taskRunUsage), " ")
	for _, phrase := range []string{
		"--class surplus|required (default surplus)",
		"--max-turns N (default 3)",
	} {
		if !strings.Contains(flat, phrase) {
			t.Errorf("task run usage does not state %q", phrase)
		}
	}
	parsed, err := parseTaskRunArgs([]string{"--", "a prompt"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.class != string(domain.TaskClassSurplus) {
		t.Errorf("the help says the class default is surplus; the parser defaults to %q", parsed.class)
	}
}

// C-8: "--run <run>" was documented as "the older spelling of --node <run>". A superseded
// path that still appears in help is advertised rather than deprecated, so the entry is
// gone. The parser still accepts both older spellings (cmd/t3-steward/wait_node.go), which
// is compatibility for the callers that already use them and not an invitation to more.
func TestWaitUsageDoesNotAdvertiseTheSupersededSpellings(t *testing.T) {
	for _, phrase := range []string{"--run <run>", "older spelling"} {
		if strings.Contains(waitCommandUsage, phrase) {
			t.Errorf("wait help still advertises %q", phrase)
		}
	}
	if !strings.Contains(waitCommandUsage, "--node <run>[/<task>]") {
		t.Error("wait help no longer documents --node, the spelling that supersedes them")
	}
	// --task current is a different flag with a different meaning and stays documented.
	if !strings.Contains(waitCommandUsage, "add --task current") {
		t.Error("wait help no longer documents the task-bound form")
	}
}
