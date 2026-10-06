package campaign

import (
	"strings"
	"testing"
)

func TestLedgerHelpDocumentsTheOptIn(t *testing.T) {
	found := false
	for _, topic := range HelpTopics() {
		found = found || (topic.Name == "ledger" && topic.Body == LedgerHelp)
	}
	if !found {
		t.Fatal("the ledger help topic is not registered")
	}
	for _, want := range []string{
		"ledger: {jocasta_project: steward}",
		"jocasta_project", "plan", "risk", "acceptance",
		"<jocasta_project>/handoffs/<run id>.md",
		"Steward record", "Executor-provided",
		"behind", "--if-revision",
		"validate",
	} {
		if !strings.Contains(LedgerHelp, want) {
			t.Fatalf("ledger help is missing %q", want)
		}
	}
}
