package campaign

import (
	"os"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

func TestAuthoringUsesStewardTaskContract(t *testing.T) {
	source, err := os.ReadFile("../backlog/task_contract.md")
	if err != nil {
		t.Fatal(err)
	}
	contract := strings.TrimSpace(string(source))
	if !strings.Contains(AuthoringHelp, contract) || !strings.Contains(backlog.FirstTurnPrompt("author prompt", nil), contract) {
		t.Fatal("help and first-turn prompt must include the canonical contract unchanged")
	}
	for _, want := range []string{"Executor prompt template:", "Goal: <bounded outcome>", "Verification: <exact commands>", "Review: <declared independent review>"} {
		if !strings.Contains(AuthoringHelp, want) {
			t.Errorf("missing template field %q", want)
		}
	}
	doc, err := os.ReadFile("../../docs/examples/campaign/README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(doc), "../../../internal/backlog/task_contract.md") {
		t.Fatal("authoring docs must link to the canonical contract rather than copy it")
	}
}
