package workerproto

import (
	"fmt"
	"strings"
	"time"
)

const SupervisionPromptByteCap = 32 << 10

// RenderSupervisionPrompt returns the exact prompt passed to the model.
func RenderSupervisionPrompt(activation SupervisionActivation) string {
	var prompt strings.Builder
	prompt.WriteString(activation.Prompt)
	if activation.EvidenceFiles {
		prompt.WriteString("\n\nFrozen evidence is available at inputs/supervision-evidence.json.\n")
		prompt.WriteString("The authored supervisor prompt is available at inputs/supervision-prompt.md.\n")
		prompt.WriteString("Read only the subjects needed for the current decision; do not reload the full inventory into context.\n")
	}
	prompt.WriteString("\nScoped commands\n")
	prompt.WriteString(fmt.Sprintf(
		"You are supervisor %q on run %s at activation epoch %d. These commands are your only authority.\n"+
			"Read the current revisions with show before every decision, and give every mutating command a\n"+
			"--request-id you have not used: repeating a key with the same payload returns the first answer,\n"+
			"and the same key with a different payload is refused.\n",
		activation.Principal, activation.RunID, activation.Epoch))
	if activation.CredentialReference != "" {
		prompt.WriteString("Admin credential reference: " + activation.CredentialReference + "\n")
	}
	if !activation.Deadline.IsZero() {
		prompt.WriteString("This activation ends at " + activation.Deadline.UTC().Format(time.RFC3339) +
			", after which your decisions are refused.\n")
	}
	prompt.WriteString(fmt.Sprintf("Turns available to this activation: %d.\n", activation.MaxTurns))
	prompt.WriteString("Do not delegate this review to a native subagent. Every separately scheduled\n" +
		"session is a campaign task the manifest declared.\n")
	for _, action := range activation.Actions {
		prompt.WriteString("\n  " + strings.Join(action.Command, " ") + "\n")
		for _, constraint := range action.Constraints {
			prompt.WriteString("    - " + constraint + "\n")
		}
	}
	prompt.WriteString("\nEnding this turn is not a decision. If the evidence does not support one you are\n" +
		"scoped to make, escalate and stop.\n")
	return prompt.String()
}
