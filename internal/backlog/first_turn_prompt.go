package backlog

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/compat"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TaskContract is the canonical contract linked by authoring docs and included in help.
//
//go:embed task_contract.md
var TaskContract string

// TaskCompletionSupplement is the Steward-owned contract plus this task's
// declared outputs. It is shared by runtime composition and size checks.
func TaskCompletionSupplement(outputs []domain.ArtifactDeclaration) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(TaskContract))
	var files, commits []string
	for _, output := range outputs {
		if output.Commit == nil {
			files = append(files, "`"+output.Name+"`")
		} else {
			commits = append(commits, "`"+output.Name+"`")
		}
	}
	if len(files) != 0 {
		b.WriteString("\nDeclared outputs, which must exist when the turn ends: " + strings.Join(files, ", ") + ".")
	}
	if len(commits) != 0 {
		b.WriteString("\nDeclared commits, which must be committed when the turn ends: " + strings.Join(commits, ", ") + ".")
	}
	return b.String()
}

// FirstTurnPrompt prepends the contract to the unchanged author prompt.
// Preflight and recovery add their own envelopes; the worker measures those.
func FirstTurnPrompt(prompt string, outputs []domain.ArtifactDeclaration) string {
	return TaskCompletionSupplement(outputs) + "\n\n" + prompt
}

// OutputDeclarations are the task's declared outputs as ingestion records
// them: the files first, in manifest order, then the commits.
func (t ManifestTask) OutputDeclarations() []domain.ArtifactDeclaration {
	var outputs []domain.ArtifactDeclaration
	if len(t.Outputs) != 0 {
		outputs = make([]domain.ArtifactDeclaration, 0, len(t.Outputs)+len(t.Commits))
	}
	for _, output := range t.Outputs {
		outputs = append(outputs, domain.ArtifactDeclaration{Name: output, MediaType: mediaType(output)})
	}
	for _, commit := range t.Commits {
		outputs = append(outputs, domain.ArtifactDeclaration{
			Name: commit.Name, MediaType: "application/json",
			Commit: &domain.CommitOutput{Revision: commit.Revision},
		})
	}
	return outputs
}

// TurnInputTooLargeError refuses a turn input T3 would refuse when the turn
// starts, which is after the thread exists and the task is counted running.
type TurnInputTooLargeError struct {
	// Length is the composed input's length as T3 counts it.
	Length int
	// Added is how much of Length the Steward added to the author's prompt.
	Added int
}

func (e *TurnInputTooLargeError) Error() string {
	return fmt.Sprintf("the first-turn prompt is %d characters (the prompt plus %d the Steward adds), over T3's turn input limit of %d characters (T3 %s to %s); "+
		"put large material such as diffs, logs or documents in files listed under inputs: and have the prompt name their paths, "+
		"or, with task run, keep the --prompt-file short and use a campaign that declares inputs:",
		e.Length, e.Added, compat.MaxTurnInputLength, compat.MinServerVersion, compat.MaxServerVersion)
}

// CheckTurnInput refuses a composed turn input over T3's limit. added is the
// part of input the Steward composed around the author's prompt.
func CheckTurnInput(input string, added int) error {
	if length := compat.TurnInputLength(input); length > compat.MaxTurnInputLength {
		return &TurnInputTooLargeError{Length: length, Added: added}
	}
	return nil
}

// CheckFirstTurnPrompt refuses a task whose first turn, composed from prompt
// and the task's declared outputs, is over T3's turn input limit.
func CheckFirstTurnPrompt(prompt string, outputs []domain.ArtifactDeclaration) error {
	composed := FirstTurnPrompt(prompt, outputs)
	return CheckTurnInput(composed, compat.TurnInputLength(composed)-compat.TurnInputLength(prompt))
}
