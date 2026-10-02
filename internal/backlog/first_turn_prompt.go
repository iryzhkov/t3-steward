package backlog

import (
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/compat"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TaskCompletionSupplement tells the agent the one rule its own harness does
// not: ending the turn completes the task. A task that started its long checks
// in the background and ended its turn to wait for them was collected at once,
// with no outputs, and failed (S12); nothing it could read said that the turn
// was the task. The supplement names the declared outputs, because they are
// what is collected, and the task-bound wait, which is the supported way to
// wait.
//
// It is part of every task's first turn, so it counts against T3's turn input
// limit; FirstTurnPrompt is the composition both the worker and the authoring
// checks use.
func TaskCompletionSupplement(outputs []domain.ArtifactDeclaration) string {
	var b strings.Builder
	b.WriteString("\n\n## How this task ends\n")
	b.WriteString("This task runs unattended and gets one turn: when your turn ends with no task-bound wait registered, the task is complete. ")
	b.WriteString("There is no next turn, so do not end with BACKLOG STATUS: continue. ")
	b.WriteString("When the turn ends, the Steward collects the declared outputs from the workspace and runs verification; ")
	b.WriteString("processes you started in the background (shell jobs, background commands) are not waited for.")
	var files, commits []string
	for _, output := range outputs {
		if output.Commit == nil {
			files = append(files, "`"+output.Name+"`")
		} else {
			commits = append(commits, "`"+output.Name+"`")
		}
	}
	if len(files) != 0 {
		b.WriteString(" Declared outputs, which must exist when the turn ends: " + strings.Join(files, ", ") + ".")
	}
	if len(commits) != 0 {
		b.WriteString(" Declared commits, which must be committed when the turn ends: " + strings.Join(commits, ", ") + ".")
	}
	b.WriteString("\nRun long checks in the foreground and wait for them. To wait for something outside this session ")
	b.WriteString("(CI, another run, a time), register a task-bound wait and then end the turn; ")
	b.WriteString("the Steward resumes this same session with the outcome, for example ")
	b.WriteString("`t3-steward wait add --task current --for 30m --or-timeout` ")
	b.WriteString("(`t3-steward wait --help` lists every kind).")
	return b.String()
}

// FirstTurnPrompt is what the worker sends T3 as a task's first turn when no
// preflight step runs: the author's prompt, unchanged, followed by the
// task-ending section. Preflight wraps the prompt in an envelope whose size
// depends on the preflight report, and a recovery retry appends its own
// section; the worker measures those when it composes them.
func FirstTurnPrompt(prompt string, outputs []domain.ArtifactDeclaration) string {
	return prompt + TaskCompletionSupplement(outputs)
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
