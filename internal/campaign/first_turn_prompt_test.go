package campaign

import (
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/compat"
)

// T3 refuses a turn input over its limit only when the turn starts, after the
// thread exists, and a task sat "running" for over an hour that way. A
// campaign whose composed first turn is over the limit is refused before it is
// submitted; one exactly at the limit is not.
func TestLoadRefusesAFirstTurnPromptOverT3sInputLimit(t *testing.T) {
	// The review task declares review.md, which the task-ending section names.
	supplement := compat.TurnInputLength(backlog.FirstTurnPrompt("", backlog.ManifestTask{Outputs: []string{"review.md"}}.OutputDeclarations()))
	for _, test := range []struct {
		name     string
		length   int
		refused  bool
		multiple bool
	}{
		{name: "below the limit", length: compat.MaxTurnInputLength - 1},
		{name: "at the limit", length: compat.MaxTurnInputLength},
		{name: "above the limit", length: compat.MaxTurnInputLength + 1, refused: true},
		// A character outside the BMP is two UTF-16 units to T3, so a prompt
		// under the limit in runes can be over it as T3 counts.
		{name: "above the limit in UTF-16 units", length: compat.MaxTurnInputLength + 1, refused: true, multiple: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tree := validTree()
			body := strings.Repeat("x", test.length-supplement)
			if test.multiple {
				body = strings.Repeat("x", test.length-supplement-2) + "\U0001F600"
			}
			tree["prompts/review.md"] = body
			root := t.TempDir()
			writeTree(t, root, tree, nil)
			_, err := Load(root, DefaultLimits)
			if !test.refused {
				if err != nil {
					t.Fatalf("a first turn of %d characters was refused: %v", test.length, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("a first turn of %d characters was accepted", test.length)
			}
			for _, want := range []string{
				"task review (prompts/review.md)",
				fmt.Sprintf("%d characters", test.length),
				fmt.Sprintf("plus %d the Steward adds", supplement),
				"limit of 120000 characters",
				"T3 " + compat.MinServerVersion,
				"inputs:",
				"--prompt-file",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal does not say %q: %v", want, err)
				}
			}
		})
	}
}
