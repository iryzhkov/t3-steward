package campaign

import (
	"os"
	"strings"
	"testing"
)

func TestCompiledReviewCalibration(t *testing.T) {
	for _, fixture := range []struct {
		name, raw, prompt, golden string
	}{
		{"implement-review", string(readCompileFixture(t)), "review", ""},
		{"plan-review-git", planningFixture, "plan-review", "git-review.md"},
		{"plan-review-fresh", strings.Replace(planningFixture, "ref: "+planningRef, "type: fresh", 1), "plan-review", "fresh-review.md"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			plan, err := ParseCompilePlan("plan.md", []byte(fixture.raw))
			if err != nil {
				t.Fatal(err)
			}
			for _, unit := range plan.Units {
				prompt := compiledFile(t, unit, "prompts/"+fixture.prompt+".md")
				for _, want := range []string{
					"realistic author input", "Each blocking finding must\nname the violated acceptance criterion",
					"give a concrete plausible author input", "explain why that input is plausible",
					"provide a reproducing command or test",
					"direct scalar fields and conventional Markdown",
					"eligible for blocking findings when acceptance fails; do not excuse that failure",
					"Syntactically representable input alone does not establish plausibility",
					"Label exotic encodings (including Unicode-encoded tokens), adversarial YAML",
					"Unicode edge cases as FOLLOW-UPS with reproduction evidence",
					"these nonblocking\nfindings cannot change the verdict",
					"VERDICT: ACCEPT", "VERDICT: CHANGES_REQUESTED",
				} {
					if !strings.Contains(prompt, want) {
						t.Errorf("%s lacks %q", unit.ID, want)
					}
				}
				verification := []string{"Do not change source", "Do not change source, execute the plans",
					"verification exit codes"}
				if fixture.prompt == "review" {
					verification = []string{"Do not modify, commit or push source",
						"first reproduce any suspected defect with a focused test or command",
						"make test-affected BASE=", "importers through the shell",
						"focused checks of the changed packages and their", "Do not rerun the full declared gate"}
				}
				for _, want := range verification {
					if !strings.Contains(prompt, want) {
						t.Errorf("%s lost %q", unit.ID, want)
					}
				}
				if fixture.golden != "" {
					want, err := os.ReadFile("testdata/compile/planning/" + fixture.golden)
					if err != nil {
						t.Fatal(err)
					}
					if prompt != string(want) {
						t.Fatalf("prompt differs from %s:\n%s", fixture.golden, prompt)
					}
				}
			}
		})
	}
}
