package campaign

import (
	"os"
	"strings"
	"testing"
)

// Campaign and schedule role resolution picks in policy order, without the
// quota ranking task run and review use, until M17-2b. An author must be able
// to read that, and that an exhausted first candidate is refused at admission
// rather than skipped, from the help and from docs/route-policy.md.
func TestRoleHelpStatesPolicyOrderWithoutRankingDeferral(t *testing.T) {
	docs, err := os.ReadFile("../../docs/route-policy.md")
	if err != nil {
		t.Fatal(err)
	}
	texts := map[string]string{"AuthoringHelp": AuthoringHelp, "RoutesHelp": RoutesHelp, "docs/route-policy.md": string(docs)}
	for name, text := range texts {
		text = strings.Join(strings.Fields(text), " ")
		for _, want := range []string{
			"Until M17-2b, campaign and schedule role resolution uses policy order",
			"not the quota ranking",
			"exhausted or gated, that route is still selected, and quota admission refuses the submission rather than falling through to a later candidate",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("%s missing %q", name, want)
			}
		}
	}
}
