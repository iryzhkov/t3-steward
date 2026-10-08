package campaign

import (
	"os"
	"strings"
	"testing"
)

// Every author-facing surface describes quota ranking and its admission fence.
func TestRoleHelpStatesRankingAndAdmission(t *testing.T) {
	docs, err := os.ReadFile("../../docs/route-policy.md")
	if err != nil {
		t.Fatal(err)
	}
	texts := map[string]string{"AuthoringHelp": AuthoringHelp, "RoutesHelp": RoutesHelp, "docs/route-policy.md": string(docs)}
	for name, text := range texts {
		text = strings.Join(strings.Fields(text), " ")
		if strings.Contains(text, "Until M17-2b") {
			t.Errorf("%s retains obsolete deferral", name)
		}
		for _, want := range []string{
			"roles use",
			"route-ranking/v1",
			"policy, catalog and worker eligibility checks",
			"candidate bands, pools and reasons",
			"quota atomically",
			"Explicit pins stay literal",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("%s missing %q", name, want)
			}
		}
	}
}
