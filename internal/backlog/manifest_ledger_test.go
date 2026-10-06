package backlog

import (
	"strings"
	"testing"
	"time"
)

const ledgerManifestPrefix = `
version: 2
name: ledgered
environment: {project: t3-steward}
tasks:
  inspect: {prompt_file: prompts/inspect.md}
`

func TestParseManifestLedgerOptIn(t *testing.T) {
	manifest := mustParseManifest(t, ledgerManifestPrefix+`
ledger:
  jocasta_project: steward
  plan: steward/plans/m16-plan.md
  risk: medium
  acceptance:
    - Opt-in validated offline
    - Restart does not duplicate records
`)
	if manifest.Ledger == nil {
		t.Fatal("ledger opt-in was dropped")
	}
	if manifest.Ledger.JocastaProject != "steward" || manifest.Ledger.Plan != "steward/plans/m16-plan.md" ||
		manifest.Ledger.Risk != "medium" || len(manifest.Ledger.Acceptance) != 2 {
		t.Fatalf("ledger = %#v", manifest.Ledger)
	}

	// The documented minimal form is the project alone.
	minimal := mustParseManifest(t, ledgerManifestPrefix+"ledger: {jocasta_project: steward}\n")
	if minimal.Ledger == nil || minimal.Ledger.JocastaProject != "steward" {
		t.Fatalf("minimal ledger = %#v", minimal.Ledger)
	}
}

func TestParseManifestWithoutLedgerHasNone(t *testing.T) {
	manifest := mustParseManifest(t, ledgerManifestPrefix)
	if manifest.Ledger != nil {
		t.Fatalf("absent ledger produced %#v", manifest.Ledger)
	}
}

func TestParseManifestLedgerRefusals(t *testing.T) {
	tests := []struct {
		name, ledger, want string
	}{
		{"empty block", "ledger: {}", "ledger.jocasta_project is required"},
		{"bad project", "ledger: {jocasta_project: Steward/x}", "ledger.jocasta_project"},
		{"unknown risk", "ledger: {jocasta_project: steward, risk: extreme}", "ledger.risk"},
		{"multi-line plan", "ledger: {jocasta_project: steward, plan: \"a\\nb\"}", "ledger.plan"},
		{"blank criterion", "ledger: {jocasta_project: steward, acceptance: [\" \"]}", "ledger.acceptance"},
		{"multi-line criterion", "ledger: {jocasta_project: steward, acceptance: [\"a\\nb\"]}", "ledger.acceptance"},
		{"too many criteria", "ledger: {jocasta_project: steward, acceptance: [" + strings.TrimSuffix(strings.Repeat("x, ", 65), ", ") + "]}", "ledger.acceptance"},
		{"unknown field", "ledger: {jocasta_project: steward, path: x}", "the ledger block"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(ledgerManifestPrefix + test.ledger + "\n"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestIngestCarriesLedgerOptInOntoTheWorkflow(t *testing.T) {
	manifest := mustParseManifest(t, ledgerManifestPrefix+`
ledger:
  jocasta_project: steward
  plan: steward/plans/m16-plan.md
  risk: high
  acceptance: [one]
`)
	ingester := BundleIngester{Now: func() time.Time { return time.Unix(0, 0) }}
	files := map[string]ingestedFile{
		"workflow.yaml":      {relative: "workflow.yaml"},
		"prompts/inspect.md": {relative: "prompts/inspect.md"},
	}
	records, _, err := ingester.buildRecords(manifest, "workflow-1", "run-1", nil, files, nil, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	ledger := records.Workflows[0].Ledger
	if ledger == nil || ledger.JocastaProject != "steward" || ledger.Plan != "steward/plans/m16-plan.md" ||
		ledger.Risk != "high" || len(ledger.Acceptance) != 1 || ledger.Acceptance[0] != "one" {
		t.Fatalf("workflow ledger = %#v", ledger)
	}

	plain, _, err := ingester.buildRecords(mustParseManifest(t, ledgerManifestPrefix), "workflow-2", "run-2", nil, files, nil, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Workflows[0].Ledger != nil {
		t.Fatalf("a workflow without the opt-in carries %#v", plain.Workflows[0].Ledger)
	}
}
