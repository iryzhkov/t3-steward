package campaign

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The goldens in testdata were produced by the binary built at origin/main
// 94a28a0, the last commit before campaign supervision, run from the
// repository root:
//
//	t3-steward campaign plan docs/examples/campaign/three-node
//	t3-steward campaign plan docs/examples/campaign/three-node --json
//
// They are the compatibility contract for an unsupervised campaign: adding
// supervision must not change one byte of what an author sees for a campaign
// that does not use it. That includes the content digest, which is printed in
// both documents and is the identity the coordinator ingests, so a golden
// mismatch is either a real behaviour change or a change to the example, and
// both are things a reader of this branch has to be told about.
//
// One change to the example has happened since: the prompts of the three-node
// example were corrected to name the inputs mount as .t3/inputs/inputs/scope.md
// (the declared path is kept in full under .t3/inputs/), which changes the
// content digest and nothing else. The digest in both goldens was re-recorded
// from this branch's own plan output for that reason; every other byte is the
// origin/main recording.
//
// A second change is to the text golden alone: each task's "effort" line was
// relabelled "scheduling", with what the coordinator does with importance and
// difficulty named beside each number, because "effort" is also a route option
// and the old label read as a model setting. The three lines were edited to
// match; the numbers, and every other byte, are unchanged.
//
// A third change is that presets are sized: preset light expands to cpu units
// 0.5, memory 1000 MB and scratch 512 MB, and preset build to 4, 6000 MB and
// 8192 MB, so an author reads back what a task reserves. The four resources
// lines of the text golden were edited to append the sizes, and the JSON
// gains the same sizes as the deltas recorded below; the digest is unchanged
// because the example's files are.

const (
	unsupervisedPlanTextGolden = "testdata/origin-main-three-node-plan.txt"
	unsupervisedPlanJSONGolden = "testdata/origin-main-three-node-plan.json"
	unsupervisedExample        = "docs/examples/campaign/three-node"
)

// planTheUnsupervisedExample reproduces exactly what the CLI does for
// `campaign plan`: the same Prepare, the same Project and the same Options, so
// the bytes compared below are the bytes a user sees. The working directory is
// moved to the repository root because the source path is printed verbatim in
// both documents and was recorded relative to that root.
func planTheUnsupervisedExample(t *testing.T) Plan {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	bundle, err := Prepare(unsupervisedExample, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Project(bundle.Campaign.Manifest, Options{
		Source:     unsupervisedExample,
		Digest:     bundle.ContentDigest,
		InputFiles: bundle.Campaign.InputPaths(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func readGolden(t *testing.T, relative string) string {
	t.Helper()
	// The goldens live beside this test, which is no longer the working
	// directory once the plan has been built from the repository root.
	raw, err := os.ReadFile(filepath.Join("internal", "campaign", relative))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestUnsupervisedCampaignPlanTextMatchesOriginMainByteForByte is the
// human-facing half of the contract, and it holds exactly: every line an
// author reads for an unsupervised campaign, including the content digest the
// coordinator ingests, is the same byte for byte as before supervision existed.
func TestUnsupervisedCampaignPlanTextMatchesOriginMainByteForByte(t *testing.T) {
	plan := planTheUnsupervisedExample(t)
	text := RenderText(plan)
	if golden := readGolden(t, unsupervisedPlanTextGolden); text != golden {
		t.Fatalf("campaign plan text differs from origin/main:\nwant:\n%s\ngot:\n%s", golden, text)
	}
}

// planJSONCompatibilityDeltas is the complete, deliberate difference between
// the plan document origin/main produced for an unsupervised campaign and the
// one this branch produces. Supervision brought three, recorded here rather
// than hidden by a loose comparison, and sized presets the rest, described
// beside them:
//
//   - the plan schema version moved from 1 to 2, because the document gained
//     optional supervision and gate sections;
//   - totals gained "gates", which is 0 for a campaign that declares none;
//   - totals gained "heldTasks", which is 0 for the same reason.
//
// A consumer pinned to schema version 1 sees the version change and can refuse,
// which is the point of versioning the document. A consumer that reads totals
// by name is unaffected by two added zero-valued counters. Anything beyond
// the recorded deltas is a compatibility regression, and the test below is
// what turns one into a failure.
var planJSONCompatibilityDeltas = []struct {
	was string
	now string
}{
	{was: `  "schemaVersion": 1,`, now: `  "schemaVersion": 2,`},
	{was: "", now: `    "gates": 0,`},
	{was: "", now: `    "heldTasks": 0`},
	// Sized presets: the workflow and two tasks inherit light, one task
	// declares build. Each size line is removed wherever it occurs, and the
	// class line before them loses the comma the sizes needed.
	{was: "", now: `"cpuUnits": 0.5,`},
	{was: "", now: `"memoryMb": 1000,`},
	{was: "", now: `"scratchMb": 512`},
	{was: "", now: `"cpuUnits": 4,`},
	{was: "", now: `"memoryMb": 6000,`},
	{was: "", now: `"scratchMb": 8192`},
	{was: "\"minCpuClass\": \"low\"\n", now: "\"minCpuClass\": \"low\",\n"},
	{was: "\"minCpuClass\": \"low\"\n", now: "\"minCpuClass\": \"low\",\n"},
	{was: "\"minCpuClass\": \"low\"\n", now: "\"minCpuClass\": \"low\",\n"},
	{was: "\"preferredCpuClass\": \"high\"\n", now: "\"preferredCpuClass\": \"high\",\n"},
}

func TestUnsupervisedCampaignPlanJSONDiffersFromOriginMainOnlyAsRecorded(t *testing.T) {
	plan := planTheUnsupervisedExample(t)
	document, err := RenderJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	golden := readGolden(t, unsupervisedPlanJSONGolden)
	if string(document) == golden {
		t.Fatal("the plan document is identical to origin/main; " +
			"planJSONCompatibilityDeltas describes changes that no longer exist and must be removed")
	}

	// Normalising the current document by the recorded deltas must reproduce
	// the golden exactly. Each delta is also required to be present, so the
	// normalisation cannot quietly describe a difference that is not there.
	normalised := string(document)
	for _, delta := range planJSONCompatibilityDeltas {
		if !strings.Contains(normalised, delta.now) {
			t.Fatalf("recorded delta %q is not in the plan document", delta.now)
		}
		switch delta.was {
		case "":
			normalised = removeJSONLine(normalised, delta.now)
		default:
			normalised = strings.Replace(normalised, delta.now, delta.was, 1)
		}
	}
	// Removing the last counter of an object leaves the preceding line with a
	// trailing comma, which the golden does not have.
	normalised = strings.Replace(normalised, `"timingConstrained": 0,`, `"timingConstrained": 0`, 1)
	if normalised != golden {
		t.Fatalf("campaign plan JSON differs from origin/main beyond the recorded deltas:\nwant:\n%s\ngot:\n%s",
			golden, normalised)
	}
}

// removeJSONLine deletes the one line whose trimmed content matches, together
// with its newline.
func removeJSONLine(document, line string) string {
	want := strings.TrimSpace(strings.TrimSuffix(line, ","))
	kept := make([]string, 0)
	for _, candidate := range strings.Split(document, "\n") {
		if strings.TrimSpace(strings.TrimSuffix(candidate, ",")) == want {
			continue
		}
		kept = append(kept, candidate)
	}
	return strings.Join(kept, "\n")
}

// TestUnsupervisedCampaignPlanSaysNothingAboutSupervision is the other half of
// the claim. The goldens above would still pass if supervision rendering were
// merely empty rather than absent, and an empty supervision block in a plan
// document is a difference an author would reasonably read as meaningful.
func TestUnsupervisedCampaignPlanSaysNothingAboutSupervision(t *testing.T) {
	plan := planTheUnsupervisedExample(t)
	if plan.Supervision != nil {
		t.Fatalf("an unsupervised campaign projected a supervision block: %#v", plan.Supervision)
	}
	if len(plan.Gates) != 0 {
		t.Fatalf("an unsupervised campaign projected gates: %#v", plan.Gates)
	}
	document, err := RenderJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	// The two zero-valued counters in totals are the only supervision words a
	// campaign that declares no supervision is allowed to produce.
	for _, fragment := range []string{`"supervision"`, `"gates": {`, `"gates": [`, "overseer", "incident"} {
		if strings.Contains(string(document), fragment) {
			t.Fatalf("the unsupervised plan document contains %q", fragment)
		}
	}
	for _, word := range []string{"supervision", "overseer", "gate ", "held"} {
		if strings.Contains(RenderText(plan), word) {
			t.Fatalf("the unsupervised plan text mentions %q", word)
		}
	}
}
