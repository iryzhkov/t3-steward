package campaign

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

var updateCompileGoldens = flag.Bool("update-compile-goldens", false, "rewrite testdata/compile/golden from the current compiler")

const compilePlanFixture = "testdata/compile/plan.md"

func readCompileFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(compilePlanFixture)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func compiledFile(t *testing.T, unit CompiledUnit, path string) string {
	t.Helper()
	for _, file := range unit.Files {
		if file.Path == path {
			return string(file.Content)
		}
	}
	t.Fatalf("unit %s has no file %s", unit.ID, path)
	return ""
}

// The generated directory is pinned by golden files: the manifest is the
// contract the coordinator ingests, and the prompts are what the executor and
// the reviewer read first, so an unnoticed change to either is a change to
// every campaign compiled afterwards.
func TestCompilePlanMatchesTheGoldens(t *testing.T) {
	plan, err := ParseCompilePlan("plan.md", readCompileFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Units) != 2 || plan.Units[0].ID != "c1" || plan.Units[1].ID != "b1" {
		t.Fatalf("units = %+v, want c1 then b1 in plan order", plan.Units)
	}
	for _, unit := range plan.Units {
		for _, file := range unit.Files {
			if strings.HasPrefix(file.Path, "inputs/") {
				continue
			}
			golden := filepath.Join("testdata", "compile", "golden", unit.ID, filepath.FromSlash(file.Path))
			if *updateCompileGoldens {
				if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, file.Content, 0o644); err != nil {
					t.Fatal(err)
				}
				continue
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v; regenerate with go test ./internal/campaign -run TestCompilePlanMatchesTheGoldens -update-compile-goldens", err)
			}
			if string(want) != string(file.Content) {
				t.Fatalf("%s/%s differs from %s:\nwant:\n%s\ngot:\n%s", unit.ID, file.Path, golden, want, file.Content)
			}
		}
	}
}

func TestCompilePlanCopiesThePlanAndTheSectionByteForByte(t *testing.T) {
	raw := readCompileFixture(t)
	plan, err := ParseCompilePlan("plan.md", raw)
	if err != nil {
		t.Fatal(err)
	}
	c1, b1 := plan.Units[0], plan.Units[1]
	if got := compiledFile(t, c1, "inputs/plan.md"); got != string(raw) {
		t.Fatalf("inputs/plan.md is not the plan byte for byte:\n%s", got)
	}
	wantC1 := "## C-1: campaign compile skeleton\n\nTurn a plan into campaign directories.\n\n" +
		"```markdown\n## B-1 is not a heading inside a fence\n```\n\n### Details\n\nNested detail stays in the C-1 section.\n\n"
	if got := compiledFile(t, c1, "inputs/unit.md"); got != wantC1 {
		t.Fatalf("c1 unit.md = %q, want %q", got, wantC1)
	}
	if got := compiledFile(t, b1, "inputs/unit.md"); got != "## B-1\n\nList the runs a thread owns.\n\n" {
		t.Fatalf("b1 unit.md = %q", got)
	}
}

func TestCompiledManifestPinsTheRefRoutesAndEfforts(t *testing.T) {
	plan, err := ParseCompilePlan("plan.md", readCompileFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := backlog.ParseManifest([]byte(compiledFile(t, plan.Units[0], "workflow.yaml")))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != "c1" || manifest.Class != "required" {
		t.Fatalf("name/class = %q/%q", manifest.Name, manifest.Class)
	}
	if manifest.Environment.Ref != "d2e827681336e76413fdab34392f029a3e131cf4" || manifest.Environment.Project != "t3-steward-github" {
		t.Fatalf("environment = %+v", manifest.Environment)
	}
	implement, review := manifest.Tasks["implement"], manifest.Tasks["review"]
	if len(implement.Routes) != 1 || implement.Routes[0].Instance != "claudeAgent" || implement.Routes[0].Model != "claude-opus-5-5" ||
		implement.Routes[0].QuotaPool != "claude-main" || implement.Routes[0].Options["effort"] != "medium" {
		t.Fatalf("implement routes = %+v", implement.Routes)
	}
	if len(review.Routes) != 1 || review.Routes[0].Instance != "codex" || review.Routes[0].Options["effort"] != "high" {
		t.Fatalf("review routes = %+v", review.Routes)
	}
	if len(implement.Commits) != 1 || implement.Commits[0].Name != "implementation" || implement.Commits[0].Revision != "HEAD" {
		t.Fatalf("implement commits = %+v", implement.Commits)
	}
	if strings.Join(implement.Verify, "|") != "go test ./...|git diff --check" {
		t.Fatalf("implement verify = %q", implement.Verify)
	}
	if len(review.Needs) != 1 || review.Needs[0] != "implement" || len(review.InputsFrom["implement"]) == 0 {
		t.Fatalf("review needs/inputs_from = %v/%v", review.Needs, review.InputsFrom)
	}
	for _, path := range []string{"prompts/implement.md", "prompts/review.md"} {
		prompt := compiledFile(t, plan.Units[0], path)
		for _, want := range []string{"d2e827681336e76413fdab34392f029a3e131cf4", "c1", ".t3/inputs/inputs/plan.md", ".t3/inputs/inputs/unit.md"} {
			if !strings.Contains(prompt, want) {
				t.Fatalf("%s does not name %q:\n%s", path, want, prompt)
			}
		}
	}
	if !strings.Contains(compiledFile(t, plan.Units[0], "prompts/review.md"), ".t3/dependencies/") {
		t.Fatal("the review prompt does not say where the implementation arrives")
	}
}

// Every refusal names the plan line and the reason, and none of them is
// reached after anything was written, because parsing writes nothing.
func TestCompilePlanRefusals(t *testing.T) {
	const head = "---\ncompile: v1\nproject: p\nref: d2e827681336e76413fdab34392f029a3e131cf4\n" +
		"routes:\n  execute: {instance: claudeAgent, model: m, quota_pool: q}\n  review: {instance: codex, model: m, quota_pool: q}\n"
	const body = "---\n## A\n\ntext\n## B\n\nmore\n"
	for _, test := range []struct {
		name string
		plan string
		want []string
	}{
		{"no front matter", "# A\n", []string{"plan.md:1:", "front matter"}},
		{"unterminated front matter", "---\ncompile: v1\n", []string{"plan.md:1:", "closing ---"}},
		{"empty front matter", "---\n---\n# A\n", []string{"plan.md:1:", "front matter is empty"}},
		{"sequence front matter", "---\n- a\n---\n# A\n", []string{"plan.md:2:", "the front matter, which must be a mapping"}},
		{"unit id too long", head + "units:\n  - {id: a" + strings.Repeat("b", MaxCompileUnitIDLength) + ", section: A}\n" + body, []string{"plan.md:9:", "at most 64"}},
		{"unknown top-level key", head + "colour: red\nunits:\n  - {id: a, section: A}\n" + body, []string{"plan.md:8:", "colour"}},
		{"unknown unit key", head + "units:\n  - id: a\n    section: A\n    owner: me\n" + body, []string{"plan.md:11:", "owner"}},
		{"unknown route key", strings.Replace(head, "quota_pool: q}", "quota_pool: q, speed: fast}", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:6:", "speed"}},
		{"missing compile version", strings.Replace(head, "compile: v1\n", "", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:1:", "compile: v1"}},
		{"other compile version", strings.Replace(head, "compile: v1", "compile: v2", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:2:", "v2"}},
		{"branch ref", strings.Replace(head, "d2e827681336e76413fdab34392f029a3e131cf4", "main", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:4:", "full 40-character commit", "main"}},
		{"short ref", strings.Replace(head, "d2e827681336e76413fdab34392f029a3e131cf4", "d2e8276", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:4:", "full 40-character commit"}},
		{"missing ref", strings.Replace(head, "ref: d2e827681336e76413fdab34392f029a3e131cf4\n", "", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:1:", "ref"}},
		{"missing unit id", head + "units:\n  - {section: A}\n" + body, []string{"plan.md:9:", "id"}},
		{"duplicate unit id", head + "units:\n  - {id: a, section: A}\n  - {id: a, section: B}\n" + body, []string{"plan.md:10:", "duplicate", `"a"`}},
		{"unit id not a directory name", head + "units:\n  - {id: A/b, section: A}\n" + body, []string{"plan.md:9:", "A/b"}},
		{"absent section", head + "units:\n  - {id: a, section: Z}\n" + body, []string{"plan.md:9:", "no heading", `"Z"`}},
		{"ambiguous section", head + "units:\n  - {id: a, section: A}\n" + "---\n## A\n\none\n## A: again\n\ntwo\n", []string{"plan.md:9:", "2 headings", "lines 11 and 14"}},
		{"no units", head + "units: []\n" + body, []string{"plan.md:8:", "no units"}},
		{"other template", head + "template: fix-chain\nunits:\n  - {id: a, section: A}\n" + body, []string{"plan.md:8:", "fix-chain", "implement-review"}},
		{"bad class", head + "class: urgent\nunits:\n  - {id: a, section: A}\n" + body, []string{"plan.md:8:", "urgent"}},
		{"missing review route", strings.Replace(head, "  review: {instance: codex, model: m, quota_pool: q}\n", "", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:5:", "routes.review"}},
		{"route without a model", strings.Replace(head, "instance: codex, model: m,", "instance: codex,", 1) + "units:\n  - {id: a, section: A}\n" + body, []string{"plan.md:7:", "model"}},
		{"key after a document end", head + "units:\n  - {id: a, section: A}\n...\ncolour: red\n" + body, []string{"plan.md:", "continues after its YAML document ends"}},
		{"document start with content", head + "units:\n  - {id: a, section: A}\n--- red\n" + body, []string{"plan.md:10:", "continues after its YAML document ends"}},
		{"malformed YAML after a document end", head + "units:\n  - {id: a, section: A}\n...\nunits: [\n" + body, []string{"plan.md:", "continues after its YAML document ends"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseCompilePlan("plan.md", []byte(test.plan))
			if err == nil {
				t.Fatal("the plan was accepted")
			}
			var refusal *CompilePlanError
			if !errors.As(err, &refusal) {
				t.Fatalf("error %T %v is not a CompilePlanError", err, err)
			}
			for _, want := range test.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal %q does not contain %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "campaign.compile") {
				t.Fatalf("refusal %q names a Go type the author never wrote", err)
			}
		})
	}
}

// An explicit document end with nothing but a comment after it is still one
// document, so the single-document rule does not refuse it.
func TestCompilePlanAcceptsABareDocumentEnd(t *testing.T) {
	raw := string(readCompileFixture(t))
	ended := strings.Replace(raw, "\n---\n# Remaining", "\n...\n# nothing follows\n---\n# Remaining", 1)
	if ended == raw {
		t.Fatal("the fixture no longer closes its front matter before # Remaining")
	}
	plan, err := ParseCompilePlan("plan.md", []byte(ended))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Units) != 2 {
		t.Fatalf("units = %d, want 2", len(plan.Units))
	}
}

func TestWriteCompiledUnitRefusesAnExistingDirectoryUnlessForced(t *testing.T) {
	plan, err := ParseCompilePlan("plan.md", readCompileFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	existing := filepath.Join(out, "c1")
	if err := os.MkdirAll(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(existing, "keep.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteCompiledUnit(out, plan.Units[0], WriteOptions{}); !errors.Is(err, ErrCompiledUnitExists) {
		t.Fatalf("err = %v, want ErrCompiledUnitExists", err)
	}
	if raw, err := os.ReadFile(filepath.Join(existing, "keep.txt")); err != nil || string(raw) != "mine" {
		t.Fatalf("the existing directory was touched: %q %v", raw, err)
	}
	dir, err := WriteCompiledUnit(out, plan.Units[0], WriteOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if dir != existing {
		t.Fatalf("dir = %s, want %s", dir, existing)
	}
	if _, err := os.Stat(filepath.Join(existing, "keep.txt")); !os.IsNotExist(err) {
		t.Fatalf("--force kept a stale file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(existing, "workflow.yaml")); err != nil {
		t.Fatal(err)
	}
	assertNoCompileLeftovers(t, out, "c1")
}

func TestWriteCompiledUnitLeavesNothingBehindWhenAWriteFails(t *testing.T) {
	plan, err := ParseCompilePlan("plan.md", readCompileFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, force := range []bool{false, true} {
		out := t.TempDir()
		if force {
			if err := os.MkdirAll(filepath.Join(out, "c1"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, "c1", "keep.txt"), []byte("mine"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		writes := 0
		injected := errors.New("disk full")
		_, err := WriteCompiledUnit(out, plan.Units[0], WriteOptions{Force: force, WriteFile: func(path string, content []byte) error {
			writes++
			if writes == 3 {
				return injected
			}
			return os.WriteFile(path, content, 0o644)
		}})
		if !errors.Is(err, injected) {
			t.Fatalf("force=%t: err = %v, want the injected failure", force, err)
		}
		if force {
			// A failed forced compile keeps the directory it would have replaced.
			if raw, err := os.ReadFile(filepath.Join(out, "c1", "keep.txt")); err != nil || string(raw) != "mine" {
				t.Fatalf("a failed --force lost the existing unit: %q %v", raw, err)
			}
			assertNoCompileLeftovers(t, out, "c1")
			continue
		}
		assertNoCompileLeftovers(t, out)
	}
}

// A compile killed between retiring the old unit and renaming the new one
// into place leaves the old unit only in a hidden sibling. The next compile
// must refuse rather than write a fresh unit beside it and orphan it.
func TestWriteCompiledUnitRefusesTheLeftoversOfAKilledCompile(t *testing.T) {
	plan, err := ParseCompilePlan("plan.md", readCompileFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	retired := filepath.Join(out, ".compile-c1-retired-248858152", "c1")
	if err := os.MkdirAll(retired, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(retired, "keep.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, other := range []string{".compile-c1-924994213", ".compile-c1-x-1", ".compile-c10-5"} {
		if err := os.MkdirAll(filepath.Join(out, other), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	leftovers, err := CompileLeftovers(out, "c1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(out, ".compile-c1-924994213"), filepath.Join(out, ".compile-c1-retired-248858152")}
	if strings.Join(leftovers, "|") != strings.Join(want, "|") {
		t.Fatalf("leftovers = %v, want %v (another unit's siblings are not this unit's)", leftovers, want)
	}
	for _, force := range []bool{false, true} {
		_, err := WriteCompiledUnit(out, plan.Units[0], WriteOptions{Force: force})
		if !errors.Is(err, ErrCompileLeftover) || !strings.Contains(err.Error(), ".compile-c1-retired-248858152") {
			t.Fatalf("force=%t: err = %v, want a refusal naming the retired copy", force, err)
		}
	}
	assertNoCompileLeftovers(t, out, ".compile-c1-924994213", ".compile-c1-retired-248858152", ".compile-c1-x-1", ".compile-c10-5")
	if raw, err := os.ReadFile(filepath.Join(retired, "keep.txt")); err != nil || string(raw) != "mine" {
		t.Fatalf("the retired copy was touched: %q %v", raw, err)
	}
}

// assertNoCompileLeftovers fails when out holds anything but the named
// entries: a temporary sibling left behind is a half-written unit.
func assertNoCompileLeftovers(t *testing.T, out string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("%s holds %v, want exactly %v", out, names, want)
	}
}
