package campaign

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/diagnostics"
)

func TestAuthoringDiagnosticsRetainAllYAMLTypeErrors(t *testing.T) {
	raw := []byte("---\ncompile: v1\nproject: project\nverify: wrong\nmax_turns: wrong\nunrecognized: true\nunits:\n  - id: one\n    title: [wrong]\n    section: One\n---\n## One\nBrief.\n")
	_, err := ParseCompilePlan("plan.md", raw)
	doc := diagnostics.FromError(err, "fallback.md")
	want := []struct {
		code, path string
		line       int
	}{{"yaml-type", "verify", 4}, {"yaml-type", "max_turns", 5}, {"yaml-unknown-field", "unrecognized", 6}, {"yaml-type", "units[0].title", 9}}
	if err == nil || doc.OK || len(doc.Entries) != len(want) {
		t.Fatalf("refusal %v, diagnostics %#v", err, doc)
	}
	for i, entry := range doc.Entries {
		if entry.Code != want[i].code || entry.Path != want[i].path || entry.Line != want[i].line || entry.File != "plan.md" || entry.Column <= 0 || entry.Severity != "error" {
			t.Errorf("entry %d = %#v, want %#v", i, entry, want[i])
		}
	}
	var owner *CompilePlanError
	if !errors.As(fmt.Errorf("outer: %w", errors.Join(err)), &owner) {
		t.Fatalf("compile owner lost: %v", err)
	}
}

func TestAuthoringYAMLHints(t *testing.T) {
	for _, tc := range []struct{ name, raw, code, hint string }{
		{"duplicate", "---\ncompile: v1\ncompile: v1\n---\n", "yaml-duplicate-key", "unique"},
		{"comma", "---\nunits: [{id: one, title: Alpha, Beta, section: One}]\n---\n", "yaml-unknown-field", "comma"},
		{"tab", "---\nunits:\n\t- id: one\n---\n", "yaml-syntax", "tab"},
		{"colon", "---\nunits:\n  - title: Alpha: Beta\n---\n", "yaml-syntax", "quot"},
		{"ambiguous", "---\nunits: [{title: [broken}\n---\n\x60\x60\x60yaml\ntitle: A: B\n\tbad: tab\n\x60\x60\x60\n", "yaml-syntax", "block"},
		{"missing", "compile: v1\n", "front-matter-missing", "front matter"},
		{"unclosed", "---\ncompile: v1\n", "front-matter-unclosed", "front matter"},
		{"documents", "---\ncompile: v1\n...\n--- second\nproject: other\n---\n", "yaml-multiple-documents", "single"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseCompilePlan("plan.md", []byte(tc.raw))
			doc := diagnostics.FromError(err, "plan.md")
			if err == nil || doc.OK || len(doc.Entries) != 1 {
				t.Fatalf("expected refusal: %v %#v", err, doc)
			}
			entry := doc.Entries[0]
			if entry.Code != tc.code || entry.File != "plan.md" || entry.Line < 1 || entry.Column < 0 || !strings.Contains(strings.ToLower(entry.Hint), tc.hint) {
				t.Errorf("diagnostic = %#v", entry)
			}
			if tc.name == "duplicate" && (entry.Line != 3 || entry.Column != 1 || entry.Path != "compile") {
				t.Errorf("duplicate location = %#v", entry)
			}
		})
	}
}

func TestHintsDoNotInterpretLiteralContent(t *testing.T) {
	raw := readCompileFixture(t)
	for _, title := range []string{`"Quoted: title, detail"`, `"Valid comma, title"`, "|\n      title: detail, data\n      \tliteral tab"} {
		input := bytes.Replace(raw, []byte("title: Campaign compile skeleton"), []byte("title: "+title), 1)
		input = append(input, []byte("\n```yaml\nunclosed: [\n\tbad: title: data\n```\n")...)
		plan, err := ParseCompilePlan("plan.md", input)
		if err != nil {
			t.Fatalf("literal content refused: %v", err)
		}
		if got := compiledFile(t, plan.Units[0], "inputs/plan.md"); got != string(input) {
			t.Fatal("literal input bytes changed")
		}
	}
}

func TestMalformedCompileDiagnosticsHaveNoEffects(t *testing.T) {
	raw := readCompileFixture(t)
	raw = bytes.Replace(raw, []byte("verify:"), []byte("inputs: [extra.md]\nverify:"), 1)
	reads := 0
	options := CompileOptions{ReadInput: func(string) ([]byte, error) { reads++; return []byte("extra"), nil }}
	if _, err := ParseCompilePlanWith("plan.md", raw, options); err != nil || reads == 0 {
		t.Fatalf("valid control: reads %d, %v", reads, err)
	}
	for _, key := range []string{"after", "group", "commands", "grants", "max_turns"} {
		input := bytes.Replace(raw, []byte("compile: v1"), []byte("compile: v1\n"+key+": wrong"), 1)
		before := reads
		_, err := ParseCompilePlanWith("plan.md", input, options)
		doc := diagnostics.FromError(err, "plan.md")
		if err == nil || doc.OK || reads != before {
			t.Fatalf("%s: reads %d -> %d, %v %#v", key, before, reads, err, doc)
		}
	}
}

func TestDiagnosticsPreserveErrorChainsAndLegacyRefusals(t *testing.T) {
	raw := bytes.Replace(readCompileFixture(t), []byte("ref: d2e827681336e76413fdab34392f029a3e131cf4"), []byte("ref: moving-branch"), 1)
	_, err := ParseCompilePlan("plan.md", raw)
	var owner *CompilePlanError
	wrapped := fmt.Errorf("outer: %w", errors.Join(err, errors.New("unrelated")))
	if !errors.As(wrapped, &owner) {
		t.Fatalf("owner chain lost: %v", wrapped)
	}
	doc := diagnostics.FromError(err, "fallback")
	if err == nil || doc.OK || len(doc.Entries) != 1 {
		t.Fatalf("semantic refusal %v %#v", err, doc)
	}
	entry := doc.Entries[0]
	if entry.Code != "legacy" || entry.File != "plan.md" || entry.Line != 4 || entry.Column != 0 || entry.Path != "" || entry.Message != err.Error() {
		t.Fatalf("legacy evidence %#v", entry)
	}
}

func TestDiagnosticsPreserveCompileCompatibility(t *testing.T) {
	TestCompilePlanMatchesTheGoldens(t)
	TestCompilePlanCopiesThePlanAndTheSectionByteForByte(t)
	TestCompiledV1PlanIsEquivalentToTheLegacyWorkflow(t)
}
