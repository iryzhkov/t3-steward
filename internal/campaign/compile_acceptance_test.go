package campaign

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func acceptancePlan(mode, body string) []byte {
	return []byte("---\ncompile: v1\nproject: project\nref: " + strings.Repeat("a", 40) + "\n" + mode + "units:\n  - id: a\n    section: A\n---\n## A\n" + body)
}
func acceptanceParse(t *testing.T, body string) CompilePlan {
	t.Helper()
	p, e := ParseCompilePlan("plan", acceptancePlan("acceptance_tests: required\n", body))
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestCompileAcceptanceTestsRequiredRefusals(t *testing.T) {
	for _, body := range []string{"", "Acceptance tests:\n", "Acceptance tests:\nParagraph\n", "Acceptance tests:\n### Heading\n", "Acceptance tests:\n- `normal`: works\n", "```\nAcceptance tests:\n- `edge`: works (edge)\n```\n", "Acceptance tests:\n~~~\n- `edge`: works (edge)\n~~~\n"} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			p, e := ParseCompilePlan("plan", acceptancePlan("acceptance_tests: required\n", body))
			var refusal *CompilePlanError
			if !errors.As(e, &refusal) || refusal.Line != 7 || !strings.Contains(refusal.Reason, `unit "a"`) || len(p.Units) != 0 {
				t.Fatalf("plan=%+v error=%v", p, e)
			}
		})
	}
}
func TestCompileAcceptanceTestsWrappedMarkdown(t *testing.T) {
	for _, body := range []string{
		"- `normal`: works\n  wrapped behavior\n- `boundary`: refuses (edge)\n",
		"- `normal`: works\n- `boundary` (edge): refuses\n",
		"- `TestCompileAcceptanceTestsWrappedMarkdown` (edge): compiles wrapped lists\n",
		"1. `normal`: works\n2) `boundary` (edge): refuses\n",
		"* `normal`: works\n  wrapped (edge)\n",
		"+ `normal`: works\n\n+ `boundary`: refuses (edge)\n",
		"1. `normal`: works\n\n2) `boundary`: refuses\n   wrapped (edge)\n",
		"```\n- `fake`: (edge)\n```\n- `real`: (edge)\n",
		"~~~\n- `fake`: (edge)\n~~~\n- `real`: (edge)\n",
	} {
		t.Run(body, func(t *testing.T) { acceptanceParse(t, "Acceptance tests:\n"+body) })
	}
	for _, end := range []string{"Paragraph", "### Heading"} {
		p, e := ParseCompilePlan("plan", acceptancePlan("acceptance_tests: required\n", "Acceptance tests:\n- `normal`: works\n\n"+end+"\n- `unrelated`: (edge)\n"))
		if e == nil || len(p.Units) != 0 {
			t.Fatalf("accepted terminated list: %s", end)
		}
	}
}
func TestCompileAcceptanceTestsUnknownValue(t *testing.T) {
	for _, value := range []string{"yes", "", "null", "true", "42", "unknown", "[]", "{}", "required", "off"} {
		t.Run(value, func(t *testing.T) {
			p, e := ParseCompilePlan("plan", acceptancePlan("acceptance_tests: "+value+"\n", "Acceptance tests:\n- `boundary`: refuses (edge)\n"))
			if value == "required" || value == "off" {
				if e != nil {
					t.Fatal(e)
				}
				return
			}
			var refusal *CompilePlanError
			if !errors.As(e, &refusal) || refusal.Line != 5 || len(p.Units) != 0 {
				t.Fatalf("plan=%+v error=%v", p, e)
			}
		})
	}
}
func TestCompileAcceptanceTestsRequiredPrompts(t *testing.T) {
	p := acceptanceParse(t, "Acceptance tests:\n- `boundary`: refuses (edge)\n")
	for path, words := range map[string][]string{"prompts/implement.md": {"named tests first", "initial failure", "cannot fail first", "handoff.md", "every case", "function", "result"}, "prompts/review.md": {"each named test exists", "matches the case", "passes", "unmet criteria"}} {
		prompt := compiledFile(t, p.Units[0], path)
		for _, word := range words {
			if !strings.Contains(prompt, word) {
				t.Fatalf("%s missing %q", path, word)
			}
		}
	}
}
func TestCompileAcceptanceTestsOffMatchesAbsent(t *testing.T) {
	body := "Acceptance tests:\n- `boundary`: refuses (edge)\n"
	absent, e := ParseCompilePlan("plan", acceptancePlan("", body))
	if e != nil {
		t.Fatal(e)
	}
	off, e := ParseCompilePlan("plan", acceptancePlan("acceptance_tests: off\n", body))
	if e != nil {
		t.Fatal(e)
	}
	required := acceptanceParse(t, body)
	for i, f := range absent.Units[0].Files {
		if f.Path == compiledPlanInput {
			continue
		}
		if f.Path != off.Units[0].Files[i].Path || string(f.Content) != string(off.Units[0].Files[i].Content) {
			t.Fatalf("off differs: %s", f.Path)
		}
		if strings.HasPrefix(f.Path, "prompts/") && string(f.Content) == string(required.Units[0].Files[i].Content) {
			t.Fatalf("required unchanged: %s", f.Path)
		}
	}
	s, o := fixFixture()
	u, e := GenerateFixChain(s, o)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(compiledFile(t, u, "inputs/fix/rules.md"), "Tests named in the original brief must still exist and pass; removal or weakening is a regression.") {
		t.Fatal("missing unconditional preservation rule")
	}
}
