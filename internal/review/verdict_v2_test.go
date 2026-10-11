package review

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type refResolverFunc func(string) bool

func (f refResolverFunc) ResolveContractRef(ref string) bool { return f(ref) }

func v2Packet() map[string]any {
	return map[string]any{
		"schema": SchemaV2, "verdict": "reject", "inputManifestDigest": testDigest,
		"reviewerRoute": "codex/sol", "checks_passed": false,
		"check_evidence": []any{"artifact:checks"},
		"findings": []any{map[string]any{
			"id": "F1", "class": "defect", "contract_ref": "docs/plan.md:7",
			"title": "Check failed", "detail": "The declared check fails.",
			"affected_paths": []any{"internal/review/verdict.go"},
			"failing_tests":  []any{"go test ./internal/review"}, "reproducer": "",
			"severity": "high", "blocking": true, "evidence": []any{"artifact:checks"},
			"recommendation": "Fix the check",
		}},
	}
}
func v2Finding(p map[string]any) map[string]any { return p["findings"].([]any)[0].(map[string]any) }
func v2Bytes(t *testing.T, p map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestVerdictV2Valid(t *testing.T) {
	for _, class := range []string{"defect", "regression", "safety", "note"} {
		for _, ref := range []string{"docs/plan.md:7", "test:internal/review/verdict_test.go:TestVerdictValidationAndBinding"} {
			t.Run(class+"/"+ref, func(t *testing.T) {
				p := v2Packet()
				f := v2Finding(p)
				f["class"], f["contract_ref"] = class, ref
				if class == "note" {
					f["blocking"], p["verdict"], p["checks_passed"] = false, "accept-with-changes", true
				}
				md := "VERDICT: CHANGES_REQUESTED\nF1"
				if class == "note" {
					md = "VERDICT: ACCEPT\nF1"
				}
				v, findings, events, err := ValidateVerdictV2(v2Bytes(t, p), []byte(md), testDigest, "codex/sol", refResolverFunc(func(got string) bool { return got == ref }))
				if err != nil || len(findings) != 1 || len(events) != 0 || v.Findings[0].Class != class {
					t.Fatalf("%+v %+v %v", v, events, err)
				}
			})
		}
	}
	p := v2Packet()
	p["checks_passed"] = true
	p["check_evidence"] = []any{"checks/result.txt:12"}
	v2Finding(p)["reproducer"] = "go test ./internal/review"
	if _, _, _, err := ValidateVerdictV2(v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED\nF1"), testDigest, "codex/sol", refResolverFunc(func(string) bool { return true })); err != nil {
		t.Fatal(err)
	}
	p["verdict"], p["findings"], p["checks_passed"], p["check_evidence"] = "accept", []any{}, true, []any{}
	if _, _, _, err := ValidateVerdictV2(v2Bytes(t, p), []byte("VERDICT: ACCEPT"), testDigest, "codex/sol", nil); err != nil {
		t.Fatal(err)
	}
}
func TestVerdictV2Normalization(t *testing.T) {
	for _, ref := range []string{"ABSENT", "", "../plan:1", "/plan:1", "plan:0", "plan:01", "test:plan:", "test:../plan:TestX", "test:plan:Test X", "test:plan:Test\u200bX", "docs/plan.md:7"} {
		t.Run(ref, func(t *testing.T) {
			p := v2Packet()
			f := v2Finding(p)
			if ref == "ABSENT" {
				delete(f, "contract_ref")
			} else {
				f["contract_ref"] = ref
			}
			f["blocking"], p["verdict"], p["checks_passed"] = false, "accept-with-changes", true
			calls := 0
			v, fs, events, err := ValidateVerdictV2(v2Bytes(t, p), []byte("VERDICT: ACCEPT\nF1"), testDigest, "codex/sol", refResolverFunc(func(string) bool { calls++; return false }))
			if err != nil || v.Findings[0].Class != "note" || fs[0].Class != "note" || len(events) != 1 || events[0].OriginalClass != "defect" || events[0].FindingID != "F1" {
				t.Fatalf("%+v %+v %v", v, events, err)
			}
			if ref != "docs/plan.md:7" && calls != 0 {
				t.Fatal("resolver called for invalid syntax")
			}
			if ref == "ABSENT" && fs[0].ContractRef != "" {
				t.Fatal("missing reference not materialized")
			}
			f["blocking"] = true
			if _, _, _, err := ValidateVerdictV2(v2Bytes(t, p), []byte("VERDICT: ACCEPT"), testDigest, "codex/sol", nil); err == nil {
				t.Fatal("downgrade accepted contradictory blocking")
			}
		})
	}
}
func TestVerdictV2Invalid(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"schema", func(p map[string]any) { p["schema"] = Schema }},
		{"digest", func(p map[string]any) { p["inputManifestDigest"] = strings.Repeat("b", 64) }},
		{"digest uppercase", func(p map[string]any) { p["inputManifestDigest"] = strings.Repeat("A", 64) }},
		{"route binding", func(p map[string]any) { p["reviewerRoute"] = "claude/opus" }},
		{"route syntax", func(p map[string]any) { p["reviewerRoute"] = "codex" }},
		{"verdict enum", func(p map[string]any) { p["verdict"] = "changes-requested" }},
		{"accept findings", func(p map[string]any) { p["verdict"] = "accept"; p["checks_passed"] = true }},
		{"accept changes blockers", func(p map[string]any) { p["verdict"] = "accept-with-changes"; p["checks_passed"] = true }},
		{"reject notes", func(p map[string]any) { v2Finding(p)["class"] = "note"; v2Finding(p)["blocking"] = false }},
		{"reject empty", func(p map[string]any) { p["findings"] = []any{} }},
		{"accept changes empty", func(p map[string]any) {
			p["verdict"] = "accept-with-changes"
			p["checks_passed"] = true
			p["findings"] = []any{}
		}},
		{"accept failed checks", func(p map[string]any) { p["verdict"] = "accept"; p["findings"] = []any{} }},
		{"run-601ead boolean list", func(p map[string]any) { p["checks_passed"] = []any{"passed"} }},
		{"failed checks no evidence", func(p map[string]any) { p["check_evidence"] = []any{} }},
		{"check citation", func(p map[string]any) { p["check_evidence"] = []any{"../result:1"} }},
		{"class", func(p map[string]any) { v2Finding(p)["class"] = "bug" }},
		{"blocking", func(p map[string]any) { v2Finding(p)["blocking"] = false }},
		{"id", func(p map[string]any) { v2Finding(p)["id"] = "bad id" }},
		{"duplicate id", func(p map[string]any) { p["findings"] = append(p["findings"].([]any), v2Finding(p)) }},
		{"severity", func(p map[string]any) { v2Finding(p)["severity"] = "urgent" }},
		{"blank title", func(p map[string]any) { v2Finding(p)["title"] = " " }},
		{"long title", func(p map[string]any) { v2Finding(p)["title"] = strings.Repeat("é", 257) }},
		{"unsafe title", func(p map[string]any) { v2Finding(p)["title"] = "bad\ntext" }},
		{"format recommendation", func(p map[string]any) { v2Finding(p)["recommendation"] = "bad\u200btext" }},
		{"blank recommendation", func(p map[string]any) { v2Finding(p)["recommendation"] = " " }},
		{"blank detail", func(p map[string]any) { v2Finding(p)["detail"] = " \n" }},
		{"empty evidence", func(p map[string]any) { v2Finding(p)["evidence"] = []any{} }},
		{"bad evidence", func(p map[string]any) { v2Finding(p)["evidence"] = []any{"file:0"} }},
		{"absolute path", func(p map[string]any) { v2Finding(p)["affected_paths"] = []any{"/file"} }},
		{"traversal path", func(p map[string]any) { v2Finding(p)["affected_paths"] = []any{"a/../file"} }},
		{"unclean path", func(p map[string]any) { v2Finding(p)["affected_paths"] = []any{"./file"} }},
		{"empty test", func(p map[string]any) { v2Finding(p)["failing_tests"] = []any{" "} }},
		{"unknown root", func(p map[string]any) { p["Checks_passed"] = true }},
		{"unknown finding", func(p map[string]any) { v2Finding(p)["Class"] = "note" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := v2Packet()
			tc.mutate(p)
			assertV2Failure(t, v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED\nF1"))
		})
	}
	for key, kind := range verdictV2Fields {
		for _, mode := range []string{"missing", "null", "wrong type"} {
			t.Run(key+"/"+mode, func(t *testing.T) {
				p := v2Packet()
				switch mode {
				case "missing":
					delete(p, key)
				case "null":
					p[key] = nil
				case "wrong type":
					p[key] = 123
				}
				assertV2Failure(t, v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED"))
			})
		}
		if kind == "strings" {
			p := v2Packet()
			p[key] = []any{nil}
			assertV2Failure(t, v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED"))
		}
	}
	for key, kind := range findingV2Fields {
		for _, mode := range []string{"missing", "null", "wrong type"} {
			if key == "contract_ref" && mode == "missing" {
				continue
			}
			t.Run("finding/"+key+"/"+mode, func(t *testing.T) {
				p := v2Packet()
				f := v2Finding(p)
				switch mode {
				case "missing":
					delete(f, key)
				case "null":
					f[key] = nil
				case "wrong type":
					f[key] = 123
				}
				assertV2Failure(t, v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED"))
			})
		}
		if kind == "strings" {
			p := v2Packet()
			v2Finding(p)[key] = []any{nil}
			assertV2Failure(t, v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED"))
		}
	}
}
func assertV2Failure(t *testing.T, raw, md []byte) {
	t.Helper()
	v, fs, events, err := ValidateVerdictV2(raw, md, testDigest, "codex/sol", refResolverFunc(func(string) bool { return true }))
	var failure *FormatFailure
	if !errors.As(err, &failure) || v.Schema != "" || fs != nil || events != nil {
		t.Fatalf("failure not typed/cleared: %+v %v %v %v", v, fs, events, err)
	}
}
func TestVerdictV2Documents(t *testing.T) {
	raw := v2Bytes(t, v2Packet())
	boundedRaw := append(append([]byte{}, raw...), []byte(strings.Repeat(" ", MaxDocumentBytes-len(raw)))...)
	boundedMD := "VERDICT: CHANGES_REQUESTED\nF1"
	boundedMD += strings.Repeat(" ", MaxDocumentBytes-len(boundedMD))
	if _, _, _, err := ValidateVerdictV2(boundedRaw, []byte(boundedMD), testDigest, "codex/sol", refResolverFunc(func(string) bool { return true })); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{append(append([]byte{}, raw...), []byte("{}")...), []byte("null"), []byte("[]"), []byte("{"), []byte(strings.Replace(string(raw), `"schema":`, `"schema":"other","schema":`, 1)), []byte(strings.Replace(string(raw), `"id":`, `"id":"other","id":`, 1)), []byte(strings.Repeat(" ", MaxDocumentBytes+1))} {
		assertV2Failure(t, bad, []byte("VERDICT: CHANGES_REQUESTED"))
	}
	for _, md := range []string{"", "VERDICT: ACCEPT", "\nVERDICT: CHANGES_REQUESTED", "VERDICT: CHANGES_REQUESTED\r\n", "VERDICT: CHANGES_REQUESTED " + strings.Repeat("x", MaxDocumentBytes)} {
		assertV2Failure(t, raw, []byte(md))
	}
	p := v2Packet()
	f := v2Finding(p)
	f["affected_paths"], f["failing_tests"], f["reproducer"] = []any{}, []any{}, ""
	f["title"] = strings.Repeat("é", 256)
	if _, _, _, err := ValidateVerdictV2(v2Bytes(t, p), []byte("VERDICT: CHANGES_REQUESTED\nF1"), testDigest, "codex/sol", refResolverFunc(func(string) bool { return true })); err != nil {
		t.Fatal(err)
	}
}
