package backlog

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestStructuredReviewParse(t *testing.T) {
	for _, synonym := range []string{"ACCEPT", "ACCEPTED", "APPROVE", "accept", "CHANGES_REQUESTED", "CHANGES REQUESTED", "REQUEST_CHANGES", "REJECT", "changes-requested"} {
		want := "accept"
		if strings.Contains(synonym, "CHANGE") || synonym == "REJECT" || synonym == "changes-requested" {
			want = "changes-requested"
		}
		for _, line := range []bool{false, true} {
			decl := domain.ReviewOutput{Verdict: "verdict.json"}
			raw := []byte(fmt.Sprintf("{\"verdict\":%q,\"blocking_findings\":2,\"finding_titles\":[\"one\",\"two\"]}", synonym))
			if line {
				decl = domain.ReviewOutput{VerdictLine: "review.md"}
				raw = []byte(synonym + "\nbody that must not be copied")
			}
			got, err := ParseReviewVerdict(decl, raw)
			if err != nil || got.Verdict != want {
				t.Fatalf("%s line=%v: %+v %v", synonym, line, got, err)
			}
		}
	}
	for _, raw := range []string{"", "{}", "null", "{", `{"verdict":"ACCEPT","blocking_findings":null}`, `{"verdict":"ACCEPT","finding_titles":[null]}`, `{"verdict":"REJECT","verdict":"ACCEPT"}`, "{\"verdict\":\"maybe\"}", "{\"verdict\":\"accept\",\"blocking_findings\":-1}", "{\"verdict\":\"accept\",\"blocking_findings\":1.5}", "{\"verdict\":\"accept\"} {}"} {
		if _, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, []byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	for _, raw := range []string{"", "\nACCEPT", "ACCEPT later", "maybe", strings.Repeat("a", MaxReviewVerdictBytes+1)} {
		if _, err := ParseReviewVerdict(domain.ReviewOutput{VerdictLine: "v.md"}, []byte(raw)); err == nil {
			t.Fatalf("accepted bad line length %d", len(raw))
		}
	}
	raw := []byte("{\"verdict\":\"reject\",\"finding_titles\":[\"" + strings.Repeat("x", 400) + "\",\"2\",\"3\",\"4\",\"5\",\"6\"]}")
	got, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, raw)
	if err != nil || len(got.FindingTitles) != 5 || len(got.FindingTitles[0]) > 160 {
		t.Fatalf("unbounded: %+v %v", got, err)
	}
}

func TestStructuredReviewInvalidUTF8(t *testing.T) {
	raw := append([]byte("{\"verdict\":\"ACCEPT\",\"finding_titles\":[\""), 0xff)
	raw = append(raw, []byte("\"]}")...)
	if _, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, raw); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestStructuredReviewDeclaration(t *testing.T) {
	base := "version: 2\nname: review-test\nenvironment:\n  project: test\ntasks:\n  review:\n    prompt_file: prompt.md\n    outputs: [verdict.json, review.md]\n"
	for _, decl := range []string{"{verdict: verdict.json}", "{verdict_line: review.md}"} {
		m, err := ParseManifest([]byte(base + "    review_output: " + decl + "\n"))
		if err != nil || m.Tasks["review"].ReviewOutput == nil {
			t.Fatalf("%s: %v", decl, err)
		}
	}
	for _, decl := range []string{"{}", "{verdict: ../bad}", "{verdict: absent.json}", "{verdict: verdict.json, verdict_line: review.md}", "{unknown: review.md}"} {
		if _, err := ParseManifest([]byte(base + "    review_output: " + decl + "\n")); err == nil {
			t.Fatalf("accepted %s", decl)
		}
	}
}

func TestStructuredReviewMissingAndLegacy(t *testing.T) {
	task := domain.Task{ReviewOutput: &domain.ReviewOutput{Verdict: "v.json"}}
	if _, err := reviewVerdictFromResult(task, nil, nil); err == nil {
		t.Fatal("missing verdict accepted")
	}
	task.ReviewOutput = nil
	if got, err := reviewVerdictFromResult(task, nil, nil); err != nil || got != nil {
		t.Fatalf("legacy: %+v %v", got, err)
	}
}
