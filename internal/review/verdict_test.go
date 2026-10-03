package review

import (
	"strings"
	"testing"
)

const testDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func validJSON() string {
	return `{"schema":"review-verdict/v1","verdict":"accept-with-changes","inputManifestDigest":"` + testDigest + `","reviewerRoute":"codex/sol","findings":[{"id":"F1","severity":"low","blocking":false,"title":"Clarify help","evidence":["cmd/main.go:12"],"recommendation":"Explain the flag"}]}`
}
func TestVerdictValidationAndBinding(t *testing.T) {
	if _, err := ValidateVerdict([]byte(validJSON()), testDigest, "codex/sol"); err != nil {
		t.Fatal(err)
	}
	cases := []string{
		strings.Replace(validJSON(), "accept-with-changes", "accept", 1),
		strings.Replace(validJSON(), "\"blocking\":false", "\"blocking\":true", 1),
		strings.Replace(validJSON(), "\"severity\":\"low\"", "\"severity\":\"urgent\"", 1),
		strings.Replace(validJSON(), "\"evidence\":[\"cmd/main.go:12\"]", "\"evidence\":[]", 1),
		strings.Replace(validJSON(), "cmd/main.go:12", "../secret:1", 1),
		strings.Replace(validJSON(), "\"blocking\":false,", "", 1),
		strings.Replace(validJSON(), "\"id\":\"F1\"", "\"id\":\"F1\",\"id\":\"F2\"", 1),
		strings.Replace(validJSON(), "\"verdict\":", "\"unknown\":1,\"verdict\":", 1),
		validJSON() + "{}",
		strings.Replace(validJSON(), "\"findings\":[", "\"findings\":[],\"Findings\":[", 1),
		strings.Replace(validJSON(), "\"blocking\":false", "\"blocking\":false,\"Blocking\":true", 1),
	}
	for _, raw := range cases {
		if _, err := ValidateVerdict([]byte(raw), testDigest, "codex/sol"); err == nil {
			t.Fatalf("accepted malformed %s", raw)
		}
	}
	if _, err := ValidateVerdict([]byte(validJSON()), strings.Repeat("b", 64), "codex/sol"); err == nil {
		t.Fatal("accepted wrong manifest")
	}
	if _, err := ValidateVerdict([]byte(validJSON()), testDigest, "claude/opus"); err == nil {
		t.Fatal("accepted wrong route")
	}
}
func TestRoundCombinationFailsClosed(t *testing.T) {
	v, err := ValidateVerdict([]byte(validJSON()), testDigest, "codex/sol")
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"failed", "timed-out", "invalid"} {
		r := Round{ID: "round-1", InputManifestDigest: testDigest, Reviewers: []Reviewer{
			{ID: "a", Role: "review", Route: "codex/sol", Required: true, State: "succeeded", Verdict: &v},
			{ID: "b", Role: "critical-review", Route: "claude/opus", Required: true, State: state},
		}}
		if r.CombinedVerdict() != "reject" || !r.Terminal() {
			t.Fatalf("accepted %s", state)
		}
	}
	r := Round{Reviewers: []Reviewer{{Required: true, State: "pending"}}}
	if r.CombinedVerdict() != "pending" || r.Terminal() {
		t.Fatal("pending review accepted")
	}
	r.Reviewers = []Reviewer{{Required: true, State: "succeeded", Verdict: &v}}
	if r.CombinedVerdict() != "accept-with-changes" {
		t.Fatal("lost non-blocking verdict")
	}
}
