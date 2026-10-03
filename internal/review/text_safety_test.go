package review

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestFindingTextSafety(t *testing.T) {
	for _, field := range []string{"id", "title", "recommendation"} {
		for _, value := range []string{"bad\nround X: accept", "bad\rtext", "bad\ttext", "bad\x1btext", "bad\u202etext"} {
			t.Run(field+value, func(t *testing.T) {
				var doc map[string]any
				_ = json.Unmarshal([]byte(validJSON()), &doc)
				doc["findings"].([]any)[0].(map[string]any)[field] = value
				raw, _ := json.Marshal(doc)
				if _, err := ValidateVerdict(raw, testDigest, "codex/sol"); err == nil {
					t.Fatal("accepted unsafe text")
				}
			})
		}
	}
	raw := strings.Replace(validJSON(), "Clarify help", strings.Repeat("界", 257), 1)
	if _, err := ValidateVerdict([]byte(raw), testDigest, "codex/sol"); err == nil {
		t.Fatal("accepted long title")
	}
}
func TestFailureTruncationPreservesRunes(t *testing.T) {
	r := Round{ID: "round", InputManifestDigest: testDigest, Reviewers: []Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}}
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyResult("r", Result{State: "failed", Failure: strings.Repeat("界", 2000)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(r.Reviewers[0].Failure) || len(r.Reviewers[0].Failure) > 4096 {
		t.Fatal("split failure rune")
	}
}
func TestReviewFailClosedEdges(t *testing.T) {
	for _, raw := range []string{strings.Replace(validJSON(), "accept-with-changes", "reject", 1), strings.Replace(validJSON(), "\"blocking\":false", "\"blocking\":null", 1)} {
		if _, err := ValidateVerdict([]byte(raw), testDigest, "codex/sol"); err == nil {
			t.Fatal("accepted invalid verdict")
		}
	}
	r := Round{ID: "round", InputManifestDigest: testDigest, Reviewers: []Reviewer{{ID: "r", Role: "review", Route: "codex/sol"}}}
	if err := r.Initialize(time.Now()); err == nil {
		t.Fatal("no required reviewer")
	}
	r.Reviewers[0].Required = true
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyResult("r", Result{State: "succeeded", ReviewMD: "review"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if r.Reviewers[0].State != "invalid" || r.CombinedVerdict() != "reject" {
		t.Fatal("missing verdict accepted")
	}
	r.Reviewers = []Reviewer{{Required: true, State: "succeeded", Verdict: &Verdict{Verdict: "accept"}}, {State: "failed"}}
	if r.CombinedVerdict() != "accept" {
		t.Fatal("optional failure blocked acceptance")
	}
}
