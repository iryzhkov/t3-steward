package review

import (
	"strings"
	"testing"
	"time"
)

func TestReviewOversizedDocumentBecomesInvalid(t *testing.T) {
	r := Round{ID: "round", InputManifestDigest: testDigest, Reviewers: []Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}}
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyResult("r", Result{State: "succeeded", ReviewMD: strings.Repeat("x", MaxDocumentBytes+1), VerdictJSON: []byte(validJSON())}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if r.Reviewers[0].State != "invalid" || r.CombinedVerdict() != "reject" || !r.Terminal() {
		t.Fatal("oversized reviewer did not fail closed")
	}
}
func TestRoundMetadataBounds(t *testing.T) {
	for _, edit := range []func(*Round){
		func(r *Round) { r.Reviewers[0].Role = strings.Repeat("x", 65) },
		func(r *Round) { r.Reviewers[0].Route = "codex/" + strings.Repeat("x", 257) },
		func(r *Round) { r.WorkflowRunID = strings.Repeat("x", 129) },
		func(r *Round) {
			r.Reviewers = make([]Reviewer, 33)
			for i := range r.Reviewers {
				r.Reviewers[i] = Reviewer{ID: strings.Repeat("x", i+1), Role: "review", Route: "codex/sol", Required: true}
			}
		},
	} {
		r := Round{ID: "round", InputManifestDigest: testDigest, Reviewers: []Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}}
		edit(&r)
		if err := r.Initialize(time.Now()); err == nil {
			t.Fatal("unbounded metadata accepted")
		}
	}
	r := Round{ID: "round", InputManifestDigest: testDigest, Reviewers: []Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}}
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyResult("r", Result{State: "failed", Failure: strings.Repeat("x", 10000)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(r.Metadata().Reviewers[0].Failure) > 4096 {
		t.Fatal("unbounded failure response")
	}
}
