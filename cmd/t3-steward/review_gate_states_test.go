package main

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/review"
	"io"
	"strings"
	"testing"
	"time"
)

func TestReviewGateRejectPendingAccept(t *testing.T) {
	for _, state := range []string{"reject", "pending", "accept"} {
		t.Run(state, func(t *testing.T) {
			r := review.Round{ID: "gate-state", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []review.Reviewer{{ID: "independent", Role: "independent", Required: true, Route: "a/full"}}}
			if err := r.Initialize(time.Now()); err != nil {
				t.Fatal(err)
			}
			if state != "pending" {
				v := review.Verdict{Schema: review.Schema, Verdict: state, Findings: []review.Finding{}, InputManifestDigest: r.InputManifestDigest, ReviewerRoute: "a/full"}
				if state == "reject" {
					v.Findings = []review.Finding{{ID: "bug", Severity: "high", Blocking: true, Title: "bug", Evidence: []string{"file.go:1"}, Recommendation: "fix"}}
				}
				raw, _ := json.Marshal(v)
				if err := r.ApplyResult("independent", review.Result{State: "succeeded", ReviewMD: "review", VerdictJSON: raw}, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			c := reviewResultCLI{results: t.TempDir(), stdout: io.Discard, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
				return backlogadmin.Response{ReviewRound: &r}, nil
			}}
			err := c.run(context.Background(), []string{r.ID, "--gate"})
			if state == "accept" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			expected := 3
			if state == "pending" {
				expected = 1
			}
			code, ok := err.(exitCodeError)
			if !ok || code.code != expected {
				t.Fatalf("%s gate: %v", state, err)
			}
		})
	}
}
