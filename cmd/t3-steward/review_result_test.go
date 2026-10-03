package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/review"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func resultRound(t *testing.T) review.Round {
	t.Helper()
	digest := strings.Repeat("a", 64)
	r := review.Round{ID: "round-1", InputManifestDigest: digest, Reviewers: []review.Reviewer{{ID: "reviewer", Role: "review", Route: "codex/sol", Required: true}}}
	if err := r.Initialize(time.Now()); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema":"review-verdict/v1","verdict":"reject","inputManifestDigest":"` + digest + `","reviewerRoute":"codex/sol","findings":[{"id":"F1","severity":"high","blocking":true,"title":"Race loses data","evidence":["main.go:12"],"recommendation":"Serialize writes"}]}`)
	if err := r.ApplyResult("reviewer", review.Result{State: "succeeded", ReviewMD: "Detailed review", VerdictJSON: raw}, time.Now()); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestReviewResultWritesStatePathsAndShortReply(t *testing.T) {
	r := resultRound(t)
	root := t.TempDir()
	var out bytes.Buffer
	metadata := r.Metadata()
	cli := reviewResultCLI{results: root, stdout: &out, query: func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		if q.Kind == backlogadmin.QueryReviewDocument {
			var content []byte
			if q.ReviewDocument == "review.md" {
				content = []byte(r.Reviewers[0].ReviewMD)
			} else {
				content = r.Reviewers[0].VerdictJSON
			}
			return backlogadmin.Response{ReviewDocument: &backlogadmin.ReviewDocument{RoundID: q.RoundID, ReviewerID: q.ReviewerID, Name: q.ReviewDocument, Content: content}}, nil
		}
		if q.Kind != backlogadmin.QueryReviewRound || q.RoundID != r.ID {
			t.Fatalf("wrong query %+v", q)
		}
		return backlogadmin.Response{ReviewRound: &metadata}, nil
	}}
	if err := cli.run(context.Background(), []string{"round-1"}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, value := range []string{"reject", "codex/sol", "blocking: 1", "Race loses data", "summary.json", "review.md", "verdict.json"} {
		if !strings.Contains(text, value) {
			t.Fatalf("missing %q: %s", value, text)
		}
	}
	if strings.Contains(text, "Detailed review") {
		t.Fatal("reply dumped full review")
	}
	for _, name := range []string{"reviewer/review.md", "reviewer/verdict.json", "summary.json"} {
		raw, err := os.ReadFile(filepath.Join(root, "reviews", "round-1", filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, ".json") && !json.Valid(raw) {
			t.Fatal("invalid output JSON")
		}
	}
	out.Reset()
	if err := cli.run(context.Background(), []string{"round-1", "--json"}); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(out.Bytes()) || strings.Contains(out.String(), "Detailed review") {
		t.Fatal("bad short JSON reply")
	}
}
func TestReviewResultWaitAndFailedReviewer(t *testing.T) {
	r := resultRound(t)
	pending := r
	pending.Reviewers = append([]review.Reviewer(nil), r.Reviewers...)
	pending.Reviewers[0].State = "pending"
	calls := 0
	var out bytes.Buffer
	cli := reviewResultCLI{results: t.TempDir(), stdout: &out, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		calls++
		if calls == 1 {
			return backlogadmin.Response{ReviewRound: &pending}, nil
		}
		return backlogadmin.Response{ReviewRound: &r}, nil
	}}
	if err := cli.run(context.Background(), []string{"round-1", "--wait", "--timeout", "2s"}); err != nil || calls != 2 {
		t.Fatalf("wait %d %v", calls, err)
	}
	r.Reviewers[0].State = "timed-out"
	r.Reviewers[0].Verdict = nil
	r.Reviewers[0].Failure = "deadline"
	err := cli.run(context.Background(), []string{"round-1"})
	if err == nil || exitCodeFor(err) != 2 {
		t.Fatalf("failed collection: %v", err)
	}
	cli.query = func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{ReviewRound: &pending}, nil
	}
	err = cli.run(context.Background(), []string{"round-1", "--wait", "--timeout", "1ms"})
	if err == nil || exitCodeFor(err) != 1 {
		t.Fatalf("timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.run(ctx, []string{"round-1", "--wait"}); exitCodeFor(err) != 130 {
		t.Fatalf("interrupt: %v", err)
	}
}
func TestReviewResultRefusesPathTraversal(t *testing.T) {
	for _, id := range []string{"../escape", "/absolute", "round/child"} {
		cli := reviewResultCLI{results: t.TempDir(), stdout: &bytes.Buffer{}}
		if err := cli.run(context.Background(), []string{id}); err == nil {
			t.Fatalf("accepted %q", id)
		}
	}
}
