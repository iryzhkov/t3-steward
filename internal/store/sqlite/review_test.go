package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/review"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewRoundPersistsValidatedResultsAndFencesUpdates(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	round := review.Round{ID: "round-1", InputManifestDigest: digest, Reviewers: []review.Reviewer{{ID: "reviewer", Role: "review", Route: "codex/sol", Required: true}}}
	created, err := s.CreateReviewRound(ctx, round)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateReviewRound(ctx, round); err == nil {
		t.Fatal("replaced existing round")
	}
	raw := []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + digest + `","reviewerRoute":"codex/sol"}`)
	result := review.Result{State: "succeeded", ReviewMD: "Looks good", VerdictJSON: raw}
	done, err := s.RecordReviewResult(ctx, round.ID, "reviewer", created.Revision, result)
	if err != nil || done.CombinedVerdict() != "accept" {
		t.Fatalf("%+v %v", done, err)
	}
	if _, err := s.RecordReviewResult(ctx, round.ID, "reviewer", created.Revision, result); err == nil {
		t.Fatal("stale update accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetReviewRound(ctx, round.ID)
	if err != nil || got.Revision != done.Revision || got.Reviewers[0].ReviewMD != "Looks good" || got.CombinedVerdict() != "accept" {
		t.Fatalf("%+v %v", got, err)
	}
	round.ID = "round-2"
	created, err = s.CreateReviewRound(ctx, round)
	if err != nil {
		t.Fatal(err)
	}
	result.VerdictJSON = []byte("{}")
	bad, err := s.RecordReviewResult(ctx, round.ID, "reviewer", created.Revision, result)
	if err != nil || bad.Reviewers[0].State != "invalid" || bad.CombinedVerdict() != "reject" {
		t.Fatalf("%+v %v", bad, err)
	}
}
