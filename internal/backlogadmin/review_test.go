package backlogadmin

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"strings"
	"testing"
)

func TestReviewRoundQueryUsesExistingReadAuthorization(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := review.Round{ID: "round-1", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []review.Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}}
	if _, err := store.CreateReviewRound(ctx, r); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	q := Query{Version: Version, Kind: QueryReviewRound, RoundID: "round-1"}
	result, err := service.Query(ctx, q)
	if err != nil || result.ReviewRound == nil || result.ReviewRound.CombinedVerdict() != "pending" {
		t.Fatalf("%+v %v", result, err)
	}
	q.RoundID = "missing"
	if _, err := service.Query(ctx, q); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if !IsQueryKind(QueryReviewRound) {
		t.Fatal("round is not a declared read view")
	}
}
