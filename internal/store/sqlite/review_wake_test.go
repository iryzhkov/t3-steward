package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"strings"
	"testing"
	"time"
)

func TestReviewWakeWaitsForCollectionAndCarriesShortReply(t *testing.T) {
	ctx := context.Background()
	store, _, now := sinkStoreFixture(t)
	defer store.Close()
	r, err := store.CreateReviewRound(ctx, review.Round{ID: "r", WorkflowRunID: "r", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []review.Reviewer{{ID: "independent", TaskID: "t", Role: "independent", Route: "a/full", Required: true}}})
	if err != nil {
		t.Fatal(err)
	}
	req := domain.NodeWaitRequest{ID: "review-wake", ThreadID: "caller", Name: "review", Target: domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}, Timeout: time.Hour}
	w, err := store.RegisterNodeWait(ctx, req, "operator", "host", now)
	if err != nil {
		t.Fatal(err)
	}
	if w.SettledAt != nil {
		t.Fatal("wake settled before review collection")
	}
	v := review.Verdict{Schema: review.Schema, Verdict: "accept", Findings: []review.Finding{}, ReviewerRoute: "a/full", InputManifestDigest: r.InputManifestDigest}
	raw, _ := json.Marshal(v)
	r, err = store.RecordReviewResult(ctx, r.ID, "independent", r.Revision, review.Result{State: "succeeded", ReviewMD: "accepted", VerdictJSON: raw})
	if err != nil {
		t.Fatal(err)
	}
	text := "round r: accept\nsummary.json"
	if err := store.PublishReviewReply(ctx, r.ID, r.Revision, text); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(waits) != 1 || waits[0].SettledAt == nil || waits[0].Observation.Reason != text || waits[0].Observation.ExitCode != 0 {
		t.Fatalf("wake: %+v", waits)
	}
	req.ID = "late-review-wake"
	w, err = store.RegisterNodeWait(ctx, req, "operator", "host", now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if w.SettledAt == nil || w.Observation.Reason != text {
		t.Fatal("late registration lost review reply")
	}
}
