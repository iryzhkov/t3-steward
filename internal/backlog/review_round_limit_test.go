package backlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReviewRoundLimitImportTable(t *testing.T) {
	for _, tc := range []struct {
		name, verdict, head, commit, code string
		limit                             int
		dirty, passed, needed             bool
	}{
		{"reject at limit", "reject", reviewGateHeadA, "", "review-round-limit-exhausted", 1, false, false, false},
		{"reject below limit", "reject", reviewGateHeadA, "", "review-not-accepted", 2, false, false, true},
		{"head changed", "accept", reviewGateHeadB, "", "review-round-limit-exhausted", 1, false, false, false},
		{"declared elsewhere", "accept", reviewGateHeadA, reviewGateHeadB, "review-round-limit-exhausted", 1, false, false, false},
		{"accepted", "accept", reviewGateHeadA, "", "accepted-head", 1, false, true, false},
		{"pending", "", reviewGateHeadA, "", "review-not-accepted", 1, false, false, false},
		{"dirty", "accept", reviewGateHeadA, "", "dirty-tree-after-review", 1, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReviewGateFixture(t, true, tc.commit != "")
			// The older shared gate fixture leaves run revision zero, while automatic
			// cancellation requires the production run identity/revision contract.
			records, err := f.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, run := range records.WorkflowRuns {
				run.Revision = 1
				run.Progress = domain.ProgressActive
				if err := f.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
					t.Fatal(err)
				}
			}
			spec := f.authority.Requirements
			spec.RoundLimit = tc.limit
			req, err := review.NewRequirements(spec)
			if err != nil {
				t.Fatal(err)
			}
			f.authority, err = review.NewFrozenAuthority(f.authority.Parent, req)
			if err != nil {
				t.Fatal(err)
			}
			f.openRound(t, "cp-1", reviewGateHeadA, tc.verdict)
			f.finishTurn(t)
			head := cleanWorkspaceHead(tc.head)
			head.Dirty = tc.dirty
			head.DirtyPaths = []string{"main.go"}
			a := f.collect(t, f.store, head, tc.commit)
			g := a.ReviewGate
			if g == nil || string(g.Code) != tc.code || g.Passed != tc.passed || g.NewRoundNeeded != tc.needed {
				t.Fatalf("attempt %+v gate %+v", a, g)
			}
			if (a.Progress == domain.ProgressSucceeded) != tc.passed {
				t.Fatalf("progress %s", a.Progress)
			}
			raw, _ := json.Marshal(g)
			var fields map[string]any
			_ = json.Unmarshal(raw, &fields)
			if fields["roundsUsed"] != float64(1) || fields["roundLimit"] != float64(tc.limit) {
				t.Fatalf("counts: %s", raw)
			}
			if tc.code == "review-round-limit-exhausted" {
				for _, text := range []string{"1 of 1", tc.verdict, "campaign rerun", "--from", "new run"} {
					if !strings.Contains(a.Failure, text) {
						t.Fatalf("failure %q missing %q", a.Failure, text)
					}
				}
			}
			// Cancellation follows import and cannot rewrite the persisted gate.
			if err := f.store.ReconcileMaterializedReviewChildren(context.Background()); err != nil {
				t.Fatal(err)
			}
			records, err = f.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, stored := range records.Attempts {
				if stored.ID == a.ID && stored.Failure != a.Failure {
					t.Fatal("cancellation rewrote gate")
				}
			}
		})
	}
}
