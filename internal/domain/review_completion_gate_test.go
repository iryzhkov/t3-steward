package domain

import (
	"strings"
	"testing"
)

var (
	gateHeadA = strings.Repeat("a", 40)
	gateHeadB = strings.Repeat("b", 40)
)

func acceptedRound(head string) *ReviewRoundHead {
	return &ReviewRoundHead{RoundID: "rc-1", Number: 1, CheckpointID: "cp-1", BaseCommit: strings.Repeat("c", 40), HeadCommit: head, Verdict: "accept", Accepted: true}
}

func cleanHead(head string) *WorkspaceHead {
	return &WorkspaceHead{Schema: WorkspaceHeadSchema, Head: head}
}

// Each structured reason is reported with its code, and an accepted head that
// equals the physical HEAD of a clean tree is the only way through.
func TestReviewCompletionGateReasons(t *testing.T) {
	pending := acceptedRound(gateHeadA)
	pending.Accepted, pending.Verdict = false, "pending"
	rejected := acceptedRound(gateHeadA)
	rejected.Accepted, rejected.Verdict = false, "reject"
	dirty := cleanHead(gateHeadA)
	dirty.Dirty, dirty.DirtyPaths = true, []string{"main.go"}
	for _, test := range []struct {
		name     string
		latest   *ReviewRoundHead
		head     *WorkspaceHead
		commits  []DeclaredCommitHead
		code     ReviewGateCode
		passed   bool
		newRound bool
		contains []string
	}{
		{name: "accepted head equals clean HEAD", latest: acceptedRound(gateHeadA), head: cleanHead(gateHeadA), code: ReviewGateAccepted, passed: true},
		{name: "no round", head: cleanHead(gateHeadA), code: ReviewGateRequired, newRound: true, contains: []string{"review-required", gateHeadA}},
		{name: "pending round", latest: pending, head: cleanHead(gateHeadA), code: ReviewGateNotAccepted, contains: []string{"review-not-accepted", "pending", "round 1"}},
		{name: "rejected round", latest: rejected, head: cleanHead(gateHeadA), code: ReviewGateNotAccepted, newRound: true, contains: []string{"review-not-accepted", "reject", "new review round"}},
		{name: "commit after acceptance", latest: acceptedRound(gateHeadA), head: cleanHead(gateHeadB), code: ReviewGateHeadChanged, newRound: true,
			contains: []string{"head-changed-after-review", gateHeadA, gateHeadB, "new review round on " + gateHeadB}},
		{name: "dirty tree", latest: acceptedRound(gateHeadA), head: dirty, code: ReviewGateDirtyTree, contains: []string{"dirty-tree-after-review", "main.go", gateHeadA}},
		{name: "no evidence", latest: acceptedRound(gateHeadA), code: ReviewGateHeadUnknown, contains: []string{"workspace-head-unknown"}},
		{name: "capture error", latest: acceptedRound(gateHeadA), head: &WorkspaceHead{Schema: WorkspaceHeadSchema, Error: "not a git repository"}, code: ReviewGateHeadUnknown, contains: []string{"not a git repository"}},
		{name: "declared commit after acceptance", latest: acceptedRound(gateHeadA), head: cleanHead(gateHeadA), commits: []DeclaredCommitHead{{Name: "change", Commit: gateHeadB}},
			code: ReviewGateHeadChanged, newRound: true, contains: []string{"head-changed-after-review", `declared commit "change"`, gateHeadB, gateHeadA}},
		{name: "declared commit at accepted head", latest: acceptedRound(gateHeadA), head: cleanHead(gateHeadA), commits: []DeclaredCommitHead{{Name: "change", Commit: gateHeadA}}, code: ReviewGateAccepted, passed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := EvaluateReviewCompletionGate(test.latest, test.head, test.commits)
			if gate.Code != test.code || gate.Passed != test.passed || gate.NewRoundNeeded != test.newRound {
				t.Fatalf("gate = %+v", gate)
			}
			if test.passed {
				if gate.Failure() != "" || gate.ReviewedHead != gateHeadA || gate.PhysicalHead != gateHeadA || gate.RoundNumber != 1 {
					t.Fatalf("passing gate = %+v failure %q", gate, gate.Failure())
				}
				return
			}
			failure := gate.Failure()
			for _, want := range test.contains {
				if !strings.Contains(failure+" "+gate.Summary(), want) {
					t.Fatalf("failure %q / summary %q does not name %q", failure, gate.Summary(), want)
				}
			}
		})
	}
}

// The heads compared are kept on the decision, so explain and task result can
// show both without recomputing anything.
func TestReviewCompletionGateRecordsBothHeads(t *testing.T) {
	gate := EvaluateReviewCompletionGate(acceptedRound(gateHeadA), cleanHead(gateHeadB), nil)
	if gate.ReviewedHead != gateHeadA || gate.PhysicalHead != gateHeadB || gate.RoundID != "rc-1" || gate.CheckpointID != "cp-1" || gate.RoundVerdict != "accept" {
		t.Fatalf("gate = %+v", gate)
	}
	if !strings.Contains(gate.Summary(), gateHeadA) || !strings.Contains(gate.Summary(), gateHeadB) {
		t.Fatalf("summary %q does not show both heads", gate.Summary())
	}
}
