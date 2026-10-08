package domain

import (
	"strings"
	"testing"
)

func TestReviewRoundLimitEscalatesOnlyNewRoundFailures(t *testing.T) {
	head := strings.Repeat("a", 40)
	for _, tc := range []struct {
		verdict     string
		accepted    bool
		used, limit int
		want        ReviewGateCode
		needed      bool
	}{
		{"invalid", false, 2, 2, ReviewGateRoundLimitExhausted, false},
		{"reject", false, 3, 2, ReviewGateRoundLimitExhausted, false},
		{"reject", false, 1, 2, ReviewGateNotAccepted, true},
		{"reject", false, 2, 0, ReviewGateNotAccepted, true},
		{"pending", false, 2, 2, ReviewGateNotAccepted, false},
		{"accept", true, 2, 2, ReviewGateAccepted, false},
	} {
		latest := &ReviewRoundHead{Number: 2, CheckpointID: "cp", HeadCommit: head, Verdict: tc.verdict, Accepted: tc.accepted, RoundsUsed: tc.used, RoundLimit: tc.limit}
		gate := EvaluateReviewCompletionGate(latest, &WorkspaceHead{Schema: WorkspaceHeadSchema, Head: head}, nil)
		if gate.Code != tc.want || gate.NewRoundNeeded != tc.needed || gate.RoundsUsed != tc.used || gate.RoundLimit != tc.limit {
			t.Fatalf("%+v => %+v", tc, gate)
		}
	}
}
