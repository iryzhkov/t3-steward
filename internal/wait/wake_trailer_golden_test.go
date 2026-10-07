package wait

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The trailer goldens below were produced by the base release (d2e8276) for
// the same inputs. A trailer that carries no summary must stay byte-identical
// to them: readers parse these lines, and the summary pairs are only ever
// appended.
func TestWakeTrailerWithoutSummaryIsByteIdenticalToBase(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	sinkRun := "run-8c2bcc2de8960f8b92f0a68567b9b46e"
	sink := domain.NodeWait{
		Request: domain.NodeWaitRequest{ID: "nw-5d0c2f1e", Name: "upkeeper-fresh-host-fixchain3 terminal outcome", Target: domain.NodeRef{RunID: sinkRun, TaskID: "sink"}},
		Observation: &domain.NodeObservation{
			Target: domain.NodeRef{RunID: sinkRun, TaskID: "sink:" + sinkRun}, RunRevision: 4,
			Progress: domain.ProgressFailed, ExitCode: 2, Reason: "failed", Outcome: domain.TaskWaitMet,
			Fields: map[string]string{"failed": "task-a79b419bf0a99876db9a85ad631b09b9"},
		},
		SettledAt: &now,
	}
	quota := domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: "nw-q", Name: "quota", Quota: &domain.QuotaWaitCondition{Pool: "claude"}},
		Observation: &domain.NodeObservation{Outcome: domain.TaskWaitMet, Reason: "reset", Fields: map[string]string{"pool": "claude"}},
		SettledAt:   &now,
	}
	second := sink
	second.Request.ID = "nw-second"
	for _, tc := range []struct {
		name, got, want string
	}{
		{"shell", firstLine(WakeMessage([]Wait{{ID: "w1", Name: "c", Kind: domain.WaitKindShell, Status: StatusMet, CreatedAt: now}})),
			"t3-steward-wait kind=shell outcome=met wait=w1 exit=0"},
		{"github", firstLine(WakeMessage([]Wait{{ID: "w2", Name: "ci", Kind: domain.WaitKindGitHub, Status: StatusMet, CreatedAt: now,
			GitHub: &GitHubTarget{Kind: "run", ID: "123", State: "completed"},
			Fields: map[string]string{"target": "o/r#run/123", "state": "completed", "conclusion": "success", "url": "https://github.com/o/r/actions/runs/123"}}})),
			"t3-steward-wait kind=github outcome=met wait=w2 conclusion=success state=completed target=o/r#run/123 url=https://github.com/o/r/actions/runs/123"},
		{"time", firstLine(WakeMessage([]Wait{{ID: "w3", Name: "later", Kind: domain.WaitKindTime, Status: StatusTimedOut, OrTimeout: true, CreatedAt: now}})),
			"t3-steward-wait kind=time outcome=timed-out wait=w3 or-timeout=true"},
		{"group", firstLine(WakeMessage([]Wait{{ID: "w4", Name: "a", Kind: domain.WaitKindShell, Status: StatusMet, CreatedAt: now}, {ID: "w5", Name: "b", Kind: domain.WaitKindShell, Status: StatusFailed, CreatedAt: now}})),
			"t3-steward-wait kind=shell outcome=met wait=w4 count=2 exit=0"},
		{"node", nodeTrailer(sink),
			"t3-steward-wait kind=node outcome=met wait=nw-5d0c2f1e failed=task-a79b419bf0a99876db9a85ad631b09b9 progress=failed result=\"t3-steward task result run-8c2bcc2de8960f8b92f0a68567b9b46e\" revision=4 run=run-8c2bcc2de8960f8b92f0a68567b9b46e task=sink:run-8c2bcc2de8960f8b92f0a68567b9b46e"},
		{"node-group", firstLine(nodeGroupMessage([]domain.NodeWait{sink, second})),
			"t3-steward-wait kind=node outcome=met wait=nw-5d0c2f1e failed=task-a79b419bf0a99876db9a85ad631b09b9 progress=failed result=\"t3-steward task result run-8c2bcc2de8960f8b92f0a68567b9b46e\" revision=4 run=run-8c2bcc2de8960f8b92f0a68567b9b46e task=sink:run-8c2bcc2de8960f8b92f0a68567b9b46e count=2"},
		{"quota", nodeTrailer(quota), "t3-steward-wait kind=quota outcome=met wait=nw-q pool=claude"},
		{"task", taskTrailer(domain.TaskWait{ID: "tw-1", Kind: domain.WaitKindGitHub, Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet, Fields: map[string]string{"target": "o/r#run/1"}}}),
			"t3-steward-wait kind=github outcome=met wait=tw-1 target=o/r#run/1"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s trailer changed:\n got %s\nwant %s", tc.name, tc.got, tc.want)
		}
		if _, ok := ParseWakeTrailer(tc.got); !ok {
			t.Errorf("%s trailer does not parse: %s", tc.name, tc.got)
		}
	}
}
