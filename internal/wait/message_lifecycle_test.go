package wait

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestWakeMessageContractReviewVerdictAfterProcessZero(t *testing.T) {
	for _, verdict := range []string{"REJECT", "ACCEPT"} {
		store := &memStore{waits: map[string]Wait{}}
		control := &memControl{threads: map[string]*domain.Thread{"t": {ID: "t"}}}
		runner := New(store, control, nil)
		now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		runner.SetClock(func() time.Time { return now })
		runner.Exec = func(context.Context, Wait) (string, int, error) { return verdict, 0, nil }
		w := Wait{ID: "review", ThreadID: "t", Name: "review", Command: []string{"review"}, Status: StatusWaiting, Every: time.Second, Timeout: time.Hour, CreatedAt: now}
		if err := store.SaveWait(context.Background(), w); err != nil {
			t.Fatal(err)
		}
		runner.Tick(context.Background(), nil, nil)
		runner.Tick(context.Background(), nil, nil)
		if len(control.texts) != 1 || store.waits["review"].LastExit != 0 {
			t.Fatal("process control failed")
		}
		text := control.texts[0]
		if !strings.Contains(text, verdict) || !strings.Contains(text, "actual result or review verdict") || !strings.Contains(text, "exit 0 does not establish") {
			t.Fatal("process zero promoted to verdict", text)
		}
	}
}
func TestWakeMessageContractMixedGroupAndSafeResult(t *testing.T) {
	now := time.Now()
	members := []domain.NodeWait{
		{Request: domain.NodeWaitRequest{ID: "first", Group: "g", Target: domain.NodeRef{RunID: "run-abc", TaskID: "a"}}, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: "run-abc", TaskID: "a"}, Progress: domain.ProgressSucceeded, Outcome: domain.TaskWaitMet}, SettledAt: &now},
		{Request: domain.NodeWaitRequest{ID: "second", Group: "g", Target: domain.NodeRef{RunID: "run-abc", TaskID: "b"}}, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: "run-abc", TaskID: "b"}, Progress: domain.ProgressFailed, Outcome: domain.TaskWaitFailed, ExitCode: 2}, SettledAt: &now},
	}
	if !nodeGroupSettled(members) {
		t.Fatal("not settled")
	}
	text := nodeGroupMessage(members)
	for _, want := range []string{"count=2", "wait=first", "wait=second", "outcome=met", "outcome=failed", "every wait outcome", "task result run-abc"} {
		if !strings.Contains(text, want) {
			t.Fatal("lost group evidence", want, text)
		}
	}
	for _, run := range []string{"", "-bad", "bad;echo", "bad\nnext", strings.Repeat("a", 129)} {
		w := members[0]
		obs := *w.Observation
		obs.Target.RunID = run
		w.Observation = &obs
		if nodeWakeResult(w) != "" {
			t.Fatal("unsafe result command", run)
		}
	}
	w := members[0]
	w.Request.Quota = &domain.QuotaWaitCondition{Pool: "pool"}
	if strings.Contains(nodeWakeProse(w), "task result") || !strings.Contains(nodeWakeProse(w), "reset deadline alone") {
		t.Fatal("quota fabricated run command")
	}
	w = members[0]
	obs := *w.Observation
	obs.Progress = domain.ProgressActive
	w.Observation = &obs
	if nodeWakeResult(w) != "" {
		t.Fatal("nonterminal command")
	}
}
