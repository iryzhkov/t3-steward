package backlog

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func loopDAG(t *testing.T) *DAGExecution {
	t.Helper()
	var tasks []domain.Task
	for n := 1; n <= 4; n++ {
		implement, review := fmt.Sprintf("implement%d", n), fmt.Sprintf("review%d", n)
		work := testTask(implement)
		if n > 1 {
			prior := fmt.Sprintf("review%d", n-1)
			work.Needs = []string{prior, fmt.Sprintf("implement%d", n-1)}
			work.NeedsVerdict = map[string]string{prior: "changes-requested"}
		}
		check := testTask(review, implement)
		check.ReviewOutput = &domain.ReviewOutput{VerdictLine: "review.md"}
		work.FixLoop = &domain.FixLoopTask{Name: "repair", Round: n, MaxRounds: 4, Kind: "implement"}
		check.FixLoop = &domain.FixLoopTask{Name: "repair", Round: n, MaxRounds: 4, Kind: "review"}
		tasks = append(tasks, work, check)
	}
	return newTestDAG(t, tasks...)
}

func finishLoopReview(t *testing.T, e *DAGExecution, n int, verdict string, pass bool) {
	t.Helper()
	succeedAttempt(t, e, fmt.Sprintf("implement%d-1", n))
	id := fmt.Sprintf("review%d-1", n)
	mustStart(t, e, id)
	// Importer records H4 verdict before the coordinator rebuilds its projection.
	a := &e.state.Attempts[e.currentAttemptIndex("task-"+fmt.Sprintf("review%d", n))]
	if verdict != "" {
		a.ReviewVerdict = &domain.ReviewVerdict{Verdict: verdict}
	}
	mustComplete(t, e, id, CompletionResult{ExplicitSuccess: true, VerificationPassed: pass})
	// A recovered coordinator must make the same scheduling choice.
	recovered, err := NewDAGExecution(e.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	*e = *recovered
}

func TestFixLoopCoordinatorRounds(t *testing.T) {
	for _, accept := range []int{1, 3, 0} {
		t.Run(fmt.Sprint(accept), func(t *testing.T) {
			e := loopDAG(t)
			rounds := 4
			if accept > 0 {
				rounds = accept
			}
			for n := 1; n <= rounds; n++ {
				verdict := "changes-requested"
				if n == accept {
					verdict = "accept"
				}
				finishLoopReview(t, e, n, verdict, true)
			}
			for n := rounds + 1; n <= 4; n++ {
				assertAttemptProgress(t, e, fmt.Sprintf("implement%d-1", n), domain.ProgressSkipped)
				assertAttemptProgress(t, e, fmt.Sprintf("review%d-1", n), domain.ProgressSkipped)
			}
			want := domain.ProgressSucceeded
			if accept == 0 {
				want = domain.ProgressFailed
			}
			assertRunProgress(t, e, want)
			state := e.Snapshot()
			settled, err := domain.ProjectRunSink(state.Run, state.Tasks, state.Attempts, nil, dagTestTime)
			if err != nil || settled.Progress != want || settled.Sink.Progress != want {
				t.Fatalf("sink %+v %v", settled, err)
			}
			s := settled.Sink.Result.FixLoops
			if len(s) != 1 || s[0].Rounds != rounds || s[0].Exhausted != (accept == 0) {
				t.Fatalf("summary %+v", s)
			}
			if accept == 0 && s[0].Escalation != domain.FixLoopExhausted {
				t.Fatalf("missing typed escalation %+v", s)
			}
		})
	}
}

func TestFixLoopFailedReviewAndCancellation(t *testing.T) {
	e := loopDAG(t)
	finishLoopReview(t, e, 1, "", false)
	assertRunProgress(t, e, domain.ProgressFailed)
	if err := e.RetryTask("task-review1", "review1-2", dagTestTime); err == nil {
		t.Fatal("loop retry must require a fresh rerun subtree")
	}
	for n := 2; n <= 4; n++ {
		assertAttemptProgress(t, e, fmt.Sprintf("implement%d-1", n), domain.ProgressSkipped)
	}
	state := e.Snapshot()
	settled, err := domain.ProjectRunSink(state.Run, state.Tasks, state.Attempts, nil, dagTestTime)
	if err != nil || settled.Progress != domain.ProgressFailed || settled.Sink.Result.FixLoops[0].Exhausted {
		t.Fatalf("failed review %+v %v", settled, err)
	}
	e = loopDAG(t)
	finishLoopReview(t, e, 1, "changes-requested", true)
	mustStart(t, e, "implement2-1")
	if err := e.CancelTask("task-implement2", dagTestTime); err != nil {
		t.Fatal(err)
	}
	assertRunProgress(t, e, domain.ProgressCancelled)
	for n := 2; n <= 4; n++ {
		assertAttemptProgress(t, e, fmt.Sprintf("review%d-1", n), domain.ProgressCancelled)
	}
}

func TestVerdictEdgesSkipMismatchAndWaitForRecordedEvidence(t *testing.T) {
	producer := testTask("review")
	producer.ReviewOutput = &domain.ReviewOutput{VerdictLine: "review.md"}
	accept, repair := testTask("accept", "review"), testTask("repair", "review")
	accept.NeedsVerdict = map[string]string{"review": "accept"}
	repair.NeedsVerdict = map[string]string{"review": "changes-requested"}
	e := newTestDAG(t, producer, accept, repair)
	succeedAttempt(t, e, "review-1")
	assertAttemptProgress(t, e, "accept-1", domain.ProgressBlocked)
	a := &e.state.Attempts[e.currentAttemptIndex(producer.ID)]
	a.ReviewVerdict = &domain.ReviewVerdict{Verdict: "accept"}
	e.refresh(dagTestTime, true)
	assertAttemptProgress(t, e, "accept-1", domain.ProgressReady)
	assertAttemptProgress(t, e, "repair-1", domain.ProgressSkipped)
	succeedAttempt(t, e, "accept-1")
	assertRunProgress(t, e, domain.ProgressSucceeded)
}

func TestFixLoopRerunFromRoundTaskPreservesBound(t *testing.T) {
	e := loopDAG(t)
	finishLoopReview(t, e, 1, "changes-requested", true)
	finishLoopReview(t, e, 2, "changes-requested", true)
	finishLoopReview(t, e, 3, "", false)
	state := e.Snapshot()
	scope, err := domain.PlanRerun(state.Run, state.Tasks, state.Attempts, "review3")
	if err != nil {
		t.Fatal(err)
	}
	if len(scope.Rerun) != 3 || len(scope.Reuse) != 5 {
		t.Fatalf("scope %+v", scope)
	}
	for _, task := range scope.Rerun {
		if task.FixLoop == nil || task.FixLoop.MaxRounds != 4 || task.FixLoop.Round < 3 {
			t.Fatalf("bound lost %+v", task)
		}
	}
}
