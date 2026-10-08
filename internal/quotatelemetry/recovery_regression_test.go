package quotatelemetry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFinalReviewCompletedBeforeImport(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			path := testStorePath(t)
			source := newFakeSource()
			clock := &fakeClock{now: testBase}
			r := newTestRecorder(t, path, source, clock)
			mustTick(t, r)
			task, attempt, assignment := testWork("late-report", 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
			start := testBase.Add(time.Second)
			attempt.StartedAt = &start
			assignment.State = domain.AssignmentClaimed
			source.addWork(task, attempt, assignment)
			source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, testBase)
			source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, start)
			clock.Advance(30 * time.Second)
			mustTick(t, r)
			assignment.State = domain.AssignmentCompleted
			assignment.UpdatedAt = clock.Now()
			attempt.Progress = domain.ProgressVerifying
			attempt.UpdatedAt = clock.Now()
			source.setAssignment(assignment)
			source.setAttempt(attempt)
			clock.Advance(30 * time.Second)
			mustTick(t, r)
			if restart {
				r.Close()
				r = newTestRecorder(t, path, source, clock)
			}
			source.artifacts = []domain.Artifact{{ID: "verify-late", TaskID: task.ID, AttemptID: attempt.ID, Kind: domain.ArtifactVerification, Name: "verification/001.json"}}
			source.content["verify-late"] = []byte(`{"command":"go test ./...","exitCode":0,"startedAt":"2026-10-07T10:00:05Z","completedAt":"2026-10-07T10:00:10Z"}`)
			completed := clock.Now()
			attempt.Progress = domain.ProgressSucceeded
			attempt.CompletedAt = &completed
			attempt.UpdatedAt = completed
			source.setAttempt(attempt)
			clock.Advance(30 * time.Second)
			mustTick(t, r)
			mustTick(t, r)
			events := allEvents(t, path, clock.Now())
			finishes, checks := eventsOfKind(events, KindFinish), eventsOfKind(events, KindCheck)
			if len(checks) != 1 || len(finishes) != 1 {
				t.Fatalf("checks=%d finishes=%d; expected one retained report and terminal finish", len(checks), len(finishes))
			}
			work := finishes[0].Work
			if work.Outcome != "succeeded" || !finishes[0].At.Equal(completed) || work.FinishedAt == nil || !work.FinishedAt.Equal(completed) || work.DurationMs == nil || *work.DurationMs != completed.Sub(start).Milliseconds() {
				t.Fatalf("finish=%+v at %s; want final succeeded outcome, completion and duration", work, finishes[0].At)
			}
			if checks[0].Work.FinishedAt == nil || !checks[0].Work.FinishedAt.Equal(completed) || checks[0].Check.Stage != StageVerification || checks[0].Check.ExitCode != 0 || checks[0].Check.DurationMs == nil || *checks[0].Check.DurationMs != 5000 {
				t.Fatalf("late check=%+v", checks[0])
			}
		})
	}
}

func TestReviewFinishPointLoadsStayWithinDeclaredBatch(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	r := newTestRecorder(t, path, source, clock)
	mustTick(t, r)
	end := testBase.Add(12 * time.Second)
	const total = 2*openBatch + 1
	for i := 0; i < total; i++ {
		task, attempt, assignment := testWork(fmt.Sprintf("batch-%04d", i), 1, "gate", opusRoute, domain.ExecutionRoleExecutor)
		attempt.Progress = domain.ProgressSucceeded
		attempt.CompletedAt = &end
		source.addWork(task, attempt, assignment)
		source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, testBase.Add(time.Second))
		source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, testBase.Add(2*time.Second))
	}
	previous := 0
	for batch := 0; batch < 4; batch++ {
		clock.Advance(30 * time.Second)
		mustTick(t, r)
		events := allEvents(t, path, clock.Now())
		finishes := eventsOfKind(events, KindFinish)
		if len(finishes)-previous > openBatch {
			t.Fatalf("one tick point-loaded and finished %d assignments; contract cap is %d", len(finishes)-previous, openBatch)
		}
		for _, finish := range finishes {
			if finish.Work.DurationMs == nil || *finish.Work.DurationMs != 10000 {
				t.Fatalf("same-tick/deferred start lost: %+v", finish.Work)
			}
		}
		previous = len(finishes)
		if batch == 0 {
			if len(eventsOfKind(events, KindDispatch)) != total || len(eventsOfKind(events, KindStart)) != total {
				t.Fatal("deferred lifecycle work was not durably recorded")
			}
			r.Close()
			r = newTestRecorder(t, path, source, clock)
		}
	}
	if previous != total {
		t.Fatalf("finished=%d; want eventual completion of %d across restart", previous, total)
	}
}

// Count source point reads only during the finish pass, separately from audit
// lifecycle reads. Both durable nonterminal work and staged work consume its
// aggregate budget even when no finish can yet be emitted.
type finishCountingSource struct {
	Source
	assignments, attempts int
}

func (s *finishCountingSource) LoadAssignment(ctx context.Context, id string) (domain.Assignment, bool, error) {
	s.assignments++
	return s.Source.LoadAssignment(ctx, id)
}
func (s *finishCountingSource) LoadAttempt(ctx context.Context, id string) (domain.Attempt, bool, error) {
	s.attempts++
	return s.Source.LoadAttempt(ctx, id)
}

func TestCombinedFinishPointReadBudget(t *testing.T) {
	path := testStorePath(t)
	source := newFakeSource()
	clock := &fakeClock{now: testBase}
	r := newTestRecorder(t, path, source, clock)
	mustTick(t, r)
	for i := 0; i < 2*openBatch+1; i++ {
		task, attempt, assignment := testWork(fmt.Sprintf("mixed-%04d", i), 1, "implement", opusRoute, domain.ExecutionRoleExecutor)
		source.addWork(task, attempt, assignment)
		source.appendAudit("assignment-offered", assignment.ID, attempt.ID, task.ID, 1, testBase.Add(time.Second))
		source.appendAudit("assignment-claimed", assignment.ID, attempt.ID, task.ID, 1, testBase.Add(2*time.Second))
		if i == openBatch/2-1 {
			clock.Advance(30 * time.Second)
			mustTick(t, r)
		}
	}
	meta := readMeta(t, path)
	result := tickResult{meta: map[string]string{}}
	if err := r.collectLifecycle(context.Background(), clock.Now(), meta, &result); err != nil {
		t.Fatal(err)
	}
	counter := &finishCountingSource{Source: source}
	r.source = counter
	if err := r.collectFinishes(context.Background(), clock.Now(), meta, &result); err != nil {
		t.Fatal(err)
	}
	if counter.assignments > openBatch || counter.attempts > openBatch {
		t.Fatalf("combined finish pass: assignment point reads=%d attempt point reads=%d; cap=%d each", counter.assignments, counter.attempts, openBatch)
	}
	if counter.assignments != openBatch || counter.attempts != openBatch {
		t.Fatalf("finish pass did not use available batch: assignments=%d attempts=%d", counter.assignments, counter.attempts)
	}
}
