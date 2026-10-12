package sqlite

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerExchangeReadsFilterBeforeDecodeAndStayFresh(t *testing.T) {
	s, reads := openAttentionStopCountingStore(t)
	ctx, scope := WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	var seed CoordinatorRecords
	for i, state := range []domain.AssignmentState{domain.AssignmentOffered, domain.AssignmentClaimed, domain.AssignmentUnknown, domain.AssignmentReleased, domain.AssignmentCompleted} {
		id := fmt.Sprint(i)
		seed.Assignments = append(seed.Assignments, domain.Assignment{ID: id, AttemptID: id, DispatchToken: id, WorkerID: "target", Epoch: int64(i), State: state})
		seed.Attempts = append(seed.Attempts, domain.Attempt{ID: id, TaskID: id, WorkflowRunID: id, Revision: int64(i)})
	}
	seed.Assignments = append(seed.Assignments, domain.Assignment{ID: "missing", AttemptID: "missing", DispatchToken: "missing", WorkerID: "target"})
	requireAttentionStopOK(t, s.SaveCoordinatorRecords(ctx, seed))
	normalized := func(q string) string { return strings.Join(strings.Fields(q), " ") }
	var small int
	for _, history := range []int{1, 60} {
		var extra CoordinatorRecords
		for i := 0; i < history; i++ {
			id := fmt.Sprintf("other-%03d", i)
			extra.Assignments = append(extra.Assignments, domain.Assignment{ID: id, AttemptID: id, DispatchToken: id, WorkerID: "other"})
			extra.Attempts = append(extra.Attempts, domain.Attempt{ID: id, TaskID: id, WorkflowRunID: id, Progress: domain.ProgressSucceeded})
		}
		requireAttentionStopOK(t, s.SaveCoordinatorRecords(ctx, extra))
		reads.reset()
		got, err := s.LoadWorkerExchangeRecords(ctx, "target", false)
		requireAttentionStopOK(t, err)
		decoded := reads.rows[normalized(workerAssignmentsQuery)] + reads.rows[normalized(workerAttemptsQuery)]
		if history == 1 {
			small = decoded
		} else if decoded != small {
			t.Fatal("unrelated history increased decoding")
		}
		reads.reset()
		baseline, err := s.LoadCoordinatorRecords(context.Background())
		requireAttentionStopOK(t, err)
		full := reads.rows["SELECT record FROM coordinator_assignments ORDER BY id"] + reads.rows["SELECT record FROM coordinator_attempts ORDER BY id"]
		other, err := s.LoadWorkerExchangeRecords(ctx, "other", false)
		requireAttentionStopOK(t, err)
		if decoded >= full || !reflect.DeepEqual(got.Assignments, baseline.Assignments[:len(seed.Assignments)]) || !reflect.DeepEqual(got.Attempts, baseline.Attempts[:len(seed.Attempts)]) || len(other.Assignments) != history {
			t.Fatal("worker-filtered baseline or scoped decoding differs")
		}
	}
	for _, q := range []string{workerAssignmentsQuery, workerAttemptsQuery, workerRunsQuery} {
		rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, "target")
		requireAttentionStopOK(t, err)
		for rows.Next() {
			var id, parent, unused int
			var detail string
			requireAttentionStopOK(t, rows.Scan(&id, &parent, &unused, &detail))
			t.Log(detail)
		}
		requireAttentionStopOK(t, rows.Err())
		requireAttentionStopOK(t, rows.Close())
	}
	second, err := Open(s.path)
	requireAttentionStopOK(t, err)
	defer second.Close()
	seed.Assignments[0].Epoch++
	seed.Attempts[0].Revision++
	requireAttentionStopOK(t, second.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: seed.Assignments[:1], Attempts: seed.Attempts[:1]}))
	got, err := s.LoadWorkerExchangeRecords(ctx, "target", false)
	requireAttentionStopOK(t, err)
	if got.Assignments[0].Epoch != seed.Assignments[0].Epoch || got.Attempts[0].Revision != seed.Attempts[0].Revision {
		t.Fatal("external mutation invisible in tick")
	}
	got.Assignments[0].WorkerID = "mutated"
	again, err := s.LoadWorkerExchangeRecords(ctx, "target", false)
	requireAttentionStopOK(t, err)
	if again.Assignments[0].WorkerID != "target" {
		t.Fatal("caller mutated another result")
	}
	if _, err := s.LoadWorkerExchangeRecords(ctx, "", false); err == nil {
		t.Fatal("empty identity accepted")
	}
	if _, err := s.ParkedTaskWaitAttemptsFor(ctx, ""); err == nil {
		t.Fatal("empty park identity accepted")
	}
}

func TestWorkerParkFreshWaitCustody(t *testing.T) {
	s, attempt, now := taskWaitFixture(t)
	ctx, scope := WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	wait, err := s.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "request", domain.WakeEach), now)
	requireAttentionStopOK(t, err)
	before, err := s.ParkedTaskWaitAttemptsFor(ctx, "worker")
	requireAttentionStopOK(t, err)
	second, err := Open(s.path)
	requireAttentionStopOK(t, err)
	defer second.Close()
	_, err = second.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now)
	requireAttentionStopOK(t, err)
	_, err = second.WakeTaskWaits(ctx, now)
	requireAttentionStopOK(t, err)
	after, err := s.ParkedTaskWaitAttemptsFor(ctx, "worker")
	requireAttentionStopOK(t, err)
	if before[attempt.ID] != wait.ID || after[attempt.ID] != wait.ID {
		t.Fatal("settlement lost custody")
	}
	global, err := s.ParkedTaskWaitAttempts(ctx)
	requireAttentionStopOK(t, err)
	if !reflect.DeepEqual(after, global) {
		t.Fatal("scoped park differs")
	}
	other, err := s.ParkedTaskWaitAttemptsFor(ctx, "other")
	requireAttentionStopOK(t, err)
	if len(other) != 0 {
		t.Fatal("other worker acquired custody")
	}
}
