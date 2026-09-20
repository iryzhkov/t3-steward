package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// A settled task-bound check holds nothing: its outcome is the coordinator's
// record, which resumes the attempt and delivers the wake, and the local row is
// never woken.
//
// Treating it as custody pinned every thread that ever parked on a task-bound
// wait forever, so the UI archive never hid those threads and cold storage
// never bundled them -- which is what filled the T3 session list with finished
// steward runs. A check that is still waiting, and an interactive outcome whose
// wake this host still owes, are custody and stay so.
func TestSettledTaskBoundCheckReleasesItsThread(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	settled := now
	for _, w := range []wait.Wait{
		{ID: "w-parked", ThreadID: "parked", TaskWaitID: "tw-1", Status: wait.StatusWaiting, CreatedAt: now, Wake: wait.WakeEach},
		{ID: "w-task-met", ThreadID: "task-done", TaskWaitID: "tw-2", Status: wait.StatusMet, SettledAt: &settled, CreatedAt: now, Wake: wait.WakeEach},
		{ID: "w-interactive", ThreadID: "undelivered", Status: wait.StatusMet, SettledAt: &settled, CreatedAt: now, Wake: wait.WakeEach},
		{ID: "w-woken", ThreadID: "woken", Status: wait.StatusWoken, SettledAt: &settled, CreatedAt: now, Wake: wait.WakeEach},
	} {
		if err := s.SaveWait(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	busy, err := s.BusyThreads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if busy["parked"] == "" {
		t.Fatal("a task parked on a live check was released")
	}
	if busy["undelivered"] == "" {
		t.Fatal("an interactive outcome with no wake delivered was released")
	}
	if reason, held := busy["task-done"]; held {
		t.Fatalf("a settled task-bound check still holds its thread: %q", reason)
	}
	if reason, held := busy["woken"]; held {
		t.Fatalf("a delivered wait still holds its thread: %q", reason)
	}
}

func TestSessionArchiveRetainsExpiredAssignmentCustody(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	records := coordinatorFixture()
	if len(records.Assignments) == 0 || len(records.Attempts) == 0 {
		t.Fatal("fixture lacks assignments")
	}
	records.Assignments[0].ThreadID = "archive-thread"
	records.Assignments[0].State = domain.AssignmentClaimed
	records.Assignments[0].LeaseExpiresAt = time.Now().Add(-24 * time.Hour)
	records.Attempts[0].ThreadID = "archive-thread"
	records.Attempts[0].Progress = domain.ProgressSucceeded
	records.Attempts[0].Control = domain.ControlStopped
	if err := s.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	states, err := s.SessionArchiveStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !states["archive-thread"].Background || states["archive-thread"].Busy == "" {
		t.Fatal("expired lease released archive guard")
	}
	records.Assignments[0].State = domain.AssignmentCompleted
	if err := s.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	states, err = s.SessionArchiveStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !states["archive-thread"].Background || states["archive-thread"].Busy != "" {
		t.Fatalf("completed custody: %+v", states)
	}
}
