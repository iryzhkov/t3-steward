package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"testing"
	"time"
)

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
