package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestJournalArchiveRefusesPendingSettlement(t *testing.T) {
	pkg := testPackage()
	journal, err := OpenJournal(t.TempDir(), pkg.WorkerID, pkg.WorkerEpoch, pkg.CoordinatorEpoch)
	if err != nil {
		t.Fatal(err)
	}
	record := AttemptRecord{Assignment: domain.Assignment{ID: pkg.Identity.AssignmentID}, Phase: PhaseCompleted, SettlePending: true}
	record.Package.Package = pkg
	if err := journal.update(func(s *journalState) error { s.Attempts[record.Assignment.ID] = record; return nil }); err != nil {
		t.Fatal(err)
	}
	states, err := JournalArchiveStates(journal.root)
	if err != nil {
		t.Fatal(err)
	}
	if !states[pkg.Identity.ThreadID].Background || states[pkg.Identity.ThreadID].Busy == "" {
		t.Fatal("pending settlement was archived")
	}
	if err := journal.update(func(s *journalState) error {
		r := s.Attempts[record.Assignment.ID]
		r.SettlePending = false
		s.Attempts[record.Assignment.ID] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	states, err = JournalArchiveStates(journal.root)
	if err != nil {
		t.Fatal(err)
	}
	if states[pkg.Identity.ThreadID].Busy != "" {
		t.Fatal("settled completed record stayed busy")
	}
}
