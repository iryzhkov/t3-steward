package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAdmittedCoordinatorRecordsRefusalIsAtomic(t *testing.T) {
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rejected := errors.New("quota exhausted")
	records := coordinatorFixture()
	err = s.SaveAdmittedCoordinatorRecords(context.Background(), records, func(snapshot QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) { return nil, rejected })
	if !errors.Is(err, rejected) {
		t.Fatalf("got %v, want rejection", err)
	}
	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Workflows)+len(got.WorkflowRuns)+len(got.Tasks)+len(got.Attempts)+len(got.Artifacts) != 0 {
		t.Fatalf("refused records survived: %#v", got)
	}
}

func TestAdmittedCoordinatorRecordsConcurrentGateSeesCommittedDemand(t *testing.T) {
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rejected := errors.New("already admitted")
	gate := func(snapshot QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
		if len(snapshot.Records.Attempts) != 0 {
			return nil, rejected
		}
		return &domain.QuotaAdmissionReceipt{AdmittedAt: time.Now().UTC()}, nil
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			records := CoordinatorRecords{Workflows: []domain.Workflow{{ID: "workflow-" + id}},
				WorkflowRuns: []domain.WorkflowRun{{ID: "run-" + id, WorkflowID: "workflow-" + id}},
				Tasks:        []domain.Task{{ID: "task-" + id, WorkflowID: "workflow-" + id}},
				Attempts:     []domain.Attempt{{ID: "attempt-" + id, WorkflowRunID: "run-" + id, TaskID: "task-" + id, Progress: domain.ProgressReady}}}
			results <- s.SaveAdmittedCoordinatorRecords(context.Background(), records, gate)
		}(id)
	}
	wg.Wait()
	close(results)
	admitted, refused := 0, 0
	for err := range results {
		if err == nil {
			admitted++
		} else if errors.Is(err, rejected) {
			refused++
		} else {
			t.Fatal(err)
		}
	}
	if admitted != 1 || refused != 1 {
		t.Fatalf("admitted %d refused %d", admitted, refused)
	}
	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.WorkflowRuns) != 1 || got.WorkflowRuns[0].QuotaAdmission == nil {
		t.Fatalf("receipt missing: %#v", got.WorkflowRuns)
	}
}

func TestAdmittedCoordinatorRecordsInsertionFailureRollsBackReceipt(t *testing.T) {
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	records := coordinatorFixture()
	records.Triggers = append(records.Triggers, domain.Trigger{ID: "duplicate", ScheduleID: "schedule-1", ScheduleVersion: 4, OccurrenceKey: records.Triggers[0].OccurrenceKey})
	err = s.SaveAdmittedCoordinatorRecords(context.Background(), records, func(QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
		return &domain.QuotaAdmissionReceipt{AdmittedAt: time.Now().UTC()}, nil
	})
	if err == nil {
		t.Fatal("duplicate trigger unexpectedly committed")
	}
	if records.WorkflowRuns[0].QuotaAdmission != nil {
		t.Fatal("failed save mutated caller receipt")
	}
	got, err := s.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.WorkflowRuns)+len(got.Attempts)+len(got.Artifacts) != 0 {
		t.Fatal("partial admitted records survived insertion failure")
	}
}

func TestQuotaAdmissionRequiresGateAndReceipt(t *testing.T) {
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.CheckQuotaAdmission(context.Background(), nil); err == nil {
		t.Fatal("nil precheck gate accepted")
	}
	if err := s.SaveAdmittedCoordinatorRecords(context.Background(), coordinatorFixture(), nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	if err := s.SaveAdmittedCoordinatorRecords(context.Background(), coordinatorFixture(), func(QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) { return nil, nil }); err == nil {
		t.Fatal("missing receipt accepted")
	}
}

func TestQuotaAdmissionCheckLoadsPendingRecordsAndFreshness(t *testing.T) {
	s, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetQuotaStaleAfter(7 * time.Minute)
	records := coordinatorFixture()
	if err := s.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	receipt := &domain.QuotaAdmissionReceipt{AdmittedAt: time.Now().UTC()}
	got, err := s.CheckQuotaAdmission(context.Background(), func(snapshot QuotaAdmissionSnapshot) (*domain.QuotaAdmissionReceipt, error) {
		if snapshot.StaleAfter != 7*time.Minute {
			t.Fatalf("stale after %v", snapshot.StaleAfter)
		}
		if len(snapshot.Records.Attempts) != len(records.Attempts) || len(snapshot.Records.Tasks) != len(records.Tasks) || len(snapshot.Records.Assignments) != len(records.Assignments) {
			t.Fatalf("incomplete demand snapshot: %#v", snapshot.Records)
		}
		return receipt, nil
	})
	if err != nil || got != receipt {
		t.Fatalf("check got %#v %v", got, err)
	}
}
