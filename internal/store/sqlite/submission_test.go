package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSubmissionReservationSerializesReplayAndConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	now := time.Date(2026, 9, 10, 14, 0, 0, 0, time.UTC)
	proposed := submissionRecordFixture("request-1", submissionDigestFixture("one"), now)

	start := make(chan struct{})
	type outcome struct {
		record domain.SubmissionRecord
		replay bool
		err    error
	}
	results := make(chan outcome, 2)
	var group sync.WaitGroup
	for _, store := range []*Store{first, second} {
		group.Add(1)
		go func(store *Store) {
			defer group.Done()
			<-start
			record, replay, err := store.ReserveSubmission(context.Background(), proposed)
			results <- outcome{record: record, replay: replay, err: err}
		}(store)
	}
	close(start)
	group.Wait()
	close(results)
	var got []outcome
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		got = append(got, result)
	}
	if len(got) != 2 || got[0].replay == got[1].replay ||
		got[0].record.WorkflowID != got[1].record.WorkflowID {
		t.Fatalf("reservation outcomes = %#v", got)
	}

	changed := proposed
	changed.Digest = submissionDigestFixture("changed")
	if _, _, err := first.ReserveSubmission(context.Background(), changed); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("changed reservation error = %v", err)
	}
	accepted, replay, err := first.CompleteSubmission(context.Background(), proposed.Key, proposed.Digest, now.Add(time.Minute))
	if err != nil || replay || accepted.State != domain.SubmissionAccepted {
		t.Fatalf("completion = %#v, replay %v, err %v", accepted, replay, err)
	}
	replayed, replay, err := second.CompleteSubmission(context.Background(), proposed.Key, proposed.Digest, now.Add(2*time.Minute))
	if err != nil || !replay || replayed.AcceptedAt == nil || !replayed.AcceptedAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("completion replay = %#v, replay %v, err %v", replayed, replay, err)
	}
}

func TestMigrationFromVersionTenAddsSubmissionJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DROP INDEX coordinator_submissions_state_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DROP TABLE coordinator_submissions`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DELETE FROM schema_version WHERE version >= 11`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatalf("migrate version 10 database: %v", err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)
	proposed := submissionRecordFixture("migration-request", submissionDigestFixture("migration"), now)
	if _, replay, err := store.ReserveSubmission(context.Background(), proposed); err != nil || replay {
		t.Fatalf("reserve after migration: replay %v, err %v", replay, err)
	}
}

func submissionRecordFixture(key, digest string, now time.Time) domain.SubmissionRecord {
	return domain.SubmissionRecord{
		Key: key, Digest: digest, WorkflowID: "workflow-" + key,
		RunID: "run-" + key, State: domain.SubmissionPending, CreatedAt: now,
	}
}

func submissionDigestFixture(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func TestCompleteSubmissionPersistsOneNativeEventAcrossReplay(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	proposed := submissionRecordFixture("event-request", submissionDigestFixture("event"), now)
	if _, _, err := store.ReserveSubmission(context.Background(), proposed); err != nil {
		t.Fatal(err)
	}
	if _, replay, err := store.CompleteSubmission(context.Background(), proposed.Key, proposed.Digest, now.Add(time.Minute)); err != nil || replay {
		t.Fatalf("first completion: replay %v, err %v", replay, err)
	}
	if _, replay, err := store.CompleteSubmission(context.Background(), proposed.Key, proposed.Digest, now.Add(2*time.Minute)); err != nil || !replay {
		t.Fatalf("completion replay: replay %v, err %v", replay, err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.AuditEvents) != 1 {
		t.Fatalf("submission events = %#v", records.AuditEvents)
	}
	event := records.AuditEvents[0]
	if event.ID != "submission-accepted:"+proposed.Key ||
		event.Kind != "submission-accepted" ||
		event.WorkflowRunID != proposed.RunID ||
		event.TargetType != domain.AuditTargetSubmission ||
		event.TargetID != proposed.Key {
		t.Fatalf("submission event = %#v", event)
	}
}

func TestCompleteSubmissionRollsBackWhenNativeEventConflicts(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 9, 10, 17, 0, 0, 0, time.UTC)
	proposed := submissionRecordFixture("conflict-request", submissionDigestFixture("conflict"), now)
	if _, _, err := store.ReserveSubmission(context.Background(), proposed); err != nil {
		t.Fatal(err)
	}
	conflict := domain.AuditEvent{
		ID: "submission-accepted:" + proposed.Key, Kind: "conflicting-event", CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{AuditEvents: []domain.AuditEvent{conflict}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CompleteSubmission(context.Background(), proposed.Key, proposed.Digest, now.Add(time.Minute)); err == nil {
		t.Fatal("completion with conflicting event succeeded")
	}
	record, found, err := store.LoadSubmission(context.Background(), proposed.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.State != domain.SubmissionPending || record.AcceptedAt != nil {
		t.Fatalf("submission after rollback = %#v, found %v", record, found)
	}
}
