package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var throttleStoreTime = time.Date(2026, time.September, 10, 22, 0, 0, 0, time.UTC)

func TestCommitQuotaAdmissionTransitionsAtomicReplayAndStaleRevision(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := throttleStoreTransition("a-pool", 0, domain.AdmissionDraining, domain.ThrottleDrain)
	z := throttleStoreTransition("z-pool", 0, domain.AdmissionClosed, domain.ThrottleStop)
	batch := []domain.QuotaAdmissionTransition{z, a}
	if err := store.CommitQuotaAdmissionTransitions(context.Background(), batch); err != nil {
		t.Fatalf("commit initial transitions: %v", err)
	}
	if err := store.CommitQuotaAdmissionTransitions(context.Background(), batch); err != nil {
		t.Fatalf("replay exact transitions: %v", err)
	}

	records, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].QuotaPoolID != "a-pool" || records[1].QuotaPoolID != "z-pool" {
		t.Fatalf("records = %#v", records)
	}
	directives, err := store.LoadThrottleDirectives(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(directives) != 2 || directives[0].QuotaPoolID != "a-pool" || directives[1].QuotaPoolID != "z-pool" {
		t.Fatalf("directives = %#v", directives)
	}
	coordinatorRecords, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(coordinatorRecords.AuditEvents) != 2 ||
		coordinatorRecords.AuditEvents[0].ID != "quota-admission:a-pool:1" ||
		coordinatorRecords.AuditEvents[1].ID != "quota-admission:z-pool:1" ||
		coordinatorRecords.AuditEvents[0].TargetType != domain.AuditTargetQuotaPool {
		t.Fatalf("quota admission events = %#v", coordinatorRecords.AuditEvents)
	}

	updateA := throttleStoreTransition("a-pool", 1, domain.AdmissionClosed, domain.ThrottleStop)
	staleZ := throttleStoreTransition("z-pool", 0, domain.AdmissionDraining, domain.ThrottleDrain)
	err = store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{updateA, staleZ})
	if !errors.Is(err, ErrStaleQuotaAdmissionRevision) {
		t.Fatalf("stale batch error = %v", err)
	}

	after, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, records) {
		t.Fatalf("stale batch was not atomic:\nafter %#v\nbefore %#v", after, records)
	}
	directivesAfter, err := store.LoadThrottleDirectives(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(directivesAfter, directives) {
		t.Fatalf("stale batch persisted directives:\nafter %#v\nbefore %#v", directivesAfter, directives)
	}
	coordinatorRecordsAfter, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(coordinatorRecordsAfter.AuditEvents, coordinatorRecords.AuditEvents) {
		t.Fatalf("stale batch persisted events:\nafter %#v\nbefore %#v", coordinatorRecordsAfter.AuditEvents, coordinatorRecords.AuditEvents)
	}
}

func TestCommitQuotaAdmissionTransitionsRejectsMismatchedDirective(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	transition := throttleStoreTransition("pool", 0, domain.AdmissionDraining, domain.ThrottleDrain)
	transition.Directive.AdmissionRevision++
	if err := store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{transition}); err == nil {
		t.Fatal("mismatched directive binding committed")
	}
	records, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("invalid transition persisted records: %#v", records)
	}
}

func throttleStoreTransition(
	poolID string,
	expectedRevision int64,
	admission domain.AdmissionState,
	severity domain.ThrottleSeverity,
) domain.QuotaAdmissionTransition {
	bucket := domain.BucketKey{
		ProviderInstanceID: "codex",
		LimitID:            "tokens",
		Window:             domain.WindowPrimary,
	}
	record := domain.QuotaAdmissionRecord{
		QuotaPoolID:  poolID,
		Revision:     expectedRevision + 1,
		Admission:    admission,
		ObservedAt:   throttleStoreTime,
		AppliedAt:    throttleStoreTime,
		BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		Reason:       string(severity),
	}
	directive := domain.ThrottleDirective{
		ID:                "directive-" + poolID + "-" + string(severity),
		QuotaPoolID:       poolID,
		AdmissionRevision: record.Revision,
		Severity:          severity,
		BucketEpochs:      append([]domain.QuotaBucketEpoch(nil), record.BucketEpochs...),
		Reason:            record.Reason,
		CreatedAt:         throttleStoreTime,
	}
	return domain.QuotaAdmissionTransition{
		ExpectedRevision: expectedRevision,
		Record:           record,
		Directive:        &directive,
	}
}

func TestCommitQuotaAdmissionTransitionRollsBackWhenNativeEventConflicts(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	transition := throttleStoreTransition("conflict-pool", 0, domain.AdmissionClosed, domain.ThrottleStop)
	conflict := domain.AuditEvent{
		ID: "quota-admission:conflict-pool:1", Kind: "conflicting-event", CreatedAt: throttleStoreTime,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{AuditEvents: []domain.AuditEvent{conflict}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{transition}); err == nil {
		t.Fatal("quota transition with conflicting event succeeded")
	}
	records, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	directives, err := store.LoadThrottleDirectives(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 || len(directives) != 0 {
		t.Fatalf("conflicting transition persisted records %#v, directives %#v", records, directives)
	}
}
