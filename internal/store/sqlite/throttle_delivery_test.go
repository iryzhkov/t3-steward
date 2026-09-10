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

func TestCommitThrottleAttemptTransitionsAtomicReplayAndStaleRevision(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := throttleAttemptStoreTransition("directive", "attempt-a", 0, domain.ThrottleDeliveryPending)
	z := throttleAttemptStoreTransition("directive", "attempt-z", 0, domain.ThrottleDeliveryPending)
	initial := []domain.ThrottleAttemptTransition{z, a}
	if err := store.CommitThrottleAttemptTransitions(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitThrottleAttemptTransitions(context.Background(), initial); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	records, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].AttemptID != "attempt-a" || records[1].AttemptID != "attempt-z" {
		t.Fatalf("records = %#v", records)
	}

	updateA := throttleAttemptStoreTransition("directive", "attempt-a", 1, domain.ThrottleDeliveryAcknowledged)
	staleZ := throttleAttemptStoreTransition("directive", "attempt-z", 0, domain.ThrottleDeliveryRejected)
	err = store.CommitThrottleAttemptTransitions(context.Background(), []domain.ThrottleAttemptTransition{updateA, staleZ})
	if !errors.Is(err, ErrStaleThrottleAttemptRevision) {
		t.Fatalf("stale error = %v", err)
	}
	after, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, records) {
		t.Fatalf("stale batch persisted partially:\nafter %#v\nbefore %#v", after, records)
	}

	if err := store.CommitThrottleAttemptTransitions(context.Background(), []domain.ThrottleAttemptTransition{updateA}); err != nil {
		t.Fatal(err)
	}
	updated, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if updated[0].Delivery != domain.ThrottleDeliveryAcknowledged || updated[0].Revision != 2 {
		t.Fatalf("updated records = %#v", updated)
	}
}

func throttleAttemptStoreTransition(
	directiveID string,
	attemptID string,
	expectedRevision int64,
	delivery domain.ThrottleDeliveryState,
) domain.ThrottleAttemptTransition {
	now := time.Date(2026, time.September, 10, 23, 30, 0, 0, time.UTC)
	command := domain.ThrottleCommand{
		ID:              "command-" + attemptID,
		DirectiveID:     directiveID,
		AttemptID:       attemptID,
		AssignmentID:    "assignment-" + attemptID,
		AssignmentEpoch: 2,
		WorkerID:        "worker",
		ThreadID:        "thread-" + attemptID,
		WorkspacePath:   "/runs/" + attemptID,
		Route: domain.ProviderRoute{
			WorkerID: "worker", ProviderInstanceID: "codex",
			Model: "gpt-5.6-sol", QuotaPoolID: "pool",
		},
		Kind:        domain.ThrottleCommandDrain,
		QuotaPoolID: "pool",
		BucketEpochs: []domain.QuotaBucketEpoch{{
			Bucket: domain.BucketKey{
				ProviderInstanceID: "codex", LimitID: "tokens", Window: domain.WindowPrimary,
			},
			Epoch: "epoch",
		}},
		CreatedAt: now,
	}
	record := domain.ThrottleAttemptRecord{
		DirectiveID: directiveID,
		AttemptID:   attemptID,
		Revision:    expectedRevision + 1,
		Command:     command,
		Delivery:    delivery,
		Control:     domain.ControlDraining,
		UpdatedAt:   now,
	}
	if delivery != domain.ThrottleDeliveryPending {
		record.AcknowledgedAt = &now
	}
	return domain.ThrottleAttemptTransition{
		ExpectedRevision: expectedRevision,
		Record:           record,
	}
}
