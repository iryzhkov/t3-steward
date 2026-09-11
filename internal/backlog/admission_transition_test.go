package backlog

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var admissionTransitionTime = time.Date(2026, time.September, 10, 21, 0, 0, 0, time.UTC)

type admissionTransitionStore struct {
	records     []domain.QuotaAdmissionRecord
	commits     [][]domain.QuotaAdmissionTransition
	commitError error
}

func (s *admissionTransitionStore) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return append([]domain.QuotaAdmissionRecord(nil), s.records...), nil
}

func (s *admissionTransitionStore) CommitQuotaAdmissionTransitions(_ context.Context, transitions []domain.QuotaAdmissionTransition) error {
	s.commits = append(s.commits, append([]domain.QuotaAdmissionTransition(nil), transitions...))
	if s.commitError != nil {
		return s.commitError
	}
	for _, transition := range transitions {
		replaced := false
		for index := range s.records {
			if s.records[index].QuotaPoolID == transition.Record.QuotaPoolID {
				s.records[index] = transition.Record
				replaced = true
			}
		}
		if !replaced {
			s.records = append(s.records, transition.Record)
		}
	}
	return nil
}

func TestReconcileQuotaAdmissionTransitionsPersistsBeforeWarnEligibility(t *testing.T) {
	bucket := admissionBucket("codex", domain.WindowPrimary)
	store := &admissionTransitionStore{records: []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "codex-shared", Revision: 4, Admission: domain.AdmissionOpen,
		BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		Reason:       "healthy",
	}}}
	deadline := admissionTransitionTime.Add(5 * time.Minute)
	derived := []QuotaPoolAdmissionSnapshot{{
		QuotaPoolID: "codex-shared", Admission: domain.AdmissionConstrained,
		ObservedAt: admissionTransitionTime, Reason: "warn threshold",
		BucketEpochs:      []QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		DirectiveDeadline: &deadline,
	}}

	directives, err := ReconcileQuotaAdmissionTransitions(context.Background(), store, derived, admissionTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.commits) != 1 || len(store.commits[0]) != 1 {
		t.Fatalf("commits = %#v, want one complete transition", store.commits)
	}
	transition := store.commits[0][0]
	if transition.ExpectedRevision != 4 || transition.Record.Revision != 5 ||
		transition.Record.Admission != domain.AdmissionConstrained {
		t.Fatalf("transition = %#v", transition)
	}
	if len(directives) != 1 || directives[0].Severity != domain.ThrottleWarn ||
		directives[0].AdmissionRevision != 5 || directives[0].QuotaPoolID != "codex-shared" {
		t.Fatalf("directives = %#v", directives)
	}
	if !reflect.DeepEqual(directives[0].BucketEpochs, []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}}) {
		t.Fatalf("directive epochs = %#v", directives[0].BucketEpochs)
	}
	if directives[0].Deadline == nil || !directives[0].Deadline.Equal(deadline) {
		t.Fatalf("directive deadline = %v, want %s", directives[0].Deadline, deadline)
	}
	if transition.Directive == nil || transition.Directive.ID != directives[0].ID {
		t.Fatalf("persisted directive = %#v, returned = %#v", transition.Directive, directives[0])
	}

	store.commitError = errors.New("disk unavailable")
	derived[0].Admission = domain.AdmissionDraining
	derived[0].Reason = "drain threshold"
	directives, err = ReconcileQuotaAdmissionTransitions(context.Background(), store, derived, admissionTransitionTime.Add(time.Minute))
	if err == nil || directives != nil {
		t.Fatalf("failed commit returned directives %#v, err %v", directives, err)
	}
}

func TestReconcileQuotaAdmissionTransitionsOrdersDrainThenStop(t *testing.T) {
	bucket := admissionBucket("codex", domain.WindowPrimary)
	store := &admissionTransitionStore{records: []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "pool", Revision: 1, Admission: domain.AdmissionConstrained,
		BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		Reason:       "warn",
	}}}
	deadline := admissionTransitionTime.Add(2 * time.Minute)
	draining := []QuotaPoolAdmissionSnapshot{{
		QuotaPoolID: "pool", Admission: domain.AdmissionDraining, Reason: "drain",
		BucketEpochs:      []QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		DirectiveDeadline: &deadline,
	}}
	got, err := ReconcileQuotaAdmissionTransitions(context.Background(), store, draining, admissionTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Severity != domain.ThrottleDrain || store.records[0].Admission != domain.AdmissionDraining {
		t.Fatalf("drain result = %#v, records = %#v", got, store.records)
	}

	stopped := draining
	stopped[0].Admission = domain.AdmissionClosed
	stopped[0].Reason = "stop"
	got, err = ReconcileQuotaAdmissionTransitions(context.Background(), store, stopped, admissionTransitionTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Severity != domain.ThrottleStop ||
		got[0].AdmissionRevision != 3 || store.records[0].Admission != domain.AdmissionClosed {
		t.Fatalf("stop result = %#v, records = %#v", got, store.records)
	}
}

func TestPlanQuotaAdmissionTransitionsRefreshesObservationAndKeepsExactReplayIdempotent(t *testing.T) {
	bucket := admissionBucket("codex", domain.WindowSecondary)
	previous := []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "pool", Revision: 7, Admission: domain.AdmissionConstrained,
		ObservedAt:   admissionTransitionTime.Add(-time.Minute),
		AppliedAt:    admissionTransitionTime.Add(-time.Minute),
		BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
		Reason:       "warn",
	}}
	derived := []QuotaPoolAdmissionSnapshot{{
		QuotaPoolID: "pool", Admission: domain.AdmissionConstrained,
		ObservedAt: admissionTransitionTime, Reason: "warn",
		BucketEpochs: []QuotaBucketEpoch{{Bucket: bucket, Epoch: "epoch-1"}},
	}}
	transitions, err := PlanQuotaAdmissionTransitions(previous, derived, admissionTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 || transitions[0].Directive != nil ||
		transitions[0].Record.Revision != 8 ||
		!transitions[0].Record.ObservedAt.Equal(admissionTransitionTime) {
		t.Fatalf("observation refresh transitions = %#v", transitions)
	}

	previous = []domain.QuotaAdmissionRecord{transitions[0].Record}
	transitions, err = PlanQuotaAdmissionTransitions(previous, derived, admissionTransitionTime.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 0 {
		t.Fatalf("exact replay transitions = %#v", transitions)
	}

	derived[0].BucketEpochs[0].Epoch = "epoch-2"
	transitions, err = PlanQuotaAdmissionTransitions(previous, derived, admissionTransitionTime.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 || transitions[0].Directive == nil ||
		transitions[0].Directive.Severity != domain.ThrottleWarn ||
		transitions[0].Record.Revision != 9 {
		t.Fatalf("new epoch transitions = %#v", transitions)
	}
}

func TestPlanQuotaAdmissionTransitionsMultiplePoolsAndInputOrder(t *testing.T) {
	a := admissionBucket("claudeAgent", "seven_day")
	z := admissionBucket("codex", domain.WindowPrimary)
	previous := []domain.QuotaAdmissionRecord{
		{QuotaPoolID: "z-pool", Revision: 2, Admission: domain.AdmissionOpen, BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: z, Epoch: "z1"}}},
		{QuotaPoolID: "a-pool", Revision: 5, Admission: domain.AdmissionOpen, BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: a, Epoch: "a1"}}},
	}
	derived := []QuotaPoolAdmissionSnapshot{
		{QuotaPoolID: "z-pool", Admission: domain.AdmissionClosed, Reason: "stop", BucketEpochs: []QuotaBucketEpoch{{Bucket: z, Epoch: "z1"}}},
		{QuotaPoolID: "a-pool", Admission: domain.AdmissionDraining, Reason: "drain", BucketEpochs: []QuotaBucketEpoch{{Bucket: a, Epoch: "a1"}}},
	}

	forward, err := PlanQuotaAdmissionTransitions(previous, derived, admissionTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	reverseRecords(previous)
	reverseSnapshots(derived)
	reversed, err := PlanQuotaAdmissionTransitions(previous, derived, admissionTransitionTime)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forward, reversed) {
		t.Fatalf("reordered result mismatch:\nforward %#v\nreverse %#v", forward, reversed)
	}
	if len(forward) != 2 || forward[0].Record.QuotaPoolID != "a-pool" ||
		forward[1].Record.QuotaPoolID != "z-pool" {
		t.Fatalf("transition order = %#v", forward)
	}
}

func reverseRecords(values []domain.QuotaAdmissionRecord) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseSnapshots(values []QuotaPoolAdmissionSnapshot) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
