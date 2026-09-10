package backlog

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var admissionDerivationTime = time.Date(2026, time.September, 10, 20, 0, 0, 0, time.UTC)

func TestDeriveQuotaPoolAdmissionsAllStates(t *testing.T) {
	keyA := admissionBucket("codex", "weekly")
	keyB := admissionBucket("codex", "five-hour")
	tests := []struct {
		name         string
		states       []domain.BucketState
		reservations []QuotaResumeReservation
		want         domain.AdmissionState
		reason       string
	}{
		{
			name:   "open",
			states: []domain.BucketState{admissionBucketState(keyA, domain.PhaseNormal, true)},
			want:   domain.AdmissionOpen, reason: "all quota buckets are healthy",
		},
		{
			name: "constrained by warning",
			states: []domain.BucketState{
				admissionBucketState(keyA, domain.PhaseNormal, true),
				admissionBucketState(keyB, domain.PhaseWarned, false),
			},
			want: domain.AdmissionConstrained, reason: "at least one quota bucket is warned",
		},
		{
			name: "draining takes precedence",
			states: []domain.BucketState{
				admissionBucketState(keyA, domain.PhaseWarned, false),
				admissionBucketState(keyB, domain.PhaseDraining, false),
			},
			want: domain.AdmissionDraining, reason: "at least one quota bucket is draining",
		},
		{
			name: "closed takes precedence",
			states: []domain.BucketState{
				admissionBucketState(keyA, domain.PhaseStopped, false),
				admissionBucketState(keyB, domain.PhaseDraining, false),
			},
			want: domain.AdmissionClosed, reason: "at least one quota bucket is stopped",
		},
		{
			name:   "recovering while paused work remains",
			states: []domain.BucketState{admissionBucketState(keyA, domain.PhaseNormal, true)},
			reservations: []QuotaResumeReservation{{
				AttemptID: "attempt-1", QuotaPoolID: "pool-a", Class: domain.TaskClassRequired,
				Status: domain.ResumeEligible, RemainingCost: 12, StopEpoch: "old-epoch",
			}},
			want: domain.AdmissionRecovering, reason: "1 paused attempt reservation(s) await recovery",
		},
		{
			name:   "normal but unhealthy remains constrained",
			states: []domain.BucketState{admissionBucketState(keyA, domain.PhaseNormal, false)},
			want:   domain.AdmissionConstrained, reason: "at least one quota bucket is not healthy for resumption",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys := []domain.BucketKey{keyA}
			if len(test.states) == 2 {
				keys = append(keys, keyB)
			}
			got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
				Now: admissionDerivationTime, MaxObservationAge: 10 * time.Minute,
				Pools:        []domain.QuotaPool{{ID: "pool-a", Buckets: keys}},
				BucketStates: test.states, ResumeReservations: test.reservations,
			})
			if err != nil {
				t.Fatalf("DeriveQuotaPoolAdmissions: %v", err)
			}
			if len(got) != 1 || got[0].Admission != test.want || got[0].Reason != test.reason {
				t.Fatalf("admission = %+v, want state %q reason %q", got, test.want, test.reason)
			}
		})
	}
}

func TestDeriveQuotaPoolAdmissionsSelectsEarliestDirectiveDeadline(t *testing.T) {
	keyA := admissionBucket("codex", "weekly")
	keyB := admissionBucket("codex", "five-hour")
	later := admissionDerivationTime.Add(5 * time.Minute)
	earlier := admissionDerivationTime.Add(2 * time.Minute)
	stateA := admissionBucketState(keyA, domain.PhaseDraining, false)
	stateA.DrainDeadline = &later
	stateB := admissionBucketState(keyB, domain.PhaseDraining, false)
	stateB.DrainDeadline = &earlier

	got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: 10 * time.Minute,
		Pools:        []domain.QuotaPool{{ID: "pool-a", Buckets: []domain.BucketKey{keyA, keyB}}},
		BucketStates: []domain.BucketState{stateA, stateB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DirectiveDeadline == nil || !got[0].DirectiveDeadline.Equal(earlier) {
		t.Fatalf("directive deadline = %#v, want %s", got, earlier)
	}
	earlier = earlier.Add(time.Hour)
	if got[0].DirectiveDeadline.Equal(earlier) {
		t.Fatal("derived deadline aliases caller-owned time")
	}
}

func TestDeriveQuotaPoolAdmissionsReservations(t *testing.T) {
	key := admissionBucket("codex", "weekly")
	reservations := []QuotaResumeReservation{
		{AttemptID: "required-pending", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 7},
		{AttemptID: "required-resuming", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: domain.ResumeResuming, RemainingCost: 5},
		{AttemptID: "surplus-eligible", QuotaPoolID: "pool", Class: domain.TaskClassSurplus, Status: domain.ResumeEligible, RemainingCost: 20},
		{AttemptID: "required-resumed", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: domain.ResumeResumed, RemainingCost: 100},
		{AttemptID: "required-cancelled", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: domain.ResumeCancelled, RemainingCost: 100},
	}
	got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools:              []domain.QuotaPool{{ID: "pool", Buckets: []domain.BucketKey{key}}},
		BucketStates:       []domain.BucketState{admissionBucketState(key, domain.PhaseNormal, true)},
		ResumeReservations: reservations,
	})
	if err != nil {
		t.Fatalf("DeriveQuotaPoolAdmissions: %v", err)
	}
	if got[0].Admission != domain.AdmissionRecovering || got[0].ActiveResumeReservations != 3 {
		t.Fatalf("recovery projection = %+v", got[0])
	}
	if got[0].PausedRequiredWorkRemainder != 12 {
		t.Fatalf("paused required remainder = %v, want 12", got[0].PausedRequiredWorkRemainder)
	}
}

func TestDeriveQuotaPoolAdmissionsFailsClosedOnObservationProblems(t *testing.T) {
	key := admissionBucket("codex", "weekly")
	valid := admissionBucketState(key, domain.PhaseNormal, true)
	tests := []struct {
		name   string
		states []domain.BucketState
		code   string
	}{
		{name: "missing", code: QuotaAdmissionIssueBucketMissing},
		{name: "stale", states: []domain.BucketState{func() domain.BucketState {
			state := valid
			state.ObservedAt = admissionDerivationTime.Add(-11 * time.Minute)
			return state
		}()}, code: QuotaAdmissionIssueObservationStale},
		{name: "future", states: []domain.BucketState{func() domain.BucketState {
			state := valid
			state.ObservedAt = admissionDerivationTime.Add(time.Second)
			return state
		}()}, code: QuotaAdmissionIssueObservationFuture},
		{name: "conflicting epochs", states: func() []domain.BucketState {
			first, second := valid, valid
			firstReset := admissionDerivationTime.Add(time.Hour)
			secondReset := admissionDerivationTime.Add(2 * time.Hour)
			first.ResetsAt, first.Epoch = &firstReset, domain.EpochFor(&firstReset)
			second.ResetsAt, second.Epoch = &secondReset, domain.EpochFor(&secondReset)
			return []domain.BucketState{first, second}
		}(), code: QuotaAdmissionIssueObservationConflict},
		{name: "invalid phase", states: []domain.BucketState{func() domain.BucketState {
			state := valid
			state.Phase = "unknown"
			return state
		}()}, code: QuotaAdmissionIssueObservationConflict},
		{name: "epoch mismatch", states: []domain.BucketState{func() domain.BucketState {
			state := valid
			state.Epoch = "wrong"
			return state
		}()}, code: QuotaAdmissionIssueEpochConflict},
		{name: "expired epoch", states: []domain.BucketState{func() domain.BucketState {
			state := valid
			reset := admissionDerivationTime
			state.ResetsAt = &reset
			state.Epoch = domain.EpochFor(&reset)
			return state
		}()}, code: QuotaAdmissionIssueEpochConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
				Now: admissionDerivationTime, MaxObservationAge: 10 * time.Minute,
				Pools:        []domain.QuotaPool{{ID: "pool", Buckets: []domain.BucketKey{key}}},
				BucketStates: test.states,
			})
			if err != nil {
				t.Fatalf("DeriveQuotaPoolAdmissions: %v", err)
			}
			if got[0].Admission != domain.AdmissionClosed {
				t.Fatalf("admission = %q, want closed", got[0].Admission)
			}
			if !admissionIssueCodes(got[0].Issues)[test.code] {
				t.Fatalf("issues = %+v, want code %q", got[0].Issues, test.code)
			}
		})
	}
}

func TestDeriveQuotaPoolAdmissionsReconcilesSharedBucketObservations(t *testing.T) {
	key := admissionBucket("codex", "weekly")
	olderStopped := admissionBucketState(key, domain.PhaseStopped, false)
	olderStopped.ObservedAt = admissionDerivationTime.Add(-2 * time.Hour)
	freshNormal := admissionBucketState(key, domain.PhaseNormal, true)
	freshNormal.ObservedAt = admissionDerivationTime.Add(-time.Hour)

	got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools:        []domain.QuotaPool{{ID: "pool", Buckets: []domain.BucketKey{key}}},
		BucketStates: []domain.BucketState{freshNormal, olderStopped},
	})
	if err != nil {
		t.Fatalf("DeriveQuotaPoolAdmissions: %v", err)
	}
	if got[0].Admission != domain.AdmissionOpen {
		t.Fatalf("newer observation did not supersede old epoch state: %+v", got[0])
	}

	equalTimeDraining := freshNormal
	equalTimeDraining.Phase = domain.PhaseDraining
	equalTimeDraining.Healthy = false
	got, err = DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools:        []domain.QuotaPool{{ID: "pool", Buckets: []domain.BucketKey{key}}},
		BucketStates: []domain.BucketState{freshNormal, equalTimeDraining},
	})
	if err != nil {
		t.Fatalf("DeriveQuotaPoolAdmissions equal time: %v", err)
	}
	if got[0].Admission != domain.AdmissionDraining {
		t.Fatalf("equal-time observations did not choose highest credible severity: %+v", got[0])
	}
}

func TestDeriveQuotaPoolAdmissionsMultipleWindowsAndInputOrder(t *testing.T) {
	keyA := admissionBucket("codex", "weekly")
	keyB := admissionBucket("codex", "five-hour")
	resetA := admissionDerivationTime.Add(7 * 24 * time.Hour)
	resetB := admissionDerivationTime.Add(5 * time.Hour)
	stateA := admissionBucketState(keyA, domain.PhaseNormal, true)
	stateA.ResetsAt, stateA.Epoch = &resetA, domain.EpochFor(&resetA)
	stateB := admissionBucketState(keyB, domain.PhaseDraining, false)
	stateB.ResetsAt, stateB.Epoch = &resetB, domain.EpochFor(&resetB)
	input := QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools: []domain.QuotaPool{
			{ID: "pool-b", Buckets: []domain.BucketKey{admissionBucket("claude", "weekly")}},
			{ID: "pool-a", Buckets: []domain.BucketKey{keyA, keyB}},
		},
		BucketStates: []domain.BucketState{
			stateA, admissionBucketState(admissionBucket("claude", "weekly"), domain.PhaseNormal, true), stateB,
		},
		ResumeReservations: []QuotaResumeReservation{
			{AttemptID: "z", QuotaPoolID: "pool-a", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 3},
			{AttemptID: "a", QuotaPoolID: "pool-a", Class: domain.TaskClassSurplus, Status: domain.ResumeEligible, RemainingCost: 8},
		},
	}
	got, err := DeriveQuotaPoolAdmissions(input)
	if err != nil {
		t.Fatalf("DeriveQuotaPoolAdmissions: %v", err)
	}

	slices.Reverse(input.Pools)
	for index := range input.Pools {
		slices.Reverse(input.Pools[index].Buckets)
	}
	slices.Reverse(input.BucketStates)
	slices.Reverse(input.ResumeReservations)
	reordered, err := DeriveQuotaPoolAdmissions(input)
	if err != nil {
		t.Fatalf("DeriveQuotaPoolAdmissions reordered: %v", err)
	}
	if !reflect.DeepEqual(got, reordered) {
		t.Fatalf("order changed result:\nfirst: %#v\nsecond: %#v", got, reordered)
	}
	if got[0].QuotaPoolID != "pool-a" || got[0].Admission != domain.AdmissionDraining {
		t.Fatalf("pool precedence = %+v", got[0])
	}
	if len(got[0].BucketEpochs) != 2 || got[0].BucketEpochs[0].Bucket != keyB || got[0].BucketEpochs[1].Bucket != keyA {
		t.Fatalf("bucket epochs are not canonical: %+v", got[0].BucketEpochs)
	}

	input.BucketStates[0].Phase = domain.PhaseStopped
	input.ResumeReservations[0].RemainingCost = 999
	if reflect.DeepEqual(got, input) || got[0].Admission != domain.AdmissionDraining || got[0].PausedRequiredWorkRemainder != 3 {
		t.Fatalf("result changed with caller input: %+v", got)
	}
}

func TestDeriveQuotaPoolAdmissionsRejectsInvalidInput(t *testing.T) {
	key := admissionBucket("codex", "weekly")
	state := admissionBucketState(key, domain.PhaseNormal, true)
	valid := QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools:        []domain.QuotaPool{{ID: "pool", Buckets: []domain.BucketKey{key}}},
		BucketStates: []domain.BucketState{state},
	}
	tests := []struct {
		name string
		edit func(*QuotaAdmissionDerivationInput)
		want string
	}{
		{name: "missing time", edit: func(input *QuotaAdmissionDerivationInput) { input.Now = time.Time{} }, want: "time must be set"},
		{name: "invalid age", edit: func(input *QuotaAdmissionDerivationInput) { input.MaxObservationAge = 0 }, want: "age must be positive"},
		{name: "duplicate pool", edit: func(input *QuotaAdmissionDerivationInput) { input.Pools = append(input.Pools, input.Pools[0]) }, want: "repeats pool"},
		{name: "shared bucket", edit: func(input *QuotaAdmissionDerivationInput) {
			input.Pools = append(input.Pools, domain.QuotaPool{ID: "other", Buckets: []domain.BucketKey{key}})
		}, want: "belongs to both pools"},
		{name: "unknown reservation pool", edit: func(input *QuotaAdmissionDerivationInput) {
			input.ResumeReservations = []QuotaResumeReservation{{AttemptID: "attempt", QuotaPoolID: "missing", Class: domain.TaskClassRequired, Status: domain.ResumePending}}
		}, want: "unknown pool"},
		{name: "duplicate reservation", edit: func(input *QuotaAdmissionDerivationInput) {
			reservation := QuotaResumeReservation{AttemptID: "attempt", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: domain.ResumePending}
			input.ResumeReservations = []QuotaResumeReservation{reservation, reservation}
		}, want: "repeats attempt"},
		{name: "invalid class", edit: func(input *QuotaAdmissionDerivationInput) {
			input.ResumeReservations = []QuotaResumeReservation{{AttemptID: "attempt", QuotaPoolID: "pool", Class: "urgent", Status: domain.ResumePending}}
		}, want: "invalid task class"},
		{name: "invalid status", edit: func(input *QuotaAdmissionDerivationInput) {
			input.ResumeReservations = []QuotaResumeReservation{{AttemptID: "attempt", QuotaPoolID: "pool", Class: domain.TaskClassRequired, Status: "waiting"}}
		}, want: "invalid status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Pools = append([]domain.QuotaPool(nil), valid.Pools...)
			input.BucketStates = append([]domain.BucketState(nil), valid.BucketStates...)
			test.edit(&input)
			_, err := DeriveQuotaPoolAdmissions(input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func admissionBucket(provider, window string) domain.BucketKey {
	return domain.BucketKey{ProviderInstanceID: provider, LimitID: "limit", Window: window}
}

func admissionBucketState(key domain.BucketKey, phase domain.Phase, healthy bool) domain.BucketState {
	return domain.BucketState{
		Key: key, Phase: phase, Healthy: healthy,
		ObservedAt: admissionDerivationTime.Add(-time.Minute),
	}
}

func admissionIssueCodes(issues []QuotaAdmissionIssue) map[string]bool {
	codes := make(map[string]bool, len(issues))
	for _, issue := range issues {
		codes[issue.Code] = true
	}
	return codes
}
