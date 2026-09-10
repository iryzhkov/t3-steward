package backlog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	QuotaAdmissionIssueBucketMissing       = "bucket-missing"
	QuotaAdmissionIssueObservationStale    = "observation-stale"
	QuotaAdmissionIssueObservationFuture   = "observation-future"
	QuotaAdmissionIssueObservationConflict = "observation-conflict"
	QuotaAdmissionIssueEpochConflict       = "epoch-conflict"
	QuotaAdmissionIssueBucketUnhealthy     = "bucket-unhealthy"
)

// QuotaResumeReservation is the scheduler-owned part of a paused attempt. It
// remains visible to quota accounting while the attempt releases its runtime
// provider slot.
type QuotaResumeReservation struct {
	AttemptID     string
	QuotaPoolID   string
	Class         domain.TaskClass
	Status        domain.ResumeStatus
	RemainingCost float64
	StopEpoch     string
}

// QuotaAdmissionDerivationInput is one immutable coordinator snapshot.
type QuotaAdmissionDerivationInput struct {
	Now                time.Time
	MaxObservationAge  time.Duration
	Pools              []domain.QuotaPool
	BucketStates       []domain.BucketState
	ResumeReservations []QuotaResumeReservation
}

// QuotaAdmissionIssue explains why a pool failed closed or remained
// constrained. Bucket is empty for a pool-level issue.
type QuotaAdmissionIssue struct {
	Code   string
	Bucket domain.BucketKey
	Detail string
}

// QuotaBucketEpoch identifies the accepted epoch for one bucket.
type QuotaBucketEpoch struct {
	Bucket domain.BucketKey
	Epoch  string
}

// QuotaPoolAdmissionSnapshot is the deterministic admission projection consumed
// by later planning and worker-directive transactions.
type QuotaPoolAdmissionSnapshot struct {
	QuotaPoolID                 string
	Admission                   domain.AdmissionState
	ObservedAt                  time.Time
	BucketEpochs                []QuotaBucketEpoch
	DirectiveDeadline           *time.Time
	PausedRequiredWorkRemainder float64
	ActiveResumeReservations    int
	Reason                      string
	Issues                      []QuotaAdmissionIssue
}

// DeriveQuotaPoolAdmissions combines the current state of every configured
// bucket conservatively. Missing, stale, future-dated, irreconcilably
// conflicting, or epoch-inconsistent observations close the affected pool.
func DeriveQuotaPoolAdmissions(input QuotaAdmissionDerivationInput) ([]QuotaPoolAdmissionSnapshot, error) {
	if input.Now.IsZero() {
		return nil, fmt.Errorf("quota admission derivation time must be set")
	}
	if input.MaxObservationAge <= 0 {
		return nil, fmt.Errorf("quota admission maximum observation age must be positive")
	}

	pools := append([]domain.QuotaPool(nil), input.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	poolByID := make(map[string]domain.QuotaPool, len(pools))
	bucketOwner := make(map[domain.BucketKey]string)
	for index, pool := range pools {
		if strings.TrimSpace(pool.ID) != pool.ID || pool.ID == "" {
			return nil, fmt.Errorf("quota pool ID must be nonempty and trimmed")
		}
		if _, exists := poolByID[pool.ID]; exists {
			return nil, fmt.Errorf("quota admission repeats pool %q", pool.ID)
		}
		pool.Buckets = append([]domain.BucketKey(nil), pool.Buckets...)
		sort.Slice(pool.Buckets, func(i, j int) bool { return pool.Buckets[i].String() < pool.Buckets[j].String() })
		for _, bucket := range pool.Buckets {
			if err := validateAdmissionBucketKey(bucket); err != nil {
				return nil, fmt.Errorf("quota pool %q: %w", pool.ID, err)
			}
			if owner, exists := bucketOwner[bucket]; exists {
				return nil, fmt.Errorf("quota bucket %q belongs to both pools %q and %q", bucket.String(), owner, pool.ID)
			}
			bucketOwner[bucket] = pool.ID
		}
		pools[index] = pool
		poolByID[pool.ID] = pool
	}

	statesByKey := make(map[domain.BucketKey][]domain.BucketState)
	for _, state := range input.BucketStates {
		if err := validateAdmissionBucketKey(state.Key); err != nil {
			return nil, err
		}
		statesByKey[state.Key] = append(statesByKey[state.Key], cloneAdmissionBucketState(state))
	}
	for key := range statesByKey {
		sort.Slice(statesByKey[key], func(i, j int) bool {
			left, right := statesByKey[key][i], statesByKey[key][j]
			if !left.ObservedAt.Equal(right.ObservedAt) {
				return left.ObservedAt.Before(right.ObservedAt)
			}
			if left.Epoch != right.Epoch {
				return left.Epoch < right.Epoch
			}
			return left.Phase < right.Phase
		})
	}

	reservationsByPool := make(map[string][]QuotaResumeReservation)
	seenReservations := make(map[string]struct{})
	for _, reservation := range input.ResumeReservations {
		if strings.TrimSpace(reservation.AttemptID) != reservation.AttemptID || reservation.AttemptID == "" {
			return nil, fmt.Errorf("quota resume reservation attempt ID must be nonempty and trimmed")
		}
		if _, exists := poolByID[reservation.QuotaPoolID]; !exists {
			return nil, fmt.Errorf("quota resume reservation %q names unknown pool %q", reservation.AttemptID, reservation.QuotaPoolID)
		}
		if !nonnegativeFinite(reservation.RemainingCost) {
			return nil, fmt.Errorf("quota resume reservation %q remaining cost must be nonnegative and finite", reservation.AttemptID)
		}
		switch reservation.Class {
		case domain.TaskClassRequired, domain.TaskClassSurplus:
		default:
			return nil, fmt.Errorf("quota resume reservation %q has invalid task class %q", reservation.AttemptID, reservation.Class)
		}
		switch reservation.Status {
		case domain.ResumePending, domain.ResumeEligible, domain.ResumeResuming,
			domain.ResumeResumed, domain.ResumeCancelled, domain.ResumeFailed:
		default:
			return nil, fmt.Errorf("quota resume reservation %q has invalid status %q", reservation.AttemptID, reservation.Status)
		}
		key := reservation.QuotaPoolID + "\x00" + reservation.AttemptID
		if _, exists := seenReservations[key]; exists {
			return nil, fmt.Errorf("quota resume reservation repeats attempt %q in pool %q", reservation.AttemptID, reservation.QuotaPoolID)
		}
		seenReservations[key] = struct{}{}
		reservationsByPool[reservation.QuotaPoolID] = append(reservationsByPool[reservation.QuotaPoolID], reservation)
	}
	for poolID := range reservationsByPool {
		sort.Slice(reservationsByPool[poolID], func(i, j int) bool {
			return reservationsByPool[poolID][i].AttemptID < reservationsByPool[poolID][j].AttemptID
		})
	}

	results := make([]QuotaPoolAdmissionSnapshot, 0, len(pools))
	for _, pool := range pools {
		results = append(results, deriveQuotaPoolAdmission(pool, statesByKey, reservationsByPool[pool.ID], input.Now, input.MaxObservationAge))
	}
	return results, nil
}

func reconcileAdmissionBucketStates(key domain.BucketKey, states []domain.BucketState) (domain.BucketState, *QuotaAdmissionIssue, bool) {
	if len(states) == 0 {
		return domain.BucketState{}, nil, false
	}
	latestAt := states[len(states)-1].ObservedAt
	latest := states[len(states)-1]
	for index := len(states) - 1; index >= 0 && states[index].ObservedAt.Equal(latestAt); index-- {
		candidate := states[index]
		if !validAdmissionPhase(candidate.Phase) {
			return domain.BucketState{}, &QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueObservationConflict, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q has invalid phase %q", key.String(), candidate.Phase),
			}, true
		}
		if candidate.Epoch != latest.Epoch || domain.EpochFor(candidate.ResetsAt) != domain.EpochFor(latest.ResetsAt) {
			return domain.BucketState{}, &QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueObservationConflict, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q has conflicting epochs at observation time %s", key.String(), latestAt.UTC().Format(time.RFC3339)),
			}, true
		}
		if candidate.Phase.Rank() > latest.Phase.Rank() {
			latest.Phase = candidate.Phase
		}
		latest.Healthy = latest.Healthy && candidate.Healthy
	}
	return latest, nil, true
}

func deriveQuotaPoolAdmission(pool domain.QuotaPool, statesByKey map[domain.BucketKey][]domain.BucketState, reservations []QuotaResumeReservation, now time.Time, maxAge time.Duration) QuotaPoolAdmissionSnapshot {
	result := QuotaPoolAdmissionSnapshot{QuotaPoolID: pool.ID}
	activeReservations := make([]QuotaResumeReservation, 0, len(reservations))
	for _, reservation := range reservations {
		if !activeResumeStatus(reservation.Status) {
			continue
		}
		activeReservations = append(activeReservations, reservation)
		result.ActiveResumeReservations++
		if reservation.Class == domain.TaskClassRequired {
			result.PausedRequiredWorkRemainder += reservation.RemainingCost
		}
	}

	if len(pool.Buckets) == 0 {
		result.Issues = append(result.Issues, QuotaAdmissionIssue{
			Code:   QuotaAdmissionIssueBucketMissing,
			Detail: fmt.Sprintf("quota pool %q has no configured buckets", pool.ID),
		})
	}

	maxPhase := domain.PhaseNormal
	allHealthy := true
	for _, key := range pool.Buckets {
		state, issue, found := reconcileAdmissionBucketStates(key, statesByKey[key])
		if !found {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueBucketMissing, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q has no observation", key.String()),
			})
			continue
		}
		if issue != nil {
			result.Issues = append(result.Issues, *issue)
			continue
		}
		result.BucketEpochs = append(result.BucketEpochs, QuotaBucketEpoch{Bucket: key, Epoch: state.Epoch})
		if state.DrainDeadline != nil && (result.DirectiveDeadline == nil || state.DrainDeadline.Before(*result.DirectiveDeadline)) {
			deadline := *state.DrainDeadline
			result.DirectiveDeadline = &deadline
		}
		if result.ObservedAt.IsZero() || state.ObservedAt.Before(result.ObservedAt) {
			result.ObservedAt = state.ObservedAt
		}
		if state.ObservedAt.IsZero() || now.Sub(state.ObservedAt) > maxAge {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueObservationStale, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q observation at %s exceeds maximum age %s", key.String(), state.ObservedAt.UTC().Format(time.RFC3339), maxAge),
			})
		} else if state.ObservedAt.After(now) {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueObservationFuture, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q observation at %s is after derivation time %s", key.String(), state.ObservedAt.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339)),
			})
		}
		expectedEpoch := domain.EpochFor(state.ResetsAt)
		if state.Epoch != expectedEpoch {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueEpochConflict, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q epoch %q does not match reset epoch %q", key.String(), state.Epoch, expectedEpoch),
			})
		}
		if state.ResetsAt != nil && !state.ResetsAt.After(now) {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueEpochConflict, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q reset epoch %q ended at %s", key.String(), state.Epoch, state.ResetsAt.UTC().Format(time.RFC3339)),
			})
		}
		switch state.Phase {
		case domain.PhaseNormal, domain.PhaseWarned, domain.PhaseDraining, domain.PhaseStopped:
		default:
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueObservationConflict, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q has invalid phase %q", key.String(), state.Phase),
			})
		}
		if state.Phase.Rank() > maxPhase.Rank() {
			maxPhase = state.Phase
		}
		if state.Phase != domain.PhaseNormal || !state.Healthy {
			allHealthy = false
		}
		if state.Phase == domain.PhaseNormal && !state.Healthy {
			result.Issues = append(result.Issues, QuotaAdmissionIssue{
				Code: QuotaAdmissionIssueBucketUnhealthy, Bucket: key,
				Detail: fmt.Sprintf("quota bucket %q is normal but not healthy for resumption", key.String()),
			})
		}
	}

	sort.Slice(result.Issues, func(i, j int) bool {
		left, right := result.Issues[i], result.Issues[j]
		if admissionIssueHard(left) != admissionIssueHard(right) {
			return admissionIssueHard(left)
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if left.Bucket.String() != right.Bucket.String() {
			return left.Bucket.String() < right.Bucket.String()
		}
		return left.Detail < right.Detail
	})
	if hasHardAdmissionIssue(result.Issues) {
		result.Admission = domain.AdmissionClosed
		result.Reason = result.Issues[0].Detail
		return result
	}

	switch maxPhase {
	case domain.PhaseStopped:
		result.Admission = domain.AdmissionClosed
		result.Reason = "at least one quota bucket is stopped"
	case domain.PhaseDraining:
		result.Admission = domain.AdmissionDraining
		result.Reason = "at least one quota bucket is draining"
	case domain.PhaseWarned:
		result.Admission = domain.AdmissionConstrained
		result.Reason = "at least one quota bucket is warned"
	default:
		if !allHealthy {
			result.Admission = domain.AdmissionConstrained
			result.Reason = "at least one quota bucket is not healthy for resumption"
		} else if len(activeReservations) > 0 {
			result.Admission = domain.AdmissionRecovering
			result.Reason = fmt.Sprintf("%d paused attempt reservation(s) await recovery", len(activeReservations))
		} else {
			result.Admission = domain.AdmissionOpen
			result.Reason = "all quota buckets are healthy"
		}
	}
	return result
}

func validAdmissionPhase(phase domain.Phase) bool {
	switch phase {
	case domain.PhaseNormal, domain.PhaseWarned, domain.PhaseDraining, domain.PhaseStopped:
		return true
	default:
		return false
	}
}

func admissionIssueHard(issue QuotaAdmissionIssue) bool {
	return issue.Code != QuotaAdmissionIssueBucketUnhealthy
}

func activeResumeStatus(status domain.ResumeStatus) bool {
	switch status {
	case domain.ResumePending, domain.ResumeEligible, domain.ResumeResuming:
		return true
	default:
		return false
	}
}

func hasHardAdmissionIssue(issues []QuotaAdmissionIssue) bool {
	for _, issue := range issues {
		if issue.Code != QuotaAdmissionIssueBucketUnhealthy {
			return true
		}
	}
	return false
}

func validateAdmissionBucketKey(key domain.BucketKey) error {
	values := []struct {
		name  string
		value string
	}{
		{"provider instance", key.ProviderInstanceID},
		{"limit", key.LimitID},
		{"window", key.Window},
	}
	for _, value := range values {
		if value.value == "" || strings.TrimSpace(value.value) != value.value {
			return fmt.Errorf("quota bucket %s must be nonempty and trimmed", value.name)
		}
	}
	if strings.TrimSpace(key.AccountID) != key.AccountID {
		return fmt.Errorf("quota bucket account must be trimmed")
	}
	return nil
}

func cloneAdmissionBucketState(state domain.BucketState) domain.BucketState {
	state.Recent = append([]domain.Reading(nil), state.Recent...)
	if state.ResetsAt != nil {
		value := *state.ResetsAt
		state.ResetsAt = &value
	}
	if state.RecoveredAt != nil {
		value := *state.RecoveredAt
		state.RecoveredAt = &value
	}
	if state.DrainDeadline != nil {
		value := *state.DrainDeadline
		state.DrainDeadline = &value
	}
	if state.ExhaustsIn != nil {
		value := *state.ExhaustsIn
		state.ExhaustsIn = &value
	}
	return state
}
