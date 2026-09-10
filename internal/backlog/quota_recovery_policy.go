package backlog

import (
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuotaRecoveryNewRequired is an already-routed, dependency-ready required
// attempt competing with paused work for a provider slot.
type QuotaRecoveryNewRequired struct {
	AttemptID   string
	QuotaPoolID string
	Deadline    *time.Time
}

// QuotaRecoverySkipTransition terminally skips an invalid surplus occurrence.
// Artifact and checkpoint references are deliberately absent so persistence can
// retain them unchanged.
type QuotaRecoverySkipTransition struct {
	AttemptID        string
	ExpectedRevision int64
	Progress         domain.ProgressState
	Control          domain.ControlState
	Reason           string
	CompletedAt      time.Time
}

// QuotaRecoveryInput is the deterministic coordinator snapshot used to order
// resumptions against new required work.
type QuotaRecoveryInput struct {
	Now                         time.Time
	DeadlineRiskWindow          time.Duration
	PlanningState               QuotaPlanningState
	ThrottleRecords             []domain.ThrottleAttemptRecord
	Admissions                  []domain.QuotaAdmissionRecord
	InteractiveResumeAttemptIDs []string
	NewRequired                 []QuotaRecoveryNewRequired
	SurplusEligiblePools        map[string]bool
}

// QuotaRecoveryPlan reserves provider slots in policy order. NewRequiredAttemptIDs
// are handed back to ordinary assignment planning; resume commands retain their
// existing execution identity.
type QuotaRecoveryPlan struct {
	ResumeTransitions     []domain.ThrottleAttemptTransition
	ResumeCommands        []domain.ThrottleCommand
	SkippedSurplus        []QuotaRecoverySkipTransition
	NewRequiredAttemptIDs []string
}

type quotaRecoveryEntry struct {
	attemptID    string
	poolID       string
	deadline     *time.Time
	reservation  *QuotaResumeReservation
	newRequired  bool
	interactive  bool
	deadlineRisk bool
}

// PlanQuotaRecovery prioritizes interactive resumptions, then immediately
// endangered required work, paused required work, ordinary new required work,
// and finally still-valid surplus resumptions.
func PlanQuotaRecovery(input QuotaRecoveryInput) (QuotaRecoveryPlan, error) {
	if input.Now.IsZero() {
		return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery time must be set")
	}
	if input.DeadlineRiskWindow <= 0 {
		return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery deadline risk window must be positive")
	}

	admissionByPool := make(map[string]domain.QuotaAdmissionRecord, len(input.Admissions))
	for _, admission := range input.Admissions {
		if admission.QuotaPoolID == "" {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery admission identity is required")
		}
		if _, duplicate := admissionByPool[admission.QuotaPoolID]; duplicate {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery repeats admission pool %q", admission.QuotaPoolID)
		}
		admissionByPool[admission.QuotaPoolID] = admission
	}

	pools := append([]domain.QuotaPool(nil), input.PlanningState.QuotaPools...)
	poolIndex := make(map[string]int, len(pools))
	for index, pool := range pools {
		if pool.ID == "" || pool.MaxConcurrent <= 0 || pool.ActiveAssignments < 0 ||
			pool.ActiveAssignments > pool.MaxConcurrent {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery pool %q has invalid concurrency", pool.ID)
		}
		if _, duplicate := poolIndex[pool.ID]; duplicate {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery repeats pool %q", pool.ID)
		}
		poolIndex[pool.ID] = index
	}

	interactive := make(map[string]struct{}, len(input.InteractiveResumeAttemptIDs))
	for _, attemptID := range input.InteractiveResumeAttemptIDs {
		if attemptID == "" {
			return QuotaRecoveryPlan{}, fmt.Errorf("interactive resume attempt identity is required")
		}
		if _, duplicate := interactive[attemptID]; duplicate {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery repeats interactive attempt %q", attemptID)
		}
		interactive[attemptID] = struct{}{}
	}

	latest, err := latestThrottleRecords(input.ThrottleRecords)
	if err != nil {
		return QuotaRecoveryPlan{}, err
	}
	var entries []quotaRecoveryEntry
	var result QuotaRecoveryPlan
	seenAttempts := make(map[string]struct{}, len(input.PlanningState.ResumeReservations)+len(input.NewRequired))
	for index := range input.PlanningState.ResumeReservations {
		reservation := &input.PlanningState.ResumeReservations[index]
		if reservation.AttemptID == "" || reservation.TaskID == "" || reservation.QuotaPoolID == "" ||
			reservation.AttemptRevision < 0 {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery reservation identity is incomplete")
		}
		if _, duplicate := seenAttempts[reservation.AttemptID]; duplicate {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery repeats attempt %q", reservation.AttemptID)
		}
		seenAttempts[reservation.AttemptID] = struct{}{}
		if _, exists := poolIndex[reservation.QuotaPoolID]; !exists {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery attempt %q names unknown pool %q", reservation.AttemptID, reservation.QuotaPoolID)
		}
		record, exists := latest[reservation.AttemptID]
		if !exists || record.DirectiveID != reservation.StopEpoch ||
			record.Command.QuotaPoolID != reservation.QuotaPoolID {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery attempt %q has no matching throttle projection", reservation.AttemptID)
		}
		_, userRequested := interactive[reservation.AttemptID]
		delete(interactive, reservation.AttemptID)
		if reservation.Status == domain.ResumeResuming {
			if record.Control == domain.ControlResuming && record.Delivery == domain.ThrottleDeliveryPending {
				result.ResumeCommands = append(result.ResumeCommands, cloneThrottleCommand(record.Command))
			}
			continue
		}
		if reservation.Status != domain.ResumePending {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery attempt %q has invalid resume status %q", reservation.AttemptID, reservation.Status)
		}
		switch reservation.Class {
		case domain.TaskClassRequired:
			entries = append(entries, quotaRecoveryEntry{
				attemptID: reservation.AttemptID, poolID: reservation.QuotaPoolID,
				deadline: reservation.Deadline, reservation: reservation, interactive: userRequested,
			})
		case domain.TaskClassSurplus:
			admission, admitted := admissionByPool[reservation.QuotaPoolID]
			validOccurrence := reservation.ExpiresAt == nil || input.Now.Before(*reservation.ExpiresAt)
			if !admitted || admission.Admission != domain.AdmissionOpen ||
				!input.SurplusEligiblePools[reservation.QuotaPoolID] || !validOccurrence {
				reason := "surplus recovery is no longer eligible"
				if !validOccurrence {
					reason = "surplus occurrence expired"
				}
				result.SkippedSurplus = append(result.SkippedSurplus, QuotaRecoverySkipTransition{
					AttemptID: reservation.AttemptID, ExpectedRevision: reservation.AttemptRevision,
					Progress: domain.ProgressSkipped, Control: domain.ControlStopped,
					Reason: reason, CompletedAt: input.Now,
				})
				continue
			}
			entries = append(entries, quotaRecoveryEntry{
				attemptID: reservation.AttemptID, poolID: reservation.QuotaPoolID,
				deadline: reservation.Deadline, reservation: reservation, interactive: userRequested,
			})
		default:
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery attempt %q has invalid class %q", reservation.AttemptID, reservation.Class)
		}
	}
	if len(interactive) != 0 {
		attemptIDs := make([]string, 0, len(interactive))
		for attemptID := range interactive {
			attemptIDs = append(attemptIDs, attemptID)
		}
		sort.Strings(attemptIDs)
		return QuotaRecoveryPlan{}, fmt.Errorf("interactive resume names unknown paused attempt %q", attemptIDs[0])
	}
	for _, candidate := range input.NewRequired {
		if candidate.AttemptID == "" || candidate.QuotaPoolID == "" {
			return QuotaRecoveryPlan{}, fmt.Errorf("new required recovery candidate identity is incomplete")
		}
		if _, duplicate := seenAttempts[candidate.AttemptID]; duplicate {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery repeats attempt %q", candidate.AttemptID)
		}
		seenAttempts[candidate.AttemptID] = struct{}{}
		if _, exists := poolIndex[candidate.QuotaPoolID]; !exists {
			return QuotaRecoveryPlan{}, fmt.Errorf("new required attempt %q names unknown pool %q", candidate.AttemptID, candidate.QuotaPoolID)
		}
		deadlineRisk := candidate.Deadline != nil &&
			candidate.Deadline.Sub(input.Now) <= input.DeadlineRiskWindow
		entries = append(entries, quotaRecoveryEntry{
			attemptID: candidate.AttemptID, poolID: candidate.QuotaPoolID,
			deadline: clonePlanningTime(candidate.Deadline), newRequired: true, deadlineRisk: deadlineRisk,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		leftRank, rightRank := quotaRecoveryRank(left), quotaRecoveryRank(right)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if left.deadline != nil && right.deadline != nil && !left.deadline.Equal(*right.deadline) {
			return left.deadline.Before(*right.deadline)
		}
		if (left.deadline == nil) != (right.deadline == nil) {
			return left.deadline != nil
		}
		return left.attemptID < right.attemptID
	})

	for _, entry := range entries {
		admission, admitted := admissionByPool[entry.poolID]
		if !admitted || (admission.Admission != domain.AdmissionOpen && admission.Admission != domain.AdmissionRecovering) {
			continue
		}
		poolAt := poolIndex[entry.poolID]
		if pools[poolAt].ActiveAssignments >= pools[poolAt].MaxConcurrent {
			continue
		}
		if entry.newRequired {
			pools[poolAt].ActiveAssignments++
			result.NewRequiredAttemptIDs = append(result.NewRequiredAttemptIDs, entry.attemptID)
			continue
		}
		record := latest[entry.attemptID]
		transitions, commands, planErr := PlanThrottleResumes(
			[]domain.ThrottleAttemptRecord{record}, input.Admissions, pools, input.Now,
		)
		if planErr != nil {
			return QuotaRecoveryPlan{}, planErr
		}
		if len(transitions) != 1 || len(commands) != 1 {
			return QuotaRecoveryPlan{}, fmt.Errorf("quota recovery attempt %q did not produce one resume", entry.attemptID)
		}
		pools[poolAt].ActiveAssignments++
		result.ResumeTransitions = append(result.ResumeTransitions, transitions[0])
		result.ResumeCommands = append(result.ResumeCommands, commands[0])
	}

	sortThrottleAttemptTransitions(result.ResumeTransitions)
	sortThrottleCommands(result.ResumeCommands)
	sort.Slice(result.SkippedSurplus, func(i, j int) bool {
		return result.SkippedSurplus[i].AttemptID < result.SkippedSurplus[j].AttemptID
	})
	return result, nil
}

func quotaRecoveryRank(entry quotaRecoveryEntry) int {
	if entry.interactive {
		return 0
	}
	if entry.newRequired && entry.deadlineRisk {
		return 1
	}
	if entry.reservation != nil && entry.reservation.Class == domain.TaskClassRequired {
		return 2
	}
	if entry.newRequired {
		return 3
	}
	return 4
}

func latestThrottleRecords(records []domain.ThrottleAttemptRecord) (map[string]domain.ThrottleAttemptRecord, error) {
	indexed, err := indexThrottleAttemptRecords(records)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]domain.ThrottleAttemptRecord)
	for _, record := range indexed {
		current, exists := latest[record.AttemptID]
		if !exists || record.UpdatedAt.After(current.UpdatedAt) ||
			(record.UpdatedAt.Equal(current.UpdatedAt) && record.DirectiveID > current.DirectiveID) {
			latest[record.AttemptID] = record
		}
	}
	return latest, nil
}
