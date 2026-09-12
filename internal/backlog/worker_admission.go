package backlog

import (
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// WorkerAdmissionPolicy is the last fail-closed quota check before an offer or
// lifecycle command can cross the worker transport. Only explicitly open pools
// may receive new work; observation, stop, and collection remain available.
type WorkerAdmissionPolicy struct {
	OpenQuotaPools map[string]struct{}
}

func WorkerAdmissionPolicyFromQuotaReport(report QuotaBridgeReport) WorkerAdmissionPolicy {
	policy := WorkerAdmissionPolicy{OpenQuotaPools: make(map[string]struct{})}
	for _, pool := range report.Derived {
		if pool.Admission == domain.AdmissionOpen {
			policy.OpenQuotaPools[pool.QuotaPoolID] = struct{}{}
		}
	}
	return policy
}

func (p WorkerAdmissionPolicy) AllowsNewWork(quotaPoolID string) bool {
	_, allowed := p.OpenQuotaPools[quotaPoolID]
	return quotaPoolID != "" && (allowed || unmeteredQuotaPoolID(quotaPoolID))
}

func forcedAssignmentIDs(assignments []domain.Assignment, attempts []domain.Attempt) map[string]struct{} {
	forcedAttempts := make(map[string]struct{})
	for _, attempt := range attempts {
		if attempt.AdminForceStart {
			forcedAttempts[attempt.ID] = struct{}{}
		}
	}
	forcedAssignments := make(map[string]struct{})
	for _, assignment := range assignments {
		if _, forced := forcedAttempts[assignment.AttemptID]; forced {
			forcedAssignments[assignment.ID] = struct{}{}
		}
	}
	return forcedAssignments
}

func (p WorkerAdmissionPolicy) filterOffers(assignments []domain.Assignment, attempts []domain.Attempt) (allowed, withheld []domain.Assignment) {
	forced := forcedAssignmentIDs(assignments, attempts)
	for _, assignment := range assignments {
		if _, override := forced[assignment.ID]; override || p.AllowsNewWork(assignment.Route.QuotaPoolID) {
			allowed = append(allowed, assignment)
		} else {
			withheld = append(withheld, assignment)
		}
	}
	return allowed, withheld
}
func (p WorkerAdmissionPolicy) filterCommands(assignments []domain.Assignment, attempts []domain.Attempt, commands []domain.WorkerCommand) (allowed, withheld []domain.WorkerCommand) {
	pools := make(map[string]string, len(assignments))
	for _, assignment := range assignments {
		pools[assignment.ID] = assignment.Route.QuotaPoolID
	}
	forced := forcedAssignmentIDs(assignments, attempts)
	for _, command := range commands {
		newWork := command.Kind == domain.WorkerCommandPrepare || command.Kind == domain.WorkerCommandDispatch
		_, override := forced[command.AssignmentID]
		if !newWork || override || p.AllowsNewWork(pools[command.AssignmentID]) {
			allowed = append(allowed, command)
		} else {
			withheld = append(withheld, command)
		}
	}
	sort.Slice(allowed, func(i, j int) bool { return allowed[i].ID < allowed[j].ID })
	sort.Slice(withheld, func(i, j int) bool { return withheld[i].ID < withheld[j].ID })
	return allowed, withheld
}
