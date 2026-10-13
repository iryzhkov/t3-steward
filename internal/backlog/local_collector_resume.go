package backlog

import (
	"fmt"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PlanLocalCollectorResumes records only a resume intent after source authority
// accepts an authenticated local stop observation. It invents no stop command ACK.
func PlanLocalCollectorResumes(snapshot domain.WorkerSnapshot, bindings []ThrottleAttemptBinding, previous []domain.ThrottleAttemptRecord, admissions []domain.QuotaAdmissionRecord, pools []domain.QuotaPool, authorize QuotaRecoveryGrantAuthorizer, now time.Time) ([]domain.ThrottleAttemptTransition, []domain.ThrottleCommand) {
	if authorize == nil {
		return nil, nil
	}
	var transitions []domain.ThrottleAttemptTransition
	var commands []domain.ThrottleCommand
	for _, binding := range bindings {
		if binding.Assignment.State != domain.AssignmentClaimed || !binding.Assignment.LeaseExpiresAt.After(now) {
			continue
		}
		for _, observation := range snapshot.Assignments {
			stop := observation.QuotaStop
			if observation.AssignmentID != binding.Assignment.ID || observation.AssignmentEpoch != binding.Assignment.Epoch ||
				observation.Control != domain.ControlPaused || stop == nil || stop.ObservedSequence <= 0 || stop.ObservedSequence > snapshot.Sequence {
				continue
			}
			directiveID := fmt.Sprintf("local-quota-stop-%s-%d-%d", binding.Assignment.ID, binding.Assignment.Epoch, stop.ObservedSequence)
			if slices.ContainsFunc(previous, func(r domain.ThrottleAttemptRecord) bool {
				return r.AttemptID == binding.Attempt.ID && r.DirectiveID == directiveID
			}) {
				continue
			}
			for index, pool := range pools {
				if pool.ID != binding.Assignment.Route.QuotaPoolID || pool.Collector == nil || pool.ActiveAssignments >= pool.MaxConcurrent {
					continue
				}
				if !slices.ContainsFunc(admissions, func(a domain.QuotaAdmissionRecord) bool {
					return a.QuotaPoolID == pool.ID && (a.Admission == domain.AdmissionOpen || a.Admission == domain.AdmissionRecovering)
				}) {
					continue
				}
				command := throttleCommand(domain.ThrottleDirective{ID: directiveID, QuotaPoolID: pool.ID,
					BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: stop.Bucket, Epoch: stop.Epoch}}}, binding, domain.ThrottleCommandResume, 1, now)
				draft := domain.ThrottleAttemptRecord{DirectiveID: directiveID, AttemptID: binding.Attempt.ID, Revision: 1, Command: command,
					Delivery: domain.ThrottleDeliveryPending, Control: domain.ControlResuming, UpdatedAt: now}
				grant := authorize(pool, draft, now)
				if grant == nil {
					continue
				}
				command.QuotaRecovery = grant
				command.Reason = "quota resume permitted by verified collector recovery"
				draft.Command = command
				transitions = append(transitions, domain.ThrottleAttemptTransition{Record: draft})
				commands = append(commands, cloneThrottleCommand(command))
				pools[index].ActiveAssignments++
			}
		}
	}
	sortThrottleAttemptTransitions(transitions)
	sortThrottleCommands(commands)
	return transitions, commands
}
