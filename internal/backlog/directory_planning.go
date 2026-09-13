package backlog

import (
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// DirectoryOwner is reconstructed from durable assignment/task records. There
// is deliberately no deadline: lease expiry is not proof the writer stopped.
type DirectoryOwner struct {
	AttemptID string
	Bindings  []directoryresource.Binding
}

func directoryOwners(attempts []domain.Attempt, assignments []domain.Assignment, runs []domain.WorkflowRun, tasks []domain.Task) []DirectoryOwner {
	byID := make(map[string]domain.Assignment, len(assignments))
	for _, a := range assignments {
		byID[a.ID] = a
	}
	var owners []DirectoryOwner
	for _, attempt := range attempts {
		if attempt.AssignmentID == "" {
			continue
		}
		assignment, exists := byID[attempt.AssignmentID]
		// Missing/mismatched assignment evidence is uncertain ownership, not release.
		if exists && assignment.AttemptID == attempt.ID && (assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased) {
			continue
		}
		task, ok := domain.TaskForAttempt(attempt, runs, tasks)
		if ok && len(task.DirectoryBindings) > 0 {
			owners = append(owners, DirectoryOwner{AttemptID: attempt.ID, Bindings: directoryresource.CloneBindings(task.DirectoryBindings)})
		}
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].AttemptID < owners[j].AttemptID })
	return owners
}

type directorySession struct{ owners []DirectoryOwner }

func newDirectorySession(owners []DirectoryOwner) *directorySession {
	s := &directorySession{}
	for _, o := range owners {
		s.owners = append(s.owners, DirectoryOwner{AttemptID: o.AttemptID, Bindings: directoryresource.CloneBindings(o.Bindings)})
	}
	return s
}

// StartPlan permits this policy to be used independently in deterministic tests.
func (s *directorySession) StartPlan(time.Time) PlanningConstraintSession {
	return newDirectorySession(s.owners)
}

func (s *directorySession) Evaluate(candidate PlanningCandidate) []PlanningBlocker {
	var blockers []PlanningBlocker
	for _, binding := range candidate.Task.DirectoryBindings {
		resource := binding.Identity.Registration.ResourceID
		if err := directoryresource.ValidateBinding(binding); err != nil {
			blockers = append(blockers, PlanningBlocker{Code: "directory-invalid", Resource: resource, Detail: err.Error()})
			continue
		}
		if binding.Identity.Registration.WorkerID != candidate.WorkerID {
			blockers = append(blockers, PlanningBlocker{Code: "directory-host", Resource: resource, Detail: "directory is registered on a different worker"})
			continue
		}
		for _, owner := range s.owners {
			if owner.AttemptID == candidate.Attempt.ID {
				continue
			}
			for _, held := range owner.Bindings {
				// Invalid persisted evidence cannot prove compatibility.
				invalid := directoryresource.ValidateBinding(held) != nil
				if invalid || directoryresource.Conflicts(binding, held) {
					blockers = append(blockers, PlanningBlocker{Code: PlanningBlockerResource, Resource: resource, OwnerID: owner.AttemptID, Detail: fmt.Sprintf("directory %q overlaps retained ownership by attempt %q", resource, owner.AttemptID)})
					break
				}
			}
		}
	}
	return blockers
}

func (s *directorySession) Reserve(candidate PlanningCandidate) {
	if len(candidate.Task.DirectoryBindings) > 0 {
		s.owners = append(s.owners, DirectoryOwner{AttemptID: candidate.Attempt.ID, Bindings: directoryresource.CloneBindings(candidate.Task.DirectoryBindings)})
	}
}
