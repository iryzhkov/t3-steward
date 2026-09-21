package domain

// AssignmentOwnsExecutorCapacity reports whether durable attempt and assignment
// state reserves executor capacity. Offered work reserves before claim so racing
// offers cannot overbook. Unknown retains its reservation because execution may
// still exist. A parked external wait releases compute while retaining its
// assignment, workspace, and locks; resuming reacquires before changing control.
func AssignmentOwnsExecutorCapacity(attempt Attempt, assignment Assignment) bool {
	if assignment.State == AssignmentCompleted || assignment.State == AssignmentReleased {
		return false
	}
	return attempt.Control != ControlWaitingExternal
}

// AssignmentExecutorDemand returns the immutable demand committed with an
// assignment. Overseer activations name no task and are known slot-only work.
// An ordinary legacy assignment without placement evidence has unknown demand;
// callers must not reconstruct it from a later mutable graph projection.
func AssignmentExecutorDemand(attempt Attempt, assignment Assignment) (ResourceDemand, bool) {
	if attempt.TaskID == "" {
		return ResourceDemand{}, true
	}
	if assignment.ExecutorDemand != nil {
		return *assignment.ExecutorDemand, true
	}
	if assignment.Placement != nil {
		return assignment.Placement.Demand, true
	}
	return ResourceDemand{}, false
}
