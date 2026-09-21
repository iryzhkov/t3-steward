package domain

// AssignmentOwnsExecutorCapacity reports whether durable attempt and assignment
// state reserves one executor slot. Offered work reserves before claim so racing
// offers cannot overbook. Unknown retains its reservation because execution may
// still exist. A parked external wait releases compute while retaining its
// assignment, workspace, and locks; resuming reacquires before changing control.
func AssignmentOwnsExecutorCapacity(attempt Attempt, assignment Assignment) bool {
	if assignment.State == AssignmentCompleted || assignment.State == AssignmentReleased {
		return false
	}
	return attempt.Control != ControlWaitingExternal
}
