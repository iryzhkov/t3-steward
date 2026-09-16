package domain

// The boundary between a run's declared work and the supervision that watches
// it, expressed on the one record both share.
//
// An overseer activation is dispatched as ordinary assigned work: it is
// claimed, leased, dispatched and recovered by the same machinery a task uses,
// and it therefore owns an Attempt and an Assignment. Everything that reasons
// about the run's own work must nevertheless not see it. The run's task graph
// is what the campaign declared, its sink settles when that graph settles, and
// its gates and holds withhold that graph. An activation counted among them
// would keep the sink open forever and could be withheld by the very gate it
// exists to decide.
//
// One predicate and one filter are the whole boundary, so a later reader can
// find every place the distinction is made by finding the callers of these two.

// IsSupervisionActivation reports whether this attempt carries an overseer
// activation rather than a declared task.
func (a Attempt) IsSupervisionActivation() bool {
	return a.SupervisionActivationID != ""
}

// DeclaredTaskAttempts keeps only the attempts of declared tasks.
//
// Every aggregate over a run's attempts applies it: the DAG projection, the
// sink's quiescence question, the planner's view of a workflow, and any status
// that counts a run's work. The result keeps the input order, and a slice with
// no activation in it is returned unchanged rather than copied, so the
// unsupervised path pays nothing for a distinction it never makes.
func DeclaredTaskAttempts(attempts []Attempt) []Attempt {
	activations := 0
	for _, attempt := range attempts {
		if attempt.IsSupervisionActivation() {
			activations++
		}
	}
	if activations == 0 {
		return attempts
	}
	declared := make([]Attempt, 0, len(attempts)-activations)
	for _, attempt := range attempts {
		if !attempt.IsSupervisionActivation() {
			declared = append(declared, attempt)
		}
	}
	return declared
}

// SupervisionActivationAttempts keeps only the activation attempts, which is
// what the activation lifecycle itself reconciles.
func SupervisionActivationAttempts(attempts []Attempt) []Attempt {
	var activations []Attempt
	for _, attempt := range attempts {
		if attempt.IsSupervisionActivation() {
			activations = append(activations, attempt)
		}
	}
	return activations
}
