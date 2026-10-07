package backlog

// ValidateOutputPaths applies the manifest's rule for a task's declared
// outputs to a list of names: each one relative, inside the bundle, free of NUL
// and glob characters, and none repeated. label leads every refusal, so a
// caller outside the manifest, such as "task run" checking its --output flags
// before it contacts the coordinator, refuses in the validator's own words
// under its own name. It is the same function the manifest validator calls,
// never a copy of it, so the two cannot disagree.
func ValidateOutputPaths(label string, paths []string) error {
	return validateUniquePaths(label, paths, false)
}
