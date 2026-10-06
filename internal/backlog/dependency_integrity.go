package backlog

import "fmt"

// DependencyIntegrityError identifies the input that prevented a worker from
// preparing or recovering a task. It remains discoverable through error wrapping.
type DependencyIntegrityError struct {
	Producer string
	Artifact string
	Err      error
}

func (e *DependencyIntegrityError) Error() string {
	return fmt.Sprintf("dependency_integrity: artifact %q from %q: %v", e.Artifact, e.Producer, e.Err)
}
func (e *DependencyIntegrityError) Unwrap() error { return e.Err }
