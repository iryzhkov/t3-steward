package backlog

import "github.com/iryzhkov/t3-steward/internal/domain"

// WorkInProgressBundleName is the result artifact that carries the uncommitted
// work of an attempt the worker failed because its turn ended while commands
// it started were still running. It is a Git bundle of one snapshot commit on
// a private ref, so the work is recoverable from the coordinator without a
// shell on the worker.
const WorkInProgressBundleName = "wip.bundle"

// WorkInProgressBundleID is the fixed identity of an attempt's work-in-progress
// bundle. Like the final message and the thread archive it is bound to the
// attempt, so a result cannot publish another attempt's bundle.
func WorkInProgressBundleID(attemptID string) string {
	return "wip-bundle-" + attemptID
}

// DeclaresAnyCommit reports whether the task declares a commit output, the
// one kind of task whose unfinished work is a commit to recover.
func DeclaresAnyCommit(outputs []domain.ArtifactDeclaration) bool {
	for _, output := range outputs {
		if output.Commit != nil {
			return true
		}
	}
	return false
}

// isWorkInProgressBundleOf reports whether name and id are the attempt's
// work-in-progress bundle and its task may publish one.
func isWorkInProgressBundleOf(task domain.Task, attemptID, id, name string) bool {
	return name == WorkInProgressBundleName && id == WorkInProgressBundleID(attemptID) && DeclaresAnyCommit(task.Outputs)
}
