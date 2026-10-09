package sqlite

import "github.com/iryzhkov/t3-steward/internal/domain"

// validateDerivedRunIDs refuses a rerun or clone whose store-derived
// identities, the run ID and the first attempt of every task, are not
// domain.PathSafeID, before any of them is written. They become directory
// names in worker and coordinator storage and parts of campaign git refs. The
// task and input IDs are derived and checked by the caller that builds them
// (backlogadmin), since the store takes them as given. namespace is the
// FirstAttemptID namespace the operation's attempts use.
func validateDerivedRunIDs(label, namespace, runID string, tasks []domain.Task) error {
	ids := []string{runID}
	for _, task := range tasks {
		ids = append(ids, domain.FirstAttemptID(namespace, task.ID))
	}
	return domain.ValidateDerivedIDs(label, ids...)
}
