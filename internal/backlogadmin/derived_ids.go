package backlogadmin

import "github.com/iryzhkov/t3-steward/internal/domain"

// validateDerivedGraphIDs refuses a rerun or clone whose derived task or input
// identities are not domain.PathSafeID before they are handed to the store,
// which checks the run and attempt identities it derives itself.
func validateDerivedGraphIDs(label string, tasks []domain.Task, inputs []domain.Artifact) error {
	ids := make([]string, 0, len(tasks)+len(inputs))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	for _, input := range inputs {
		ids = append(ids, input.ID)
	}
	return domain.ValidateDerivedIDs(label, ids...)
}
