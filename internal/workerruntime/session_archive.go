package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/sessionarchive"
	"path/filepath"
)

// JournalArchiveStates is read-only: atomic journal publication provides one
// coherent view without epoch adoption or creating worker state.
func JournalArchiveStates(root string) (map[string]sessionarchive.State, error) {
	journal := &Journal{root: root, path: filepath.Join(root, "journal.json")}
	state, err := journal.read()
	if err != nil {
		return nil, err
	}
	out := map[string]sessionarchive.State{}
	for _, record := range state.Attempts {
		id := record.Package.Package.Identity.ThreadID
		if id == "" {
			continue
		}
		view := out[id]
		view.Background = true
		if record.Phase != PhaseCompleted || record.SettlePending || record.PendingThrottle != nil {
			view.Busy = "worker custody " + record.Assignment.ID
		}
		out[id] = view
	}
	return out, nil
}
