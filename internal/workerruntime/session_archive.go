package workerruntime

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/sessionarchive"
)

// HostJournalRoots are the worker journals under this host's own worker
// storage, whatever worker identities have run here.
//
// A worker whose catalog comes from the private bootstrap has no
// backlog_v2.storage in any configuration file, so a caller that derives the
// journal only from configured storage finds none -- and then every thread the
// steward dispatched here looks like a person's own session. That is what kept
// finished task and supervision sessions in the T3 list for a day instead of
// the two hours a background session gets.
//
// Every worker identity is listed rather than the current one alone, because a
// thread dispatched under a previous identity is still not a person's session.
func HostJournalRoots(home string) []string {
	workspaces := config.WorkerWorkspacesRoot(home)
	if workspaces == "" {
		return nil
	}
	matches, err := filepath.Glob(filepath.Join(workspaces, "workers", "*", "journal"))
	if err != nil {
		return nil
	}
	roots := make([]string, 0, len(matches))
	for _, root := range matches {
		if info, err := os.Stat(filepath.Join(root, "journal.json")); err != nil || !info.Mode().IsRegular() {
			continue
		}
		roots = append(roots, root)
	}
	sort.Strings(roots)
	return roots
}

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
