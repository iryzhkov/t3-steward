package workerruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// WorkerJournalSummary reports what a worker's durable journal still owns. It is
// read-only evidence for an operator or an updater deciding whether restarting
// the persistent worker interrupts execution.
type WorkerJournalSummary struct {
	WorkerID        string         `json:"workerId"`
	CatalogRevision string         `json:"catalogRevision,omitempty"`
	Attempts        int            `json:"attempts"`
	Dispatched      int            `json:"dispatched"`
	SettlePending   int            `json:"settlePending"`
	Phases          map[string]int `json:"phases"`
	// CatalogActivatable reports whether this build can reproduce the retained
	// projection's revision. False means the worker would start degraded and
	// await republication. The attempt counts are still authoritative, because
	// they come from the journal rather than from the catalog.
	CatalogActivatable bool `json:"catalogActivatable"`
}

// InspectWorkerJournal reads the retained catalog and journal of the worker
// configured in this home directory. A worker with no catalog or no journal owns
// no execution and reports zero counts.
func InspectWorkerJournal(home string) (WorkerJournalSummary, error) {
	bootstrap, _, err := LoadWorkerBootstrap(home)
	if err != nil {
		return WorkerJournalSummary{}, err
	}
	summary := WorkerJournalSummary{WorkerID: bootstrap.WorkerID, Phases: map[string]int{}}
	raw, err := readPrivateFile(filepath.Join(home, ".local/state/t3-steward/worker/catalog.json"), 8<<20)
	if errors.Is(err, os.ErrNotExist) {
		return summary, nil
	}
	if err != nil {
		return summary, err
	}
	var retained retainedCatalog
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&retained); err != nil {
		return summary, err
	}
	summary.CatalogRevision = retained.Projection.Revision
	settings, err := retained.Projection.Settings(bootstrap, home)
	summary.CatalogActivatable = err == nil
	if err != nil && !errors.Is(err, ErrCatalogProjectionDigestMismatch) {
		return summary, err
	}
	// A retained catalog this build cannot activate must not hide the journal.
	// An updater asks this question precisely when it is about to replace the
	// binary, which is the case that changes how a revision is derived, so
	// refusing here would deny the caller its answer exactly when it matters and
	// would leave the upgrade stuck. The storage paths used below are derived
	// from the host, not from the projection, so they remain valid.
	_, _, root := WorkerRoots(settings, bootstrap.WorkerID)
	attempts, err := JournalAttempts(root)
	if err != nil {
		return summary, err
	}
	for _, record := range attempts {
		summary.Attempts++
		summary.Phases[string(record.Phase)]++
		if record.SettlePending {
			summary.SettlePending++
		}
		switch record.Phase {
		case PhaseDispatching, PhaseRunning, PhaseStopping, PhaseCollecting:
			summary.Dispatched++
		}
	}
	return summary, nil
}
