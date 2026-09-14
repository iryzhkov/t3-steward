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
	if err != nil {
		return summary, err
	}
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
