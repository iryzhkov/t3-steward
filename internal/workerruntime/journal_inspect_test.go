package workerruntime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// bootstrapWorkerFile writes the private bootstrap this host would receive from
// UpKeeper, so journal inspection can resolve the worker identity.
func bootstrapWorkerFile(t *testing.T, host *CatalogHost) {
	t.Helper()
	directory := filepath.Join(host.Home, ".config/t3-steward")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{
		"schema_version": host.Bootstrap.SchemaVersion, "worker_id": host.Bootstrap.WorkerID,
		"coordinator_id": host.Bootstrap.CoordinatorID, "transport": host.Bootstrap.Transport,
		"capabilities": host.Bootstrap.Capabilities, "provider_routes": host.Bootstrap.ProviderRoutes,
		"credential_ref": host.Bootstrap.CredentialRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "worker-bootstrap.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestInspectWorkerJournalReportsDispatchedExecution(t *testing.T) {
	host, projection := catalogHostFixture(t)
	bootstrapWorkerFile(t, host)

	empty, err := InspectWorkerJournal(host.Home)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Attempts != 0 || empty.Dispatched != 0 || empty.CatalogRevision != "" {
		t.Fatalf("worker without a catalog reported %+v", empty)
	}

	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	record := AttemptRecord{Phase: PhaseRunning}
	record.Assignment.ID = "assignment-live"
	record.Package.Package.Identity.AssignmentID = "assignment-live"
	if err = host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["assignment-live"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	busy, err := InspectWorkerJournal(host.Home)
	if err != nil {
		t.Fatal(err)
	}
	if busy.WorkerID != "normandy" || busy.CatalogRevision != projection.Revision ||
		busy.Attempts != 1 || busy.Dispatched != 1 || busy.Phases[string(PhaseRunning)] != 1 {
		t.Fatalf("dispatched execution reported %+v", busy)
	}

	settled := record
	settled.Phase = PhaseCompleted
	if err = host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["assignment-live"] = settled
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	idle, err := InspectWorkerJournal(host.Home)
	if err != nil {
		t.Fatal(err)
	}
	if idle.Dispatched != 0 || idle.Attempts != 1 {
		t.Fatalf("settled execution reported %+v", idle)
	}
}
