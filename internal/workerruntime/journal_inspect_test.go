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

// An updater asks for the journal precisely when it is about to replace the
// binary, and replacing the binary is what changes how a catalog revision is
// derived. Inspection that refused a catalog this build cannot activate would
// therefore fail exactly when it is needed, leaving the upgrade stuck with no
// way to learn whether a restart would interrupt execution.
func TestInspectWorkerJournalReadsAnUnactivatableCatalog(t *testing.T) {
	host, projection := catalogHostFixture(t)
	bootstrapWorkerFile(t, host)
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})

	record := AttemptRecord{Phase: PhaseRunning}
	record.Assignment.ID = "assignment-live"
	record.Package.Package.Identity.AssignmentID = "assignment-live"
	if err := host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["assignment-live"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	activatable, err := InspectWorkerJournal(host.Home)
	if err != nil {
		t.Fatal(err)
	}
	if !activatable.CatalogActivatable {
		t.Fatalf("a catalog this build published reported itself unactivatable: %+v", activatable)
	}

	rewriteRetainedRevision(t, host, "derived-by-another-release")

	summary, err := InspectWorkerJournal(host.Home)
	if err != nil {
		t.Fatalf("an unactivatable catalog hid the journal from an updater: %v", err)
	}
	if summary.CatalogActivatable {
		t.Fatalf("a revision this build never derives reported itself activatable: %+v", summary)
	}
	// The counts come from the journal, not from the catalog, so they stay
	// authoritative and an updater can still see that a restart would interrupt
	// a dispatched attempt.
	if summary.Attempts != 1 || summary.Dispatched != 1 || summary.Phases[string(PhaseRunning)] != 1 {
		t.Fatalf("journal counts were lost with the catalog: %+v", summary)
	}
	if summary.CatalogRevision != "derived-by-another-release" {
		t.Fatalf("retained revision = %q", summary.CatalogRevision)
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
