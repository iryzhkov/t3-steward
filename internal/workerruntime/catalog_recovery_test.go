package workerruntime

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// publishCatalog applies one catalog projection through the normal protocol.
func publishCatalog(t *testing.T, host *CatalogHost, id string, request CatalogRequest) {
	t.Helper()
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	if _, err := host.HandleFrame(context.Background(), mustEncodeEnvelope(t, codec, catalogEnvelope(t, id, request))); err != nil {
		t.Fatal(err)
	}
}

// rewriteRetainedRevision simulates a release whose catalog digest derivation
// changed: the stored projection is intact but no longer matches this build.
func rewriteRetainedRevision(t *testing.T, host *CatalogHost, revision string) {
	t.Helper()
	data, err := os.ReadFile(host.catalogPath())
	if err != nil {
		t.Fatal(err)
	}
	var retained retainedCatalog
	if err = json.Unmarshal(data, &retained); err != nil {
		t.Fatal(err)
	}
	retained.Projection.Revision = revision
	if data, err = json.Marshal(retained); err != nil {
		t.Fatal(err)
	}
	if err = writeCatalogFile(host.catalogPath(), data); err != nil {
		t.Fatal(err)
	}
}

func snapshotFrame(t *testing.T) []byte {
	t.Helper()
	now := time.Now()
	envelope, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, "snapshot", "snapshot", "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	return mustEncodeEnvelope(t, workerproto.Codec{MaxBytes: 8 << 20}, envelope)
}

func TestUnusableRetainedCatalogWaitsForRepublication(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	rewriteRetainedRevision(t, host, "derived-by-another-release")

	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err := restarted.Load(ctx); err != nil {
		t.Fatalf("unusable retained catalog stranded the worker: %v", err)
	}
	if restarted.unusable == nil || restarted.service != nil || restarted.retained == nil {
		t.Fatalf("unusable=%v service=%v retained=%v", restarted.unusable, restarted.service != nil, restarted.retained != nil)
	}
	if _, err := restarted.HandleFrame(ctx, snapshotFrame(t)); err == nil ||
		!strings.Contains(err.Error(), "no accepted catalog") {
		t.Fatalf("execution served without a usable catalog: %v", err)
	}

	// The coordinator's expected revision cannot match a digest this build never
	// derives, so republication must not be fenced by it.
	publishCatalog(t, restarted, "republish", CatalogRequest{Projection: projection, ExpectedRevision: "obsolete"})
	if restarted.unusable != nil || restarted.service == nil {
		t.Fatalf("republication did not recover the worker: unusable=%v", restarted.unusable)
	}
	if _, err := restarted.HandleFrame(ctx, snapshotFrame(t)); err != nil {
		t.Fatalf("recovered worker refused a snapshot: %v", err)
	}
}

func TestUnusableRetainedCatalogStillDrainsBeforeChange(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})

	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	_, _, root := WorkerRoots(settings, host.Bootstrap.WorkerID)
	journal, err := OpenJournal(root, host.Bootstrap.WorkerID, projection.Worker.Epoch, 9)
	if err != nil {
		t.Fatal(err)
	}
	record := AttemptRecord{Phase: PhaseRunning}
	record.Assignment.ID = "assignment-live"
	record.Package.Package.Identity.AssignmentID = "assignment-live"
	if err = journal.update(func(state *journalState) error {
		state.Attempts["assignment-live"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rewriteRetainedRevision(t, host, "derived-by-another-release")

	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err = restarted.Load(ctx); err != nil {
		t.Fatal(err)
	}
	for name, project := range settings.Projects {
		project.T3Project = ""
		settings.Projects[name] = project
		break
	}
	changed, err := BuildCatalogProjection(settings, projection.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Revision == projection.Revision {
		t.Fatal("catalog did not change")
	}
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	_, err = restarted.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "change", CatalogRequest{Projection: changed})))
	if err == nil || !strings.Contains(err.Error(), "draining retained execution") {
		t.Fatalf("catalog change bypassed the drain guard: %v", err)
	}
}
