package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCatalogHostAppliesAdmissionSettingWithoutReplacingExecution(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy=%t", busy), func(t *testing.T) {
			ctx := context.Background()
			host, projection := catalogHostFixture(t)
			settings, err := projection.Settings(host.Bootstrap, host.Home)
			if err != nil {
				t.Fatal(err)
			}
			worker := settings.Workers["normandy"]
			worker.AcceptBacklog = false
			settings.Workers["normandy"] = worker
			projection, err = BuildCatalogProjection(settings, "normandy")
			if err != nil {
				t.Fatal(err)
			}
			codec := workerproto.Codec{MaxBytes: 8 << 20}
			push := func(id string, p CatalogProjection) {
				t.Helper()
				envelope := catalogEnvelope(t, id, CatalogRequest{Projection: p, ExpectedRevision: projection.Revision})
				raw, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, envelope))
				if err != nil {
					t.Fatal(err)
				}
				var response workerproto.Envelope
				if err := codec.Decode(bytes.NewReader(raw), &response); err != nil {
					t.Fatal(err)
				}
				var accepted map[string]string
				if err := workerproto.DecodePayload(response, MessageCatalog, &accepted); err != nil || accepted["revision"] != p.Revision {
					t.Fatalf("catalog push refused: %+v %v", response, err)
				}
			}
			push("initial", projection)
			service := host.service
			runtime := service.Exchange.Runtime
			// Keep inventory observation deterministic so the snapshot tests
			// coordinator policy independently of host tool availability.
			runtime.config.ObserveInventory = func(_ context.Context, wanted domain.WorkerInventory) (domain.WorkerInventory, error) {
				return wanted, nil
			}
			if busy {
				for id, phase := range map[string]Phase{"running": PhaseRunning, "parked": PhaseWaiting} {
					record := AttemptRecord{Phase: phase}
					record.Assignment.ID = id
					record.Package.Package.Identity.AssignmentID = id
					record.Package.Package.Environment.CatalogRevision = projection.Revision
					if err := runtime.journal.update(func(s *journalState) error { s.Attempts[id] = record; return nil }); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, err := runtime.journal.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			driver := runtime.driver
			for i, accept := range []bool{true, false} {
				worker.AcceptBacklog = accept
				settings.Workers["normandy"] = worker
				changed, err := BuildCatalogProjection(settings, "normandy")
				if err != nil {
					t.Fatal(err)
				}
				if changed.Revision != projection.Revision {
					t.Fatal("admission change replaced execution revision")
				}
				push(fmt.Sprintf("admission-%d", i), changed)
				if host.service != service || runtime.driver != driver {
					t.Fatal("admission change replaced runtime or execution driver")
				}
				after, err := runtime.journal.snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before.Attempts, after.Attempts) {
					t.Fatal("admission change disturbed retained execution")
				}
				snapshot, err := runtime.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.Inventory.AcceptBacklog != accept || runtime.config.Inventory.AcceptBacklog != accept {
					t.Fatalf("worker did not report accept_backlog=%t: %+v", accept, snapshot.Inventory)
				}
				if host.Options.Settings.Workers["normandy"].AcceptBacklog != accept {
					t.Fatal("host settings did not adopt admission change")
				}
				before, err = runtime.journal.snapshot()
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(host.catalogPath())
				if err != nil {
					t.Fatal(err)
				}
				var retained retainedCatalog
				if err := json.Unmarshal(data, &retained); err != nil {
					t.Fatal(err)
				}
				if retained.Projection.Worker.AcceptBacklog != accept || retained.Projection.Revision != projection.Revision {
					t.Fatal("durable catalog did not adopt admission change")
				}
				restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
				if err := restarted.Load(ctx); err != nil {
					t.Fatal(err)
				}
				if restarted.unusable != nil || restarted.service.Exchange.Runtime.desiredInventory.AcceptBacklog != accept {
					t.Fatalf("restart lost admission change: %v", restarted.unusable)
				}
			}
		})
	}
}
