package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
)

func TestManagedCatalogReloadWithRetainedExecution(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		phase                             Phase
		confirmed, stop, pending, allowed bool
	}{
		{"confirmed cancellation", PhaseStopped, true, true, false, true},
		{"unconfirmed cancellation", PhaseStopped, false, true, false, false},
		{"natural stop", PhaseStopped, false, false, false, false},
		{"throttle stop", PhaseStopped, true, false, false, false},
		{"pending settlement", PhaseStopped, true, true, true, false},
		{"running", PhaseRunning, false, false, false, false},
		{"completed", PhaseCompleted, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			host, projection := catalogHostFixture(t)
			codec := workerproto.Codec{MaxBytes: 8 << 20}
			if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "initial", CatalogRequest{Projection: projection}))); err != nil {
				t.Fatal(err)
			}
			record := AttemptRecord{Phase: tc.phase, StopConfirmed: tc.confirmed, SettlePending: tc.pending}
			record.Assignment.ID = "retained"
			record.Package.Package.Identity.AssignmentID = "retained"
			if tc.stop {
				record.CommandRequests = map[string]domain.WorkerCommand{"stop": {Kind: domain.WorkerCommandStop}}
			}
			if err := host.service.Exchange.Runtime.journal.update(func(s *journalState) error { s.Attempts["retained"] = record; return nil }); err != nil {
				t.Fatal(err)
			}
			settings, err := projection.Settings(host.Bootstrap, host.Home)
			if err != nil {
				t.Fatal(err)
			}
			for _, project := range settings.Projects {
				project.T3Project = ""
				settings.Projects["managed"] = project
				break
			}
			changed, err := BuildCatalogProjection(settings, projection.WorkerID)
			if err != nil {
				t.Fatal(err)
			}
			if changed.Revision == projection.Revision {
				t.Fatal("catalog did not change")
			}
			_, err = host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "managed", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision})))
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v error=%v", tc.allowed, err)
			}
			restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
			if err := restarted.Load(ctx); err != nil {
				t.Fatal(err)
			}
			want := projection.Revision
			if tc.allowed {
				want = changed.Revision
			}
			if restarted.retained.Projection.Revision != want {
				t.Fatal("durable catalog changed incorrectly")
			}
			state, err := restarted.service.Exchange.Runtime.journal.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			got, ok := state.Attempts["retained"]
			if !ok || got.Phase != record.Phase || got.StopConfirmed != record.StopConfirmed || got.SettlePending != record.SettlePending {
				t.Fatal("retained evidence changed")
			}
		})
	}
}
