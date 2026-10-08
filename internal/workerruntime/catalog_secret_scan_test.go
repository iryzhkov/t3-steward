package workerruntime

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestCatalogLocalSecretScanSurvivesBusyCapacityThenActivation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local config.V2ResultSecretScan
	}{
		{"warn", config.V2ResultSecretScan{PatternPolicy: "warn", MaxObjectBytes: 32 << 20}},
		{"block", config.V2ResultSecretScan{PatternPolicy: "block", MaxObjectBytes: 32 << 20}},
		{"custom-cap", config.V2ResultSecretScan{PatternPolicy: "warn", MaxObjectBytes: 1234567}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, projection := catalogHostFixture(t)
			host.Options.Settings.ResultSecretScan = tc.local
			publishCatalog(t, host, "initial", CatalogRequest{Projection: projection})
			service := host.service
			assertScan := func(when string) {
				t.Helper()
				if got := host.Options.Settings.ResultSecretScan; got != tc.local {
					t.Errorf("%s: host scan = %+v, want %+v", when, got, tc.local)
				}
				got := host.service.Exchange.Custody.config.SecretScan
				if got.PatternPolicy != tc.local.PatternPolicy || got.MaxBytes != tc.local.MaxObjectBytes {
					t.Errorf("%s: custody scan policy=%q cap=%d, want %+v", when, got.PatternPolicy, got.MaxBytes, tc.local)
				}
			}
			assertScan("initial activation")
			record := AttemptRecord{Phase: PhaseRunning}
			record.Assignment.ID = "running"
			record.Package.Package.Identity.AssignmentID = "running"
			record.Package.Package.Environment.CatalogRevision = projection.Revision
			if err := service.Exchange.Runtime.journal.update(func(s *journalState) error {
				s.Attempts["running"] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			settings, err := projection.Settings(host.Bootstrap, host.Home)
			if err != nil {
				t.Fatal(err)
			}
			worker := settings.Workers[projection.WorkerID]
			worker.Executors.CPUUnits++
			settings.Workers[projection.WorkerID] = worker
			capacity, err := BuildCatalogProjection(settings, projection.WorkerID)
			if err != nil || capacity.Revision == projection.Revision {
				t.Fatalf("capacity catalog did not change: %v", err)
			}
			publishCatalog(t, host, "capacity", CatalogRequest{Projection: capacity, ExpectedRevision: projection.Revision})
			if host.service != service || host.retained.Projection.Revision != capacity.Revision {
				t.Fatal("busy capacity adoption did not keep the service and adopt the revision")
			}
			snapshot, err := service.Exchange.Runtime.journal.snapshot()
			if err != nil || snapshot.Attempts["running"].Phase != PhaseRunning {
				t.Fatalf("capacity adoption disturbed running attempt: %+v, %v", snapshot, err)
			}
			assertScan("busy capacity adoption")
			if err := service.Exchange.Runtime.journal.update(func(s *journalState) error {
				delete(s.Attempts, "running")
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for name, project := range settings.Projects {
				project.T3Project = ""
				settings.Projects[name] = project
				break
			}
			activation, err := BuildCatalogProjection(settings, projection.WorkerID)
			if err != nil || activation.Revision == capacity.Revision {
				t.Fatalf("activation catalog did not change: %v", err)
			}
			publishCatalog(t, host, "activation", CatalogRequest{Projection: activation, ExpectedRevision: capacity.Revision})
			if host.service == service || host.retained.Projection.Revision != activation.Revision {
				t.Fatal("idle project change did not fully activate a new service")
			}
			assertScan("full reactivation")
		})
	}
}

// The result secret scan is worker-local policy that no catalog carries. A
// persistent worker builds its runtime from each catalog it accepts, and once
// replaced the configured scan block with the defaults on every activation, so
// a warn policy set on the host had no effect.
func TestCatalogActivationKeepsTheWorkerResultSecretScan(t *testing.T) {
	local := config.V2ResultSecretScan{MaxObjectBytes: 32 << 20, PatternPolicy: "warn"}
	host, projection := catalogHostFixture(t)
	host.Options.Settings.ResultSecretScan = local
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	assertServedScan := func(host *CatalogHost, when string) {
		t.Helper()
		if host.service == nil {
			t.Fatalf("%s: no service", when)
		}
		served := host.service.Exchange.Custody.config.SecretScan
		if served.PatternPolicy != local.PatternPolicy || served.MaxBytes != local.MaxObjectBytes {
			t.Fatalf("%s: served result secret scan policy=%q max=%d, configured %+v", when, served.PatternPolicy, served.MaxBytes, local)
		}
	}
	assertServedScan(host, "published catalog")

	// A republished catalog builds the runtime again.
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	for name, project := range settings.Projects {
		project.T3Project = ""
		settings.Projects[name] = project
		break
	}
	changed, err := BuildCatalogProjection(settings, projection.WorkerID)
	if err != nil || changed.Revision == projection.Revision {
		t.Fatalf("catalog did not change: %v", err)
	}
	publishCatalog(t, host, "second", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision})
	if host.retained.Projection.Revision != changed.Revision {
		t.Fatal("republished catalog not applied")
	}
	assertServedScan(host, "republished catalog")

	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	restarted.Options.Settings = config.BacklogV2{ResultSecretScan: local}
	if err := restarted.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertServedScan(restarted, "retained catalog after restart")
}
