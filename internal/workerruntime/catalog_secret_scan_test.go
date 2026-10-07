package workerruntime

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

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
