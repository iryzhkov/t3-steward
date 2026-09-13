package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
)

func TestFreshCatalogProjectionReachesWorker(t *testing.T) {
	host, projection := catalogHostFixture(t)
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range settings.Projects {
		p.Type = "fresh"
		p.Repository = ""
		p.DefaultRef = ""
		p.T3Project = ""
		p.SetupProfile = ""
		settings.Projects["fresh"] = p
		break
	}
	next, err := BuildCatalogProjection(settings, projection.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if next.Revision == projection.Revision {
		t.Fatal("fresh project did not fence catalog")
	}
	env := catalogEnvelope(t, "fresh-catalog", CatalogRequest{Projection: next})
	if _, err := host.HandleFrame(context.Background(), mustEncodeEnvelope(t, workerproto.Codec{MaxBytes: 8 << 20}, env)); err != nil {
		t.Fatal(err)
	}
	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err := restarted.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.retained.Projection.Projects["fresh"].Type != "fresh" {
		t.Fatal("mode lost across restart")
	}
}
