package workerruntime

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"testing"
	"time"
)

func catalogHostFixture(t *testing.T) (*CatalogHost, CatalogProjection) {
	t.Helper()
	settings := testWorkerServiceSettings(t)
	w := settings.Workers["normandy"]
	w.Epoch = "worker-1"
	w.Connection = "persistent-ssh"
	w.Capabilities = []string{"git", "huyang"}
	w.Credential = "secretref:f02-protocol/normandy"
	settings.Workers["normandy"] = w
	settings.MessageLimits.MaxFiles = 100
	settings.Freshness.QuotaMaxAge = config.Duration(time.Minute)
	settings.Leases.RenewInterval = config.Duration(time.Minute)
	projection, err := BuildCatalogProjection(settings, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	host := &CatalogHost{Home: t.TempDir(), Bootstrap: WorkerBootstrap{SchemaVersion: 1, WorkerID: "normandy", CoordinatorID: "coordinator", Transport: "ssh", Capabilities: w.Capabilities, ProviderRoutes: []string{"codex"}, CredentialRef: w.Credential}, Options: WorkerServiceOptions{ProtocolCredentials: &staticProtocolResolver{credentials: testProtocolCredentials()}, DryRun: true}}
	return host, projection
}
func catalogEnvelope(t *testing.T, id string, request CatalogRequest) workerproto.Envelope {
	t.Helper()
	now := time.Now()
	env, err := workerproto.NewEnvelope(MessageCatalog, id, id, "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&env, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	return env
}
func TestCatalogHostAuthenticationReplayRestartAndRevisionFence(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	request := CatalogRequest{Projection: projection}
	env := catalogEnvelope(t, "first", request)
	forged := env
	forged.Authentication.Signature = "forged"
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, forged)); err == nil {
		t.Fatal("forged catalog accepted")
	}
	if _, err := os.Stat(host.catalogPath()); !os.IsNotExist(err) {
		t.Fatal("forged request published catalog", err)
	}
	raw := mustEncodeEnvelope(t, codec, env)
	first, err := host.HandleFrame(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := host.HandleFrame(ctx, raw)
	if err != nil || string(first) != string(replay) {
		t.Fatal("catalog replay changed", err)
	}
	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err = restarted.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.retained.Projection.Revision != projection.Revision {
		t.Fatal("catalog lost on restart")
	}
	// Ordinary snapshots use the same runtime after reconnect, with no catalog copy in bootstrap.
	now := time.Now()
	snapshot, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, "snapshot", "snapshot", "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&snapshot, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.HandleFrame(ctx, mustEncodeEnvelope(t, codec, snapshot)); err != nil {
		t.Fatal(err)
	}
	// A coordinator cannot silently overwrite a newer catalog by replaying an old configuration.
	changed := projection
	changed.Revision = "different"
	rejected := catalogEnvelope(t, "stale", CatalogRequest{Projection: changed, ExpectedRevision: "obsolete"})
	if _, err = restarted.HandleFrame(ctx, mustEncodeEnvelope(t, codec, rejected)); err == nil {
		t.Fatal("stale catalog replaced current")
	}
	data, err := os.ReadFile(host.catalogPath())
	if err != nil {
		t.Fatal(err)
	}
	var retained retainedCatalog
	if err = json.Unmarshal(data, &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Projection.Revision != projection.Revision {
		t.Fatal("failed reload changed retained catalog")
	}
}
