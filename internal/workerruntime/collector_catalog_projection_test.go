package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"reflect"
	"testing"
	"time"
)

func collectorCatalogFixture(t *testing.T) (config.Config, WorkerBootstrap) {
	t.Helper()
	cfg := config.Default()
	cfg.BacklogV2 = testWorkerServiceSettings(t)
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{}
	cfg.BacklogV2.MessageLimits.MaxFiles = 100
	cfg.BacklogV2.Freshness.QuotaMaxAge = config.Duration(time.Minute)
	cfg.BacklogV2.Leases.RenewInterval = config.Duration(time.Minute)
	worker := cfg.BacklogV2.Workers["normandy"]
	worker.Connection = "persistent-ssh"
	worker.Epoch = "worker-1"
	worker.Capabilities = []string{"git", "huyang"}
	worker.Credential = "secretref:f02-protocol/normandy"
	worker.Providers = map[string]config.V2Provider{"claudeWorker": {QuotaPool: "claude", Models: []string{"claude-haiku-5-5"}}}
	cfg.BacklogV2.Workers["normandy"] = worker
	cfg.BacklogV2.Workers["collector"] = config.V2Worker{Providers: map[string]config.V2Provider{"claudeSource": {QuotaPool: "claude", Models: []string{"claude-fable-5-1"}}}}
	cfg.BacklogV2.QuotaPools["claude"] = config.V2QuotaPool{Provider: "claude", AccountID: "11111111-2222-3333-4444-555555555555", ObservationMaxAge: config.Duration(390 * time.Second), MaxConcurrent: 2, Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "collector", ProviderInstanceID: "claudeSource"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "fable", Models: []string{"claude-fable-5-1"}}}}}
	cfg.Policy.WarnPercent = 80
	cfg.Policy.DrainPercent = 88
	cfg.Resume.BelowPercent = 40
	cfg.Resume.ResetSettleDelay = config.Duration(17 * time.Second)
	cfg.BacklogV2.CollectorRecovery = cfg.CollectorRecoveryMetadataForWorker("")
	bootstrap := WorkerBootstrap{SchemaVersion: 1, WorkerID: "normandy", CoordinatorID: "coordinator", Transport: "ssh", Capabilities: worker.Capabilities, ProviderRoutes: []string{"claudeWorker"}, CredentialRef: worker.Credential}
	return cfg, bootstrap
}
func TestCollectorCatalogProjectionPreservesRecoveryWithoutExecutionExpansion(t *testing.T) {
	cfg, bootstrap := collectorCatalogFixture(t)
	projection, err := BuildCatalogProjection(cfg.BacklogV2, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	settings, err := projection.Settings(bootstrap, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(settings.Workers) != 1 || len(settings.Workers["normandy"].Providers) != 1 {
		t.Fatal("foreign execution route imported")
	}
	projected := config.Default()
	projected.BacklogV2 = settings
	route := domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claudeWorker", Model: "claude-haiku-5-5", QuotaPoolID: "claude"}
	originalPool, ok := cfg.CollectorCoverage(route)
	if !ok {
		t.Fatal("configured route not covered")
	}
	projectedPool, ok := projected.CollectorCoverage(route)
	if !ok {
		t.Fatal("projected route lost coverage")
	}
	if !reflect.DeepEqual(originalPool, projectedPool) || daemon.CollectorRecoveryDigest(cfg, originalPool, route) != daemon.CollectorRecoveryDigest(projected, projectedPool, route) {
		t.Fatal("full pool/policy digest changed during projection")
	}
	route.Model = "claude-fable-5-1"
	if _, ok := projected.CollectorCoverage(route); ok {
		t.Fatal("foreign pool model authorized for execution")
	}
	route.Model = "haiku"
	if _, ok := projected.CollectorCoverage(route); ok {
		t.Fatal("family alias authorized")
	}
	if _, ok := projected.CollectorCoverageForThread("claudeSource", "claude-fable-5-1"); ok {
		t.Fatal("source metadata became watchdog execution route")
	}
	if projected.ClaudeQuotaCollector.Enabled {
		t.Fatal("projection enabled credential acquisition")
	}
}
func TestCollectorCatalogRefreshRestartAndBusyCustody(t *testing.T) {
	ctx := context.Background()
	cfg, bootstrap := collectorCatalogFixture(t)
	projection, err := BuildCatalogProjection(cfg.BacklogV2, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	host := &CatalogHost{Home: t.TempDir(), Bootstrap: bootstrap, Options: WorkerServiceOptions{ProtocolCredentials: &staticProtocolResolver{credentials: testProtocolCredentials()}, ModelLoginFiles: []string{}, DryRun: true}}
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	request := CatalogRequest{Projection: projection}
	frame := mustEncodeEnvelope(t, codec, catalogEnvelope(t, "collector-first", request))
	raw, err := host.HandleFrame(ctx, frame)
	if err != nil {
		t.Fatal(err)
	}
	var envelope workerproto.Envelope
	if err := codec.Decode(bytes.NewReader(raw), &envelope); err != nil {
		t.Fatal(err)
	}
	var ack map[string]string
	if err := workerproto.DecodePayload(envelope, MessageCatalog, &ack); err != nil || ack["collectorProvenance"] != workerproto.CapabilityQuotaCollectorProvenance {
		t.Fatalf("signed capability ACK absent %v %+v", err, ack)
	}
	duplicate, err := host.HandleFrame(ctx, frame)
	if err != nil || !bytes.Equal(raw, duplicate) {
		t.Fatal("duplicate authenticated ACK changed")
	}
	guard, ok := host.service.Exchange.Runtime.config.Quota.(HostQuotaGuard)
	if !ok || guard.Config.BacklogV2.CollectorRecovery == nil {
		t.Fatal("activation did not install credential-free guard")
	}
	before, err := os.ReadFile(host.catalogPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.service.Exchange.Runtime.journal.update(func(s *journalState) error {
		record := AttemptRecord{Phase: PhaseRunning}
		record.Assignment.ID = "busy"
		record.Package.Package.Identity.AssignmentID = "busy"
		record.Package.Package.Environment.CatalogRevision = projection.Revision
		s.Attempts["busy"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var changed CatalogProjection
	encoded, _ := json.Marshal(projection)
	_ = json.Unmarshal(encoded, &changed)
	changed.CollectorRecovery.BelowPercent--
	changedSettings, err := changed.Settings(bootstrap, host.Home)
	if err != ErrCatalogProjectionDigestMismatch {
		t.Fatalf("policy mutation kept revision %v", err)
	}
	changed, err = BuildCatalogProjection(changedSettings, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "collector-busy", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision}))); err == nil {
		t.Fatal("busy coverage/policy replaced")
	}
	after, _ := os.ReadFile(host.catalogPath())
	if !bytes.Equal(before, after) {
		t.Fatal("busy refusal changed retained bytes")
	}
	if err := host.service.Exchange.Runtime.journal.update(func(s *journalState) error { delete(s.Attempts, "busy"); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "collector-change", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision}))); err != nil {
		t.Fatal(err)
	}
	restart := &CatalogHost{Home: host.Home, Bootstrap: bootstrap, Options: host.Options}
	if err := restart.Load(ctx); err != nil {
		t.Fatal(err)
	}
	guard = restart.service.Exchange.Runtime.config.Quota.(HostQuotaGuard)
	if guard.Config.CollectorRecoveryConfig().Resume.BelowPercent != changed.CollectorRecovery.BelowPercent {
		t.Fatal("restart lost shared recovery policy")
	}
	old, err := projection.Settings(bootstrap, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old.CollectorRecovery = nil
	legacy, err := BuildCatalogProjection(old, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Revision == projection.Revision || legacy.CollectorRecovery != nil {
		t.Fatal("coverage removal did not fence revision")
	}
}
