package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
	"time"
)

func TestCatalogRotationRebindsAuthenticationWithoutChangingExecutionIdentity(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	first := catalogEnvelope(t, "initial", CatalogRequest{Projection: projection})
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, first)); err != nil {
		t.Fatal(err)
	}
	resolver := host.Options.ProtocolCredentials.(*staticProtocolResolver)
	resolver.credentials.CoordinatorKeyID = "coordinator-rotated"
	resolver.credentials.CoordinatorSecret = []byte("new-coordinator-secret")
	resolver.credentials.WorkerKeyID = "worker-rotated"
	resolver.credentials.WorkerSecret = []byte("new-worker-secret")
	rotated := catalogEnvelope(t, "rotation", CatalogRequest{Projection: projection, ExpectedRevision: projection.Revision})
	if err := workerproto.SignEnvelope(&rotated, resolver.credentials.CoordinatorPrincipal, resolver.credentials.CoordinatorKeyID, resolver.credentials.CoordinatorSecret); err != nil {
		t.Fatal(err)
	}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, rotated)); err != nil {
		t.Fatal(err)
	}
	if host.activeCredentials.CoordinatorKeyID != "coordinator-rotated" || host.retained.Projection.Revision != projection.Revision {
		t.Fatal("rotation failed or changed execution identity")
	}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, first)); err == nil {
		t.Fatal("retired credential accepted")
	}
	now := time.Now()
	snapshot, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, "after-rotation", "after-rotation", "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(time.Minute), workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err = workerproto.SignEnvelope(&snapshot, resolver.credentials.CoordinatorPrincipal, resolver.credentials.CoordinatorKeyID, resolver.credentials.CoordinatorSecret); err != nil {
		t.Fatal(err)
	}
	if _, err = host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, snapshot)); err != nil {
		t.Fatal("runtime retained retired credentials", err)
	}
}

func TestCatalogCannotReplaceUnsettledCompletedExecution(t *testing.T) {
	ctx := context.Background()
	host, projection := catalogHostFixture(t)
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, catalogEnvelope(t, "initial", CatalogRequest{Projection: projection}))); err != nil {
		t.Fatal(err)
	}
	if err := host.service.Exchange.Runtime.journal.update(func(state *journalState) error {
		state.Attempts["retained"] = AttemptRecord{Phase: PhaseCompleted, SettlePending: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	settings.Leases.Duration += config.Duration(time.Second)
	changed, err := BuildCatalogProjection(settings, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if changed.Revision == projection.Revision {
		t.Fatal("policy change lost identity")
	}
	request := catalogEnvelope(t, "changed", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision})
	if _, err := host.HandleFrame(ctx, mustEncodeEnvelope(t, codec, request)); err == nil {
		t.Fatal("unsettled execution replaced")
	}
	if host.retained.Projection.Revision != projection.Revision {
		t.Fatal("failed reload changed effective catalog")
	}
}
