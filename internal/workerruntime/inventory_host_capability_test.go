package workerruntime

import (
	"context"
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Observed host capabilities are advertised beside the configured ones, once
// each, and the snapshot carries them after the build's own are merged in.
// Their absence never degrades the worker: a task that needs one waits for a
// capable worker, while a configured capability that fails its probe still
// does.
func TestHostInventoryProbeAdvertisesObservedHostCapabilities(t *testing.T) {
	probe := HostInventoryProbe{
		DataDir:    t.TempDir(),
		Capability: func(_ context.Context, name string) bool { return name == "git" },
		Observed:   []string{workerproto.CapabilityAskRelay, "git"},
	}
	wanted := domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Capabilities: []string{"git"}}
	got, err := probe.Observe(context.Background(), wanted)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Capabilities, []string{"git", workerproto.CapabilityAskRelay}) || got.Health != domain.WorkerHealthReady || !got.AcceptBacklog {
		t.Fatalf("inventory = %+v", got)
	}
	if advertised := AdvertisedCapabilities(got.Capabilities); !slices.Contains(advertised, workerproto.CapabilityAskRelay) {
		t.Fatalf("advertised = %v", advertised)
	}

	probe.Observed = nil
	got, err = probe.Observe(context.Background(), wanted)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Capabilities, workerproto.CapabilityAskRelay) || got.Health != domain.WorkerHealthReady {
		t.Fatalf("inventory without the host capability = %+v", got)
	}
}
