package workerruntime

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// fakeCampaignRefs is a worker-local campaign ref store that records what it
// was asked to release.
type fakeCampaignRefs struct {
	held     []string
	released []string
	listErr  error
	failOn   string
}

func (f *fakeCampaignRefs) Runs() ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return slices.Clone(f.held), nil
}

func (f *fakeCampaignRefs) ReleaseRun(_ context.Context, runID string, _ io.Writer) error {
	if runID == f.failOn {
		return errors.New("git refused")
	}
	f.released = append(f.released, runID)
	f.held = slices.DeleteFunc(f.held, func(held string) bool { return held == runID })
	return nil
}

func runtimeWithCampaignRefs(t *testing.T, refs CampaignRefCustodian) *Runtime {
	t.Helper()
	runtime := newTestRuntime(t, t.TempDir(), &fakeDriver{})
	runtime.config.CampaignRefs = refs
	return runtime
}

// A worker on another host cannot see coordinator state, so it is told which
// campaigns are still alive and releases the rest. Without this its ref store
// grows for as long as the worker exists.
func TestWorkerReleasesCampaignRunsTheCoordinatorNoLongerRetains(t *testing.T) {
	refs := &fakeCampaignRefs{held: []string{"run-done", "run-live", "run-unknown"}}
	runtime := runtimeWithCampaignRefs(t, refs)
	request := workerproto.SnapshotRequest{
		CampaignRefsReported: true,
		RetainedCampaignRuns: []string{"run-live", "run-not-on-this-worker"},
	}
	if err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if strings.Join(refs.released, ",") != "run-done,run-unknown" {
		t.Fatalf("released %v, want the runs the statement did not name", refs.released)
	}
	if strings.Join(refs.held, ",") != "run-live" {
		t.Fatalf("held %v after the statement", refs.held)
	}
	// Applying the same statement again releases nothing new: what is held is
	// what was retained.
	refs.released = nil
	if err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(refs.released) != 0 {
		t.Fatalf("a repeated statement released %v", refs.released)
	}
}

// Silence is not a statement. An older coordinator sends no list, and reading
// that as "retain nothing" would delete every campaign commit on the worker.
func TestWorkerKeepsEverythingWhenTheCoordinatorSaysNothing(t *testing.T) {
	refs := &fakeCampaignRefs{held: []string{"run-1", "run-2"}}
	runtime := runtimeWithCampaignRefs(t, refs)
	if err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), workerproto.SnapshotRequest{
		ParkedReported: true,
	}); err != nil {
		t.Fatal(err)
	}
	if len(refs.released) != 0 {
		t.Fatalf("an unreported statement released %v", refs.released)
	}
}

// A statement that cannot be trusted whole is refused whole, and nothing is
// released on the strength of the part that parsed.
func TestWorkerRefusesAnInvalidCampaignStatementWithoutReleasingAnything(t *testing.T) {
	refs := &fakeCampaignRefs{held: []string{"run-1"}}
	runtime := runtimeWithCampaignRefs(t, refs)
	err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), workerproto.SnapshotRequest{
		RetainedCampaignRuns: []string{"run-2"},
	})
	if err == nil || !strings.Contains(err.Error(), "without the reported flag") {
		t.Fatalf("error = %v", err)
	}
	if len(refs.released) != 0 {
		t.Fatalf("a refused statement released %v", refs.released)
	}
}

// Releasing is housekeeping. A ref that cannot be deleted, or a store that
// cannot be listed, is reported and retried on the next exchange; it never
// fails the exchange, because a leftover ref costs disk and a failed exchange
// costs work.
func TestWorkerCampaignReleaseFailureNeverFailsTheExchange(t *testing.T) {
	refs := &fakeCampaignRefs{held: []string{"run-broken", "run-done"}, failOn: "run-broken"}
	runtime := runtimeWithCampaignRefs(t, refs)
	request := workerproto.SnapshotRequest{CampaignRefsReported: true}
	if err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), request); err != nil {
		t.Fatalf("a failed release failed the exchange: %v", err)
	}
	if strings.Join(refs.released, ",") != "run-done" {
		t.Fatalf("released %v; one failure must not stop the others", refs.released)
	}
	if !slices.Contains(refs.held, "run-broken") {
		t.Fatal("the failed run was dropped from the store's own view")
	}

	unreadable := &fakeCampaignRefs{held: []string{"run-1"}, listErr: errors.New("permission denied")}
	if err := runtimeWithCampaignRefs(t, unreadable).ReleaseUnretainedCampaignRuns(
		context.Background(), request); err != nil {
		t.Fatalf("an unreadable store failed the exchange: %v", err)
	}
	if len(unreadable.released) != 0 {
		t.Fatalf("an unreadable store released %v", unreadable.released)
	}
}

// A worker that keeps no campaign commits has nothing to release and must not
// fail the exchange over it.
func TestWorkerWithoutACampaignStoreIgnoresTheStatement(t *testing.T) {
	runtime := newTestRuntime(t, t.TempDir(), &fakeDriver{})
	if err := runtime.ReleaseUnretainedCampaignRuns(context.Background(), workerproto.SnapshotRequest{
		CampaignRefsReported: true, RetainedCampaignRuns: []string{"run-1"},
	}); err != nil {
		t.Fatal(err)
	}
}
