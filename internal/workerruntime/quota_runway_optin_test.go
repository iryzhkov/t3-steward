package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var incidentBucket = domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}

// incidentGuard is the host state of the incident at 00:40 on 2026-10-07,
// observed at now: the five-hour bucket warned at 86%, burning 0.8% a minute
// toward a 90% drain threshold.
func incidentGuard(now time.Time) HostQuotaGuard {
	return HostQuotaGuard{Buckets: runwayBuckets{{Key: incidentBucket, Phase: domain.PhaseWarned, UsedPercent: 86, Healthy: true,
		RatePerMinute: 0.8, ObservedAt: now, AppliedThresholds: &domain.ThresholdSet{WarnPercent: 85, DrainPercent: 90, StopPercent: 95}}}}
}

// snapshotSource is the coordinator's durable record of the worker's last
// snapshot, which decides what the coordinator asks of it.
type snapshotSource []domain.WorkerSnapshot

func (s snapshotSource) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return s, nil
}

type emptyQuotaStore struct{}

func (emptyQuotaStore) ListBuckets(context.Context) ([]domain.BucketState, error) { return nil, nil }
func (emptyQuotaStore) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}
func (emptyQuotaStore) CommitQuotaAdmissionTransitions(context.Context, []domain.QuotaAdmissionTransition) error {
	return nil
}

// exchangeSnapshot runs one snapshot exchange the way the coordinator and the
// worker do: the coordinator builds its request from the worker's previous
// snapshot, the worker answers, and the coordinator decodes the answer
// strictly.
func exchangeSnapshot(t *testing.T, r *Runtime, previous domain.WorkerSnapshot) (workerproto.SnapshotRequest, domain.WorkerSnapshot, []byte) {
	t.Helper()
	request, err := backlog.ParkedAssignmentsFor(context.Background(), snapshotSource{previous}, previous.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyParkedAssignments(request); err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded domain.WorkerSnapshot
	if err := (workerproto.Codec{MaxBytes: 1 << 20}).Decode(bytes.NewReader(raw), &decoded); err != nil {
		t.Fatalf("coordinator refuses the worker snapshot: %v\n%s", err, raw)
	}
	return request, decoded, raw
}

func admissionBlockers(t *testing.T, now time.Time, snapshot domain.WorkerSnapshot) []backlog.PlanningBlocker {
	t.Helper()
	bridge := backlog.QuotaBridge{Store: emptyQuotaStore{}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
		Pools: []backlog.QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}}}
	report, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{snapshot})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := backlog.NewQuotaAdmissionPolicy(backlog.QuotaAdmissionInput{Windows: report.Windows, MaxObservationAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	candidate := backlog.PlanningCandidate{Task: domain.Task{Class: domain.TaskClassRequired}, Attempt: domain.Attempt{ID: "attempt"},
		Route:    &domain.ProviderRoute{QuotaPoolID: "claude-main"},
		Estimate: &backlog.TaskAdmissionEstimate{ExpectedRuntime: 5 * time.Minute, CheckpointMargin: time.Minute}}
	return policy.StartPlan(now).Evaluate(candidate)
}

func hasBlocker(blockers []backlog.PlanningBlocker, code string) bool {
	for _, b := range blockers {
		if string(b.Code) == code {
			return true
		}
	}
	return false
}

// The incident's dispatch failure, end to end through the real producer: a
// coordinator that sees quota-runway-v1 asks for the runway, the worker sends
// the projected drain crossing at 00:45, and a task needing six minutes waits
// with the quota-drain-runway blocker instead of starting.
func TestWorkerRunwayReachesAdmissionWhenAsked(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	r := runningRuntime(t, driver, incidentGuard(now), &now)
	previous := domain.WorkerSnapshot{WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch, Sequence: 1,
		Inventory: domain.WorkerInventory{Capabilities: AdvertisedCapabilities(nil)}}
	request, snapshot, _ := exchangeSnapshot(t, r, previous)
	if !request.QuotaObservationsWanted || !request.QuotaRunwayWanted {
		t.Fatalf("coordinator did not ask this build for the runway: %+v", request)
	}
	if len(snapshot.QuotaObservations) != 1 {
		t.Fatalf("observations = %+v", snapshot.QuotaObservations)
	}
	observed := snapshot.QuotaObservations[0]
	if observed.DrainPercent != 90 || observed.RatePerMinute != 0.8 || observed.DrainsAt == nil || !observed.DrainsAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("runway observation = %+v", observed)
	}
	if blockers := admissionBlockers(t, now, snapshot); !hasBlocker(blockers, string(backlog.PlanningBlockerQuotaDrainRunway)) {
		t.Fatalf("worker reaches drain in 5m but a 6m task is admitted: blockers=%+v", blockers)
	}
}

// A coordinator that asks for observations but not for the runway (an rc.115
// coordinator, or this build's coordinator facing an older worker's
// capability list) receives observations its strict decoder accepts, and
// admission behaves as it did before the runway existed.
func TestWorkerRunwayIsWithheldUnlessAsked(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
	r := runningRuntime(t, driver, incidentGuard(now), &now)
	// The previous snapshot's capabilities without quota-runway-v1, as a
	// coordinator that predates it reads them: it never asks.
	var older []string
	for _, capability := range AdvertisedCapabilities(nil) {
		if capability != workerproto.CapabilityQuotaRunway {
			older = append(older, capability)
		}
	}
	previous := domain.WorkerSnapshot{WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch, Sequence: 1, Inventory: domain.WorkerInventory{Capabilities: older}}
	request, snapshot, raw := exchangeSnapshot(t, r, previous)
	if !request.QuotaObservationsWanted || request.QuotaRunwayWanted {
		t.Fatalf("request = %+v", request)
	}
	var envelope struct {
		QuotaObservations []json.RawMessage `json:"quotaObservations"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.QuotaObservations) != 1 {
		t.Fatalf("snapshot observations = %s (%v)", raw, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.QuotaObservations[0]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rc115WorkerQuotaObservation{}); err != nil {
		t.Fatalf("rc.115 coordinator rejects the unasked observation %s: %v", envelope.QuotaObservations[0], err)
	}
	if blockers := admissionBlockers(t, now, snapshot); len(blockers) != 0 {
		t.Fatalf("observation without runway changed admission: %+v", blockers)
	}
	// A request that sets the runway flag without asking for observations
	// asks for nothing.
	if err := r.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, QuotaRunwayWanted: true}); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Snapshot(context.Background()); err != nil || got.QuotaObservations != nil {
		t.Fatalf("runway flag alone produced observations: %+v %v", got.QuotaObservations, err)
	}
}

func TestHostRunwayObservationFallbackAndNoProjection(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		used, rate float64
		projected  bool
	}{
		{"configured ladder", 86, 0.8, true}, {"unknown rate", 86, 0, false}, {"falling rate", 86, -1, false}, {"already draining", 90, 0.8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := now.Add(time.Minute)
			cfg := config.Default()
			cfg.Policy.DrainPercent = 90
			guard := HostQuotaGuard{Config: cfg, Buckets: runwayBuckets{{Key: incidentBucket, UsedPercent: tc.used, RatePerMinute: tc.rate, ObservedAt: now, DrainDeadline: &deadline}}}
			got, err := guard.RunwayObservations(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].DrainPercent != 90 || (got[0].DrainsAt != nil) != tc.projected || got[0].DrainDeadline == nil || !got[0].DrainDeadline.Equal(deadline) {
				t.Fatalf("observation: %+v", got)
			}
			if tc.projected && !got[0].DrainsAt.Equal(now.Add(5*time.Minute)) {
				t.Fatalf("drains at %v", got[0].DrainsAt)
			}
		})
	}
}
