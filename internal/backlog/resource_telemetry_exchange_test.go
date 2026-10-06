package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type telemetryExchangeTransport struct {
	*exchangeTransport
	telemetry *domain.WorkerTelemetry
}

func (t *telemetryExchangeTransport) SnapshotObservations(ctx context.Context, request workerproto.SnapshotRequest) (workerproto.Observations, error) {
	snapshot, err := t.Snapshot(ctx, request)
	return workerproto.Observations{Snapshot: snapshot, Telemetry: t.telemetry}, err
}
func TestResourceTelemetrySurvivesLeaseRenewal(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := coordinatorSnapshot(4)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, ObservedAt: coordinatorTestTime}}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: "attempt-1", WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", LeaseExpiresAt: coordinatorTestTime.Add(time.Minute), CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	renewed := snapshot
	renewed.Sequence++
	renewed.ObservedAt = coordinatorTestTime.Add(time.Second)
	renewed.ValidUntil = coordinatorTestTime.Add(time.Hour)
	renewed.Assignments[0].ObservedAt = renewed.ObservedAt
	telemetry := &domain.WorkerTelemetry{ObservedAt: coordinatorTestTime}
	transport := &telemetryExchangeTransport{exchangeTransport: &exchangeTransport{snapshots: []domain.WorkerSnapshot{snapshot}, renewalSnapshot: &renewed}, telemetry: telemetry}
	report, err := (FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}).ReconcileWorker(ctx, transport, testOfferBuilder{}, WorkerAdmissionPolicy{}, nil, nil, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Renewed) != 1 {
		t.Fatalf("renewals=%v", report.Renewed)
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Snapshot.Inventory.Telemetry == nil || len(snapshots) != 1 || snapshots[0].Inventory.Telemetry == nil || !snapshots[0].Inventory.Telemetry.ObservedAt.Equal(telemetry.ObservedAt) {
		t.Fatalf("telemetry dropped during renewal: %+v", snapshots)
	}
}

func TestResourceTelemetryRequestOnlyForCapableWorkers(t *testing.T) {
	for _, capable := range []bool{false, true} {
		source := &ackParkStore{t: t, snapshot: domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "epoch", Sequence: 1}}
		if capable {
			source.snapshot.Inventory.Capabilities = []string{workerproto.CapabilityResourceTelemetry}
		}
		request, err := ParkedAssignmentsFor(context.Background(), source, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if request.ReportResourceTelemetry != capable {
			t.Fatalf("capable=%v request=%+v", capable, request)
		}
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if !capable && strings.Contains(string(raw), "reportResourceTelemetry") {
			t.Fatalf("new field sent to strict old decoder: %s", raw)
		}
	}
}

// These declarations freeze the pre-F1 placement JSON schema for strict old peers.
type legacyAssignmentBase domain.Assignment
type legacyPlacementDecision struct {
	TaskID           string                       `json:"taskId,omitempty"`
	AttemptID        string                       `json:"attemptId,omitempty"`
	AssignmentID     string                       `json:"assignmentId,omitempty"`
	SelectedWorkerID string                       `json:"selectedWorkerId,omitempty"`
	ReservationID    string                       `json:"reservationId,omitempty"`
	Demand           domain.ResourceDemand        `json:"demand"`
	CandidateIDs     []string                     `json:"candidateIds,omitempty"`
	Rejections       []domain.PlacementRejection  `json:"rejections,omitempty"`
	Scores           []domain.PlacementScore      `json:"scores,omitempty"`
	Snapshots        []domain.CapacitySnapshotRef `json:"snapshots,omitempty"`
	DecidedAt        time.Time                    `json:"decidedAt"`
}

func TestResourcePlacementOffersRespectWorkerCapability(t *testing.T) {
	for _, capable := range []bool{false, true} {
		t.Run(fmt.Sprint(capable), func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			task := testTask("alpha")
			task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"}}
			input := plannerInput([]domain.Task{task}, nil)
			input.Now = coordinatorTestTime
			input.Workflows[0].State.Run.CreatedAt = coordinatorTestTime
			input.Workflows[0].State.Run.UpdatedAt = coordinatorTestTime
			input.Workflows[0].State.Attempts[0].UpdatedAt = coordinatorTestTime
			input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}
			input.RouteEstimates = []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
				Workflows: []domain.Workflow{input.Workflows[0].Workflow}, WorkflowRuns: []domain.WorkflowRun{input.Workflows[0].State.Run},
				Tasks: []domain.Task{task}, Attempts: input.Workflows[0].State.Attempts, QuotaPools: input.QuotaPools,
			}); err != nil {
				t.Fatal(err)
			}
			first := coordinatorSnapshot(1)
			if capable {
				first.Inventory.Capabilities = []string{workerproto.CapabilityResourceTelemetry}
			}
			if err := store.SaveWorkerSnapshot(ctx, first); err != nil {
				t.Fatal(err)
			}
			coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}
			planned, err := coordinator.PlanAndCommit(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			if len(planned.Assignments) != 1 || planned.Assignments[0].ThreadID == "" {
				t.Fatalf("planned assignments = %+v", planned.Assignments)
			}

			transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{first, {
				WorkerID: first.WorkerID, WorkerEpoch: first.WorkerEpoch, CoordinatorEpoch: first.CoordinatorEpoch,
				Sequence: 2, Connected: true, Inventory: first.Inventory,
				Assignments: []domain.WorkerAssignmentObservation{{
					AssignmentID: planned.Assignments[0].ID, AssignmentEpoch: planned.Assignments[0].Epoch,
					State: domain.AssignmentClaimed, Control: domain.ControlPreparing, ThreadID: planned.Assignments[0].ThreadID,
					ObservedAt: coordinatorTestTime.Add(time.Second),
				}},
				ObservedAt: coordinatorTestTime.Add(time.Second), ValidUntil: coordinatorTestTime.Add(time.Minute),
			}}}
			report, err := coordinator.ReconcileWorker(
				ctx, transport, testOfferBuilder{}, WorkerAdmissionPolicyFromQuotaReport(QuotaBridgeReport{Derived: []QuotaPoolAdmissionSnapshot{{
					QuotaPoolID: "pool", Admission: domain.AdmissionOpen,
				}}}), nil, input.QuotaPools, time.Minute, time.Hour,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Offered) != 1 || len(report.Claimed) != 1 ||
				len(report.Delivery.Pending) != 1 || report.Delivery.Pending[0].Kind != domain.WorkerCommandPrepare ||
				len(report.Delivery.Acknowledgements) != 1 {
				t.Fatalf("exchange report = %+v", report)
			}
			if len(transport.offers) != 1 {
				t.Fatalf("offers = %+v", transport.offers)
			}
			wire, err := json.Marshal(workerproto.AssignmentOffers{Offers: transport.offers})
			if err != nil {
				t.Fatal(err)
			}
			if capable {
				if !bytes.Contains(wire, []byte("resourceEvaluations")) {
					t.Fatalf("capable worker lost resource evidence: %s", wire)
				}
			} else {
				var legacy struct {
					Offers []struct {
						Assignment struct {
							legacyAssignmentBase
							Placement *legacyPlacementDecision `json:"placement,omitempty"`
						} `json:"assignment"`
						Package   workerproto.ExecutionPackageManifest `json:"package"`
						ExpiresAt time.Time                            `json:"expiresAt"`
					} `json:"offers"`
				}
				decoder := json.NewDecoder(bytes.NewReader(wire))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&legacy); err != nil {
					t.Fatalf("old worker cannot decode offer: %v", err)
				}
				if legacy.Offers[0].Assignment.Placement == nil {
					t.Fatal("legacy placement trace omitted")
				}
			}
			if planned.Assignments[0].Placement == nil || len(planned.Assignments[0].Placement.ResourceEvaluations) == 0 {
				t.Fatal("input placement mutated")
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentClaimed ||
				!records.Assignments[0].LeaseExpiresAt.Equal(coordinatorTestTime.Add(time.Hour)) {
				t.Fatalf("durable assignments = %+v", records.Assignments)
			}
			if records.Assignments[0].Placement == nil || len(records.Assignments[0].Placement.ResourceEvaluations) == 0 {
				t.Fatal("durable placement evidence lost")
			}
		})
	}
}
