package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type displayReplayInventory struct {
	*sqlite.Store
	unavailable bool
}

func (s *displayReplayInventory) LoadWorkerSnapshots(ctx context.Context) ([]domain.WorkerSnapshot, error) {
	if s.unavailable {
		return nil, errors.New("optional inventory unavailable")
	}
	return s.Store.LoadWorkerSnapshots(ctx)
}

type displayReplayClaimStore struct {
	*sqlite.Store
	fail     bool
	failures int
}

func (s *displayReplayClaimStore) ClaimAssignment(ctx context.Context, c domain.AssignmentClaimRequest) (domain.Assignment, error) {
	if s.fail {
		s.fail = false
		s.failures++
		return domain.Assignment{}, errors.New("injected claim persistence failure")
	}
	return s.Store.ClaimAssignment(ctx, c)
}

type displayReplayTransport struct {
	runtime *Runtime
	drop    bool
	offers  []workerproto.AssignmentOffer
	claims  []domain.AssignmentClaimRequest
}

func (t *displayReplayTransport) WorkerID() string    { return t.runtime.config.WorkerID }
func (t *displayReplayTransport) WorkerEpoch() string { return t.runtime.config.WorkerEpoch }
func (t *displayReplayTransport) Snapshot(ctx context.Context, _ workerproto.SnapshotRequest) (domain.WorkerSnapshot, error) {
	// Coordinator snapshots must advance time as well as sequence.
	snapshotTime := t.runtime.now().Add(time.Millisecond)
	t.runtime.config.Now = func() time.Time { return snapshotTime }
	return t.runtime.Snapshot(ctx)
}
func (t *displayReplayTransport) DeliverOffers(ctx context.Context, offers []workerproto.AssignmentOffer) ([]domain.AssignmentClaimRequest, error) {
	t.offers = append(t.offers, offers...)
	// Exercise the real strict wire boundary. JSON omitempty normalizes empty
	// optional collections, as it does on an actual coordinator connection.
	data, err := json.Marshal(workerproto.AssignmentOffers{Offers: offers})
	if err != nil {
		return nil, err
	}
	var request workerproto.AssignmentOffers
	if err := workerproto.DecodePayload(workerproto.Envelope{Type: workerproto.MessageOffers, Payload: data}, workerproto.MessageOffers, &request); err != nil {
		return nil, err
	}
	response, err := t.runtime.AcceptOffers(ctx, request)
	if err != nil {
		return nil, err
	}
	t.claims = append(t.claims, response.Claims...)
	if t.drop {
		t.drop = false
		return nil, nil
	}
	return response.Claims, nil
}
func (t *displayReplayTransport) DeliverWorkerCommands(context.Context, domain.WorkerSnapshot, []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	return nil, nil
}
func (t *displayReplayTransport) DeliverLeaseRenewals(ctx context.Context, renewals []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error) {
	if err := t.runtime.ApplyLeaseRenewals(workerproto.LeaseRenewals{Renewals: renewals}); err != nil {
		return domain.WorkerSnapshot{}, err
	}
	// Coordinator snapshots must advance time as well as sequence.
	snapshotTime := t.runtime.now().Add(time.Millisecond)
	t.runtime.config.Now = func() time.Time { return snapshotTime }
	return t.runtime.Snapshot(ctx)
}
func (t *displayReplayTransport) DeliverThrottle(context.Context, domain.WorkerSnapshot, []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	return nil, nil
}

func TestFrozenProducerLostAndUnpersistedClaimReplay(t *testing.T) {
	for _, mode := range []string{"lost", "unpersisted"} {
		for _, supported := range []bool{false, true} {
			name := mode + "/omission"
			if supported {
				name = mode + "/display"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				now := runtimeTestNow
				store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				store.SetClock(func() time.Time { return now })
				assignment := testOffer(t).Assignment
				assignment.WorkerEpoch = "worker-1"
				assignment.Epoch = 1
				records := sqlite.CoordinatorRecords{
					Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "Campaign", Project: "steward", Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: backlog.EnvironmentScopeTask}, Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}, CreatedAt: now}},
					WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}},
					Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "Build", Class: domain.TaskClassRequired, PromptArtifactID: "prompt-1", MaxTurns: 4}},
					Attempts:     []domain.Attempt{{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: assignment.ID, UpdatedAt: now}},
					Assignments:  []domain.Assignment{assignment},
					Artifacts:    []domain.Artifact{{ID: "prompt-1", WorkflowRunID: "run-1", TaskID: "task-1", Kind: domain.ArtifactInput, Name: "task.md", MediaType: "text/plain", Size: 10, SHA256: testPackage().Prompt.SHA256, StoragePath: "objects/prompt-1", Producer: "coordinator", CreatedAt: now}},
				}
				if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
					t.Fatal(err)
				}
				catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Repository: "ssh://git/steward", DefaultRef: "main", SetupProfile: "go", T3ProjectTemplate: "development"}}, []backlog.SetupProfile{{Name: "go", Commands: []string{"go test ./..."}, Timeout: time.Minute}})
				if err != nil {
					t.Fatal(err)
				}
				inventory := &displayReplayInventory{Store: store, unavailable: !supported}
				builder := backlog.CoordinatorOfferBuilder{Store: inventory, Catalog: catalog, CatalogRevision: "catalog-1", CoordinatorID: "coordinator", CoordinatorEpoch: 1, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20}
				config := testConfig(func() time.Time { return now })
				config.CoordinatorEpoch = 1
				journal, err := OpenJournal(t.TempDir(), "normandy", "worker-1", 1)
				if err != nil {
					t.Fatal(err)
				}
				runtime, err := New(config, journal, &fakeDriver{})
				if err != nil {
					t.Fatal(err)
				}
				claimStore := &displayReplayClaimStore{Store: store, fail: mode == "unpersisted"}
				coordinator := backlog.FleetCoordinator{Store: claimStore, Now: func() time.Time { return now }}
				transport := &displayReplayTransport{runtime: runtime, drop: mode == "lost"}
				admission := backlog.WorkerAdmissionPolicy{OpenQuotaPools: map[string]struct{}{assignment.Route.QuotaPoolID: {}}}
				first, err := coordinator.ReconcileWorker(ctx, transport, builder, admission, nil, nil, time.Minute, 2*time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if len(first.Offered) != 1 || len(first.Claimed) != 0 || len(transport.claims) != 1 {
					t.Fatalf("first unpersisted claim: %+v claims=%d", first, len(transport.claims))
				}
				durable, err := store.LoadCoordinatorRecords(ctx)
				if err != nil || durable.Assignments[0].State != domain.AssignmentOffered {
					t.Fatalf("claim prematurely persisted: %+v %v", durable.Assignments, err)
				}
				if (transport.offers[0].Package.Package.Display != nil) != supported {
					t.Fatal("invalid initial display setup")
				}
				inventory.unavailable = supported // recover omission case; lose inventory in display case
				second, err := coordinator.ReconcileWorker(ctx, transport, builder, admission, nil, nil, time.Minute, 2*time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if len(transport.offers) == 2 && !reflect.DeepEqual(transport.offers[0].Package, transport.offers[1].Package) {
					firstJSON, _ := json.Marshal(transport.offers[0].Package.Package)
					secondJSON, _ := json.Marshal(transport.offers[1].Package.Package)
					t.Fatalf("producer packages differ: %s\n%s", firstJSON, secondJSON)
				}
				if len(second.Claimed) != 1 || len(transport.offers) != 2 || len(transport.claims) != 2 {
					t.Fatalf("frozen replay did not return/persist claim: %+v offers=%d claims=%d", second, len(transport.offers), len(transport.claims))
				}
				if !reflect.DeepEqual(transport.offers[0].Package, transport.offers[1].Package) {
					t.Fatal("producer changed replay package")
				}
				if mode == "unpersisted" && claimStore.failures != 1 {
					t.Fatal("did not exercise actual claim persistence failure")
				}
			})
		}
	}
}
