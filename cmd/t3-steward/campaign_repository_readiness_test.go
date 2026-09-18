package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var probeNow = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

// coordinatorProbeSettings is the configuration the observer reads when its
// transport is replaced. Only the bounds matter here; the addresses and
// credential references belong to the transport a test does not build.
func coordinatorProbeSettings() config.BacklogV2 { return config.BacklogV2{} }

// probeWorkerSnapshot is a fresh, connected worker that can serve the project.
func probeWorkerSnapshot(workerID string) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: workerID, WorkerEpoch: "worker-1", CoordinatorEpoch: 1, Connected: true,
		Sequence:   1,
		ObservedAt: probeNow, ValidUntil: probeNow.Add(time.Minute),
		Inventory: domain.WorkerInventory{
			ID: workerID, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			CatalogRevision: probeCatalogDigest, Epoch: "worker-1",
			Capabilities: []string{"git"},
			Projects:     []domain.WorkerProjectInventory{{Name: "dev-fleet", Available: true}},
			// Intake refuses a task with no route, so the fixture campaign names
			// one and the worker has to advertise it.
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "t3-primary", Models: []string{"opus"}, QuotaPoolID: "pool-1", Available: true,
			}},
			ObservedAt: probeNow,
		},
	}
}

// probeQuotaPools is the one pool the fixture route resolves to.
func probeQuotaPools() sqlite.CoordinatorRecords {
	return sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{
		ID: "pool-1", Provider: "t3", ProviderInstanceIDs: []string{"t3-primary"},
		MaxConcurrent: 4, Admission: domain.AdmissionOpen,
	}}}
}

// probeReadinessService builds a coordinator that answers viability from a real
// store, with the repository observer under test wired the way the runtime wires
// it.
func probeReadinessService(t *testing.T, observer backlogadmin.RepositoryObserver, workerIDs ...string) (*backlogadmin.Service, *sqlite.Store) {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.SaveCoordinatorRecords(context.Background(), probeQuotaPools()); err != nil {
		t.Fatal(err)
	}
	for _, workerID := range workerIDs {
		if err := store.SaveWorkerSnapshot(context.Background(), probeWorkerSnapshot(workerID)); err != nil {
			t.Fatal(err)
		}
	}
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return probeNow })
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Epoch: 1, Mode: "coordinator", Owner: "coordinator",
		MaxWorkerSnapshotAge: time.Minute,
	})
	service.SetViability(backlogadmin.ViabilitySettings{
		Projects: []backlog.ProjectDefinition{{
			Name: "dev-fleet", Repository: probeRepository, DefaultRef: "main",
			SetupProfile: "go", RequiredCredentials: []string{probeCredentialRef},
		}},
		SetupProfiles: []backlog.SetupProfile{{Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute}},
		Repository:    observer,
	})
	return service, store
}

func probeViabilityMatrix(t *testing.T, service *backlogadmin.Service) backlogadmin.ViabilityMatrix {
	t.Helper()
	request := backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{
		Name: "implement", Project: "dev-fleet", Ref: "main",
		Class: domain.TaskClassRequired, Capabilities: []string{"git"},
		Routes: []domain.ProviderRoute{{ProviderInstanceID: "t3-primary", Model: "opus"}},
	}}}
	response, err := service.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryViability,
		Principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
		Viability: &request,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Viability == nil {
		t.Fatal("the coordinator answered no matrix")
	}
	return *response.Viability
}

func probeCandidate(t *testing.T, matrix backlogadmin.ViabilityMatrix, workerID string) backlogadmin.ViabilityCandidate {
	t.Helper()
	for _, task := range matrix.Tasks {
		for _, candidate := range task.Candidates {
			if candidate.Worker == workerID {
				return candidate
			}
		}
	}
	t.Fatalf("the matrix has no candidate for worker %q", workerID)
	return backlogadmin.ViabilityCandidate{}
}

func probeHasReason(candidate backlogadmin.ViabilityCandidate, code string) bool {
	for _, reason := range candidate.Reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

// TestViabilityReportsReachabilityPerWorker is the per-worker matrix: one worker
// can read the repository and one cannot, and the matrix says so about each of
// them rather than collapsing both into one verdict about the campaign.
func TestViabilityReportsReachabilityPerWorker(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab":  reachableWorker(t, "homelab"),
		"normandy": unauthenticatedWorker(t, "normandy"),
	})
	service, _ := probeReadinessService(t, observer, "homelab", "normandy")
	matrix := probeViabilityMatrix(t, service)

	if matrix.Outcome != backlogadmin.ViabilityReady {
		t.Fatalf("outcome = %q, want %q: one viable candidate is enough", matrix.Outcome, backlogadmin.ViabilityReady)
	}
	reachable := probeCandidate(t, matrix, "homelab")
	if reachable.Outcome != backlogadmin.ViabilityReady ||
		probeHasReason(reachable, backlogadmin.ReasonRepositoryAuthFailed) {
		t.Fatalf("the reachable worker = %+v", reachable)
	}
	refused := probeCandidate(t, matrix, "normandy")
	if refused.Outcome != backlogadmin.ViabilityImpossible ||
		!probeHasReason(refused, backlogadmin.ReasonRepositoryAuthFailed) {
		t.Fatalf("the unauthenticated worker = %+v", refused)
	}
	for _, reason := range refused.Reasons {
		if strings.Contains(reason.Detail, "secretref:") {
			t.Fatalf("a reason named a credential reference: %q", reason.Detail)
		}
	}
}

// TestViabilityDegradesWhenNoWorkerAnswers is the honest-degradation case: with
// nobody to ask, reachability is reported as unobserved and temporary. It is
// never a pass, and it never refuses the campaign permanently.
func TestViabilityDegradesWhenNoWorkerAnswers(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{})
	service, _ := probeReadinessService(t, observer, "homelab")
	matrix := probeViabilityMatrix(t, service)

	candidate := probeCandidate(t, matrix, "homelab")
	if candidate.Outcome != backlogadmin.ViabilityAcceptedWaiting {
		t.Fatalf("candidate = %+v, want a temporary obstruction", candidate)
	}
	if matrix.Outcome == backlogadmin.ViabilityImpossible {
		t.Fatal("an unobserved reachability refused the campaign permanently")
	}
	var stated bool
	for _, reason := range candidate.Reasons {
		if reason.Code == backlogadmin.ReasonNetworkUnavailable {
			stated = true
			if reason.Permanent {
				t.Fatalf("an unobserved reachability was reported as permanent: %+v", reason)
			}
			if !strings.Contains(reason.Detail, "could not be observed") {
				t.Fatalf("the matrix did not say reachability was unobserved: %q", reason.Detail)
			}
		}
	}
	if !stated {
		t.Fatalf("the matrix reported nothing about reachability: %+v", candidate.Reasons)
	}
}

// TestViabilityRefusesEveryCandidateForAnAbsentRepository is the campaign that
// started this work: a repository that does not exist, refused permanently
// because every candidate said so.
func TestViabilityRefusesEveryCandidateForAnAbsentRepository(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab":  absentRepositoryWorker(t, "homelab"),
		"normandy": absentRepositoryWorker(t, "normandy"),
	})
	service, _ := probeReadinessService(t, observer, "homelab", "normandy")
	matrix := probeViabilityMatrix(t, service)
	if matrix.Outcome != backlogadmin.ViabilityImpossible {
		t.Fatalf("outcome = %q, want %q", matrix.Outcome, backlogadmin.ViabilityImpossible)
	}
	for _, workerID := range []string{"homelab", "normandy"} {
		candidate := probeCandidate(t, matrix, workerID)
		if !probeHasReason(candidate, backlogadmin.ReasonRepositoryNotFound) {
			t.Fatalf("worker %q = %+v", workerID, candidate.Reasons)
		}
	}
}
