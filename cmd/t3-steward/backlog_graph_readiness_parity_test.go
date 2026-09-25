package main

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// parityTask is one task as both readiness and amendment validation see it.
type parityTask struct {
	hosts        []string
	capabilities []string
	resources    domain.ResourceDemand
	routes       []domain.ProviderRoute
}

// readinessRefuses answers the task through the same viability query that
// "campaign check" and submit use, against snapshots carrying exactly the
// inventory each configured worker would report for itself.
func readinessRefuses(t *testing.T, settings config.BacklogV2, task parityTask) bool {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{
		ID: "pool", Provider: "test", ProviderInstanceIDs: []string{"test"},
		MaxConcurrent: 1, Admission: domain.AdmissionOpen,
	}}}); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(settings.Workers))
	for id := range settings.Workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		binding, err := workerruntime.BuildWorkerBinding(settings, id, probeNow)
		if err != nil {
			t.Fatal(err)
		}
		inventory := binding.Inventory
		inventory.Epoch = settings.Workers[id].Epoch
		inventory.Capabilities = workerruntime.AdvertisedCapabilities(inventory.Capabilities)
		if err := store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
			WorkerID: id, WorkerEpoch: inventory.Epoch, CoordinatorEpoch: 1, Connected: true, Sequence: 1,
			ObservedAt: probeNow, ValidUntil: probeNow.Add(time.Minute), Inventory: inventory,
		}); err != nil {
			t.Fatal(err)
		}
	}
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return probeNow })
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Epoch: 1, Mode: "coordinator", Owner: "coordinator", MaxWorkerSnapshotAge: time.Minute,
	})
	service.SetViability(backlogadmin.ViabilitySettings{
		Projects: []backlog.ProjectDefinition{{
			Name: "steward", Repository: "https://example.invalid/steward.git", DefaultRef: "main",
			T3ProjectTemplate: "test", SetupProfile: "test",
		}},
		SetupProfiles: []backlog.SetupProfile{{Name: "test", Commands: []string{"true"}, Timeout: time.Minute}},
	})
	response, err := service.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryViability,
		Principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
		Viability: &backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{
			Name: "implement", Project: "steward", Ref: "main", Class: domain.TaskClassRequired,
			Hosts: task.hosts, Capabilities: task.capabilities, Resources: task.resources, Routes: task.routes,
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Viability == nil || len(response.Viability.Tasks) != 1 {
		t.Fatalf("readiness answered no task verdict: %+v", response.Viability)
	}
	return response.Viability.Tasks[0].Outcome == backlogadmin.ViabilityImpossible
}

// Amendment validation and readiness answer the same question, whether any
// configured worker could ever serve this task, and S11 was the two answering
// it differently: an amendment was refused for a task check and submit had
// accepted. Each case below runs one task through both, and the verdicts must
// match. The cases marked as former disagreements were judged differently by
// the copy of the matching rules amendment validation used to keep.
func TestGraphTaskValidatorAgreesWithReadiness(t *testing.T) {
	pinned := []string{qualificationWorkerID()}
	route := domain.ProviderRoute{ProviderInstanceID: "test", Model: "test", QuotaPoolID: "pool"}
	cases := []struct {
		name   string
		config func(*config.BacklogV2)
		task   parityTask
		refuse bool
	}{
		{name: "servable", task: parityTask{routes: []domain.ProviderRoute{route}}},
		{name: "build-supplied capability", task: parityTask{
			capabilities: []string{workerproto.CapabilityTaskWaitCollectionFence}, routes: []domain.ProviderRoute{route}}},
		{name: "unknown capability", refuse: true, task: parityTask{
			capabilities: []string{"gpu"}, routes: []domain.ProviderRoute{route}}},
		{name: "unknown model", refuse: true, task: parityTask{
			routes: []domain.ProviderRoute{{ProviderInstanceID: "test", Model: "other"}}}},
		{name: "host outside the fleet", refuse: true, task: parityTask{
			hosts: []string{"elsewhere"}, routes: []domain.ProviderRoute{route}}},
		// Former disagreement: a route list with an unservable fallback. The
		// planner takes the first route that resolves, so readiness accepts the
		// task; the old copy demanded that every route resolve.
		{name: "unservable fallback route", task: parityTask{routes: []domain.ProviderRoute{
			route, {ProviderInstanceID: "missing", Model: "test"}}}},
		// Former disagreement: a worker that takes no backlog work serves only a
		// task that names it. The old copy never read accept_backlog.
		{name: "backlog disabled", refuse: true, config: disableBacklog,
			task: parityTask{routes: []domain.ProviderRoute{route}}},
		{name: "backlog disabled, host named", config: disableBacklog,
			task: parityTask{hosts: pinned, routes: []domain.ProviderRoute{route}}},
		// Former disagreement: a CPU-class floor no configured worker meets. The
		// old copy never read the resource demand.
		{name: "cpu class floor", refuse: true, task: parityTask{
			resources: domain.ResourceDemand{MinCPUClass: domain.CPUClassHigh}, routes: []domain.ProviderRoute{route}}},
		// A sole "*" authorizes any concrete model; a worker observes and
		// reports the concrete ones, which configuration cannot enumerate.
		{name: "wildcard model", config: wildcardModels, task: parityTask{
			routes: []domain.ProviderRoute{{ProviderInstanceID: "test", Model: "anything"}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := qualificationConfig(root)
			cfg.Path = filepath.Join(root, "config.yaml")
			writeReloadConfig(t, cfg.Path, cfg)
			loaded, err := config.LoadFile(cfg.Path)
			if err != nil {
				t.Fatal(err)
			}
			settings := loaded.BacklogV2
			if test.config != nil {
				test.config(&settings)
			}
			workflow := domain.Workflow{ID: "workflow-1", Project: "steward",
				Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: "main"}}
			task := domain.Task{ID: "task-1", WorkflowID: workflow.ID, Name: "implement",
				Placement:      domain.Placement{Hosts: test.task.hosts, Capabilities: test.task.capabilities},
				ResourceDemand: test.task.resources, Routes: test.task.routes}
			amendmentRefuses := graphTaskValidator(settings)(workflow, task) != nil
			if readiness := readinessRefuses(t, wildcardObserved(settings, test.task), test.task); readiness != test.refuse {
				t.Fatalf("readiness refuses = %v, want %v", readiness, test.refuse)
			}
			if amendmentRefuses != test.refuse {
				t.Fatalf("amendment validation refuses = %v, readiness refuses = %v", amendmentRefuses, test.refuse)
			}
		})
	}
}

// disableBacklog leaves each worker configured but closed to backlog work,
// which configuration allows only for a worker with a persistent connection.
func disableBacklog(settings *config.BacklogV2) {
	workers := make(map[string]config.V2Worker, len(settings.Workers))
	for id, worker := range settings.Workers {
		worker.AcceptBacklog = false
		worker.Connection = "persistent-ssh"
		workers[id] = worker
	}
	settings.Workers = workers
}

func wildcardModels(settings *config.BacklogV2) {
	workers := make(map[string]config.V2Worker, len(settings.Workers))
	for id, worker := range settings.Workers {
		providers := make(map[string]config.V2Provider, len(worker.Providers))
		for instance, provider := range worker.Providers {
			provider.Models = []string{"*"}
			providers[instance] = provider
		}
		worker.Providers = providers
		workers[id] = worker
	}
	settings.Workers = workers
}

// wildcardObserved stands in for model discovery on the readiness side: a
// worker authorized for "*" reports the concrete models it observed, and the
// case asks for one of them.
func wildcardObserved(settings config.BacklogV2, task parityTask) config.BacklogV2 {
	workers := make(map[string]config.V2Worker, len(settings.Workers))
	for id, worker := range settings.Workers {
		providers := make(map[string]config.V2Provider, len(worker.Providers))
		for instance, provider := range worker.Providers {
			if len(provider.Models) == 1 && provider.Models[0] == "*" {
				provider.Models = nil
				for _, route := range task.routes {
					if route.ProviderInstanceID == instance {
						provider.Models = append(provider.Models, route.Model)
					}
				}
			}
			providers[instance] = provider
		}
		worker.Providers = providers
		workers[id] = worker
	}
	settings.Workers = workers
	return settings
}
