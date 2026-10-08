package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
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

func campaignRankedQuotaFixture(t *testing.T) (*backlogadmin.Service, *sqlite.Store, *backlog.SubmissionQuotaAdmission, func(string, float64)) {
	t.Helper()
	admin, store := probeReadinessService(t, nil)
	pools := []domain.QuotaPool{}
	bindings := []backlog.QuotaPoolBinding{}
	for _, instance := range []string{"claude", "codex"} {
		var keys []domain.BucketKey
		for _, window := range domain.DeclaredQuotaWindows(instance, nil) {
			keys = append(keys, domain.BucketKey{ProviderInstanceID: instance, LimitID: "limit", Window: window})
		}
		pools = append(pools, domain.QuotaPool{ID: instance + "-pool", Provider: instance, ProviderInstanceIDs: []string{instance}, Admission: domain.AdmissionOpen, BucketSelection: domain.BucketSelectionResolved, Buckets: keys, MaxConcurrent: 10})
		bindings = append(bindings, backlog.QuotaPoolBinding{ID: instance + "-pool", Provider: instance, ProviderInstanceIDs: []string{instance}, MaxConcurrent: 10})
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: pools}); err != nil {
		t.Fatal(err)
	}
	snapshot := probeWorkerSnapshot("homelab")
	snapshot.Inventory.Providers = []domain.WorkerProviderInventory{
		{InstanceID: "claude", Models: []string{"model"}, QuotaPoolID: "claude-pool", Available: true},
		{InstanceID: "codex", Models: []string{"model"}, QuotaPoolID: "codex-pool", Available: true},
	}
	snapshot.Sequence++
	snapshot.ObservedAt = probeNow.Add(-time.Second + time.Duration(snapshot.Sequence))
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	admissionRevisions := map[string]int64{}
	setUsed := func(instance string, used float64) {
		t.Helper()
		poolID := instance + "-pool"
		admission := domain.AdmissionOpen
		if used >= 100 {
			admission = domain.AdmissionClosed
		}
		revision := admissionRevisions[poolID]
		if err := store.CommitQuotaAdmissionTransitions(context.Background(), []domain.QuotaAdmissionTransition{{ExpectedRevision: revision, Record: domain.QuotaAdmissionRecord{QuotaPoolID: poolID, Revision: revision + 1, Admission: admission, ObservedAt: probeNow, AppliedAt: probeNow, Reason: "fixture quota"}}}); err != nil {
			t.Fatal(err)
		}
		admissionRevisions[poolID]++
		reset := probeNow.Add(2 * time.Hour)
		for _, window := range domain.DeclaredQuotaWindows(instance, nil) {
			state := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: instance, LimitID: "limit", Window: window}, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: used, ObservedAt: probeNow, ResetsAt: &reset}
			if err := store.SaveBucket(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			// Admin role resolution consumes worker quota telemetry; admission reads
			// the store and must independently admit exactly the selected pool.
			observation := domain.WorkerQuotaObservation{Key: state.Key, Phase: state.Phase, Healthy: state.Healthy, UsedPercent: state.UsedPercent, ObservedAt: state.ObservedAt, ResetsAt: state.ResetsAt}
			replaced := false
			for i := range snapshot.QuotaObservations {
				if snapshot.QuotaObservations[i].Key == state.Key {
					snapshot.QuotaObservations[i] = observation
					replaced = true
				}
			}
			if !replaced {
				snapshot.QuotaObservations = append(snapshot.QuotaObservations, observation)
			}
		}
		snapshot.Sequence++
		snapshot.ObservedAt = probeNow.Add(-time.Second + time.Duration(snapshot.Sequence))
		if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
	}
	requirement := domain.WorkerRequirement{WorkerID: "homelab", WorkerEpoch: snapshot.WorkerEpoch, CatalogRevision: snapshot.Inventory.CatalogRevision, CredentialRef: "fixture-credential", Connection: "persistent-ssh"}
	if err := store.SaveWorkerRequirements(context.Background(), []domain.WorkerRequirement{requirement}); err != nil {
		t.Fatal(err)
	}
	enrollment := domain.WorkerEnrollment{Request: domain.WorkerEnrollmentRequest{ID: "fixture-enrollment", WorkerID: "homelab", CatalogRevision: requirement.CatalogRevision, Reason: "test fixture"}, WorkerEpoch: requirement.WorkerEpoch, CoordinatorID: "coordinator", CredentialRef: requirement.CredentialRef, Principal: "ssh:homelab", Connection: requirement.Connection, Actor: "fixture", EnrolledAt: probeNow}
	if _, err := store.CommitWorkerEnrollment(context.Background(), enrollment, snapshot); err != nil {
		t.Fatal(err)
	}
	setUsed("claude", 100)
	setUsed("codex", 10)
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyPath, []byte("schema: route-policy/v1\nroles:\n- name: execute\n  candidates:\n  - {route: claude/model, effort: medium, tier: standard}\n  - {route: codex/model, effort: low, tier: standard}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := coordinatorRoleResolver{PolicyPath: policyPath, Now: func() time.Time { return probeNow }}
	admin.SetRuntimeInfo(backlogadmin.RuntimeInfo{Epoch: 1, Mode: "coordinator", Owner: "coordinator", MaxWorkerSnapshotAge: time.Minute, MaxQuotaObservationAge: time.Hour})
	admin.SetWorkerAuthorization(map[string][]backlogadmin.WorkerProviderAuthorization{"homelab": {
		{Instance: "claude", Models: []string{"model"}, QuotaPool: "claude-pool"},
		{Instance: "codex", Models: []string{"model"}, QuotaPool: "codex-pool"},
	}})
	admin.SetViability(backlogadmin.ViabilitySettings{
		ResolveRankedRoles: resolver.ResolveWithQuota,
		Projects:           []backlog.ProjectDefinition{{Name: "dev-fleet", Repository: probeRepository, DefaultRef: "main", SetupProfile: "go"}},
		SetupProfiles:      []backlog.SetupProfile{{Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute}},
		ReviewRoutes: map[string]config.ReviewRouteMetadata{
			"claude/model": {ProviderFamily: "claude", Tier: "executor"},
			"codex/model":  {ProviderFamily: "codex", Tier: "executor"},
		},
	})
	gate := &backlog.SubmissionQuotaAdmission{Now: func() time.Time { return probeNow }, Bridge: backlog.QuotaBridge{Store: store, MaxObservationAge: time.Hour, SafetyMargin: 5, LongWindowCap: 100, Pools: bindings}}
	return admin, store, gate, setUsed
}

func campaignRankedQuotaBundle(t *testing.T, pinned bool) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "prompts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "prompts", "inspect.md"), []byte("inspect"), 0600); err != nil {
		t.Fatal(err)
	}
	route := "role: execute"
	if pinned {
		route = "routes:\n  - {instance: claude, model: model, quota_pool: claude-pool}"
	}
	manifest := fmt.Sprintf("version: 2\nname: ranked-role-quota\nclass: required\nenvironment: {project: dev-fleet}\n%s\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    estimated_cost: 60\n", route)
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestCampaignRoleQuotaActualResolverAndAdmission(t *testing.T) {
	admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
	service := &backlog.SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
	t.Cleanup(func() {
		if err := filepath.WalkDir(service.StorageRoot, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
	})
	bundle := campaignRankedQuotaBundle(t, false)
	result, err := service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "ranked", BundleDir: bundle})
	if err != nil {
		t.Fatalf("healthy authorized fallback was refused: %v", err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 1 || len(records.Tasks) != 1 {
		t.Fatalf("submission %+v did not persist one run/task: %+v", result, records)
	}
	selection := records.Tasks[0].RoleSelection
	if selection == nil || selection.Route != "codex/model" || selection.Effort != "low" || selection.PolicyDigest == "" || selection.Reason == "" || selection.Ranking != domain.RouteRankingV1 {
		t.Fatalf("actual resolver receipt lost: %+v", selection)
	}
	receipt := records.WorkflowRuns[0].QuotaAdmission
	if receipt == nil || len(receipt.Roots) != 1 || receipt.Roots[0].Route.ProviderInstanceID != "codex" || receipt.Roots[0].Route.QuotaPoolID != "codex-pool" {
		t.Fatalf("selected healthy route did not pass actual quota admission: %+v", receipt)
	}
	setUsed("claude", 10)
	setUsed("codex", 100)
	if _, err := service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "ranked", BundleDir: bundle}); err != nil {
		t.Fatalf("idempotent replay was re-admitted against changed quota: %v", err)
	}
	replayed, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.WorkflowRuns) != 1 || len(replayed.Tasks) != 1 || replayed.Tasks[0].RoleSelection.Route != "codex/model" || !replayed.Tasks[0].RoleSelection.ResolvedAt.Equal(selection.ResolvedAt) {
		t.Fatalf("replay mutated durable selection: %+v", replayed)
	}
}

func TestCampaignRoleQuotaAllGatedAndExplicitPinRefuse(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("pinned=%t", pinned), func(t *testing.T) {
			admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
			if !pinned {
				setUsed("codex", 100)
			}
			storage := filepath.Join(t.TempDir(), "storage")
			service := &backlog.SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
			_, err := service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "refused", BundleDir: campaignRankedQuotaBundle(t, pinned)})
			if err == nil || !strings.Contains(err.Error(), "quota admission refused") {
				t.Fatalf("gated route bypassed admission: %v", err)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Workflows)+len(records.WorkflowRuns)+len(records.Tasks)+len(records.Attempts) != 0 {
				t.Fatalf("refused route left executable records: %+v", records)
			}
			entries, err := os.ReadDir(filepath.Join(storage, "workflows"))
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("refused route left bundle directories: %v", entries)
			}
		})
	}
}

func TestScheduleRoleQuotaActualResolverAcrossTimerAndManual(t *testing.T) {
	admin, _, _, setUsed := campaignRankedQuotaFixture(t)
	store := &roleScheduleStore{records: sqlite.CoordinatorRecords{
		Schedules:         []domain.Schedule{{ID: "schedule", Version: 1}},
		ScheduleTemplates: []domain.ScheduleTemplate{{ScheduleID: "schedule", Version: 1, WorkflowID: "workflow"}},
		Workflows:         []domain.Workflow{{ID: "workflow", Project: "dev-fleet"}},
		Tasks:             []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "inspect", Role: "execute"}},
	}}
	decorator := coordinatorRoleScheduleStore{Store: store, admin: admin}
	first, err := decorator.ResolveScheduleTrigger(context.Background(), domain.ScheduleTriggerRequest{ScheduleID: "schedule", Source: domain.ScheduleTriggerScheduled})
	if err != nil || first.RoleResolutionError != "" || first.RouteSelections["task"].Route != "codex/model" {
		t.Fatalf("timer selected exhausted policy leader: %+v %v", first, err)
	}
	setUsed("claude", 10)
	setUsed("codex", 100)
	second, err := decorator.ResolveScheduleTrigger(context.Background(), domain.ScheduleTriggerRequest{ScheduleID: "schedule", Source: domain.ScheduleTriggerManual})
	if err != nil || second.RoleResolutionError != "" || second.RouteSelections["task"].Route != "claude/model" {
		t.Fatalf("manual occurrence ignored fresh quota: %+v %v", second, err)
	}
	if first.RouteSelections["task"].Route != "codex/model" || store.records.Tasks[0].RoleSelection != nil || len(store.records.Tasks[0].Routes) != 0 {
		t.Fatalf("later resolution mutated an earlier receipt or template: first=%+v template=%+v", first, store.records.Tasks[0])
	}
}

type campaignQuotaQueryAfter struct {
	admin roleReadinessQuery
	after func()
}

func (q campaignQuotaQueryAfter) Query(ctx context.Context, request backlogadmin.Query) (backlogadmin.Response, error) {
	response, err := q.admin.Query(ctx, request)
	if err == nil {
		q.after()
	}
	return response, err
}
func TestCampaignRoleQuotaChangeAfterResolutionStillRefusesAdmission(t *testing.T) {
	admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
	query := campaignQuotaQueryAfter{admin: admin, after: func() { setUsed("codex", 100) }}
	service := &backlog.SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: query}}
	_, err := service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "quota-race", BundleDir: campaignRankedQuotaBundle(t, false)})
	if err == nil || !strings.Contains(err.Error(), "quota admission refused") || !strings.Contains(err.Error(), "codex-pool") {
		t.Fatalf("quota change after real resolution escaped admission: %v", err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows)+len(records.WorkflowRuns)+len(records.Tasks)+len(records.Attempts) != 0 {
		t.Fatalf("quota race persisted executable records: %+v", records)
	}
}

func TestCampaignRoleQuotaUnreliableWindowsStillFailAdmission(t *testing.T) {
	for _, mode := range []string{"stale", "absent", "invalid", "unknown-pool"} {
		t.Run(mode, func(t *testing.T) {
			admin, store, gate, _ := campaignRankedQuotaFixture(t)
			// Neither candidate may become a usable fallback from bad telemetry.
			snapshots, err := store.LoadWorkerSnapshots(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshots[0]
			for i := range snapshot.QuotaObservations {
				reading := &snapshot.QuotaObservations[i]
				if mode == "stale" {
					reading.ObservedAt = probeNow.Add(-2 * time.Hour)
				}
				if mode == "invalid" {
					reading.UsedPercent = -1
				}
				if err := store.SaveBucket(context.Background(), domain.BucketState{Key: reading.Key, Phase: reading.Phase, UsedPercent: reading.UsedPercent, Healthy: true, ObservedAt: reading.ObservedAt, ResetsAt: reading.ResetsAt}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "absent" {
				snapshot.QuotaObservations = nil
				records, err := store.LoadCoordinatorRecords(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for i := range records.QuotaPools {
					records.QuotaPools[i].Buckets = []domain.BucketKey{{ProviderInstanceID: records.QuotaPools[i].Provider, LimitID: "absent", Window: "primary"}}
				}
				if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: records.QuotaPools}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unknown-pool" {
				for i := range snapshot.Inventory.Providers {
					snapshot.Inventory.Providers[i].QuotaPoolID = "unknown"
				}
				gate.Bridge.Pools = nil
				if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: nil}); err != nil {
					t.Fatal(err)
				}
			}
			snapshot.Sequence++
			snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Nanosecond)
			if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
				t.Fatal(err)
			}
			service := &backlog.SubmissionService{Store: store, StorageRoot: filepath.Join(t.TempDir(), "storage"), MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
			_, err = service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: mode, BundleDir: campaignRankedQuotaBundle(t, false)})
			if err == nil {
				t.Fatalf("unreliable quota admitted an executable run: %s", mode)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records.WorkflowRuns)+len(records.Tasks)+len(records.Attempts) != 0 {
				t.Fatalf("unreliable quota leaked records: %+v", records)
			}
		})
	}
}

func TestCampaignRoleQuotaChecksDisabledKeepsPolicyOrderAndAdmission(t *testing.T) {
	admin, store, gate, _ := campaignRankedQuotaFixture(t)
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range records.QuotaPools {
		records.QuotaPools[i].ChecksDisabled = true
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: records.QuotaPools}); err != nil {
		t.Fatal(err)
	}
	gate.Bridge.Disabled = true
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() {
		if err := filepath.WalkDir(storage, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
	})
	service := &backlog.SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
	_, err = service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "disabled", BundleDir: campaignRankedQuotaBundle(t, false)})
	if err != nil {
		t.Fatalf("operator-disabled quota was silently enabled: %v", err)
	}
	records, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Tasks) != 1 || records.Tasks[0].RoleSelection == nil || records.Tasks[0].RoleSelection.Route != "claude/model" {
		t.Fatalf("disabled quota changed policy-order compatibility: %+v", records.Tasks)
	}
}
