package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const roleQuotaViewPolicy = "schema: route-policy/v1\nroles:\n- name: execute\n  candidates:\n  - {route: claude/model, effort: medium, tier: standard}\n  - {route: codex/model, effort: low, tier: standard}\n"

// adminAsClient asks the coordinator as the client transport would, at
// this release's read version.
func adminAsClient(admin roleReadinessQuery) func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
	return func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		q.Version = backlogadmin.CurrentReadVersion
		q.Principal = backlogadmin.Principal{ID: "coordinator", Roles: []string{backlogadmin.LocalAdminRole}}
		return admin.Query(ctx, q)
	}
}

// taskRunRoleRanking ranks the execute role exactly as "task run --role"
// does: the coordinator's quota and workers answers, read into the shared
// view, composed with the task's class gate.
func taskRunRoleRanking(t *testing.T, admin roleReadinessQuery) policySelection {
	t.Helper()
	ctx := context.Background()
	query := adminAsClient(admin)
	p, err := parseRoutePolicy([]byte(roleQuotaViewPolicy))
	if err != nil {
		t.Fatal(err)
	}
	workers, err := query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
	if err != nil {
		t.Fatal(err)
	}
	projects, err := query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryProjects})
	if err != nil {
		t.Fatal(err)
	}
	var project backlogadmin.Project
	for _, candidate := range projects.Projects {
		if candidate.Name == "dev-fleet" {
			project = candidate
		}
	}
	c := taskRunCLI{query: query, quotaStaleAfter: time.Hour}
	view := c.routeRankingView(ctx, workers.Workers, domain.TaskClassRequired)
	selection, err := selectPolicyRouteRanked(p, "execute", "", "", "", project, nil, view)
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

func campaignCheckRoleRanking(t *testing.T, admin roleReadinessQuery) domain.RoleSelection {
	t.Helper()
	selections, err := queryRoleSelections(context.Background(), admin, backlogadmin.ViabilityRequest{SchemaVersion: campaignCheckSchemaVersion, Tasks: []backlogadmin.ViabilityTask{{Name: "inspect", Project: "dev-fleet", Class: domain.TaskClassRequired, Role: "execute"}}})
	if err != nil {
		t.Fatal(err)
	}
	return selections["inspect"]
}

func scheduleRoleRanking(t *testing.T, admin roleReadinessQuery) domain.RoleSelection {
	t.Helper()
	store := &roleScheduleStore{records: sqlite.CoordinatorRecords{
		Schedules:         []domain.Schedule{{ID: "schedule", Version: 1}},
		ScheduleTemplates: []domain.ScheduleTemplate{{ScheduleID: "schedule", Version: 1, WorkflowID: "workflow"}},
		Workflows:         []domain.Workflow{{ID: "workflow", Project: "dev-fleet"}},
		Tasks:             []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "inspect", Class: domain.TaskClassRequired, Role: "execute"}},
	}}
	resolved, err := coordinatorRoleScheduleStore{Store: store, admin: admin}.ResolveScheduleTrigger(context.Background(), domain.ScheduleTriggerRequest{ScheduleID: "schedule", Source: domain.ScheduleTriggerScheduled})
	if err != nil || resolved.RoleResolutionError != "" {
		t.Fatalf("occurrence unresolved: %+v %v", resolved, err)
	}
	return resolved.RouteSelections["task"]
}

func submittedRoleRanking(t *testing.T, admin roleReadinessQuery, store *sqlite.Store, gate *backlog.SubmissionQuotaAdmission) domain.RoleSelection {
	t.Helper()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() {
		_ = filepath.WalkDir(storage, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
	})
	service := &backlog.SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return probeNow }, QuotaAdmission: gate, Roles: coordinatorManifestRoleResolver{admin: admin}}
	if _, err := service.SubmitDirectory(context.Background(), backlog.DirectorySubmission{IdempotencyKey: "one-view", BundleDir: campaignRankedQuotaBundle(t, false)}); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Tasks) != 1 || records.Tasks[0].RoleSelection == nil {
		t.Fatalf("submission retained no selection: %+v", records.Tasks)
	}
	return *records.Tasks[0].RoleSelection
}

type rankedCandidate struct{ route, band, pool string }

func taskRunCandidates(s policySelection) []rankedCandidate {
	var out []rankedCandidate
	for _, c := range s.Candidates {
		out = append(out, rankedCandidate{c.Route, c.Band, c.Pool})
	}
	return out
}

func roleCandidates(s domain.RoleSelection) []rankedCandidate {
	var out []rankedCandidate
	for _, c := range s.Candidates {
		out = append(out, rankedCandidate{c.Route, c.Band, c.Pool})
	}
	return out
}

// One quota snapshot ranks a role the same through "task run", "campaign
// check", campaign submission and a schedule occurrence. The policy leader's
// short window is at 95%: admission stays open, so only the readings can move
// the choice off policy order, and they must, with checks enabled or disabled.
func TestOneQuotaViewRanksTaskRunCheckSubmissionAndScheduleAlike(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "checks-enabled"
		if disabled {
			name = "checks-disabled"
		}
		t.Run(name, func(t *testing.T) {
			pinModelsNow(t, probeNow)
			admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
			setUsed("claude", 95)
			if disabled {
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
			}
			taskRun := taskRunRoleRanking(t, admin)
			check := campaignCheckRoleRanking(t, admin)
			occurrence := scheduleRoleRanking(t, admin)
			submitted := submittedRoleRanking(t, admin, store, gate)
			want := []rankedCandidate{{"claude/model", "gated", "claude-pool"}, {"codex/model", "healthy", "codex-pool"}}
			for label, got := range map[string]struct {
				route      string
				candidates []rankedCandidate
			}{
				"task run":           {taskRun.Route, taskRunCandidates(taskRun)},
				"campaign check":     {check.Route, roleCandidates(check)},
				"schedule":           {occurrence.Route, roleCandidates(occurrence)},
				"submission receipt": {submitted.Route, roleCandidates(submitted)},
			} {
				if got.route != "codex/model" || len(got.candidates) != len(want) || got.candidates[0] != want[0] || got.candidates[1] != want[1] {
					t.Fatalf("%s ranked differently: route=%s candidates=%+v want codex/model %+v", label, got.route, got.candidates, want)
				}
			}
			// The receipt the coordinator retains keeps each candidate's band,
			// pool and reason, including the pool's observation age.
			for _, candidate := range submitted.Candidates {
				if !strings.Contains(candidate.Reason, candidate.Pool+" quota fresh, observed 0s ago") || submitted.Ranking != domain.RouteRankingV1 {
					t.Fatalf("retained receipt lost its evidence: %+v", submitted)
				}
			}
		})
	}
}

// Stale quota is reported per pool with its age through both the client and
// the coordinator, and cannot supply headroom.
func TestOneQuotaViewReportsStaleQuotaWithItsAge(t *testing.T) {
	pinModelsNow(t, probeNow)
	admin, store, _, _ := campaignRankedQuotaFixture(t)
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshots[0]
	for i := range snapshot.QuotaObservations {
		if snapshot.QuotaObservations[i].Key.ProviderInstanceID == "codex" {
			snapshot.QuotaObservations[i].ObservedAt = probeNow.Add(-2 * time.Hour)
		}
	}
	snapshot.Sequence++
	snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Nanosecond)
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	const stale = "codex-pool quota stale, observed 2h0m0s ago (maximum age 1h0m0s)"
	taskRun := taskRunRoleRanking(t, admin)
	check := campaignCheckRoleRanking(t, admin)
	if len(taskRun.Candidates) != 2 || taskRun.Candidates[1].Band != "unknown" || !strings.Contains(taskRun.Candidates[1].Reason, stale) {
		t.Fatalf("task run hid stale quota or its age: %+v", taskRun.Candidates)
	}
	if len(check.Candidates) != 2 || check.Candidates[1].Band != "unknown" || !strings.Contains(check.Candidates[1].Reason, stale) {
		t.Fatalf("campaign check hid stale quota or its age: %+v", check.Candidates)
	}
}

// route-selection.json, which travels with the run, retains every candidate's
// eligibility, band, pool and reason, but no live percentage or age, so a
// replay with changed readings in the same bands keeps its key.
func TestRouteSelectionReceiptRetainsCandidates(t *testing.T) {
	p, err := parseRoutePolicy([]byte(roleQuotaViewPolicy))
	if err != nil {
		t.Fatal(err)
	}
	selection := policySelection{Schema: "route-selection/v1", Role: "execute", Route: "codex/model", PolicyDigest: p.Digest, Effort: "low", Ranking: domain.RouteRankingV1,
		Reason: "route-ranking/v1: codex-pool healthy headroom; maximum used 10%; codex-pool quota fresh, observed 12s ago",
		Candidates: []policyRankCandidate{
			{Route: "claude/model", Ordinal: 0, Eligible: true, Band: "gated", Pool: "claude-pool", Reason: "route-ranking/v1: claude-pool five_hour 95% >= 90 (gated); claude-pool quota fresh, observed 12s ago"},
			{Route: "codex/model", Ordinal: 1, Eligible: true, Band: "healthy", Pool: "codex-pool", Reason: "route-ranking/v1: codex-pool healthy headroom; maximum used 10%; codex-pool quota fresh, observed 12s ago"},
			{Route: "other/model", Ordinal: 2, Reason: "no ready worker advertises other/model"},
		}}
	snapshot, err := policySnapshotInputs(pinnedinput.Snapshot{}, p, []policySelection{selection})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := snapshot.Write(dir); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Name() == "route-selection.json" {
			raw, err = os.ReadFile(path)
		}
		return err
	}); err != nil || raw == nil {
		t.Fatalf("route-selection.json not retained: %v", err)
	}
	var retained []policySelection
	if err := json.Unmarshal(raw, &retained); err != nil {
		t.Fatal(err)
	}
	want := []policyRankCandidate{
		{Route: "claude/model", Ordinal: 0, Eligible: true, Band: "gated", Pool: "claude-pool", Reason: "route-ranking/v1: claude-pool gated"},
		{Route: "codex/model", Ordinal: 1, Eligible: true, Band: "healthy", Pool: "codex-pool", Reason: "route-ranking/v1: codex-pool healthy"},
		{Route: "other/model", Ordinal: 2, Reason: "no ready worker advertises other/model"},
	}
	if len(retained) != 1 || retained[0].Route != "codex/model" || retained[0].Reason != domain.RouteRankingV1 || len(retained[0].Candidates) != len(want) {
		t.Fatalf("receipt not retained: %s", raw)
	}
	for i := range want {
		if retained[0].Candidates[i] != want[i] {
			t.Fatalf("candidate %d = %+v, want %+v", i, retained[0].Candidates[i], want[i])
		}
	}
	if strings.Contains(string(raw), "maximum used") || strings.Contains(string(raw), "observed") {
		t.Fatalf("live readings pinned into the run key: %s", raw)
	}
	if selection.Candidates[0].Reason == want[0].Reason {
		t.Fatal("pinning mutated the printed receipt")
	}
}
