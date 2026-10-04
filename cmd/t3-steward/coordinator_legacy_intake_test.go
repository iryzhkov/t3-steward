package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestRunBacklogV2CoordinatorDefaultIntakeLeavesFilesAndQuarantine(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	raw := "---\nproject: t3-steward development\ntitle: ignored\ninstance: codex\nmodel: test\nmax_turns: 3\ngate: false\n---\nprompt\n"
	files := map[string]string{"legacy.md": raw, "invalid.md": "not a task"}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(cfg.Backlog.Dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if _, _, err := store.QuarantineSubmission(ctx, backlog.LegacySubmissionKey("legacy"),
		strings.Repeat("a", 64), "retained refusal", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before, err := store.ListQuarantinedSubmissions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	handled, err := runCoordinatorUntilStarted(t, cfg, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 0 || len(records.WorkflowRuns) != 0 {
		t.Fatalf("files submitted: %+v", records.WorkflowRuns)
	}
	after, err := store.ListQuarantinedSubmissions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("quarantine changed: before=%+v after=%+v", before, after)
	}
	entries, err := os.ReadDir(cfg.Backlog.Dir)
	if err != nil || len(entries) != len(files) {
		t.Fatalf("drop entries=%v err=%v", entries, err)
	}
	for name, content := range files {
		got, err := os.ReadFile(filepath.Join(cfg.Backlog.Dir, name))
		if err != nil || string(got) != content {
			t.Fatalf("file %s changed: %q %v", name, got, err)
		}
	}
	if !strings.Contains(logs.String(), "enabled=false") || !strings.Contains(logs.String(), "task run / campaign run") {
		t.Fatalf("missing effective intake diagnostic: %s", logs.String())
	}
}

func TestCoordinatorLegacyIntakeSkipsDisabledAliasAndPathValidation(t *testing.T) {
	cfg := config.Default()
	cfg.Backlog.Dir = "~someone-else/drop"
	cfg.BacklogV2.Projects = map[string]config.V2Project{
		"one": {T3Project: "shared"}, "two": {T3Project: "shared"},
	}
	source, err := coordinatorLegacyIntake(cfg, nil, nil)
	if err != nil || source != nil {
		t.Fatalf("disabled source=%v err=%v", source, err)
	}
	cfg.BacklogV2.Coordinator.LegacyFileIntakeEnabled = true
	if _, err := coordinatorLegacyIntake(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "legacy_file_intake_enabled") {
		t.Fatalf("opt-in invalid aliases: %v", err)
	}
}

func TestCoordinatorReloadLegacyIntakeGateRequiresRestartWithoutPartialLoad(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enable", true: "disable"}[enabled], func(t *testing.T) {
			f := newReloadFixture(t)
			f.cfg.BacklogV2.Coordinator.LegacyFileIntakeEnabled = enabled
			next := cloneReloadConfig(t, f.cfg)
			next.BacklogV2.Coordinator.LegacyFileIntakeEnabled = !enabled
			next.BacklogV2.Leases.Duration += config.Duration(time.Second)
			writeReloadConfig(t, f.cfg.Path, next)
			before, _ := coordinatorConfigurationDigest(f.cfg.BacklogV2)
			decision := evaluateCoordinatorReload(context.Background(), f.cfg, f.logger, f.store, f.receipts)
			receipt := f.readReceipt(t)
			if decision.proceed || receipt.Outcome != backlogadmin.ReloadRejected ||
				receipt.ConfigurationDigest != before || receipt.PreviousDigest != before ||
				!strings.Contains(receipt.Error, "legacy_file_intake_enabled changes require coordinator restart") {
				t.Fatalf("partial/rejected gate reload: %+v", receipt)
			}
		})
	}
}

func TestLegacyIntakeDiagnosticsUseEffectiveRemoteStatus(t *testing.T) {
	for _, state := range []string{"disabled", "enabled", ""} {
		t.Run(state, func(t *testing.T) {
			f := triageFixture{now: time.Now().UTC()}
			sources := f.sources()
			query := sources.query
			sources.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				response, err := query(ctx, q)
				if response.Status != nil {
					response.Status.Runtime.LegacyFileIntake = state
				}
				return response, err
			}
			report := collectTriage(context.Background(), sources, triageOptions{})
			found := false
			for _, item := range report.Items {
				if item.Kind == "legacy-intake-disabled" {
					found = true
					if !strings.Contains(item.Summary, "files are not submitted") || !strings.Contains(item.Summary, "task run or campaign run") {
						t.Fatalf("triage=%+v", item)
					}
				}
			}
			if found != (state == "disabled") {
				t.Fatalf("state=%q disabled notice=%v", state, found)
			}
			var out bytes.Buffer
			renderStatus(&out, &backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{LegacyFileIntake: state}})
			if state != "" && !strings.Contains(out.String(), "intake: "+state) {
				t.Fatalf("status=%s", out.String())
			}
		})
	}
}

// Disabled construction can be used directly by boundary-cycle tests: nil is
// intentional, and other reconciliations must continue through the same cycle.
func TestCoordinatorBoundaryCycleWithoutLegacyIntake(t *testing.T) {
	var quotaCalls, scheduleCalls, planningCalls, adminCalls int
	cycle := coordinatorBoundaryCycle{
		quota:     failingCoordinatorQuotaTicker{calls: &quotaCalls},
		schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
		planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
		admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	cycle.Tick(context.Background())
	if quotaCalls != 1 || scheduleCalls != 1 || planningCalls != 0 || adminCalls != 0 {
		t.Fatalf("quota fail-closed cycle calls: quota=%d schedule=%d planning=%d admin=%d", quotaCalls, scheduleCalls, planningCalls, adminCalls)
	}
}
