package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func quotaSubmissionIntegrationFixture(t *testing.T) (*sqlite.Store, *SubmissionQuotaAdmission, time.Time, domain.BucketState) {
	t.Helper()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	reset := now.Add(2 * time.Hour)
	state := domain.BucketState{Key: admissionBucket("codex", "primary"), Phase: domain.PhaseNormal, Healthy: true, ObservedAt: now, ResetsAt: &reset}
	pool := domain.QuotaPool{ID: "openai", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Admission: domain.AdmissionOpen, BucketSelection: domain.BucketSelectionResolved, Buckets: []domain.BucketKey{state.Key}}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{pool}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveBucket(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	gate := &SubmissionQuotaAdmission{Now: func() time.Time { return now }, Bridge: QuotaBridge{Store: store, MaxObservationAge: time.Hour, SafetyMargin: 5, LongWindowCap: 100, Pools: []QuotaPoolBinding{{ID: "openai", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 10}}}}
	return store, gate, now, state
}

func quotaSubmissionBundle(t *testing.T, cost float64, extra string) string {
	t.Helper()
	bundle := validBundle(t)
	rewriteBundleManifest(t, bundle, fmt.Sprintf(`version: 2
name: quota-test
class: required
environment: {project: t3-steward}
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    estimated_cost: %g
%s`, cost, extra))
	return bundle
}

func quotaSubmissionService(t *testing.T, store *sqlite.Store, gate *SubmissionQuotaAdmission, now time.Time) *SubmissionService {
	t.Helper()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	return &SubmissionService{Store: store, StorageRoot: storage, MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return now }, QuotaAdmission: gate}
}

func assertQuotaSubmissionEmpty(t *testing.T, store *sqlite.Store, storage string) {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows)+len(records.WorkflowRuns)+len(records.Tasks)+len(records.Attempts)+len(records.Artifacts) != 0 {
		t.Fatalf("refusal persisted records: %#v", records)
	}
	entries, err := os.ReadDir(filepath.Join(storage, "workflows"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("refusal left bundle directories: %v", entries)
	}
}

func TestQuotaSubmissionRefusesWithoutRecordsOrFiles(t *testing.T) {
	for _, scenario := range []string{"stale", "missing", "exhausted", "over-capacity"} {
		t.Run(scenario, func(t *testing.T) {
			store, gate, now, state := quotaSubmissionIntegrationFixture(t)
			cost := 60.0
			switch scenario {
			case "stale":
				state.ObservedAt = now.Add(-2 * time.Hour)
			case "missing":
				state.Key.Window = "secondary"
			case "exhausted":
				state.UsedPercent = 100
			case "over-capacity":
				cost = 96
			}
			if scenario == "missing" {
				// The original primary remains present, while the declared
				// secondary has no reading: name it explicitly rather than delete SQL.
				records, err := store.LoadCoordinatorRecords(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				records.QuotaPools[0].Buckets = append(records.QuotaPools[0].Buckets, state.Key)
				if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: records.QuotaPools}); err != nil {
					t.Fatal(err)
				}
			} else if err := store.SaveBucket(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			service := quotaSubmissionService(t, store, gate, now)
			_, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: scenario, BundleDir: quotaSubmissionBundle(t, cost, "")})
			if err == nil || !strings.Contains(err.Error(), "quota admission refused") || !strings.Contains(err.Error(), "openai") {
				t.Fatalf("refusal = %v", err)
			}
			var refusal *QuotaSubmissionRefusal
			if !errors.As(err, &refusal) || refusal.Receipt == nil {
				t.Fatalf("missing typed refusal receipt: %v", err)
			}
			if scenario == "over-capacity" {
				for _, part := range []string{"primary", "needs 96", "available", "admitted", "margin 5", "campaign ceiling 96", "no run was created"} {
					if !strings.Contains(err.Error(), part) {
						t.Fatalf("refusal %q missing %q", err, part)
					}
				}
			}
			assertQuotaSubmissionEmpty(t, store, service.StorageRoot)
		})
	}
}

func TestQuotaSubmissionPrecheckBeforeIDsAndTransactionalRollback(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprintf("race=%t", race), func(t *testing.T) {
			store, gate, _, state := quotaSubmissionIntegrationFixture(t)
			storage := filepath.Join(t.TempDir(), "storage")
			t.Cleanup(func() { _ = removeIngestedTree(storage) })
			calls := 0
			cost := 96.0
			if race {
				cost = 60
			}
			ingester := BundleIngester{Store: store, StorageRoot: storage, QuotaAdmission: gate, NewTypedID: func(kind string) string {
				calls++
				if race && calls == 1 {
					state.UsedPercent = 100
					if err := store.SaveBucket(context.Background(), state); err != nil {
						t.Fatal(err)
					}
				}
				return fmt.Sprintf("%s-%d", kind, calls)
			}}
			_, err := ingester.Ingest(context.Background(), quotaSubmissionBundle(t, cost, ""))
			if err == nil || !strings.Contains(err.Error(), "quota admission refused") {
				t.Fatalf("refusal = %v", err)
			}
			if !race && calls != 0 {
				t.Fatalf("precheck consumed %d IDs", calls)
			}
			if race && calls == 0 {
				t.Fatal("race did not pass precheck")
			}
			assertQuotaSubmissionEmpty(t, store, storage)
		})
	}
}

func TestQuotaSubmissionConcurrentAdmissionAndAcceptedReplay(t *testing.T) {
	store, gate, now, state := quotaSubmissionIntegrationFixture(t)
	bundle := quotaSubmissionBundle(t, 60, "")
	services := []*SubmissionService{quotaSubmissionService(t, store, gate, now), quotaSubmissionService(t, store, gate, now)}
	type outcome struct {
		result SubmissionResult
		err    error
		index  int
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for i, service := range services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, e := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: fmt.Sprintf("request-%d", i), BundleDir: bundle})
			results <- outcome{r, e, i}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	accepted, refused := 0, 0
	var winner outcome
	for got := range results {
		if got.err == nil {
			accepted++
			winner = got
		} else if strings.Contains(got.err.Error(), "quota admission refused") {
			refused++
		} else {
			t.Fatalf("unexpected submission error: %v", got.err)
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatalf("accepted=%d refused=%d", accepted, refused)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 1 || len(records.Tasks) != 1 || len(records.Attempts) != 1 || len(records.Workflows) != 1 {
		t.Fatalf("concurrent records: %#v", records)
	}
	raw, err := json.Marshal(records.WorkflowRuns[0])
	if err != nil {
		t.Fatal(err)
	}
	receipt := records.WorkflowRuns[0].QuotaAdmission
	if receipt == nil || !receipt.AdmittedAt.Equal(now) || len(receipt.Roots) != 1 || len(receipt.Pools) != 1 || receipt.Pools[0].PoolID != "openai" || receipt.Pools[0].RootCost != 60 || receipt.Pools[0].CampaignBudgetCeiling != 60 || receipt.Pools[0].Decision != "admitted" || !strings.Contains(string(raw), "\"quotaAdmission\"") {
		t.Fatalf("missing or incorrect admission receipt: %s", raw)
	}
	state.UsedPercent = 100
	if err := store.SaveBucket(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	replay, err := services[winner.index].SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: fmt.Sprintf("request-%d", winner.index), BundleDir: bundle})
	if err != nil || !replay.Replay || replay.Record.RunID != winner.result.Record.RunID {
		t.Fatalf("accepted replay after exhaustion=%#v, %v", replay, err)
	}
}

func TestQuotaSubmissionExternalOnlyNeedsAreBlockedNotRoots(t *testing.T) {
	store, gate, now, state := quotaSubmissionIntegrationFixture(t)
	state.UsedPercent = 100
	if err := store.SaveBucket(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	service := quotaSubmissionService(t, store, gate, now)
	source, err := (BundleIngester{Store: store, StorageRoot: service.StorageRoot}).Ingest(context.Background(), validBundle(t))
	if err != nil {
		t.Fatal(err)
	}
	bundle := quotaSubmissionBundle(t, 100, fmt.Sprintf("    needs: [%s/inspect]\n", source.RunID))
	if _, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "external", BundleDir: bundle}); err != nil {
		t.Fatalf("blocked external descendant consumed root quota: %v", err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var run domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID != source.RunID {
			run = candidate
		}
	}
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.WorkflowRunID == run.ID {
			attempt = candidate
		}
	}
	if run.ID == "" || attempt.Progress != domain.ProgressBlocked {
		t.Fatalf("external-only attempt = %#v", records.Attempts)
	}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	receipt := run.QuotaAdmission
	if receipt == nil || len(receipt.Roots) != 0 || len(receipt.Pools) != 1 || receipt.Pools[0].CampaignBudgetCeiling != 100 || receipt.Pools[0].RootCost != 0 {
		t.Fatalf("external task missing from informational ceiling or counted as root: %s", raw)
	}
}

func TestQuotaSubmissionDescendantsAndFutureRootsOnlyAffectCeiling(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(fmt.Sprintf("future=%t", future), func(t *testing.T) {
			store, gate, now, _ := quotaSubmissionIntegrationFixture(t)
			cost := 60.0
			extra := ""
			if future {
				cost = 120
				extra = fmt.Sprintf("    not_before: %s\n", now.Add(time.Hour).Format(time.RFC3339))
			}
			extra += "  child:\n    prompt_file: prompts/inspect.md\n    needs: [inspect]\n    estimated_cost: 100\n"
			service := quotaSubmissionService(t, store, gate, now)
			_, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "descendant", BundleDir: quotaSubmissionBundle(t, cost, extra)})
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			receipt := records.WorkflowRuns[0].QuotaAdmission
			wantRoot := cost
			if future {
				wantRoot = 0
			}
			if receipt == nil || len(receipt.Pools) != 1 || receipt.Pools[0].RootCost != wantRoot || receipt.Pools[0].CampaignBudgetCeiling != cost+100 {
				t.Fatalf("root/ceiling receipt=%#v", receipt)
			}
		})
	}
}

func TestQuotaSubmissionPoolDisabledAndUnmeteredSkip(t *testing.T) {
	for _, unmetered := range []bool{false, true} {
		t.Run(fmt.Sprintf("unmetered=%t", unmetered), func(t *testing.T) {
			store, gate, now, state := quotaSubmissionIntegrationFixture(t)
			state.UsedPercent = 100
			if err := store.SaveBucket(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			bundle := quotaSubmissionBundle(t, 100, "")
			if unmetered {
				manifest, err := os.ReadFile(filepath.Join(bundle, "workflow.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				rewriteBundleManifest(t, bundle, strings.ReplaceAll(string(manifest), "quota_pool: openai", "quota_pool: openai-free"))
			} else {
				records, err := store.LoadCoordinatorRecords(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				records.QuotaPools[0].ChecksDisabled = true
				if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: records.QuotaPools}); err != nil {
					t.Fatal(err)
				}
			}
			service := quotaSubmissionService(t, store, gate, now)
			if _, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "pool-skip", BundleDir: bundle}); err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			receipt := records.WorkflowRuns[0].QuotaAdmission
			if receipt == nil || len(receipt.Roots) != 1 || !strings.HasPrefix(receipt.Roots[0].Decision, "not checked") {
				t.Fatalf("skip receipt=%#v", receipt)
			}
		})
	}
}

func TestQuotaSubmissionDisabledAndRegisterOnlySkip(t *testing.T) {
	for _, register := range []bool{false, true} {
		t.Run(fmt.Sprintf("register=%t", register), func(t *testing.T) {
			store, gate, now, state := quotaSubmissionIntegrationFixture(t)
			state.UsedPercent = 100
			if err := store.SaveBucket(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			if !register {
				gate.Bridge.Disabled = true
			}
			service := quotaSubmissionService(t, store, gate, now)
			result, err := service.SubmitDirectory(context.Background(), DirectorySubmission{IdempotencyKey: "skip", BundleDir: quotaSubmissionBundle(t, 100, ""), RegisterOnly: register})
			if err != nil {
				t.Fatal(err)
			}
			if result.Record.State != domain.SubmissionAccepted {
				t.Fatalf("result=%#v", result)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if register {
				if len(records.WorkflowRuns) != 0 || len(records.Attempts) != 0 {
					t.Fatalf("registered run: %#v", records)
				}
			} else {
				raw, _ := json.Marshal(records.WorkflowRuns[0])
				if !strings.Contains(string(raw), "not checked") {
					t.Fatalf("skip receipt=%s", raw)
				}
			}
		})
	}
}
