package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A gate declared without a timeout must dispatch against a coordinator that
// runs with its default configuration. A default gate timeout above the default
// verification.command_timeout made every offer fail to build, and the
// assignment was withheld on every cycle with no recorded reason.
func TestGateDefaultTimeoutDispatchesUnderDefaultConfig(t *testing.T) {
	m := mustParseManifest(t, "version: 2\nname: gate-test\nenvironment:\n  project: steward\ntasks:\n  build:\n    prompt_file: build.md\n    gate:\n      commands: [make check-review]\n")
	gate := m.Tasks["build"].Gate
	if gate == nil {
		t.Fatal("gate missing")
	}
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, a := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			copy := *gate
			records.Tasks[i].Gate = &copy
		}
	}
	b := packageBuilder(t, records)
	defaults := config.Default().BacklogV2.Verification
	b.VerificationTimeout = defaults.CommandTimeout.D()
	b.WorkerCapabilities = map[string][]string{a.WorkerID: {workerproto.PackageCapabilityWorkerOwnedGate}}
	offer, err := b.BuildAssignmentOffer(context.Background(), a, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("default gate does not dispatch under default config: %v", err)
	}
	if offer.Package.Package.Gate == nil || offer.Package.Package.Gate.Timeout != gate.Timeout {
		t.Fatalf("gate lost: %+v", offer.Package.Package.Gate)
	}
}

// A stored gate that bypassed the submission check (a rerun or amendment of a
// task accepted under a larger command_timeout, or a coordinator whose setting
// was lowered afterwards) must still dispatch. Building no package withheld
// it on every cycle with the reason visible only in a coordinator log; it now
// runs bounded by the coordinator maximum and a slow gate fails visibly.
func TestGateTimeoutAboveCoordinatorMaximumStillDispatches(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, a := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].Gate = &domain.TaskGate{Commands: []string{"make check-review"}, Timeout: 45 * time.Minute}
		}
	}
	b := packageBuilder(t, records)
	b.VerificationTimeout = 30 * time.Minute
	b.WorkerCapabilities = map[string][]string{a.WorkerID: {workerproto.PackageCapabilityWorkerOwnedGate}}
	offer, err := b.BuildAssignmentOffer(context.Background(), a, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("over-limit stored gate withheld: %v", err)
	}
	if gate := offer.Package.Package.Gate; gate == nil || gate.Timeout != 30*time.Minute || len(gate.Commands) != 1 {
		t.Fatalf("gate not bounded by coordinator maximum: %+v", gate)
	}
	for _, task := range records.Tasks {
		if task.ID == "task-consumer" && task.Gate.Timeout != 45*time.Minute {
			t.Fatal("stored task gate mutated")
		}
	}
}

// A gate timeout the coordinator can never honour is refused at submission
// with the reason, rather than accepted and then withheld forever.
func TestSubmissionRefusesGateTimeoutBeyondCoordinatorMaximum(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
		MaxGateTimeout: 30 * time.Minute,
	}
	manifest := func(timeout string) string {
		return "version: 2\nname: bundle\nenvironment: {project: t3-steward}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    gate:\n      commands: [make check-review]\n" + timeout
	}
	too := validBundle(t)
	rewriteBundleManifest(t, too, manifest("      timeout: 45m\n"))
	_, err = service.SubmitDirectory(ctx, DirectorySubmission{BundleDir: too, IdempotencyKey: "gate-too-long"})
	if err == nil || !strings.Contains(err.Error(), "verification.command_timeout") || !strings.Contains(err.Error(), "inspect") {
		t.Fatalf("oversized gate timeout error = %v", err)
	}
	fits := validBundle(t)
	rewriteBundleManifest(t, fits, manifest(""))
	if _, err := service.SubmitDirectory(ctx, DirectorySubmission{BundleDir: fits, IdempotencyKey: "gate-default"}); err != nil {
		t.Fatalf("default gate timeout refused: %v", err)
	}
}
