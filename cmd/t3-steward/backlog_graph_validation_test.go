package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// S11: "campaign rerun --from final-integrated-review --prompt" was refused
// with "no configured worker supports project, placement and route" for a task
// that check and submit had accepted and that had just run. The task required
// task-wait-collection-fence-v1, which every current worker supplies from its
// build and no operator configures, and amendment validation read only the
// configured list. Reproduced against the live coordinator's catalog: the same
// task passes without that capability and is refused with it.
func TestGraphTaskValidatorAcceptsBuildSuppliedCapabilities(t *testing.T) {
	root := t.TempDir()
	cfg := qualificationConfig(root)
	cfg.Path = filepath.Join(root, "config.yaml")
	writeReloadConfig(t, cfg.Path, cfg)
	loaded, err := config.LoadFile(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	validate := graphTaskValidator(loaded.BacklogV2)
	workflow := domain.Workflow{ID: "workflow-1", Project: "steward",
		Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: "main"}}
	task := func(capabilities ...string) domain.Task {
		return domain.Task{ID: "task-1", WorkflowID: workflow.ID, Name: "final-integrated-review",
			Placement: domain.Placement{Hosts: []string{qualificationWorkerID()}, Capabilities: capabilities},
			Routes:    []domain.ProviderRoute{{ProviderInstanceID: "test", Model: "test", QuotaPoolID: "pool"}}}
	}
	for _, capability := range []string{
		workerproto.CapabilityTaskWaitCollectionFence,
		workerproto.CapabilityCampaignSupervision,
		workerproto.PackageCapabilityPreflight,
	} {
		if err := validate(workflow, task(capability)); err != nil {
			t.Fatalf("build-supplied capability %q refused: %v", capability, err)
		}
	}
	// A capability neither configured nor supplied by the build is still a
	// refusal: no worker can ever satisfy it.
	err = validate(workflow, task("gpu"))
	if err == nil || !strings.Contains(err.Error(), "no configured worker supports") {
		t.Fatalf("an unknown capability was accepted: %v", err)
	}
}
