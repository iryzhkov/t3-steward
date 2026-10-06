package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ingestPresetTasks ingests a workflow whose tasks override a build preset's
// CPU classes, or declare build's classes without the preset, and returns the
// tasks as the coordinator reloads them from the store.
func ingestPresetTasks(t *testing.T) map[string]domain.Task {
	t.Helper()
	bundle := validBundle(t)
	rewriteBundleManifest(t, bundle, `
version: 2
name: bundle
class: required
environment: {project: t3-steward}
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
tasks:
  plain:
    prompt_file: prompts/inspect.md
    resources: {preset: build}
  high:
    prompt_file: prompts/inspect.md
    resources: {preset: build, min_cpu_class: high}
  medium:
    prompt_file: prompts/inspect.md
    resources: {preset: build, preferred_cpu_class: medium}
  classes:
    prompt_file: prompts/inspect.md
    resources: {min_cpu_class: medium, preferred_cpu_class: high}
`)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	next := 0
	ingester := BundleIngester{
		StorageRoot: filepath.Join(t.TempDir(), "artifacts"),
		Store:       store,
		Now:         func() time.Time { return placementTestTime },
		NewID: func() string {
			next++
			return fmt.Sprintf("%02d", next)
		},
	}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	if _, err := ingester.Ingest(context.Background(), bundle); err != nil {
		t.Fatalf("ingest bundle: %v", err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tasks := map[string]domain.Task{}
	for _, task := range loaded.Tasks {
		// Keep the sizing as ingested and place the task like the other
		// placement fixtures: any worker, unrouted, in the fixture workflow.
		task.Placement = domain.Placement{Capabilities: []string{"internet"}}
		task.WorkflowID, task.Routes = "workflow", nil
		tasks[task.Name] = task
	}
	return tasks
}

// Overriding a build preset's CPU class changes which CPUs qualify, not how
// much memory or scratch a build uses. Each build must still be refused by a
// slot-only worker that fails both build floors, while a task that merely
// declares build's classes without the preset is sized as unsized work.
func TestResourcePlacementPresetClassOverrideKeepsBuildNeeds(t *testing.T) {
	tasks := ingestPresetTasks(t)
	for _, tc := range []struct {
		name     string
		selected string
	}{{"plain", ""}, {"high", ""}, {"medium", ""}, {"classes", "a"}} {
		t.Run(tc.name, func(t *testing.T) {
			task, ok := tasks[tc.name]
			if !ok {
				t.Fatalf("task %q not ingested", tc.name)
			}
			if task.ResourceDemand.CPUUnits != 0 || task.ResourceDemand.MemoryMB != 0 || task.ResourceDemand.ScratchMB != 0 {
				t.Fatalf("task %s reserves configured capacity: %+v", tc.name, task.ResourceDemand)
			}
			worker := resourceWorker("a")
			worker.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4}
			worker.Telemetry.MemoryAvailableMB = resourcePtr(int64(3072))
			worker.Telemetry.TempFreeMB = resourcePtr(int64(8192))
			s, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{worker})
			if err != nil {
				t.Fatal(err)
			}
			if s.Decision.SelectedWorkerID != tc.selected {
				t.Fatalf("task %s: %+v", tc.name, s.Decision)
			}
			if tc.selected != "" {
				return
			}
			codes := map[string]bool{}
			for _, rejection := range s.Decision.Rejections {
				codes[rejection.Code] = true
			}
			if !codes[ExclusionResourceMemory] || !codes[ExclusionResourceTempDisk] {
				t.Fatalf("task %s rejections = %+v", tc.name, s.Decision.Rejections)
			}
		})
	}
}

// Within one planning cycle each class-overridden build reserves a build's
// memory, so a worker with room for one build is not handed a second one.
func TestResourcePlacementPresetClassOverrideReservesBuildInCycle(t *testing.T) {
	tasks := ingestPresetTasks(t)
	for _, pair := range [][2]string{{"high", "high"}, {"medium", "medium"}} {
		t.Run(pair[0], func(t *testing.T) {
			first, second := tasks[pair[0]], tasks[pair[1]]
			first.ID, first.Name = "task-t1", "t1"
			second.ID, second.Name = "task-t2", "t2"
			a, b := resourceWorker("a"), resourceWorker("b")
			for _, w := range []*domain.WorkerInventory{&a, &b} {
				w.ObservedAt = plannerTestTime
				w.Telemetry.ObservedAt = plannerTestTime
				w.Telemetry.MemoryAvailableMB = resourcePtr(int64(8192))
			}
			b.Telemetry.Load1 = resourcePtr(5.0)
			b.Telemetry.Load5 = resourcePtr(5.0)
			plan, err := BuildPlan(plannerInput([]domain.Task{first, second}, []domain.WorkerInventory{a, b}))
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, proposal := range plan.Proposals {
				counts[proposal.WorkerID]++
			}
			if len(plan.Proposals) != 2 || counts["a"] != 1 || counts["b"] != 1 {
				t.Fatalf("class-overridden builds did not reserve build memory: %v", counts)
			}
		})
	}
}
