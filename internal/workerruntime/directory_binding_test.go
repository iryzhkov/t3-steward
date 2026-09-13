package workerruntime

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestDirectoryConfigurationFencesOnlyRegisteredWorker(t *testing.T) {
	raw := `workers:
  worker-a: {address: worker-a, credential: ssh:a, accept_backlog: true}
  worker-b: {address: worker-b, credential: ssh:b, accept_backlog: true}
projects:
  scratch:
    type: fresh
    workers: [worker-a, worker-b]
    directory_resources:
      - access: read-only
        identity:
          registration: {workerId: worker-a, resourceId: data, revision: '1', path: /srv/data}
          object: {device: 1, inode: 2, birthSeconds: 3, birthNanos: 4}
          mountId: 5
          ancestors: [{device: 1, inode: 1, birthSeconds: 1, birthNanos: 0}]
`
	var settings config.BacklogV2
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&settings); err != nil {
		t.Fatal(err)
	}
	a, err := BuildWorkerBinding(settings, "worker-a", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildWorkerBinding(settings, "worker-b", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	binding := settings.Projects["scratch"].DirectoryResources[0]
	workflow := domain.Workflow{ID: "workflow", Project: "scratch", Environment: domain.ExecutionEnvironment{Type: "fresh", Scope: "task"}}
	task := domain.Task{ID: "task", WorkflowID: "workflow", DirectoryBindings: []directoryresource.Binding{binding}}
	if _, err := a.Catalog.Resolve(workflow, task); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Catalog.Resolve(workflow, task); err == nil {
		t.Fatal("foreign worker catalog approved resource")
	}
	// Editing the operator evidence must fence only the affected worker.
	project := settings.Projects["scratch"]
	project.DirectoryResources[0].Identity.Ancestors[0].Inode = 99
	settings.Projects["scratch"] = project
	changed, err := BuildWorkerBinding(settings, "worker-a", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := BuildWorkerBinding(settings, "worker-b", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if changed.CatalogRevision == a.CatalogRevision || unchanged.CatalogRevision != b.CatalogRevision {
		t.Fatal("incorrect directory catalog revision scope")
	}
	// Original catalog holds a detached copy, so old resolved evidence stays valid there.
	binding.Identity.Ancestors = append([]directoryresource.Object(nil), binding.Identity.Ancestors...)
	binding.Identity.Ancestors[0].Inode = 1
	task.DirectoryBindings = []directoryresource.Binding{binding}
	if _, err := a.Catalog.Resolve(workflow, task); err != nil {
		t.Fatalf("catalog mutated through configuration: %v", err)
	}
	if _, err := changed.Catalog.Resolve(workflow, task); err == nil {
		t.Fatal("changed identity accepted old package")
	}
	project.DirectoryResources[0].Identity.Registration.WorkerID = "unregistered"
	settings.Projects["scratch"] = project
	if _, err := BuildWorkerBinding(settings, "worker-a", runtimeTestNow); err == nil {
		t.Fatal("ineligible directory worker accepted")
	}
}
