package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestDirectoryBindingCatalogAndPackageFence(t *testing.T) {
	records, assignment := packageBuilderFixture(plannerTestTime)
	binding := directoryTestBinding(directoryresource.ReadOnly)
	binding.Identity.Registration.WorkerID = assignment.WorkerID
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].DirectoryBindings = []directoryresource.Binding{binding}
		}
	}
	builder := packageBuilder(t, records)
	if _, err := builder.BuildAssignmentOffer(context.Background(), assignment, plannerTestTime.Add(time.Minute)); err == nil {
		t.Fatal("unregistered directory produced package")
	}
	project := builder.Catalog.projects["steward"]
	project.DirectoryBindings = []directoryresource.Binding{binding}
	builder.Catalog.projects["steward"] = project
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, plannerTestTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(offer.Package.Package.Environment.DirectoryBindings) != 1 {
		t.Fatal("binding dropped")
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}
	offer.Package.Package.Environment.DirectoryBindings[0].Identity.Object.Inode++
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err == nil {
		t.Fatal("identity tampering accepted")
	}
	offer.Package.Package.Environment.DirectoryBindings[0].Identity.Registration.WorkerID = "other-worker"
	if _, err := workerproto.BuildExecutionPackageManifest(offer.Package.Package); err == nil {
		t.Fatal("wrong host accepted")
	}
}
