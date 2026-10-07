package workerruntime

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The worker keys a delivered bundle by the campaign ref of the run that
// produced it, which for a carried input is the source run rather than the
// consuming one, and it hands preparation the source binding of every carried
// dependency so that the carried record is checked against it.
func TestPreparationReceivesSourceRunBundlesAndBindings(t *testing.T) {
	pkg := testPackage()
	carried := workerproto.ArtifactObject{
		ID: "bundle-source", Path: "commit-bundles/run-source/task-source/repair.bundle", Kind: "commit-bundle",
		MediaType: backlog.CommitBundleMediaType, Size: 12, SHA256: strings.Repeat("c", 64),
	}
	pkg.Dependencies = []workerproto.DependencyInput{{
		TaskID: "external-implement",
		Provenance: &workerproto.DependencyProvenance{
			RunID: "run-source", TaskID: "task-source", AttemptID: "attempt-source",
			SourceArtifacts: map[string]string{"reference-1": "provenance-source"},
		},
	}, {TaskID: "task-producer"}}
	pkg.CommitBundles = []workerproto.CommitBundleInput{
		{WorkflowRunID: "run-source", TaskID: "task-source", Name: "repair", Bundle: &carried},
		{WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: "task-producer", Name: "followup", Omitted: "over the total"},
	}
	var opened []string
	deliveries := commitBundleDeliveries(pkg, func(object workerproto.ArtifactObject) (io.ReadCloser, error) {
		opened = append(opened, object.ID)
		return io.NopCloser(strings.NewReader("")), nil
	})
	source, ok := deliveries[backlog.CampaignRef("run-source", "task-source", "repair")]
	if !ok || source.SHA256 != carried.SHA256 || source.Size != carried.Size || source.Omitted != "" {
		t.Fatalf("deliveries = %+v, want the carried bundle under the source run's ref", deliveries)
	}
	if _, wrong := deliveries[backlog.CampaignRef(pkg.Identity.WorkflowRunID, "task-source", "repair")]; wrong {
		t.Fatal("the carried bundle was keyed by the consuming run")
	}
	if reader, err := source.Open(context.Background()); err != nil || !slices.Equal(opened, []string{"bundle-source"}) {
		t.Fatalf("open = %v, opened %v", err, opened)
	} else {
		reader.Close()
	}
	if omitted := deliveries[backlog.CampaignRef(pkg.Identity.WorkflowRunID, "task-producer", "followup")]; omitted.Omitted != "over the total" || omitted.Open != nil {
		t.Fatalf("omitted delivery = %+v", omitted)
	}
	want := map[string]backlog.DependencySource{"external-implement": {WorkflowRunID: "run-source", TaskID: "task-source", AttemptID: "attempt-source"}}
	if got := dependencySources(pkg); !reflect.DeepEqual(got, want) {
		t.Fatalf("dependency sources = %+v, want %+v", got, want)
	}
}
