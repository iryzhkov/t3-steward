package backlog

import (
	"context"
	"strings"
	"testing"
)

func TestOrdinaryCampaignLargeInput(t *testing.T) {
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "inputs/large.md", strings.Repeat("x", 2<<20))
	rewriteBundleManifest(t, bundle, "version: 2\nname: bundle\nenvironment: {project: t3-steward}\ninputs: [inputs/large.md]\ntasks:\n  inspect: {prompt_file: prompts/inspect.md}\n")
	store := &ingestionStore{}
	ingester := BundleIngester{StorageRoot: t.TempDir(), Store: store}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	got, err := ingester.Ingest(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records.Tasks[0].InputArtifactIDs) != 1 {
		t.Fatal("large input was not submitted")
	}
	if got.Records.Workflows[0].InputManifest != nil {
		t.Fatal("ordinary campaign acquired pinned limits")
	}
	rewriteBundleManifest(t, bundle, "version: 2\nname: bundle\npinned_inputs: true\nenvironment: {project: t3-steward}\ninputs: [inputs/large.md]\ntasks:\n  inspect: {prompt_file: prompts/inspect.md}\n")
	if _, err := ingester.Ingest(context.Background(), bundle); err == nil {
		t.Fatal("pinned oversized input accepted")
	}
}
