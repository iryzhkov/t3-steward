package backlog

import (
	"context"
	"strings"
	"testing"
)

func TestIngestionRejectsOversizedPinnedInput(t *testing.T) {
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "large.md", strings.Repeat("x", (1<<20)+1))
	rewriteBundleManifest(t, bundle, "version: 2\nname: bundle\npinned_inputs: true\nenvironment: {project: t3-steward}\ninputs: [large.md]\ntasks:\n  inspect: {prompt_file: prompts/inspect.md}\n")
	store := &ingestionStore{}
	root := t.TempDir()
	t.Cleanup(func() { _ = removeIngestedTree(root) })
	if _, err := (BundleIngester{StorageRoot: root, Store: store}).Ingest(context.Background(), bundle); err == nil {
		t.Fatal("oversized pinned input was accepted")
	}
	if store.calls != 0 {
		t.Fatal("invalid input consumed durable records")
	}
}
