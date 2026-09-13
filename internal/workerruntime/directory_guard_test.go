package workerruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

func TestDirectoryPackageRefusesUncontainedEffects(t *testing.T) {
	pkg := testPackage()
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{{Access: directoryresource.ReadOnly}}
	// No filesystem, credentials, clock or provider is wired. The refusal must
	// precede every such effect, including on recovery paths using executionRecords.
	driver := &LocalDriver{}
	if _, err := driver.Prepare(context.Background(), pkg); err == nil || !strings.Contains(err.Error(), "containment backend") {
		t.Fatalf("prepare: %v", err)
	}
	if _, _, _, err := driver.executionRecords(pkg); err == nil || !strings.Contains(err.Error(), "containment backend") {
		t.Fatalf("reconstruction: %v", err)
	}
}
