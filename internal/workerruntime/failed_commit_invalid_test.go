package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFailedCommitInvalidProvenanceRefDoesNotBecomeOrdinaryFile(t *testing.T) {
	for _, failed := range []bool{false, true} {
		raw := []byte(`{"version":"campaign-commit/v1","workflowRunId":"source","taskId":"producer","name":"implementation","repository":"repo","base":"` + strings.Repeat("a", 40) + `","commit":"` + strings.Repeat("b", 40) + `","ref":"refs/unrelated"}`)
		if failed {
			raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"failedAttempt":{"id":"attempt","verificationFailures":["verification command failed (7): exit 7"]}}`)
		}
		root := t.TempDir()
		path := filepath.Join(root, "record")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		driver := &LocalDriver{}
		dependency := workerproto.DependencyInput{TaskID: "producer", Provenance: &workerproto.DependencyProvenance{RunID: "source", TaskID: "producer", AttemptID: "attempt"}}
		if err := driver.restoreDependencyCommit(context.Background(), testPackage(), dependency, path, root); err == nil {
			t.Fatalf("malformed candidate accepted as ordinary file, failed=%t", failed)
		}
	}
}
