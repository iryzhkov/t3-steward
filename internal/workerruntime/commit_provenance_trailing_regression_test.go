package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"path/filepath"
	"testing"
)

func TestRecognizedCommitWithTrailingDataCannotBecomeOrdinaryWorkerInput(t *testing.T) {
	root := t.TempDir()
	raw := []byte(`{"version":"campaign-commit/v1","workflowRunId":"r","taskId":"t","name":"candidate","repository":"repo","base":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ref":"refs/heads/spoof"} trailing`)
	path := filepath.Join(root, "record")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := (&LocalDriver{}).restoreDependencyCommit(context.Background(), testPackage(), workerproto.DependencyInput{TaskID: "producer"}, "producer/record", path, root); err == nil {
		t.Fatal("malformed recognized record became ordinary worker input")
	}
}
