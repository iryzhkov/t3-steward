package workerruntime

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// rc.117 combination of M16-6 continuation checkpoints with rc.116's result
// secret scan, found by the integration self-review: a snapshot handed to the
// coordinator while the attempt runs passes the scan like every other
// checkpoint.
func TestLiveContinuationHandOnPassesTheSecretScan(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	secret := "synthetic-continuation-credential-0123456789"
	custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	custody.config.SecretScan.StaticCanaries = []string{secret}
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Publisher: custody, Now: func() time.Time { return runtimeTestNow }}
	pkg := testPackage()
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityContinuationCheckpoint}
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	writeContinuation(t, workspace, "token="+secret+"\n")
	if _, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-1"); err != nil {
		t.Fatalf("a refused hand-on must not fail the snapshot: %v", err)
	}
	upload, err := custody.PendingUploadByPurpose("checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	if upload == nil {
		return
	}
	for _, object := range upload.Manifest.Objects {
		reader, err := custody.OpenArtifact(ctx, object)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(reader)
		reader.Close()
		if strings.Contains(string(raw), secret) {
			t.Fatalf("live continuation upload %s carries the credential unscanned", object.ID)
		}
	}
}

// A credential in continuation.md costs a successful result only its
// optional checkpoint, never its publication.
func TestSecretInContinuationDoesNotFailASuccessfulResult(t *testing.T) {
	secret := "synthetic-continuation-credential-0123456789"
	f := newCollectionFixtureWith(t, 8192, 16384, 100, 100, func(pkg *workerproto.ExecutionPackage, driver *LocalDriver, workspace string) {
		pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint)
		writeContinuation(t, workspace, "token="+secret+"\n")
	})
	f.custody.config.SecretScan.StaticCanaries = []string{secret}
	err := f.runtime.collect(context.Background(), "assignment-1")
	record := f.record(t)
	if record.Phase == PhaseFailed || permanentCollectionIntent(record.Failure) {
		t.Fatalf("successful turn failed because of its optional checkpoint: phase=%s err=%v failure=%q", record.Phase, err, record.Failure)
	}
}
