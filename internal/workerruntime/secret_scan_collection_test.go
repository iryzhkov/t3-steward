package workerruntime

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestSecretScanCollectionFailsPermanentlyAndRedacts(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	secret := "synthetic-final-message-credential"
	f.custody.config.SecretScan.StaticCanaries = []string{secret}
	f.control.message = secret
	err := f.runtime.collect(context.Background(), "assignment-1")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe collection error: %v", err)
	}
	record := f.record(t)
	if record.Phase != PhaseFailed || !permanentCollectionIntent(record.Failure) || strings.Contains(record.Failure, secret) {
		t.Fatalf("unsafe or retryable failure: phase=%s", record.Phase)
	}
	if err := f.runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatal("bounded failure not published", err)
	}
	for _, object := range pending.Manifest.Objects {
		reader, err := f.custody.OpenArtifact(context.Background(), object)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || strings.Contains(string(raw), secret) {
			t.Fatal("secret in failure custody")
		}
	}
}
