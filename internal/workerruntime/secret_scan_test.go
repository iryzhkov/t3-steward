package workerruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSecretScanCanaryEncodingsAndRedaction(t *testing.T) {
	secret := "synthetic credential+/with spaces?"
	for _, value := range []string{secret, base64.StdEncoding.EncodeToString([]byte(secret)), base64.RawURLEncoding.EncodeToString([]byte(secret)), url.QueryEscape(secret), url.PathEscape(secret)} {
		scan := newResultScanner(SecretScanConfig{MaxBytes: 1 << 20}, []string{secret}, nil)
		err := scan.scan("output", "output", strings.NewReader("prefix\n"+value))
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" || finding.Offset != 7 {
			t.Fatalf("expected located canary refusal, got %v", err)
		}
		raw, _ := json.Marshal(finding)
		if strings.Contains(err.Error(), secret) || strings.Contains(string(raw), secret) || strings.Contains(string(raw), value) {
			t.Fatal("secret exposed")
		}
	}
}

func TestSecretScanPatternsPolicyAllowlistAndCap(t *testing.T) {
	token := "ghp_" + strings.Repeat("A", 36)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	for _, kind := range []string{"output", "archive", "summary", "commit", "bundle"} {
		scan := newResultScanner(SecretScanConfig{Log: logger}, nil, nil)
		err := scan.scan("fixture", kind, strings.NewReader(token))
		blocks := kind == "commit" || kind == "bundle"
		if (err != nil) != blocks {
			t.Fatalf("%s policy: %v", kind, err)
		}
	}
	if strings.Contains(logs.String(), token) || !strings.Contains(logs.String(), secretFingerprint(token)) {
		t.Fatal("warning redaction failed")
	}
	allow := map[string]bool{secretFingerprint(token): true}
	if err := newResultScanner(SecretScanConfig{}, nil, allow).scan("fixture", "commit", strings.NewReader(token)); err != nil {
		t.Fatal(err)
	}
	if err := newResultScanner(SecretScanConfig{}, []string{token}, allow).scan("fixture", "output", strings.NewReader(token)); err == nil {
		t.Fatal("allowlist bypassed canary")
	}
	for _, n := range []int{16, 17} {
		err := newResultScanner(SecretScanConfig{MaxBytes: 16}, nil, nil).scan("fixture", "output", strings.NewReader(strings.Repeat("x", n)))
		if (err != nil) != (n > 16) {
			t.Fatalf("cap %d: %v", n, err)
		}
	}
	if err := newResultScanner(SecretScanConfig{PatternPolicy: "block"}, nil, nil).scan("fixture", "output", strings.NewReader(token)); err == nil {
		t.Fatal("block override")
	}
}

func BenchmarkSecretScan10MB(b *testing.B) {
	data := bytes.Repeat([]byte("safe result line\n"), (10<<20)/17)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := newResultScanner(SecretScanConfig{}, []string{"synthetic-secret"}, nil).scan("output", "output", bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

// admission must refuse every class before creating a custody object.
func TestCustodySecretCanaryObjects(t *testing.T) {
	secret := "synthetic-execution-secret"
	for _, kind := range []string{"output", "archive", "summary", "bundle"} {
		for _, encode := range []func(string) string{func(s string) string { return s }, func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }, url.QueryEscape} {
			t.Run(kind, func(t *testing.T) {
				value := encode(secret)
				store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
				store.config.SecretScan.StaticCanaries = []string{secret}
				result := PublishedResult{Finalized: backlog.FinalizedAttempt{StorageDir: t.TempDir()}, FinalMessage: "done", ThreadArchive: []byte("{}")}
				switch kind {
				case "archive":
					result.ThreadArchive = []byte(value)
				case "summary":
					result.FinalMessage = value
				default:
					name := "fixture"
					media := "text/plain"
					artifactKind := domain.ArtifactOutput
					if kind == "bundle" {
						media = backlog.CommitBundleMediaType
						artifactKind = domain.ArtifactGitState
					}
					source := filepath.Join(result.Finalized.StorageDir, "artifacts", name)
					if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(source, []byte(value), 0600); err != nil {
						t.Fatal(err)
					}
					object := objectForBytes("fixture", "unused", string(artifactKind), media, []byte(value))
					result.Finalized.Artifacts = []domain.Artifact{{ID: object.ID, Name: name, Kind: artifactKind, MediaType: media, Size: object.Size, SHA256: object.SHA256, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", StoragePath: "runs/run-1/task-1/attempt-1/artifacts/" + name}}
				}
				for _, err := range []error{store.AdmitResult(testPackage(), result), store.PublishResult(context.Background(), testPackage(), result)} {
					var finding *SecretScanError
					if !errors.As(err, &finding) || strings.Contains(err.Error(), secret) {
						t.Fatalf("redacted refusal required: %v", err)
					}
				}
				entries, err := os.ReadDir(filepath.Join(store.config.Root, "objects"))
				if err != nil || len(entries) != 0 {
					t.Fatal("refused result stored bytes", err)
				}
			})
		}
	}
}
