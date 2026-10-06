package workerruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// baselineRepo commits allow (when non-empty) at the base, records the base in
// .t3/base-commit as preparation does, then commits content as the task.
func baselineRepo(t *testing.T, allow string, content []byte) (string, string, string) {
	t.Helper()
	repo := t.TempDir()
	secretGit(t, repo, "init", "-q")
	if allow != "" {
		if err := os.MkdirAll(filepath.Join(repo, ".t3"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, ".t3", "secret-scan-allow"), []byte(allow), 0o600); err != nil {
			t.Fatal(err)
		}
		secretGit(t, repo, "add", ".t3/secret-scan-allow")
	}
	secretGit(t, repo, "commit", "-q", "--allow-empty", "-m", "base")
	base := secretGit(t, repo, "rev-parse", "HEAD")
	if err := os.MkdirAll(filepath.Join(repo, ".t3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".t3", "base-commit"), []byte(base+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "added.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	return repo, base, ""
}

func commitTaskWork(t *testing.T, repo string) string {
	t.Helper()
	secretGit(t, repo, "add", "added.txt")
	secretGit(t, repo, "commit", "-q", "-m", "task")
	return secretGit(t, repo, "rev-parse", "HEAD")
}

func TestSecretScanBaselineAllowlistComesFromBaseCommit(t *testing.T) {
	token := "ghp_" + strings.Repeat("D", 36)
	repo, base, _ := baselineRepo(t, "# fixture tokens\n"+secretFingerprint(token)+"\n", []byte(token))
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	if err := store.RecordScanBaseline(context.Background(), testPackage(), repo); err != nil {
		t.Fatal(err)
	}
	head := commitTaskWork(t, repo)
	// Deleting the workspace copy after the baseline changes nothing.
	if err := os.Remove(filepath.Join(repo, ".t3", "secret-scan-allow")); err != nil {
		t.Fatal(err)
	}
	if err := publishSecretScanCommit(t, store, repo, base, head); err != nil {
		t.Fatalf("base-commit fixture refused: %v", err)
	}
}

// Thread creation records the baseline before the first turn, and a later
// creation for the same execution keeps the first record.
func TestSecretScanCreateThreadRecordsBaselineOnce(t *testing.T) {
	token := "ghp_" + strings.Repeat("F", 36)
	repo, base, _ := baselineRepo(t, secretFingerprint(token)+"\n", []byte(token))
	pkg := testPackage()
	root := t.TempDir()
	store := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root}, T3: &recordingT3{projectID: "project-uuid"}, Publisher: store}
	cachePath := filepath.Join(root, "objects", pkg.Prompt.SHA256)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(context.Background(), pkg, repo); err != nil {
		t.Fatal(err)
	}
	head := commitTaskWork(t, repo)
	if err := os.WriteFile(filepath.Join(repo, ".t3", "base-commit"), []byte(head+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(context.Background(), pkg, repo); err != nil {
		t.Fatal(err)
	}
	baseline, err := store.loadScanBaseline(pkg)
	if err != nil || baseline == nil || baseline.Base != base || len(baseline.Allow) != 1 || baseline.Allow[0] != secretFingerprint(token) {
		t.Fatalf("baseline = %+v, %v", baseline, err)
	}
}

func TestSecretScanBaselineIgnoresAllowlistAddedByTask(t *testing.T) {
	token := "ghp_" + strings.Repeat("E", 36)
	repo, base, _ := baselineRepo(t, "", []byte(token))
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	if err := store.RecordScanBaseline(context.Background(), testPackage(), repo); err != nil {
		t.Fatal(err)
	}
	// The task commits its own allowlist and rewrites the recorded base.
	if err := os.WriteFile(filepath.Join(repo, ".t3", "secret-scan-allow"), []byte(secretFingerprint(token)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	secretGit(t, repo, "add", ".t3/secret-scan-allow")
	head := commitTaskWork(t, repo)
	if err := os.WriteFile(filepath.Join(repo, ".t3", "base-commit"), []byte(head+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordScanBaseline(context.Background(), testPackage(), repo); err != nil {
		t.Fatal(err)
	}
	err := publishSecretScanCommit(t, store, repo, base, head)
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "github" {
		t.Fatalf("task-committed allowlist suppressed a finding: %v", err)
	}
}

// A provenance base rewritten to the commit itself would make the range empty;
// the recorded base still bounds the scan.
func TestSecretScanBaselineBoundsRewrittenProvenanceBase(t *testing.T) {
	secret := "synthetic-rewritten-base-credential"
	repo, _, _ := baselineRepo(t, "", []byte(secret))
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	store.config.SecretScan.StaticCanaries = []string{secret}
	if err := store.RecordScanBaseline(context.Background(), testPackage(), repo); err != nil {
		t.Fatal(err)
	}
	head := commitTaskWork(t, repo)
	err := publishSecretScanCommit(t, store, repo, head, head)
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("rewritten base hid the commit: %v", err)
	}
}

func TestSecretScanBaselineRefusesUncommittedOrLinkedAllowlist(t *testing.T) {
	repo, _, _ := baselineRepo(t, "", nil)
	if err := os.Symlink("elsewhere", filepath.Join(repo, ".t3", "secret-scan-allow")); err != nil {
		t.Fatal(err)
	}
	secretGit(t, repo, "add", ".t3/secret-scan-allow")
	secretGit(t, repo, "commit", "-q", "-m", "link")
	if err := os.WriteFile(filepath.Join(repo, ".t3", "base-commit"), []byte(secretGit(t, repo, "rev-parse", "HEAD")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	err := store.RecordScanBaseline(context.Background(), testPackage(), repo)
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "allowlist-read" {
		t.Fatalf("linked allowlist accepted: %v", err)
	}
}

// Wrapped-base64 detection must not refuse ordinary prose, code or logs.
func TestSecretScanWrappedBase64NoFalsePositive(t *testing.T) {
	secret := "synthetic-credential-for-wrapping-0123456789"
	var text bytes.Buffer
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&text, "line %d of ordinary output with identifiers like abcdefghijklmnopqrstuvwxyz%d\n", i, i)
	}
	text.WriteString(base64.StdEncoding.EncodeToString([]byte("synthetic-credential-for-wrapping-0123X56789")))
	if err := newResultScanner(SecretScanConfig{}, []string{secret}, nil).scan("output", "output", &text); err != nil {
		t.Fatalf("ordinary text refused: %v", err)
	}
}

// The archive-shaped benchmark mirrors the review's slowest realistic case:
// six credentials (two JWT-length) with matching history and 40 more history
// signatures whose prefixes start with common letters.
func BenchmarkSecretScan10MBArchiveShaped(b *testing.B) {
	// Only the snapshot directory is used.
	store := &CustodyStore{config: CustodyConfig{Root: b.TempDir()}}
	pkg := testPackage()
	jwt := func(seed string) string {
		return "eyJhbGciOiJSUzI1NiJ9." + strings.Repeat(base64.RawURLEncoding.EncodeToString([]byte(seed)), 30) + ".sig"
	}
	current := []string{"synthetic-project-token-0001", "synthetic-coordinator-secret", "synthetic-worker-secret-0003", "sk-ant-synthetic-model-key-000000000000", jwt("access-claims"), jwt("refresh-claims")}
	if err := store.RecordSecretValues(context.Background(), pkg, current); err != nil {
		b.Fatal(err)
	}
	var older []string
	for i := 0; i < 40; i++ {
		older = append(older, fmt.Sprintf("%c%cold-rotated-credential-%04d", "etaoinshr"[i%9], "etaoinshr"[(i/9)%9], i))
	}
	if err := store.RecordSecretValues(context.Background(), pkg, older); err != nil {
		b.Fatal(err)
	}
	fill := func(line func(int) string) []byte {
		var data bytes.Buffer
		for data.Len() < 10<<20 {
			data.WriteString(line(data.Len()))
		}
		return data.Bytes()
	}
	shapes := map[string][]byte{
		"archive": fill(func(n int) string {
			return fmt.Sprintf(`{"role":"assistant","text":"the test at %d passed; see https://example.com/a?b=c+d%%20e and the token rotation notes"},`+"\n", n)
		}),
		// Every line break joins base64 runs, so the unwrapped view always runs.
		"wrapped-base64": fill(func(n int) string {
			return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%056d", n)))[:76] + "\n"
		}),
		"percent-plus": fill(func(n int) string { return fmt.Sprintf("q=%%41+%%42+%d+%%2F%%3D+\n", n) }),
	}
	for name, data := range shapes {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				scanner := newResultScanner(SecretScanConfig{}, current, nil)
				if err := store.addSecretHistory(pkg, scanner); err != nil {
					b.Fatal(err)
				}
				if err := scanner.scan("results/thread.json", "archive", bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
