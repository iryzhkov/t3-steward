package workerruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A failed attempt whose thread archive or failure text carries an execution
// credential must still reach the coordinator as a redacted failed result,
// rather than being refused on every reconcile.
func TestSecretScanFailedAttemptCanaryPublishesRedactedFailure(t *testing.T) {
	secret := "synthetic-failed-attempt-credential"
	for _, test := range []struct {
		name    string
		archive bool
		failure string
	}{
		{name: "archive", archive: true, failure: "attempt failed on the worker"},
		{name: "failure-text", failure: "verification failed: token=" + secret},
		{name: "encoded-failure-text", failure: "verification failed: token=" + base64.StdEncoding.EncodeToString([]byte("u:"+secret))},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.runtime.log = logger
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			if test.archive {
				f.control.archive = []byte(`{"thread":{"id":"thread-1"},"messages":["echo ` + secret + `"]}`)
			}
			if err := f.runtime.markPhase("assignment-1", PhaseFailed, test.failure, f.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			err := f.runtime.collect(context.Background(), "assignment-1")
			if err != nil {
				t.Fatalf("failed result was not published: %v", err)
			}
			record := f.record(t)
			if record.Phase != PhaseCompleted {
				t.Fatalf("phase=%s", record.Phase)
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatal("failed result not pending", err)
			}
			var summary string
			for _, object := range pending.Manifest.Objects {
				reader, err := f.custody.OpenArtifact(context.Background(), object)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(reader)
				reader.Close()
				if err != nil || strings.Contains(string(raw), secret) || strings.Contains(string(raw), test.failure) && test.failure != "attempt failed on the worker" {
					t.Fatalf("credential-bearing failure custody in %s", object.Path)
				}
				if object.Kind == "summary" {
					summary = string(raw)
				}
			}
			// An archive finding is reported by the scanner; a credential in
			// the failure text is already redacted when the failure is recorded.
			redacted := "detector=canary"
			if !test.archive {
				redacted = "token="
				if !strings.Contains(summary, "[redacted]") {
					t.Fatalf("failure reason not redacted in place: %q", summary)
				}
			}
			if !strings.HasPrefix(summary, FailedMarker+"\n") || !strings.Contains(summary, redacted) {
				t.Fatalf("redacted failure reason missing: %q", summary)
			}
			if strings.Contains(logs.String(), secret) || test.archive && !strings.Contains(logs.String(), "detector") {
				t.Fatalf("unsafe or missing log evidence: %s", logs.String())
			}
		})
	}
}

// The supervision activation lane converts a refusal into the same permanent
// collection failure the task lane uses, instead of retrying it forever.
func TestSecretScanActivationCanaryFailsPermanently(t *testing.T) {
	root := t.TempDir()
	secret := "synthetic-activation-credential"
	custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	custody.config.SecretScan.StaticCanaries = []string{secret}
	control := &recordingT3{
		thread:  &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"},
		message: "done",
		archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}},"padding":"` + secret + `"}`),
	}
	driver := &LocalDriver{
		Config:    LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Finalizer: backlog.AttemptFinalizer{StorageRoot: filepath.Join(root, "artifacts"), Processes: &collectionCountingProcess{}, Now: func() time.Time { return runtimeTestNow }},
		Publisher: custody, T3: control, Now: func() time.Time { return runtimeTestNow }, scoped: true,
	}
	pkg := testActivationPackage()
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	err := driver.collect(context.Background(), pkg, workspace, "")
	var permanent *permanentCollectionFailure
	if !errors.As(err, &permanent) || strings.Contains(err.Error(), secret) {
		t.Fatalf("activation refusal is not a permanent redacted failure: %v", err)
	}
}

// Base64 canaries are found at any byte alignment and through line wrapping.
func TestSecretScanUnalignedAndWrappedBase64(t *testing.T) {
	secret := "synthetic-base64-credential-0123456789abcdefghijklmnopqrstuvwxyz"
	wrap := func(s string, width int, sep string) string {
		var out strings.Builder
		for len(s) > width {
			out.WriteString(s[:width] + sep)
			s = s[width:]
		}
		out.WriteString(s)
		return out.String()
	}
	encodings := map[string]string{
		"offset-1":         base64.StdEncoding.EncodeToString([]byte("a" + secret)),
		"offset-2":         base64.StdEncoding.EncodeToString([]byte("ab" + secret)),
		"offset-2-url-raw": base64.RawURLEncoding.EncodeToString([]byte("ab" + secret + "\xff\xfe")),
		"basic-oauth2":     "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("oauth2:"+secret)),
		"docker-auth":      `{"auths":{"ghcr.io":{"auth":"` + base64.StdEncoding.EncodeToString([]byte("robot:"+secret)) + `"}}}`,
		"mime-wrapped":     wrap(base64.StdEncoding.EncodeToString([]byte("user:"+secret+secret)), 76, "\n"),
		"pem-wrapped-crlf": wrap(base64.StdEncoding.EncodeToString([]byte("xy"+secret)), 64, "\r\n"),
	}
	for name, encoded := range encodings {
		var logs bytes.Buffer
		scanner := newResultScanner(SecretScanConfig{Log: slog.New(slog.NewTextHandler(&logs, nil))}, []string{secret}, nil)
		err := scanner.scan("output", "output", strings.NewReader("prefix line\n"+encoded+"\n"))
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" || finding.Offset < 12 {
			t.Fatalf("%s: base64 canary escaped: %v", name, err)
		}
		raw, _ := json.Marshal(finding)
		if strings.Contains(string(raw), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("%s: secret exposed", name)
		}
	}
	// Unrelated base64 and wrapped text stay admitted.
	clean := wrap(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("unrelated data "), 400)), 76, "\n")
	if err := newResultScanner(SecretScanConfig{}, []string{secret}, nil).scan("output", "output", strings.NewReader(clean)); err != nil {
		t.Fatalf("clean base64 refused: %v", err)
	}
}

// The task can write its own workspace; a fingerprint it adds there must not
// suppress a pattern hit in its declared commit.
func TestSecretScanIgnoresTaskWrittenAllowlist(t *testing.T) {
	token := "ghp_" + strings.Repeat("C", 36)
	repo, base, head, _ := secretGitRepo(t, []byte(token))
	if err := os.Mkdir(filepath.Join(repo, ".t3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".t3", "secret-scan-allow"), []byte(secretFingerprint(token)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := publishSecretScanCommit(t, testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow }), repo, base, head)
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "github" {
		t.Fatalf("task-written allowlist suppressed a commit finding: %v", err)
	}
}

// publishSecretScanCommit publishes a result declaring one commit from repo.
func publishSecretScanCommit(t *testing.T, store *CustodyStore, repo, base, head string) error {
	t.Helper()
	pkg := testPackage()
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{Revision: "HEAD"}}}
	provenance := backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run-1", TaskID: "task-1", Name: "implementation", Repository: repo, Base: base, Commit: head, Ref: backlog.CampaignRef("run-1", "task-1", "implementation"), CreatedAt: runtimeTestNow}
	raw, err := backlog.MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}
	finalized := backlog.FinalizedAttempt{StorageDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(finalized.StorageDir, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(finalized.StorageDir, "artifacts", "implementation"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	obj := objectForBytes("fixture", "unused", "output", "application/json", raw)
	finalized.Artifacts = []domain.Artifact{{ID: obj.ID, Name: "implementation", Kind: domain.ArtifactOutput, MediaType: obj.MediaType, Size: obj.Size, SHA256: obj.SHA256, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", StoragePath: "runs/run-1/task-1/attempt-1/artifacts/implementation"}}
	return store.PublishResult(context.Background(), pkg, PublishedResult{Finalized: finalized, FinalMessage: "done", ThreadArchive: []byte("{}"), WorkspaceDir: repo})
}

// Replace refs, grafts, shallow markers and commit-graph files are all
// workspace-writable; none of them may hide committed content from the scan.
func TestSecretScanCommitRangeIgnoresWorkspaceHistoryRewrites(t *testing.T) {
	secret := "synthetic-hidden-history-credential"
	setup := func(t *testing.T) (string, string, string, string) {
		repo := t.TempDir()
		secretGit(t, repo, "init", "-q")
		secretGit(t, repo, "commit", "-q", "--allow-empty", "-m", "base")
		base := secretGit(t, repo, "rev-parse", "HEAD")
		if err := os.WriteFile(filepath.Join(repo, "leak.txt"), []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		secretGit(t, repo, "add", "leak.txt")
		secretGit(t, repo, "commit", "-q", "-m", "leak")
		leak := secretGit(t, repo, "rev-parse", "HEAD")
		return repo, base, leak, secretGit(t, repo, "rev-parse", "HEAD:leak.txt")
	}
	scan := func(t *testing.T, repo, base, head string) error {
		listing, err := scanCommitListing(context.Background(), repo, base, head)
		if err != nil {
			return err
		}
		return newResultScanner(SecretScanConfig{}, []string{secret}, nil).scanGitObjects(context.Background(), repo, "implementation", "commit", string(listing))
	}
	t.Run("replace", func(t *testing.T) {
		repo, base, head, blob := setup(t)
		clean := filepath.Join(repo, "clean")
		if err := os.WriteFile(clean, []byte("clean replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		secretGit(t, repo, "replace", blob, secretGit(t, repo, "hash-object", "-w", clean))
		if scan(t, repo, base, head) == nil {
			t.Fatal("replace ref hid a committed canary")
		}
	})
	for _, file := range []string{"shallow", "info/grafts"} {
		t.Run(file, func(t *testing.T) {
			repo, base, _, _ := setup(t)
			secretGit(t, repo, "rm", "-q", "leak.txt")
			secretGit(t, repo, "commit", "-q", "-m", "remove")
			head := secretGit(t, repo, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(repo, ".git", file), []byte(head+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if scan(t, repo, base, head) == nil {
				t.Fatalf("%s hid intermediate history", file)
			}
		})
	}
}

// Checkpoints are published to the coordinator like results and are scanned.
func TestSecretScanCheckpointCanary(t *testing.T) {
	secret := "synthetic-checkpoint-credential"
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	store.config.SecretScan.StaticCanaries = []string{secret}
	_, err := store.PublishCheckpoint(context.Background(), testPackage(), ".t3/checkpoint.md", []byte("# checkpoint\nexport TOKEN="+secret+"\n"))
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" || strings.Contains(err.Error(), secret) {
		t.Fatalf("checkpoint canary published: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(store.config.Root, "outbox"))
	if err != nil || len(entries) != 0 {
		t.Fatal("refused checkpoint advertised", err)
	}
	if _, err := store.PublishCheckpoint(context.Background(), testPackage(), ".t3/checkpoint.md", []byte("# checkpoint\nclean\n")); err != nil {
		t.Fatal(err)
	}
}

// Bundle decoding uses the worker's custody root, not the system TMPDIR.
func TestSecretScanBundleDecodesUnderCustodyRoot(t *testing.T) {
	repo, _, _, bundle := secretGitRepo(t, []byte("clean bundle content"))
	raw, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	finalized := backlog.FinalizedAttempt{StorageDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(finalized.StorageDir, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(finalized.StorageDir, "artifacts", "bundle"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	object := objectForBytes("fixture", "unused", string(domain.ArtifactGitState), backlog.CommitBundleMediaType, raw)
	finalized.Artifacts = []domain.Artifact{{ID: object.ID, Name: "bundle", Kind: domain.ArtifactGitState, MediaType: backlog.CommitBundleMediaType, Size: object.Size, SHA256: object.SHA256, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1", StoragePath: "runs/run-1/task-1/attempt-1/artifacts/bundle"}}
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
	if err := store.AdmitResult(testPackage(), PublishedResult{Finalized: finalized, FinalMessage: "done", ThreadArchive: []byte("{}"), WorkspaceDir: repo}); err != nil {
		t.Fatalf("bundle decode depends on TMPDIR: %v", err)
	}
}

// Short credentials cannot be matched reliably and must not fail preparation;
// login metadata such as a token type is not a credential.
func TestSecretScanShortValuesAndLoginMetadata(t *testing.T) {
	store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
	if err := store.RecordSecretValues(context.Background(), testPackage(), []string{"pin1", "synthetic-long-credential"}); err != nil {
		t.Fatalf("short credential failed preparation: %v", err)
	}
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, []byte(`{"token_type":"Bearer","tokens":{"access_token":"synthetic-access-token","expires_at":"2026-10-06T00:00:00Z","key_id":"k1"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	values, err := modelLoginCanaries([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	scanner := newResultScanner(SecretScanConfig{}, values, nil)
	if err := scanner.scan("output", "output", strings.NewReader("Authorization: Bearer abc expires_at 2026-10-06T00:00:00Z")); err != nil {
		t.Fatalf("login metadata treated as credential: %v", err)
	}
	if err := scanner.scan("output", "output", strings.NewReader("synthetic-access-token")); err == nil {
		t.Fatal("login token admitted")
	}
}
