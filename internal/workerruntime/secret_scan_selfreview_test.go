package workerruntime

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A wrapped base64 canary is found wherever its line break falls relative to
// the boundary between two scan windows.
func TestSecretScanWrappedBase64AtWindowBoundary(t *testing.T) {
	secret := "synthetic-window-boundary-credential-0123456789"
	overlap := newResultScanner(SecretScanConfig{}, []string{secret}, nil).overlap
	cut := 64<<10 - overlap
	// The credential starts late in a 76-column MIME line, so the line break
	// falls a few bytes into it; every placement around the cut is tried.
	for prefix := 50; prefix < 58; prefix++ {
		encoded := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("P", prefix) + secret))
		wrapped := encoded[:76] + "\n" + encoded[76:]
		for start := cut - 90; start < cut+4; start++ {
			// Trailing filler makes the first read a full 64 KiB window.
			input := strings.Repeat(".", start-1) + "\n" + wrapped + "\n" + strings.Repeat(".", 64<<10)
			err := newResultScanner(SecretScanConfig{}, []string{secret}, nil).scan("output", "output", strings.NewReader(input))
			var finding *SecretScanError
			if !errors.As(err, &finding) || finding.Detector != "canary" {
				t.Fatalf("prefix %d, encoding at %d (cut %d): %v", prefix, start, cut, err)
			}
		}
	}
}

// Redaction covers the longest credential at a position, not a shorter
// credential that is its prefix.
func TestSecretScanSafeNameRedactsLongestCanary(t *testing.T) {
	short, long := "abcdefgh-SHORT", "abcdefgh-SHORT-LONGER-TAIL-XYZ"
	for _, canaries := range [][]string{{short, long}, {long, short}} {
		name := newResultScanner(SecretScanConfig{}, canaries, nil).safeName("results/file-" + long + ".txt")
		if name != "results/file-[redacted].txt" {
			t.Fatalf("partial redaction: %q", name)
		}
	}
}

// A drained thread whose checkpoint the scan refuses has stopped; the pause
// stands without checkpoint evidence instead of waiting for an escalation.
func TestLocalQuotaDrainWithRefusedCheckpointRecordsPause(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	driver.checkpointErr = &SecretScanError{Object: ".t3/checkpoint.md", Detector: "canary", Offset: 3, Fingerprint: "synt:000000000000"}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	// The drain notice is asynchronous: the first pass sends it, the second
	// observes the stopped turn and reads, and here refuses, its checkpoint.
	for range 2 {
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	record := journalRecord(t, runtime)
	if driver.checkpointCalls != 1 || driver.stopCalls != 0 || record.Phase != PhaseStopped ||
		record.LocalThrottle == nil || record.LocalThrottle.StoppedAt == nil || record.LocalThrottle.Checkpoint != nil {
		t.Fatalf("checkpoints=%d stops=%d record=%+v", driver.checkpointCalls, driver.stopCalls, record)
	}
}

// A worker-local failure to assemble the scan is retried, never a permanent
// failure of a good result.
func TestSecretScanResolutionFailureIsRetryable(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	resolved := false
	f.custody.config.SecretScan.Canaries = func(context.Context, workerproto.ExecutionPackage) ([]string, error) {
		if !resolved {
			return nil, errors.New("login file temporarily unreadable")
		}
		return []string{"synthetic-resolved-credential"}, nil
	}
	if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
		t.Fatal("collection succeeded without canaries")
	}
	if record := f.record(t); record.Phase != PhaseCollecting || permanentCollectionIntent(record.Failure) {
		t.Fatalf("transient failure became permanent: phase=%s failure=%q", record.Phase, record.Failure)
	}
	resolved = true
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted {
		t.Fatalf("phase=%s", record.Phase)
	}
}

// A canary in a successful turn's thread archive ends in a published,
// redacted failed result through the runtime.
func TestSecretScanArchiveCanaryEndToEnd(t *testing.T) {
	secret := "synthetic-archive-credential-end-to-end"
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	f.custody.config.SecretScan.StaticCanaries = []string{secret}
	// The fixture archive's only free text is the final assistant message,
	// which every compaction keeps whole, so the credential goes there.
	archive := strings.Replace(string(f.control.archive), `"text":"`, `"text":"`+secret, 1)
	if archive == string(f.control.archive) {
		t.Fatal("fixture archive has no assistant text to carry the credential")
	}
	f.control.archive = []byte(archive)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe or missing refusal: %v", err)
	}
	if record := f.record(t); record.Phase != PhaseFailed || !strings.HasPrefix(record.Failure, permanentSecretFailurePrefix) {
		t.Fatalf("phase=%s failure=%q", record.Phase, record.Failure)
	}
	if err := f.runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRedactedResult(t, f.custody, secret)
}

func assertRedactedResult(t *testing.T, custody *CustodyStore, secret string) {
	t.Helper()
	pending, err := custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatal("redacted result not published", err)
	}
	summary := ""
	for _, object := range pending.Manifest.Objects {
		reader, err := custody.OpenArtifact(context.Background(), object)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || strings.Contains(string(raw), secret) {
			t.Fatalf("secret in %s", object.Path)
		}
		if object.Kind == "summary" {
			summary = string(raw)
		}
	}
	if !strings.HasPrefix(summary, FailedMarker+"\n"+permanentSecretFailurePrefix) {
		t.Fatalf("summary = %q", summary)
	}
}

// The activation's permanent failure is then published as a redacted failed
// result by the same failure path every failed activation takes.
func TestSecretScanActivationPermanentFailurePublishes(t *testing.T) {
	root := t.TempDir()
	secret := "synthetic-activation-end-to-end"
	custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	custody.config.SecretScan.StaticCanaries = []string{secret}
	control := &recordingT3{
		thread:  &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"},
		message: "done " + secret,
		archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
	}
	driver := &LocalDriver{
		Config:    LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Finalizer: backlog.AttemptFinalizer{StorageRoot: filepath.Join(root, "artifacts"), Processes: &collectionCountingProcess{}, Now: func() time.Time { return runtimeTestNow }},
		Publisher: custody, T3: control, Now: func() time.Time { return runtimeTestNow }, scoped: true,
	}
	pkg := testActivationPackage()
	workspace := t.TempDir()
	err := driver.collect(context.Background(), pkg, workspace, "")
	var permanent *permanentCollectionFailure
	if !errors.As(err, &permanent) {
		t.Fatalf("not permanent: %v", err)
	}
	// What failCollection records, and what the runtime then hands back.
	if err := driver.CollectFailure(context.Background(), pkg, workspace, permanentSecretFailurePrefix+permanent.Error()); err != nil {
		t.Fatal(err)
	}
	assertRedactedResult(t, custody, secret)
}
