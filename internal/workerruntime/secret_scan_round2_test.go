package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A failure reason is reported to the coordinator in every snapshot, before
// and after the failed result is collected, and the coordinator copies it into
// attempt evidence. It must never carry an execution credential.
func TestSecretScanFailureObservationRedacted(t *testing.T) {
	secret := "synthetic-failed-attempt-credential"
	for _, test := range []struct {
		name    string
		failure string
		write   func(f *collectionFixture, failure string) error
	}{
		{name: "mark-phase", failure: "verification failed: token=" + secret, write: func(f *collectionFixture, failure string) error {
			return f.runtime.markPhase("assignment-1", PhaseFailed, failure, f.workspace, "thread-1")
		}},
		{name: "mark-failed", failure: "verification failed: token=" + base64.StdEncoding.EncodeToString([]byte("u:"+secret)), write: func(f *collectionFixture, failure string) error {
			return f.runtime.markFailed(context.Background(), "assignment-1", failure)
		}},
		{name: "mark-unknown", failure: "thread lost: token=" + secret, write: func(f *collectionFixture, failure string) error {
			return f.runtime.markUnknown("assignment-1", failure)
		}},
		// A journal written by an earlier binary holds the raw reason;
		// collection redacts it durably before it publishes.
		{name: "legacy-record", failure: "verification failed: token=" + secret, write: func(f *collectionFixture, failure string) error {
			return f.runtime.writePhase("assignment-1", PhaseFailed, failure, f.workspace, "thread-1")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.runtime.log = logger
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			if err := test.write(f, test.failure); err != nil {
				t.Fatal(err)
			}
			observed := func(stage string) {
				t.Helper()
				record := f.record(t)
				for _, detailed := range []bool{false, true} {
					raw, err := json.Marshal(observation(record, runtimeTestNow, detailed))
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(raw), secret) || strings.Contains(string(raw), test.failure) {
						t.Fatalf("%s observation (detailed=%v) carries the credential: %s", stage, detailed, raw)
					}
				}
				if !strings.Contains(record.Failure, "[redacted]") {
					t.Fatalf("%s failure lost its redacted reason: %q", stage, record.Failure)
				}
			}
			if test.name != "legacy-record" {
				observed("recorded")
			}
			if test.name != "mark-unknown" {
				if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
					t.Fatalf("failed result was not published: %v", err)
				}
				if record := f.record(t); record.Phase != PhaseCompleted {
					t.Fatalf("phase=%s", record.Phase)
				}
				observed("collected")
			}
			if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), test.failure) {
				t.Fatalf("credential logged: %s", logs.String())
			}
		})
	}
}

// redactingDriver stands in for the local driver's scanner around a fake.
type redactingDriver struct {
	*fakeDriver
	secret string
}

func (d redactingDriver) RedactFailure(_ context.Context, _ workerproto.ExecutionPackage, failure string) (string, error) {
	return strings.ReplaceAll(failure, d.secret, "[redacted]"), nil
}

// A command acknowledgement is sent to the coordinator and stored there; a
// preparation error quoting a credential-bearing setup command must not reach
// it raw.
func TestSecretScanCommandAcknowledgementRedacted(t *testing.T) {
	secret := "synthetic-prepare-credential"
	driver := &fakeDriver{prepareErr: errors.New(`setup command "npm config set //registry/:_authToken=` + secret + `" failed`)}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	runtime.driver = redactingDriver{driver, secret}
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	acks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(acks)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) || !strings.Contains(string(raw), "[redacted]") {
		t.Fatalf("acknowledgement not redacted: %s", raw)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record, _ := json.Marshal(state.Attempts["assignment-1"]); strings.Contains(string(record), secret) {
		t.Fatalf("journal record carries the credential: %s", record)
	}
}

// A journal written by an earlier release can hold a raw reason in a phase
// collection never visits again; the first reconcile redacts it, before the
// snapshot that follows reports it.
func TestSecretScanLegacyFailureRedactedOnFirstReconcile(t *testing.T) {
	secret := "synthetic-legacy-credential"
	driver := &fakeDriver{}
	runtime := newClaimedRuntime(t, t.TempDir(), driver)
	runtime.driver = redactingDriver{driver, secret}
	if err := runtime.writePhase("assignment-1", PhaseCompleted, "verification failed: token="+secret, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if failure := state.Attempts["assignment-1"].Failure; failure != "verification failed: token=[redacted]" {
		t.Fatalf("legacy failure not redacted: %q", failure)
	}
}

// History recorded by an earlier release names a short credential by its
// first four bytes; the fingerprint reported for it is masked.
func TestSecretScanLegacyShortHistoryFingerprintMasked(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	pkg := testPackage()
	secret := "q9Z7x2Pa1k"
	sum := sha256.Sum256([]byte(secret))
	legacy := secretSnapshot{Version: 1, Signatures: []canarySignature{{
		Prefix: []byte(secret[:4]), Length: len(secret), SHA256: hex.EncodeToString(sum[:]),
		Fingerprint: fmt.Sprintf("%s:%x", secret[:4], sum[:6]),
	}}}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if err := os.MkdirAll(filepath.Join(root, "secret-scans"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "secret-scans", secretSnapshotKey(pkg)+"-"+hex.EncodeToString(digest[:])+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	err = store.AdmitResult(pkg, PublishedResult{FinalMessage: "token=" + secret, ThreadArchive: []byte("{}")})
	var finding *SecretScanError
	if !errors.As(err, &finding) || finding.Detector != "canary" {
		t.Fatalf("legacy history credential published: %v", err)
	}
	if strings.Contains(finding.Error(), secret[:4]) || !strings.HasPrefix(finding.Fingerprint, "****:") {
		t.Fatalf("finding shows the credential's first bytes: %v", finding)
	}
}

// A failure the scanner cannot check is withheld rather than recorded raw.
func TestSecretScanFailureWithheldWhenHistoryUnreadable(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	secret := "synthetic-unchecked-credential"
	f.custody.config.SecretScan.StaticCanaries = []string{secret}
	history := filepath.Join(f.custody.config.Root, "secret-scans")
	if err := os.RemoveAll(history); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(history, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.markPhase("assignment-1", PhaseFailed, "token="+secret, f.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	if record := f.record(t); record.Failure != withheldFailure {
		t.Fatalf("unchecked failure recorded: %q", record.Failure)
	}
}

// Credentials of five to seven bytes are matched exactly, in the current
// configuration and through the recorded history after rotation, and their
// redacted evidence does not show their first bytes.
func TestSecretScanSevenByteCredentialBlocked(t *testing.T) {
	for _, secret := range []string{"q9Z7x2P", "q9Z7x"} {
		store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
		store.config.SecretScan.StaticCanaries = []string{secret}
		pkg := testPackage()
		if err := store.RecordSecretValues(context.Background(), pkg, []string{secret}); err != nil {
			t.Fatalf("short credential failed preparation: %v", err)
		}
		err := store.PublishResult(context.Background(), pkg, PublishedResult{FinalMessage: "token=" + secret, ThreadArchive: []byte("{}")})
		var finding *SecretScanError
		if !errors.As(err, &finding) || finding.Detector != "canary" {
			t.Fatalf("%d-byte credential published: %v", len(secret), err)
		}
		if strings.Contains(finding.Error(), secret[:4]) {
			t.Fatalf("finding shows the credential's first bytes: %v", finding)
		}
		store.config.SecretScan.StaticCanaries = nil
		err = store.AdmitResult(pkg, PublishedResult{FinalMessage: "rotated " + secret, ThreadArchive: []byte("{}")})
		if !errors.As(err, &finding) || finding.Detector != "canary" {
			t.Fatalf("%d-byte credential escaped through history: %v", len(secret), err)
		}
	}
}
