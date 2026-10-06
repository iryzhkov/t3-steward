package workerruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
