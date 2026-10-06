package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A journal written by an earlier release can hold a raw reason while the
// credential history cannot be read. The snapshot must not report that
// unchecked reason; once the scanner can run again, the redacted reason is
// reported instead.
func TestSecretScanLegacyFailureWithheldFromSnapshotWhenHistoryUnreadable(t *testing.T) {
	secret := "synthetic-legacy-review-credential"
	for _, phase := range []Phase{PhaseFailed, PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.runtime.log = logger
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			failure := "token=" + secret
			if err := f.runtime.writePhase("assignment-1", phase, failure, f.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			history := filepath.Join(f.custody.config.Root, "secret-scans")
			if err := os.RemoveAll(history); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(history, []byte("not a directory"), 0o600); err != nil {
				t.Fatal(err)
			}
			snapshot, err := f.runtime.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), secret) {
				t.Fatalf("snapshot carries the unchecked credential: %s", raw)
			}
			if len(snapshot.Assignments) != 1 || snapshot.Assignments[0].Journal == nil ||
				snapshot.Assignments[0].Journal.Failure != withheldFailure {
				t.Fatalf("unchecked failure not withheld: %s", raw)
			}

			if err := os.Remove(history); err != nil {
				t.Fatal(err)
			}
			snapshot, err = f.runtime.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			raw, err = json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), secret) || len(snapshot.Assignments) != 1 ||
				snapshot.Assignments[0].Journal.Failure != "token=[redacted]" {
				t.Fatalf("recovered snapshot does not report the redacted reason: %s", raw)
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
		})
	}
}

// failingRedactor stands in for a scanner that cannot check any text.
type failingRedactor struct {
	*fakeDriver
}

func (failingRedactor) RedactFailure(context.Context, workerproto.ExecutionPackage, string) (string, error) {
	return "", errors.New("credential history unreadable")
}

// A preparation retry returns the driver's error to reconcile, which logs it.
// A setup command quoting a credential must not reach the runtime log raw,
// whether or not the scanner can check the text.
func TestSecretScanPrepareRetryLogRedacted(t *testing.T) {
	secret := "synthetic-prepare-review-credential"
	for _, test := range []struct {
		name   string
		wrap   func(*fakeDriver) Driver
		expect string
	}{
		{name: "redacted", wrap: func(d *fakeDriver) Driver { return redactingDriver{d, secret} }, expect: "[redacted]"},
		{name: "withheld", wrap: func(d *fakeDriver) Driver { return failingRedactor{d} }, expect: withheldFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeDriver{prepareErr: errors.New("npm config set authToken=" + secret + " failed")}
			runtime := newClaimedRuntime(t, t.TempDir(), driver)
			runtime.driver = test.wrap(driver)
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
			if err := runtime.writePhase("assignment-1", PhasePreparing, "", "", ""); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if driver.prepareCalls == 0 {
				t.Fatal("preparation was not retried")
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "attempt reconciliation deferred") || !strings.Contains(logs.String(), test.expect) {
				t.Fatalf("deferred reconciliation not logged with its %s error: %s", test.name, logs.String())
			}
		})
	}
}
