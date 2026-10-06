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
)

// A legacy failure reason carrying a protocol credential, with no recorded
// credential history, must not reach the coordinator while the execution's
// canaries cannot be resolved. The real service canary callback fails on a
// malformed model login and discards the protocol credentials it holds, so a
// partial set cannot establish that the reason was checked.
func TestSecretScanLegacyFailureWithheldWhenCanaryResolutionFails(t *testing.T) {
	secret := "synthetic-protocol-round4-credential"
	for _, phase := range []Phase{PhaseFailed, PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.runtime.log = logger
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			login := filepath.Join(t.TempDir(), "auth.json")
			if err := os.WriteFile(login, []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			f.custody.config.SecretScan.StaticCanaries = nil
			f.custody.config.SecretScan.Canaries = serviceScanCanaries(WorkerServiceOptions{ModelLoginFiles: []string{login}}, ProtocolCredentials{WorkerSecret: []byte(secret)}, t.TempDir())
			if _, err := f.custody.config.SecretScan.Canaries(context.Background(), f.pkg); err == nil || !strings.Contains(err.Error(), "malformed") {
				t.Fatalf("canary resolution error = %v, want malformed login", err)
			}
			if err := os.RemoveAll(filepath.Join(f.custody.config.Root, "secret-scans")); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.writePhase("assignment-1", phase, "token="+secret, f.workspace, "thread-1"); err != nil {
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

			// Log redaction falls back the same way: the withheld notice,
			// never the raw text.
			if got := f.runtime.loggedError(context.Background(), "assignment-1", errors.New("stop failed: token="+secret)); got != withheldFailure {
				t.Fatalf("runtime logged text = %q, want withheld notice", got)
			}
			if got := f.driver.loggedText(context.Background(), f.pkg, "reason token="+secret); got != withheldFailure {
				t.Fatalf("driver logged text = %q, want withheld notice", got)
			}

			// Once the login is readable, the full set resolves and the
			// worker-local raw reason is redacted rather than lost.
			if err := os.WriteFile(login, []byte(`{"token":"synthetic-round4-model-login"}`), 0o600); err != nil {
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
