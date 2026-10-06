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

// A model token the provider refreshes mid-run is known to the worker once a
// resolution has seen it. A task that then deletes or empties its login file
// must not shrink the canary set: a later redaction, snapshot or result scan
// still matches the refreshed token from recorded history.
func TestSecretScanObservedLoginTokenSurvivesLoginRemoval(t *testing.T) {
	for _, removal := range []struct {
		name   string
		remove func(path string) error
	}{
		{name: "deleted", remove: os.Remove},
		{name: "emptied", remove: func(path string) error { return os.WriteFile(path, []byte("{}"), 0o600) }},
	} {
		t.Run(removal.name, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.runtime.log = logger
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			login := filepath.Join(t.TempDir(), "auth.json")
			first := "synthetic-model-login-dispatch-token"
			refreshed := "synthetic-model-login-refreshed-token"
			if err := os.WriteFile(login, []byte(`{"access_token":"`+first+`"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			f.custody.config.SecretScan.StaticCanaries = nil
			f.custody.config.SecretScan.Canaries = serviceScanCanaries(WorkerServiceOptions{ModelLoginFiles: []string{login}}, ProtocolCredentials{WorkerSecret: []byte("synthetic-worker-secret-round4")}, t.TempDir())
			if err := f.custody.SnapshotSecrets(context.Background(), f.pkg); err != nil {
				t.Fatal(err)
			}
			// The provider refreshes its token; a redaction observes it.
			if err := os.WriteFile(login, []byte(`{"access_token":"`+refreshed+`"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if got, err := f.custody.RedactText(context.Background(), f.pkg, "said "+refreshed); err != nil || got != "said [redacted]" {
				t.Fatalf("live login redaction = %q, %v", got, err)
			}
			if err := removal.remove(login); err != nil {
				t.Fatal(err)
			}

			got, err := f.custody.RedactText(context.Background(), f.pkg, "said "+refreshed)
			if err == nil && strings.Contains(got, refreshed) {
				t.Fatalf("redaction after login removal treats the refreshed token as clean: %q", got)
			}
			scanner, _, err := f.custody.executionScanner(context.Background(), f.pkg)
			if err == nil {
				if scanErr := scanner.scan("answer.txt", "output", strings.NewReader("said "+refreshed)); scanErr == nil {
					t.Fatal("result scan after login removal admits the refreshed token")
				}
			}
			if err := f.runtime.markPhase("assignment-1", PhaseFailed, "provider said "+refreshed, f.workspace, "thread-1"); err != nil {
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
			if strings.Contains(string(raw), refreshed) {
				t.Fatalf("snapshot carries the refreshed model token: %s", raw)
			}
			if strings.Contains(logs.String(), refreshed) {
				t.Fatalf("refreshed model token logged: %s", logs.String())
			}
		})
	}
}

// executionHistoryFiles counts the credential history snapshots recorded for
// the fixture's execution, excluding the baseline and other executions.
func executionHistoryFiles(t *testing.T, f *collectionFixture) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.custody.config.Root, "secret-scans"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), secretSnapshotKey(f.pkg)+"-") {
			count++
		}
	}
	return count
}

// An unchanged credential set is one history snapshot, however often it is
// resolved and in whatever order the resolver returns it. The service
// resolver walks login JSON maps in unspecified order, and every scan and
// redaction records what it resolved; a snapshot per ordering would make
// each later redaction read a growing pile of duplicates.
func TestSecretScanStableLoginHistoryIsIdempotent(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	names := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"}
	login := map[string]string{}
	for _, name := range names {
		login["token_"+name] = "synthetic-stable-model-" + name
	}
	raw, err := json.Marshal(login)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	f.custody.config.SecretScan.StaticCanaries = nil
	f.custody.config.SecretScan.Canaries = serviceScanCanaries(WorkerServiceOptions{ModelLoginFiles: []string{path}}, ProtocolCredentials{WorkerSecret: []byte("synthetic-independent-protocol")}, t.TempDir())
	for i := 0; i < 40; i++ {
		got, err := f.custody.RedactText(context.Background(), f.pkg, "said synthetic-stable-model-seven")
		if err != nil || got != "said [redacted]" {
			t.Fatalf("redaction %d = %q, %v", i, got, err)
		}
	}
	if got := executionHistoryFiles(t, f); got != 1 {
		t.Fatalf("an unchanged login resolved 40 times left %d history snapshots, want 1", got)
	}
}

// Recording is canonical in the values, not in their order, including when
// one credential's encoded variant is another credential's plain value: the
// signature metadata (fingerprint, base64) must not depend on which of the
// two came first.
func TestSecretScanHistoryRecordIgnoresValueOrder(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	plain := "synthetic/order/credential/value"
	escaped := "synthetic%2Forder%2Fcredential%2Fvalue"
	other := "synthetic-order-other-credential"
	orders := [][]string{
		{plain, escaped, other},
		{escaped, plain, other},
		{other, escaped, plain},
		{other, plain, escaped, plain},
	}
	for _, values := range orders {
		if err := f.custody.RecordSecretValues(context.Background(), f.pkg, values); err != nil {
			t.Fatal(err)
		}
	}
	if got := executionHistoryFiles(t, f); got != 1 {
		t.Fatalf("one credential set recorded in %d orders left %d history snapshots, want 1", len(orders), got)
	}
}
