package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A higher-epoch offer stops the superseded execution while the journal lock
// is held. A stop error quoting a credential must not reach the runtime log
// raw, whether or not the scanner can check the text.
func TestSecretScanSupersededStopLogRedacted(t *testing.T) {
	secret := "synthetic-superseded-review-credential"
	for _, test := range []struct {
		name   string
		wrap   func(*fakeDriver) Driver
		expect string
	}{
		{name: "redacted", wrap: func(d *fakeDriver) Driver { return redactingDriver{d, secret} }, expect: "[redacted]"},
		{name: "withheld", wrap: func(d *fakeDriver) Driver { return failingRedactor{d} }, expect: withheldFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, driver := preparedRunningRuntime(t)
			driver.stopErr = errors.New("kill helper --token=" + secret + " failed")
			runtime.driver = test.wrap(driver)
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
			offer := testOffer(t)
			offer.Assignment.Epoch++
			pkg := testPackage()
			pkg.Identity.AssignmentEpoch = offer.Assignment.Epoch
			manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
			if err != nil {
				t.Fatal(err)
			}
			offer.Package = manifest
			claims, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
			if err != nil {
				t.Fatal(err)
			}
			if driver.stopCalls == 0 || len(claims.Claims) != 0 {
				t.Fatalf("superseded execution not stopped and withheld: stops=%d claims=%+v", driver.stopCalls, claims)
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "superseded assignment could not be contained") || !strings.Contains(logs.String(), test.expect) {
				t.Fatalf("containment failure not logged with its %s error: %s", test.name, logs.String())
			}
		})
	}
}

// An activation's completion reason is logged before publication can scan
// it. A provider failure quoting a credential must not reach that log raw,
// whether or not the scanner can check the text.
func TestSecretScanActivationFailureLogRedacted(t *testing.T) {
	secret := "synthetic-activation-log-review-credential"
	for _, test := range []struct {
		name        string
		unavailable bool
		expect      string
	}{
		{name: "redacted", expect: "token=[redacted]"},
		{name: "withheld", unavailable: true, expect: withheldFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			if test.unavailable {
				makeSecretHistoryUnreadable(t, f.custody)
			}
			f.control.message = FailedMarker + "\nprovider failed: token=" + secret
			if err := f.driver.collectActivation(context.Background(), testActivationPackage(), f.workspace); err == nil {
				t.Fatal("credential-bearing activation result was published")
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "supervision activation turn ended") || !strings.Contains(logs.String(), test.expect) {
				t.Fatalf("activation reason not logged %s: %s", test.name, logs.String())
			}
		})
	}
}

// A repeated collection that reuses the earlier clean turn discards the
// current reading, so the result scan never sees it. The reading it logs must
// not carry a credential raw, whether or not the scanner can check the text.
func TestSecretScanRepeatedCollectionLogRedacted(t *testing.T) {
	secret := "synthetic-repeated-review-credential"
	for _, test := range []struct {
		name        string
		unavailable bool
		expect      string
	}{
		{name: "redacted", expect: "token=[redacted]"},
		{name: "withheld", unavailable: true, expect: withheldFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			f.driver.Log = logger
			f.custody.config.SecretScan.Log = logger
			f.custody.config.SecretScan.StaticCanaries = []string{secret}
			f.driver.Publisher = &collectionTransientPublisher{CustodyStore: f.custody, failures: 1}
			if err := f.driver.collect(context.Background(), f.pkg, f.workspace, ""); err == nil {
				t.Fatal("transient publication failure missing")
			}
			if test.unavailable {
				// Publication stays transient so the reading is still logged.
				f.driver.Publisher = &collectionTransientPublisher{CustodyStore: f.custody, failures: 1}
				makeSecretHistoryUnreadable(t, f.custody)
			}
			f.control.message = FailedMarker + "\nprovider failed: token=" + secret
			err := f.driver.collect(context.Background(), f.pkg, f.workspace, "")
			if !test.unavailable && err != nil {
				t.Fatal(err)
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "judging the turn this collection recorded") || !strings.Contains(logs.String(), test.expect) {
				t.Fatalf("repeated-collection reading not logged %s: %s", test.name, logs.String())
			}
		})
	}
}

// stopPreparationFailure is a driver whose preparation stop fails and whose
// redaction is redact.
type stopPreparationFailure struct {
	*fakeDriver
	err    error
	redact func(string) (string, error)
}

func (d stopPreparationFailure) StopPreparation(context.Context, workerproto.ExecutionPackage) error {
	return d.err
}

func (d stopPreparationFailure) RedactFailure(_ context.Context, _ workerproto.ExecutionPackage, failure string) (string, error) {
	return d.redact(failure)
}

// A task timeout before dispatch stops the preparation before it records the
// failure. A stop error quoting a credential is deferred to the next pass and
// must not leave Reconcile, which the worker daemon logs, or reach the runtime
// log raw, whether or not the scanner can check the text.
func TestSecretScanTimeoutStopPreparationRedacted(t *testing.T) {
	secret := "synthetic-timeout-preparation-credential"
	for _, test := range []struct {
		name   string
		redact func(string) (string, error)
		expect string
	}{
		{name: "redacted", redact: func(text string) (string, error) { return strings.ReplaceAll(text, secret, "[redacted]"), nil }, expect: "[redacted]"},
		{name: "withheld", redact: func(string) (string, error) { return "", errors.New("credential history unreadable") }, expect: withheldFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeDriver{}
			runtime := newClaimedRuntime(t, t.TempDir(), driver)
			runtime.driver = stopPreparationFailure{driver, errors.New("export thread failed: token=" + secret), test.redact}
			if err := runtime.journal.update(func(state *journalState) error {
				record := state.Attempts["assignment-1"]
				record.Package.Package.Timeout = time.Minute
				record.Package.Package.CreatedAt = runtimeTestNow.Add(-time.Hour)
				state.Attempts["assignment-1"] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewTextHandler(&logs, nil))
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile returned a driver error: %v", err)
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("credential logged: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "task timeout reconciliation deferred") || !strings.Contains(logs.String(), test.expect) {
				t.Fatalf("deferred timeout not logged with its %s error: %s", test.name, logs.String())
			}
			state, err := runtime.journal.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if phase := state.Attempts["assignment-1"].Phase; phase == PhaseFailed {
				t.Fatal("timeout released an unproven preparation")
			}
		})
	}
}

// Redaction runs inline on log and failure text an agent controls. Text with
// many credential echoes must cost time in proportion to its length, not to
// its length times its matches.
func TestSecretScanRedactionLinearInMatches(t *testing.T) {
	secret := "synthetic-many-matches-credential"
	scanner := newResultScanner(SecretScanConfig{StaticCanaries: []string{secret}}, nil, nil)
	const lines = 10000
	text := strings.Repeat("token="+secret+" 100% done\n", lines)
	start := time.Now()
	redacted := scanner.safeName(text)
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("redacting %d bytes with %d matches took %s", len(text), lines, elapsed)
	}
	if strings.Contains(redacted, secret) || strings.Count(redacted, "[redacted]") != lines {
		t.Fatalf("not every match redacted: %d of %d", strings.Count(redacted, "[redacted]"), lines)
	}
}

// Redaction searches bounded windows; a credential, raw or percent-encoded,
// that straddles a window edge is still redacted.
func TestSecretScanRedactionAcrossWindowEdges(t *testing.T) {
	secret := "synthetic-window-edge-credential"
	scanner := newResultScanner(SecretScanConfig{StaticCanaries: []string{secret}}, nil, nil)
	var encoded strings.Builder
	for i := 0; i < len(secret); i++ {
		fmt.Fprintf(&encoded, "%%%02X", secret[i])
	}
	step := max(4096, scanner.overlap)
	for _, form := range []string{secret, encoded.String()} {
		for _, edge := range []int{step, 2 * step, step + scanner.overlap} {
			for at := edge - len(form) - 2; at <= edge+2; at++ {
				text := strings.Repeat("a", at) + form + strings.Repeat("b", 3*step)
				if redacted := scanner.safeName(text); strings.Contains(redacted, secret) || strings.Contains(redacted, form) ||
					redacted != strings.Repeat("a", at)+"[redacted]"+strings.Repeat("b", 3*step) {
					t.Fatalf("credential at %d (edge %d, %d bytes) not redacted exactly", at, edge, len(form))
				}
			}
		}
	}
}

// makeSecretHistoryUnreadable replaces the credential history directory with
// a regular file, so the scanner cannot check any text.
func makeSecretHistoryUnreadable(t *testing.T, custody *CustodyStore) {
	t.Helper()
	history := filepath.Join(custody.config.Root, "secret-scans")
	if err := os.RemoveAll(history); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(history, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
}
