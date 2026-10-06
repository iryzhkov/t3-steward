package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
