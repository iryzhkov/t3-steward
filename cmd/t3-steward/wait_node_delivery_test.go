package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func deliveryTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "state.db")
	return cfg
}

// The daemon's half and the command's half have to meet on the same file, or
// the command proves nothing and no wake is ever promised. They are written in
// one package and read through one configuration, and this states that they
// agree: what the wait runner recorded on its tick is what "task run" reads a
// moment later.
func TestTheDaemonRecordsNodeWakeDeliveryWhereACommandCanReadIt(t *testing.T) {
	cfg := deliveryTestConfig(t)
	now := time.Now()
	delivery := nodeWakeDeliveryFor(cfg)

	// Before the daemon has recorded anything -- a host whose binary is this
	// release and whose daemon has not been restarted -- nothing is promised.
	reason := undeliverableHere("omarchy-pc", delivery, now)
	if !strings.Contains(reason, "recorded no node wake delivery") {
		t.Fatalf("a host with no receipt reported %q", reason)
	}

	record := recordNodeWakeDelivery(cfg, nodeWakeDeliveryHostRelease, nil)
	if record == nil {
		t.Fatal("the daemon could not record node wake delivery for a file-backed state path")
	}
	record(context.Background(), "omarchy-pc")
	if reason := undeliverableHere("omarchy-pc", delivery, now); reason != "" {
		t.Fatalf("a running daemon's own record did not prove delivery: %q", reason)
	}

	// The receipt says what it is, so an operator reading the file sees the
	// release and the host rather than a bare timestamp.
	receipt, path, err := delivery()
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Host != "omarchy-pc" || receipt.Release != nodeWakeDeliveryHostRelease ||
		receipt.SchemaVersion != nodeWakeDeliveryReceiptSchema || receipt.Interval == "" {
		t.Fatalf("receipt = %+v", receipt)
	}
	if filepath.Dir(path) != filepath.Dir(cfg.StatePath) {
		t.Fatalf("the receipt lives at %q, want it beside the state database", path)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt mode = %v, err = %v", info.Mode().Perm(), err)
	}

	// And it is evidence about now. A daemon that stopped leaves its last
	// receipt behind, and a command that trusted it would promise a wake that
	// nothing sends.
	stopped := now.Add(receipt.freshnessWindow()).Add(time.Second)
	reason = undeliverableHere("omarchy-pc", delivery, stopped)
	if !strings.Contains(reason, "Nothing here is delivering node wakes now") {
		t.Fatalf("a receipt older than its own window was accepted: %q", reason)
	}
	// And it reports what it knows rather than a cause it cannot see. A stale
	// receipt is also what a live daemon leaves when it cannot reach the
	// coordinator, and what one running with wait dry run on leaves always, so
	// the message names those beside a stopped daemon and its remedy covers all
	// of them.
	for _, want := range []string{"cannot reach ", "wait dry run", "restart it if it is stopped"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("the stale report does not say %q: %q", want, reason)
		}
	}
}

// A state path with no file behind it -- an in-memory store -- can hold no
// receipt, and the daemon says so once rather than writing nothing silently.
func TestAnInMemoryStateHasNoNodeWakeDeliveryReceipt(t *testing.T) {
	cfg := config.Default()
	cfg.StatePath = ":memory:"
	if _, err := nodeWakeDeliveryReceiptPath(cfg); err == nil {
		t.Fatal("an in-memory state path was given a receipt path")
	}
	if record := recordNodeWakeDelivery(cfg, nodeWakeDeliveryHostRelease, nil); record != nil {
		t.Fatal("a daemon with nowhere to write recorded node wake delivery anyway")
	}
	if reason := undeliverableHere("omarchy-pc", nodeWakeDeliveryFor(cfg), time.Now()); reason == "" {
		t.Fatal("a host whose receipt cannot be read promised a wake")
	}
}

// The command that registers a node wait builds the seam from the same
// configuration the daemon writes through. A construction that forgot it would
// report every wake as undeliverable, which is safe but useless, so it is
// stated here rather than left to the one test that happens to print a promise.
func TestTheCampaignCLIReadsThisHostsDeliveryRecord(t *testing.T) {
	cfg := deliveryTestConfig(t)
	if campaignCLIFor(cfg).delivery == nil {
		t.Fatal("the campaign CLI cannot read what this host's daemon delivers")
	}
}
