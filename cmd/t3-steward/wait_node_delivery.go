package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// A node wake is sent by the steward daemon of the host the wait names, and
// never by the command that registered the wait. The command therefore knows
// nothing about the thing that has to keep its promise: on a host whose binary
// is v0.11.0-rc.71 and whose daemon has not been restarted, or is stopped, or
// runs with wait dry run on, the registration succeeds and nothing ever wakes
// the thread. Comparing the recorded host with this host's name cannot see any
// of that, because all of it is true of this host's name either way.
//
// So the daemon writes down what it is doing, where a command can read it. On
// every tick on which its wait runner read the node waits of this host and was
// in a position to send their wakes, it rewrites this receipt beside the state
// database. A receipt that is present, fresh and names this host is evidence
// that the wake will be delivered here; its absence is evidence of nothing, and
// is reported as exactly that rather than as a promise.
//
// The freshness window is what makes the receipt evidence about now rather than
// about the last time a daemon ran here: a stopped daemon stops rewriting it,
// and a stale file is refused. The interval the daemon writes into the receipt
// is the one it rewrites the receipt on, so the window is derived from the
// daemon's own configuration rather than guessed by the reader.
const nodeWakeDeliveryReceiptSchema = 1

// nodeWakeDeliveryFloor is the shortest freshness window used, however short
// the daemon's tick is. One missed tick must not read as a stopped daemon, and
// a tick that spent its budget on an unreachable coordinator is a missed tick.
const nodeWakeDeliveryFloor = 2 * time.Minute

type nodeWakeDeliveryReceipt struct {
	SchemaVersion int `json:"schema_version"`
	// Release is the steward release the daemon that wrote this is running. It
	// is recorded for the report rather than for the decision: a release that
	// does not deliver node wakes for this host writes no receipt at all.
	Release   string    `json:"release"`
	Host      string    `json:"host"`
	Interval  string    `json:"interval"`
	UpdatedAt time.Time `json:"updated_at"`
}

// nodeWakeDeliveryReceiptPath is <state dir>/node-wake-delivery.json, beside
// the state database. It is derived from the configured state path and never
// from $HOME, so a daemon and a command that load the same configuration agree
// on it and a test or an alternate state path relocates both.
func nodeWakeDeliveryReceiptPath(cfg config.Config) (string, error) {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return "", err
	}
	if statePath == ":memory:" {
		return "", errors.New("a node wake delivery receipt requires a file-backed state path")
	}
	absolute, err := filepath.Abs(statePath)
	if err != nil {
		return "", fmt.Errorf("resolve the node wake delivery receipt: %w", err)
	}
	return filepath.Join(filepath.Dir(absolute), "node-wake-delivery.json"), nil
}

// recordNodeWakeDelivery is the daemon's half: the wait runner's NodeDelivery
// seam, bound to this configuration and release. It returns nil when there is
// no path to write to, which leaves every registration on this host reported as
// undeliverable rather than promised on no evidence.
func recordNodeWakeDelivery(cfg config.Config, release string, log *slog.Logger) func(context.Context, string) {
	path, err := nodeWakeDeliveryReceiptPath(cfg)
	if err != nil {
		if log != nil {
			log.Warn("node wake delivery cannot be recorded, so commands on this host will not promise a wake", "err", err)
		}
		return nil
	}
	interval := cfg.Polling.SnapshotInterval.D()
	var reported bool
	return func(_ context.Context, host string) {
		receipt := nodeWakeDeliveryReceipt{
			SchemaVersion: nodeWakeDeliveryReceiptSchema,
			Release:       release,
			Host:          host,
			Interval:      interval.String(),
			UpdatedAt:     time.Now().UTC(),
		}
		if err := writeNodeWakeDeliveryReceipt(path, receipt); err != nil && log != nil && !reported {
			// Once per process: a receipt that cannot be written is a real
			// fault, and it is also one that would otherwise be logged four
			// times a minute forever.
			reported = true
			log.Warn("the node wake delivery receipt could not be written, so commands on this host will not promise a wake",
				"path", path, "err", err)
		}
	}
}

// writeNodeWakeDeliveryReceipt replaces the receipt atomically, so a reader
// never sees half of one.
func writeNodeWakeDeliveryReceipt(path string, receipt nodeWakeDeliveryReceipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

// readNodeWakeDeliveryReceipt reads what the daemon last recorded. A missing
// file is not an error of this function's making and is returned as one anyway,
// because the caller's question is "can I prove delivery", and "there is no
// receipt" is one of the ways the answer is no.
func readNodeWakeDeliveryReceipt(path string) (nodeWakeDeliveryReceipt, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nodeWakeDeliveryReceipt{}, err
	}
	var receipt nodeWakeDeliveryReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return nodeWakeDeliveryReceipt{}, fmt.Errorf("decode the node wake delivery receipt: %w", err)
	}
	return receipt, nil
}

// nodeWakeDelivery is the seam a command reads this evidence through: the
// receipt and the path it was looked for at, or the error that says why there
// is none. The path travels with the answer because it is the one thing an
// operator needs in order to check the claim by hand.
type nodeWakeDelivery func() (nodeWakeDeliveryReceipt, string, error)

// nodeWakeDeliveryFor binds that seam to a configuration.
func nodeWakeDeliveryFor(cfg config.Config) nodeWakeDelivery {
	return func() (nodeWakeDeliveryReceipt, string, error) {
		path, err := nodeWakeDeliveryReceiptPath(cfg)
		if err != nil {
			return nodeWakeDeliveryReceipt{}, "", err
		}
		receipt, err := readNodeWakeDeliveryReceipt(path)
		return receipt, path, err
	}
}

// freshnessWindow is how old a receipt may be and still describe a running
// daemon: four of its own ticks, never less than the floor.
func (r nodeWakeDeliveryReceipt) freshnessWindow() time.Duration {
	interval, err := time.ParseDuration(r.Interval)
	if err != nil || interval <= 0 {
		return nodeWakeDeliveryFloor
	}
	if window := 4 * interval; window > nodeWakeDeliveryFloor {
		return window
	}
	return nodeWakeDeliveryFloor
}

// undeliverableHere says why a wake registered for this host cannot be shown to
// arrive here, and is empty when it can be.
//
// Every branch reports what was and was not established, in D-4's form: the
// command says what it knows rather than what it hopes. A daemon that has not
// been restarted onto this release is the expected cause during an upgrade, so
// the branches that establish nothing about any daemon here name restarting the
// steward as the remedy. The stale branch is the one that does have evidence of
// a daemon and cannot say which way it failed, so it names its own remedy.
func undeliverableHere(local string, delivery nodeWakeDelivery, now time.Time) string {
	remedy := ". A node wake is sent by this host's steward daemon and not by this command, so restart the steward on " +
		local + " -- replacing the binary does not restart it -- and run this again"
	if delivery == nil {
		return "this command cannot read what this host's steward daemon is doing, so nothing here shows that a wake " +
			"registered for " + local + " would be delivered" + remedy
	}
	receipt, path, err := delivery()
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "this host's steward daemon has recorded no node wake delivery (there is no receipt at " + path +
			"), so nothing shows that a wake registered for " + local + " would be delivered" + remedy
	case err != nil:
		return "this host's record of node wake delivery could not be read (" + path + ": " + err.Error() +
			"), so nothing shows that a wake registered for " + local + " would be delivered" + remedy
	case receipt.Host != local:
		return "this host's steward daemon last recorded node wake delivery for " + strconv.Quote(receipt.Host) +
			" rather than for " + local + " (" + path + "), so nothing shows that a wake registered for " +
			local + " would be delivered" + remedy
	case now.Sub(receipt.UpdatedAt) > receipt.freshnessWindow():
		// What is established here is the absence of a fresh receipt, not a
		// stopped daemon. A daemon that is alive rewrites this file only on a
		// tick that read this host's node waits and could send their wakes, so a
		// tick that returned early on an unreachable coordinator or an unreadable
		// T3 server leaves the receipt exactly as stale as a daemon that exited,
		// and a daemon running with wait dry run on never writes it at all. Each
		// of those means the same thing for this wake -- nothing here is
		// delivering node wakes now -- and naming only one of them would send the
		// reader to a remedy that does not apply, so the message names the fact
		// and then the causes it cannot distinguish.
		return fmt.Sprintf("no steward daemon here has recorded a node wake delivery for %s within its own "+
			"freshness window of %s: the last one was %s ago, at %s (%s). Nothing here is delivering node "+
			"wakes now, and this cannot tell which cause it is -- the daemon is stopped, or it was never "+
			"restarted onto this release, or it is running and its ticks end early because it cannot reach "+
			"the coordinator or its own T3 server, or it runs with wait dry run on, where a wake is held "+
			"rather than sent. A node wake is sent by this host's steward daemon and not by this command, "+
			"so on %s check that the steward is running, that its log shows it reaching the coordinator, "+
			"and that wait dry run is off; restart it if it is stopped or still on an older release -- "+
			"replacing the binary does not restart it -- and run this again",
			local, receipt.freshnessWindow(), now.Sub(receipt.UpdatedAt).Round(time.Second),
			receipt.UpdatedAt.Format(time.RFC3339), path, local)
	}
	return ""
}
