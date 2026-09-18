package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"gopkg.in/yaml.v3"
)

// receiptProbeHandler is a slog handler that, on every record, notes whether
// the receipt file already existed. Contract 2 says the receipt is written
// before the log line that reports the same outcome, and this is how a test
// sees the order.
type receiptProbeHandler struct {
	path    string
	lines   *[]string
	existed *map[string]bool
}

func (h receiptProbeHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h receiptProbeHandler) Handle(_ context.Context, record slog.Record) error {
	_, err := os.Stat(h.path)
	(*h.existed)[record.Message] = err == nil
	var attributes []string
	record.Attrs(func(attribute slog.Attr) bool {
		attributes = append(attributes, attribute.String())
		return true
	})
	*h.lines = append(*h.lines, record.Message+" "+strings.Join(attributes, " "))
	return nil
}
func (h receiptProbeHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h receiptProbeHandler) WithGroup(string) slog.Handler      { return h }

// reloadFixture is a coordinator with a configuration file on disk and an open
// store, ready to be asked to reload.
type reloadFixture struct {
	cfg      config.Config
	store    *sqlite.Store
	receipts *reloadReceiptWriter
	logger   *slog.Logger
	lines    []string
	existed  map[string]bool
}

func newReloadFixture(t *testing.T) *reloadFixture {
	t.Helper()
	root := t.TempDir()
	cfg := qualificationConfig(root)
	cfg.Path = filepath.Join(root, "config.yaml")
	writeReloadConfig(t, cfg.Path, cfg)
	loaded, err := config.LoadFile(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	receipts, err := newReloadReceiptWriter(loaded, "test-release")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &reloadFixture{cfg: loaded, store: store, receipts: receipts, existed: map[string]bool{}}
	fixture.logger = slog.New(receiptProbeHandler{path: receipts.path, lines: &fixture.lines, existed: &fixture.existed})
	return fixture
}

func writeReloadConfig(t *testing.T, path string, cfg config.Config) {
	t.Helper()
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// retainAssignment stores one nonterminal assignment on the fixture's worker,
// which is what blocks a catalog change for that worker.
func (f *reloadFixture) retainAssignment(t *testing.T, workerID string) domain.Assignment {
	t.Helper()
	now := time.Now().UTC()
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "implement", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 1,
		AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: workerID, WorkerEpoch: "worker-1",
		Epoch: 1, State: domain.AssignmentClaimed, CreatedAt: now, LeaseExpiresAt: now.Add(time.Hour),
	}
	if err := f.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	return assignment
}

func (f *reloadFixture) readReceipt(t *testing.T) backlogadmin.ReloadReceipt {
	t.Helper()
	receipt, err := readReloadReceipt(f.receipts.path)
	if err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	return receipt
}

// F-3: a refused reload used to be one WARN line and nothing else. Now it is a
// receipt with the outcome, the error and the blocking assignment, written
// before that WARN line, and the effective digest is reported unchanged.
func TestCoordinatorReloadRejectionWritesReceiptBeforeWarn(t *testing.T) {
	fixture := newReloadFixture(t)
	workerID := qualificationWorkerID()
	assignment := fixture.retainAssignment(t, workerID)
	next := cloneReloadConfig(t, fixture.cfg)
	// A project change alters the execution catalog of every worker eligible
	// for it, which is the change a retained assignment blocks.
	project := next.BacklogV2.Projects["steward"]
	project.DefaultRef = "release"
	next.BacklogV2.Projects["steward"] = project
	writeReloadConfig(t, fixture.cfg.Path, next)
	currentDigest, err := coordinatorConfigurationDigest(fixture.cfg.BacklogV2)
	if err != nil {
		t.Fatal(err)
	}

	decision := evaluateCoordinatorReload(context.Background(), fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if decision.proceed {
		t.Fatal("a catalog change on a worker with retained work was accepted")
	}
	receipt := fixture.readReceipt(t)
	if receipt.Outcome != backlogadmin.ReloadRejected {
		t.Fatalf("outcome = %q, want rejected", receipt.Outcome)
	}
	if receipt.Error == "" || !strings.Contains(receipt.Error, assignment.ID) {
		t.Fatalf("receipt error %q does not name the assignment", receipt.Error)
	}
	if receipt.ConfigurationDigest != currentDigest || receipt.PreviousDigest != currentDigest {
		t.Fatalf("a rejection changed the reported digest: %+v (effective %s)", receipt, currentDigest)
	}
	if receipt.Release != "test-release" {
		t.Fatalf("release = %q", receipt.Release)
	}
	if len(receipt.Blockers) != 1 {
		t.Fatalf("blockers = %+v, want the one retained assignment", receipt.Blockers)
	}
	blocker := receipt.Blockers[0]
	if blocker.WorkerID != workerID || blocker.AssignmentID != assignment.ID || blocker.AttemptID != assignment.AttemptID || blocker.Unblock == "" {
		t.Fatalf("blocker = %+v", blocker)
	}
	if receipt.CompletedAt.Before(receipt.RequestedAt) || receipt.RequestedAt.IsZero() {
		t.Fatalf("receipt times are not ordered: %+v", receipt)
	}
	var warned bool
	for _, line := range fixture.lines {
		if strings.HasPrefix(line, "configuration reload rejected") {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("no WARN line reported the rejection: %v", fixture.lines)
	}
	if !fixture.existed["configuration reload rejected; retaining effective configuration"] {
		t.Fatal("the WARN line was logged before the receipt existed")
	}
	if last := fixture.receipts.Last(); last == nil || last.Outcome != backlogadmin.ReloadRejected {
		t.Fatalf("the in-memory receipt for the status query is %+v", last)
	}
	info, err := os.Stat(fixture.receipts.path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt mode = %v, err = %v", info.Mode(), err)
	}
}

// An accepted reload is receipted once the new configuration is active, with
// the digest after and the digest before.
func TestCoordinatorReloadAcceptedWritesTheNewDigest(t *testing.T) {
	fixture := newReloadFixture(t)
	next := cloneReloadConfig(t, fixture.cfg)
	next.BacklogV2.Leases.Duration += config.Duration(time.Second)
	writeReloadConfig(t, fixture.cfg.Path, next)
	currentDigest, _ := coordinatorConfigurationDigest(fixture.cfg.BacklogV2)
	nextDigest, _ := coordinatorConfigurationDigest(next.BacklogV2)
	if currentDigest == nextDigest {
		t.Fatal("fixture change does not alter the digest")
	}

	decision := evaluateCoordinatorReload(context.Background(), fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if !decision.proceed {
		t.Fatalf("a policy change was refused: %+v", decision.receipt)
	}
	if _, err := os.Stat(fixture.receipts.path); err == nil {
		t.Fatal("an accepted reload was receipted before the new configuration was active")
	}
	if decision.receipt.Outcome != backlogadmin.ReloadAccepted || decision.receipt.ConfigurationDigest != nextDigest || decision.receipt.PreviousDigest != currentDigest {
		t.Fatalf("pending receipt = %+v", decision.receipt)
	}
	// The loop completes the receipt once the new services are ready.
	pending := decision.receipt
	pending.CompletedAt = fixture.receipts.now()
	if err := fixture.receipts.Write(pending); err != nil {
		t.Fatal(err)
	}
	receipt := fixture.readReceipt(t)
	if receipt.Outcome != backlogadmin.ReloadAccepted || receipt.ConfigurationDigest != nextDigest || receipt.PreviousDigest != currentDigest || receipt.Error != "" {
		t.Fatalf("receipt = %+v", receipt)
	}

	// A later receipt is never older than this one, whatever the clock says.
	older := receipt
	older.RequestedAt = receipt.RequestedAt.Add(-time.Hour)
	older.CompletedAt = older.RequestedAt
	older.Outcome = backlogadmin.ReloadUnchanged
	if err := fixture.receipts.Write(older); err != nil {
		t.Fatal(err)
	}
	if again := fixture.readReceipt(t); again.RequestedAt.Before(receipt.RequestedAt) {
		t.Fatalf("a receipt older than the previous one was written: %v < %v", again.RequestedAt, receipt.RequestedAt)
	}
}

// A signal for a file that did not change is answered with "unchanged": a
// receipt whose digests are equal, and no service restart.
func TestCoordinatorReloadUnchangedWritesReceipt(t *testing.T) {
	fixture := newReloadFixture(t)
	currentDigest, _ := coordinatorConfigurationDigest(fixture.cfg.BacklogV2)

	decision := evaluateCoordinatorReload(context.Background(), fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if decision.proceed {
		t.Fatal("an unchanged file restarted the configuration services")
	}
	receipt := fixture.readReceipt(t)
	if receipt.Outcome != backlogadmin.ReloadUnchanged || receipt.ConfigurationDigest != currentDigest || receipt.PreviousDigest != currentDigest || receipt.Error != "" {
		t.Fatalf("receipt = %+v", receipt)
	}
	if !fixture.existed["configuration reload unchanged; effective configuration retained"] {
		t.Fatalf("the INFO line was logged before the receipt existed, or not at all: %v", fixture.lines)
	}
	// A new writer for the same state directory adopts the receipt, so the
	// status query reports it after a coordinator restart.
	again, err := newReloadReceiptWriter(fixture.cfg, "test-release")
	if err != nil {
		t.Fatal(err)
	}
	if last := again.Last(); last == nil || last.Outcome != backlogadmin.ReloadUnchanged {
		t.Fatalf("adopted receipt = %+v", last)
	}
}

// The receipt and the pid file live under the coordinator's state directory,
// derived from the configured state path rather than from $HOME.
func TestCoordinatorStateDirectoryFollowsTheStatePath(t *testing.T) {
	cfg := config.Default()
	cfg.StatePath = filepath.Join(t.TempDir(), "custom", "state.db")
	path, err := coordinatorReloadReceiptPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(filepath.Dir(cfg.StatePath), "coordinator", "reload-receipt.json"); path != want {
		t.Fatalf("receipt path = %s, want %s", path, want)
	}
	pid, err := coordinatorPIDPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(pid) != filepath.Dir(path) || filepath.Base(pid) != "coordinator.pid" {
		t.Fatalf("pid path = %s", pid)
	}
	remove, err := writeCoordinatorPID(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(pid); err != nil || strings.TrimSpace(string(raw)) == "" {
		t.Fatalf("pid file = %q, %v", raw, err)
	}
	remove()
	if _, err := os.Stat(pid); err == nil {
		t.Fatal("pid file survived removal")
	}
	cfg.StatePath = ":memory:"
	if _, err := coordinatorReloadReceiptPath(cfg); err == nil {
		t.Fatal("an in-memory state path has no coordinator state directory")
	}
}

// The same receipt is carried by "coordinator identity": as the
// lastReloadReceipt field with --json, and as reload lines in text, with the
// error on a rejection.
func TestCoordinatorIdentityCarriesTheLastReload(t *testing.T) {
	completed := time.Date(2026, 9, 18, 3, 4, 5, 0, time.UTC)
	receipt := &backlogadmin.ReloadReceipt{
		RequestedAt: completed.Add(-time.Second), CompletedAt: completed,
		Outcome: backlogadmin.ReloadRejected, Error: "worker normandy has retained assignment assignment-1",
		ConfigurationDigest: "digest-before", PreviousDigest: "digest-before", Release: "rc.69",
		Blockers: []backlogadmin.ReloadBlocker{{WorkerID: "normandy", AssignmentID: "assignment-1", AttemptID: "attempt-1", Unblock: "t3-steward backlog cancel run-1/implement --reason TEXT"}},
	}
	status := backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{
		Owner: "coordinator", Release: "rc.69", ConfigurationDigest: "digest-before", Epoch: 3, Health: "healthy", LastReloadReceipt: receipt,
	}}
	description := backlogadmin.TransportDescription{Carrier: backlogadmin.CarrierLocal, CoordinatorID: "coordinator", Endpoint: "/run/state.db.admin.sock"}

	var out bytes.Buffer
	if err := renderCoordinatorIdentity(&out, true, coordinatorIdentityFrom(description, status)); err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	last, _ := document["lastReloadReceipt"].(map[string]any)
	if last["outcome"] != "rejected" || last["error"] != receipt.Error || last["configurationDigest"] != "digest-before" {
		t.Fatalf("lastReloadReceipt = %#v", document["lastReloadReceipt"])
	}
	blockers, _ := last["blockers"].([]any)
	if len(blockers) != 1 {
		t.Fatalf("blockers = %#v", last["blockers"])
	}

	out.Reset()
	if err := renderCoordinatorIdentity(&out, false, coordinatorIdentityFrom(description, status)); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"reload       rejected at 2026-09-18T03:04:05Z (digest-before)",
		"error: worker normandy has retained assignment assignment-1",
		"assignment-1 attempt attempt-1",
		"t3-steward backlog cancel run-1/implement",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("identity text lacks %q:\n%s", want, text)
		}
	}

	// Before the first reload there is no receipt and no reload line.
	status.Runtime.LastReloadReceipt = nil
	out.Reset()
	if err := renderCoordinatorIdentity(&out, false, coordinatorIdentityFrom(description, status)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "reload") {
		t.Fatalf("identity text reports a reload that never happened:\n%s", out.String())
	}
}
