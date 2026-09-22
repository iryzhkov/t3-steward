package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// coordinatorStateDir is the directory beside the coordinator's state database
// that holds what the coordinator process says about itself: its pid file and
// its reload receipt. It is derived from the configured state path, never from
// $HOME directly, so a test or an alternate state path relocates it too.
func coordinatorStateDir(cfg config.Config) (string, error) {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return "", err
	}
	if statePath == ":memory:" {
		return "", errors.New("the coordinator state directory requires a file-backed state path")
	}
	absolute, err := filepath.Abs(statePath)
	if err != nil {
		return "", fmt.Errorf("resolve coordinator state directory: %w", err)
	}
	return filepath.Join(filepath.Dir(absolute), "coordinator"), nil
}

// coordinatorReloadReceiptPath is where the coordinator writes the verdict of
// every SIGHUP: <state dir>/coordinator/reload-receipt.json, which is
// ~/.local/state/t3-steward/coordinator/reload-receipt.json under the default
// state path.
func coordinatorReloadReceiptPath(cfg config.Config) (string, error) {
	dir, err := coordinatorStateDir(cfg)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "reload-receipt.json"), nil
}

// coordinatorPIDPath is the pid file the coordinator writes beside its receipt,
// so that "coordinator reload" on the same host can signal it without asking
// systemd which process is the coordinator.
func coordinatorPIDPath(cfg config.Config) (string, error) {
	dir, err := coordinatorStateDir(cfg)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "coordinator.pid"), nil
}

// reloadReceiptWriter records the verdict of every reload request. It is the
// coordinator's half of contract 2: one receipt per signal, written atomically
// (temporary file and rename) before the log line that reports the same
// outcome, never older than the receipt before it, and kept in memory so the
// status query carries the same record.
type reloadReceiptWriter struct {
	path    string
	release string
	now     func() time.Time

	mu   sync.Mutex
	last *backlogadmin.ReloadReceipt
}

func newReloadReceiptWriter(cfg config.Config, release string) (*reloadReceiptWriter, error) {
	path, err := coordinatorReloadReceiptPath(cfg)
	if err != nil {
		return nil, err
	}
	writer := &reloadReceiptWriter{path: path, release: release, now: func() time.Time { return time.Now().UTC() }}
	// A receipt left by a previous coordinator process is still the last
	// verdict this host gave, so status keeps reporting it across a restart.
	if existing, err := readReloadReceipt(path); err == nil {
		writer.last = &existing
	}
	return writer, nil
}

// Last returns a copy of the most recent receipt, or nil before the first.
func (w *reloadReceiptWriter) Last() *backlogadmin.ReloadReceipt {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == nil {
		return nil
	}
	copied := *w.last
	copied.Blockers = append([]backlogadmin.ReloadBlocker(nil), w.last.Blockers...)
	return &copied
}

// Write records one receipt. A request time earlier than the previous receipt's
// is clamped to it, so a wall clock stepping backwards can never make the file
// look older than the verdict it replaces.
func (w *reloadReceiptWriter) Write(receipt backlogadmin.ReloadReceipt) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if receipt.Release == "" {
		receipt.Release = w.release
	}
	if w.last != nil && receipt.RequestedAt.Before(w.last.RequestedAt) {
		receipt.RequestedAt = w.last.RequestedAt
	}
	if receipt.CompletedAt.Before(receipt.RequestedAt) {
		receipt.CompletedAt = receipt.RequestedAt
	}
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return fmt.Errorf("encode reload receipt: %w", err)
	}
	raw = append(raw, '\n')
	dir := filepath.Dir(w.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create coordinator state directory: %w", err)
	}
	stage, err := os.CreateTemp(dir, ".reload-receipt-*")
	if err != nil {
		return fmt.Errorf("stage reload receipt: %w", err)
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	if _, err := stage.Write(raw); err != nil {
		stage.Close()
		return fmt.Errorf("write reload receipt: %w", err)
	}
	if err := stage.Chmod(0o600); err != nil {
		stage.Close()
		return fmt.Errorf("write reload receipt: %w", err)
	}
	if err := stage.Close(); err != nil {
		return fmt.Errorf("write reload receipt: %w", err)
	}
	if err := os.Rename(stagePath, w.path); err != nil {
		return fmt.Errorf("publish reload receipt: %w", err)
	}
	w.last = &receipt
	return nil
}

// readReloadReceipt decodes the receipt file at path.
func readReloadReceipt(path string) (backlogadmin.ReloadReceipt, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return backlogadmin.ReloadReceipt{}, err
	}
	var receipt backlogadmin.ReloadReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return backlogadmin.ReloadReceipt{}, fmt.Errorf("decode reload receipt %s: %w", path, err)
	}
	if receipt.RequestedAt.IsZero() || receipt.Outcome == "" {
		return backlogadmin.ReloadReceipt{}, fmt.Errorf("decode reload receipt %s: no request time or outcome", path)
	}
	return receipt, nil
}

// writeCoordinatorPID publishes this process's pid beside the receipt and
// returns the function that removes it. The file is owner-only like the rest of
// the state directory.
func writeCoordinatorPID(cfg config.Config) (func(), error) {
	path, err := coordinatorPIDPath(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create coordinator state directory: %w", err)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return nil, fmt.Errorf("write coordinator pid file: %w", err)
	}
	return func() { _ = os.Remove(path) }, nil
}

// reloadRecordSource is what a reload decision reads from the coordinator
// store: the retained assignments and attempts, and the last snapshot every
// worker sent.
type reloadRecordSource interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
}

// reloadDecision is the outcome of one SIGHUP before activation. A rejected or
// unchanged request has already been receipted and logged; an accepted one
// carries the receipt the loop completes once the new configuration is active.
type reloadDecision struct {
	proceed bool
	next    config.Config
	receipt backlogadmin.ReloadReceipt
}

// evaluateCoordinatorReload re-reads the configuration file and decides. The
// receipt is written before the WARN or INFO line that reports the verdict, so
// a reader that saw the line finds the receipt.
func evaluateCoordinatorReload(ctx context.Context, current config.Config, logger *slog.Logger, store reloadRecordSource, receipts *reloadReceiptWriter) reloadDecision {
	requestedAt := receipts.now()
	currentDigest, digestErr := coordinatorConfigurationDigest(current.BacklogV2)
	receipt := backlogadmin.ReloadReceipt{
		RequestedAt: requestedAt, PreviousDigest: currentDigest, ConfigurationDigest: currentDigest,
	}
	next, blockers, err := loadCoordinatorReload(ctx, current, store)
	if err == nil && digestErr != nil {
		err = digestErr
	}
	if err != nil {
		receipt.Outcome = backlogadmin.ReloadRejected
		receipt.Error = err.Error()
		receipt.Blockers = blockers
		receipt.CompletedAt = receipts.now()
		if writeErr := receipts.Write(receipt); writeErr != nil {
			logger.Warn("reload receipt not written", "error", writeErr, "path", receipts.path)
		}
		logger.Warn("configuration reload rejected; retaining effective configuration",
			"error", err, "blockers", len(blockers), "configurationDigest", currentDigest, "receipt", receipts.path)
		return reloadDecision{receipt: receipt}
	}
	nextDigest, err := coordinatorConfigurationDigest(next.BacklogV2)
	if err != nil {
		receipt.Outcome = backlogadmin.ReloadRejected
		receipt.Error = err.Error()
		receipt.CompletedAt = receipts.now()
		if writeErr := receipts.Write(receipt); writeErr != nil {
			logger.Warn("reload receipt not written", "error", writeErr, "path", receipts.path)
		}
		logger.Warn("configuration reload rejected; retaining effective configuration", "error", err, "receipt", receipts.path)
		return reloadDecision{receipt: receipt}
	}
	if nextDigest == currentDigest {
		receipt.Outcome = backlogadmin.ReloadUnchanged
		receipt.CompletedAt = receipts.now()
		if writeErr := receipts.Write(receipt); writeErr != nil {
			logger.Warn("reload receipt not written", "error", writeErr, "path", receipts.path)
		}
		logger.Info("configuration reload unchanged; effective configuration retained",
			"configurationDigest", currentDigest, "receipt", receipts.path)
		return reloadDecision{receipt: receipt}
	}
	receipt.Outcome = backlogadmin.ReloadAccepted
	receipt.ConfigurationDigest = nextDigest
	return reloadDecision{proceed: true, next: next, receipt: receipt}
}

// A reload replaces configuration-bound services under the same acquired
// coordinator authority. Existing assignment records and packages remain immutable.
func coordinatorConfigLoop(ctx context.Context, cfg config.Config, logger *slog.Logger, store *sqlite.Store, epoch int64) error {
	receipts, err := newReloadReceiptWriter(cfg, version)
	if err != nil {
		return err
	}
	removePID, err := writeCoordinatorPID(cfg)
	if err != nil {
		return err
	}
	defer removePID()
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	watchdogDone := make(chan struct{})
	go func() { defer close(watchdogDone); runWatchdogAlongside(watchdogCtx, cfg, logger, store) }()
	defer func() { stopWatchdog(); <-watchdogDone }()

	run := func(instanceCtx context.Context, active config.Config, ready func()) error {
		return runCoordinatorConfiguration(instanceCtx, active, logger, store, epoch, receipts, ready)
	}
	return coordinatorConfigurationActivationLoop(ctx, cfg, logger, store, epoch, receipts, reload, run)
}

type coordinatorConfigurationRunner func(context.Context, config.Config, func()) error

// coordinatorConfigurationActivationLoop is the stop/start/readiness/receipt
// transaction used by the production SIGHUP loop. Its runner seam keeps tests
// isolated from listeners while preserving the exact production state machine.
func coordinatorConfigurationActivationLoop(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
	store *sqlite.Store,
	epoch int64,
	receipts *reloadReceiptWriter,
	reload <-chan os.Signal,
	run coordinatorConfigurationRunner,
) error {
	var fallback *config.Config
	// pending is the accepted receipt of the reload being activated. It is
	// written once the new configuration is active, or as a rejection when the
	// activation fails and the prior configuration comes back.
	var pending *backlogadmin.ReloadReceipt
	for {
		instanceCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		ready := make(chan struct{})
		go func(active config.Config) {
			done <- run(instanceCtx, active, func() { close(ready) })
		}(cfg)
		select {
		case <-ready:
			fallback = nil
			if pending != nil {
				pending.CompletedAt = receipts.now()
				if err := receipts.Write(*pending); err != nil {
					logger.Warn("reload receipt not written", "error", err, "path", receipts.path)
				}
				logger.Info("coordinator configuration activated", "epoch", epoch,
					"configurationDigest", pending.ConfigurationDigest, "previousDigest", pending.PreviousDigest, "receipt", receipts.path)
				pending = nil
			}
		case err := <-done:
			stop()
			if fallback != nil {
				if pending != nil {
					rejected := *pending
					rejected.Outcome = backlogadmin.ReloadRejected
					rejected.Error = "activation failed; prior configuration restored: " + err.Error()
					rejected.ConfigurationDigest = rejected.PreviousDigest
					rejected.CompletedAt = receipts.now()
					if writeErr := receipts.Write(rejected); writeErr != nil {
						logger.Warn("reload receipt not written", "error", writeErr, "path", receipts.path)
					}
					pending = nil
				}
				logger.Error("configuration activation failed; restoring prior configuration", "error", err, "receipt", receipts.path)
				cfg = *fallback
				fallback = nil
				continue
			}
			return err
		case <-ctx.Done():
			stop()
			return <-done
		}
	selectLoop:
		for {
			select {
			case <-ctx.Done():
				stop()
				return <-done
			case err := <-done:
				stop()
				return err
			case <-reload:
				decision := evaluateCoordinatorReload(ctx, cfg, logger, store, receipts)
				if !decision.proceed {
					continue
				}
				stop()
				if err := <-done; err != nil {
					logger.Warn("configuration services stopped for reload", "error", err)
				}
				prior := cfg
				fallback = &prior
				cfg = decision.next
				accepted := decision.receipt
				pending = &accepted
				logger.Info("activating coordinator configuration", "epoch", epoch,
					"configurationDigest", accepted.ConfigurationDigest, "previousDigest", accepted.PreviousDigest)
				break selectLoop
			}
		}
	}
}

func coordinatorConfigurationDigest(settings config.BacklogV2) (string, error) {
	raw, err := json.Marshal(settings)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// loadCoordinatorReload re-reads the configuration file and validates it
// against the effective one. On a refusal caused by retained work it also
// returns the blockers, one per assignment, so the receipt can name them.
func loadCoordinatorReload(ctx context.Context, current config.Config, store reloadRecordSource) (config.Config, []backlogadmin.ReloadBlocker, error) {
	if current.Path == "" {
		return current, nil, errors.New("reload requires a configuration file")
	}
	if _, err := os.Stat(current.Path); err != nil {
		return current, nil, err
	}
	next, err := config.LoadFile(current.Path)
	if err != nil {
		return current, nil, err
	}
	if err = validateCoordinatorReload(current, next); err != nil {
		return current, nil, err
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return current, nil, err
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil {
		return current, nil, err
	}
	blockers, err := reloadBlockers(current.BacklogV2, next.BacklogV2, records, snapshots, time.Now())
	if err != nil {
		return current, nil, err
	}
	if len(blockers) != 0 {
		return current, blockers, errors.New(reloadBlockersMessage(blockers))
	}
	return next, nil, nil
}

// reloadBlockers names every retained assignment on a worker whose execution
// catalog the next configuration would change. Every one of them is listed, not
// only the first, with the attempt's coordinator-side progress and control, the
// phase the worker last reported for it, and the action that unblocks it.
//
// The rule itself is unchanged from rc.68: an assignment that is not completed
// or released blocks a catalog change on its worker, at any phase. Accepting
// the reload for dispatched work is not done here; see the handoff for the
// worker-side drain guard that keeps it that way.
func reloadBlockers(current, next config.BacklogV2, records sqlite.CoordinatorRecords, snapshots []domain.WorkerSnapshot, now time.Time) ([]backlogadmin.ReloadBlocker, error) {
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	phases := map[string]string{}
	for _, snapshot := range snapshots {
		for _, observation := range snapshot.Assignments {
			if observation.Journal != nil && observation.Journal.Phase != "" {
				phases[snapshot.WorkerID+"/"+observation.AssignmentID] = observation.Journal.Phase
			}
		}
	}
	changed := map[string]bool{}
	var blockers []backlogadmin.ReloadBlocker
	for _, assignment := range records.Assignments {
		if assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased {
			continue
		}
		touched, known := changed[assignment.WorkerID]
		if !known {
			before, err := workerruntime.BuildWorkerBinding(current, assignment.WorkerID, now)
			if err != nil {
				return nil, err
			}
			after, err := workerruntime.BuildWorkerBinding(next, assignment.WorkerID, now)
			touched = err != nil || before.CatalogRevision != after.CatalogRevision
			changed[assignment.WorkerID] = touched
		}
		if !touched {
			continue
		}
		blocker := backlogadmin.ReloadBlocker{
			WorkerID: assignment.WorkerID, AssignmentID: assignment.ID, AttemptID: assignment.AttemptID,
			JournalPhase: phases[assignment.WorkerID+"/"+assignment.ID],
		}
		attempt, found := attempts[assignment.AttemptID]
		if found {
			blocker.Progress = string(attempt.Progress)
			blocker.Control = string(attempt.Control)
		}
		blocker.Unblock = reloadUnblockAction(attempt, found)
		blockers = append(blockers, blocker)
	}
	sort.Slice(blockers, func(i, j int) bool {
		if blockers[i].WorkerID != blockers[j].WorkerID {
			return blockers[i].WorkerID < blockers[j].WorkerID
		}
		return blockers[i].AssignmentID < blockers[j].AssignmentID
	})
	return blockers, nil
}

// reloadUnblockAction is the operator action that removes one blocker: the
// cancel command for the attempt's task, or, for a paused or parked attempt,
// waiting for the pause or the wait to end first.
func reloadUnblockAction(attempt domain.Attempt, found bool) string {
	if !found || attempt.WorkflowRunID == "" || attempt.TaskID == "" {
		return "drain the worker (accept_backlog: false) and let the assignment settle, or cancel its task with t3-steward backlog cancel <run>/<task> --reason TEXT"
	}
	cancel := fmt.Sprintf("t3-steward backlog cancel %s/%s --reason TEXT", attempt.WorkflowRunID, attempt.TaskID)
	switch attempt.Control {
	case domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlDraining, domain.ControlResuming:
		return "wait for the pause to lift and the attempt to settle, or " + cancel
	case domain.ControlWaitingExternal:
		return "wait for the task wait to settle and the attempt to finish, or " + cancel
	}
	return "let the attempt settle, or " + cancel
}

// reloadBlockersMessage is the one-line refusal that names every blocker.
func reloadBlockersMessage(blockers []backlogadmin.ReloadBlocker) string {
	var b strings.Builder
	workers := map[string]bool{}
	for _, blocker := range blockers {
		workers[blocker.WorkerID] = true
	}
	fmt.Fprintf(&b, "%d retained assignment(s) on %d worker(s) whose execution catalog would change; drain and settle, or cancel, before changing it:", len(blockers), len(workers))
	for i, blocker := range blockers {
		if i > 0 {
			b.WriteString(";")
		}
		state := blocker.Progress
		if blocker.Control != "" {
			state += "/" + blocker.Control
		}
		if state == "" {
			state = "attempt record missing"
		}
		if blocker.JournalPhase != "" {
			state += ", worker journal " + blocker.JournalPhase
		}
		fmt.Fprintf(&b, " worker %s assignment %s attempt %s (%s): %s", blocker.WorkerID, blocker.AssignmentID, blocker.AttemptID, state, blocker.Unblock)
	}
	return b.String()
}

func validateCoordinatorReload(current, next config.Config) error {
	oldOuter, newOuter := current.LifecycleView(), next.LifecycleView()
	oldOuter.BacklogV2 = config.BacklogV2{}
	newOuter.BacklogV2 = config.BacklogV2{}
	if !reflect.DeepEqual(oldOuter, newOuter) {
		return errors.New("reload only accepts backlog_v2 catalog and policy; host lifecycle settings require restart")
	}
	a, b := current.BacklogV2, next.BacklogV2
	if a.Mode != b.Mode || !reflect.DeepEqual(a.Coordinator, b.Coordinator) || !reflect.DeepEqual(a.LocalWorker, b.LocalWorker) || !reflect.DeepEqual(a.Storage, b.Storage) {
		return errors.New("coordinator identity, epochs and storage are lifecycle operations")
	}
	for id, old := range a.Workers {
		if newer, ok := b.Workers[id]; ok && old.Epoch != newer.Epoch {
			return errors.New("worker epoch changes require explicit custody recovery")
		}
	}
	targets := map[string]string{}
	for name, project := range b.Projects {
		targets[name] = project.T3Project
	}
	if _, err := backlog.LegacyProjectAliases(targets); err != nil {
		return err
	}
	for id := range b.Workers {
		if _, err := workerruntime.BuildWorkerBinding(b, id, time.Now()); err != nil {
			return err
		}
	}
	return nil
}
