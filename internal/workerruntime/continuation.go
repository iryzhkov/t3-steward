package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The continuation checkpoint store: the latest snapshot of a task's
// continuation.md, kept in the attempt directory beside the workspace. That
// directory is the attempt's durable state on this host and is never inside
// the task's tree, so nothing the task commits can carry a snapshot. Cleanup
// removes it with the attempt, after the result that carries the latest
// snapshot to the coordinator is durable.
//
// A snapshot is idempotent per attempt and turn: the first snapshot taken for
// a turn stands, and a replay of that turn after a worker or coordinator
// restart returns the latest snapshot unchanged rather than taking a new one,
// however many turns ago it was. Unchanged content is not a new snapshot
// either. The sequence therefore only grows, and the latest snapshot never
// moves back to an older one.
//
// A snapshot taken at a turn end or a pause is handed to the coordinator at
// once when the package declares that the coordinator accepts it, so that it
// survives an attempt that is superseded before it has a result.

// continuationTurnLimit bounds how many turn keys the state file lists. A
// key that leaves the list is kept as a marker file under turns/, written
// before the state that drops it, so the replay fence never expires.
const continuationTurnLimit = 256

// continuationLock serializes snapshots on this host: a turn-end observation
// and a collection of the same attempt can run concurrently.
var continuationLock sync.Mutex

type continuationState struct {
	Latest *domain.ContinuationCheckpoint `json:"latest,omitempty"`
	// Turns are the latest turn keys already considered, with or without a
	// file; earlier ones are marker files under turns/.
	Turns []string `json:"turns,omitempty"`
	// Published is the sequence of the latest snapshot handed to the
	// coordinator while the attempt runs.
	Published int64 `json:"published,omitempty"`
}

// continuationPublisher is a publisher that can hand a running attempt's
// snapshot to the coordinator; see CustodyStore.PublishContinuation.
type continuationPublisher interface {
	PublishContinuation(context.Context, workerproto.ExecutionPackage, ContinuationSnapshot) error
}

// ContinuationSnapshot is the latest snapshot as a result carries it.
type ContinuationSnapshot struct {
	Checkpoint domain.ContinuationCheckpoint
	Data       []byte
}

// continuationRecorder is the driver capability the runtime uses to take a
// snapshot at a turn end or a pause.
type continuationRecorder interface {
	RecordContinuation(context.Context, workerproto.ExecutionPackage, string, domain.ContinuationBoundary, string) (*domain.ContinuationCheckpoint, error)
}

func (d *LocalDriver) continuationDir(pkg workerproto.ExecutionPackage) string {
	return filepath.Join(d.workspacePath(pkg), "continuation")
}

// RecordContinuation snapshots workspace/continuation.md for the given turn
// and returns the attempt's latest checkpoint, which is nil while the task
// has never had a continuation.md. A missing file is not an error.
func (d *LocalDriver) RecordContinuation(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string, boundary domain.ContinuationBoundary, turn string) (*domain.ContinuationCheckpoint, error) {
	if pkg.IsActivation() || d.Config.RunsRoot == "" {
		return nil, nil
	}
	if turn == "" {
		turn = string(boundary)
	}
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	checkpoint, err := recordContinuation(d.continuationDir(pkg), workspace, pkg.Identity.AttemptID, boundary, turn, now().UTC())
	if err != nil || boundary == domain.ContinuationCollection {
		// The result hands on a collection snapshot.
		return checkpoint, err
	}
	// A replay retries a hand-on that failed; one that succeeded is not
	// repeated.
	if err := d.handOnContinuation(ctx, pkg); err != nil {
		d.logger().Warn("the continuation checkpoint could not be handed to the coordinator yet", "attempt", pkg.Identity.AttemptID, "error", err)
	}
	return checkpoint, nil
}

// handOnContinuation hands the attempt's latest snapshot to the coordinator
// while the attempt runs, once per snapshot, when the package declares that
// the coordinator accepts it.
func (d *LocalDriver) handOnContinuation(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	publisher, ok := d.Publisher.(continuationPublisher)
	if !ok || !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint) {
		return nil
	}
	continuationLock.Lock()
	defer continuationLock.Unlock()
	dir := d.continuationDir(pkg)
	state, err := loadContinuationState(dir)
	if err != nil || state.Latest == nil || state.Latest.Sequence <= state.Published {
		return err
	}
	data, err := readContinuationBody(dir, *state.Latest)
	if err != nil {
		return err
	}
	if err := publisher.PublishContinuation(ctx, pkg, ContinuationSnapshot{Checkpoint: *state.Latest, Data: data}); err != nil {
		return err
	}
	state.Published = state.Latest.Sequence
	return saveContinuationState(dir, state)
}

// LatestContinuation returns the attempt's latest checkpoint and its bytes.
func (d *LocalDriver) LatestContinuation(pkg workerproto.ExecutionPackage) (*domain.ContinuationCheckpoint, []byte, error) {
	if d.Config.RunsRoot == "" {
		return nil, nil, nil
	}
	continuationLock.Lock()
	defer continuationLock.Unlock()
	dir := d.continuationDir(pkg)
	state, err := loadContinuationState(dir)
	if err != nil || state.Latest == nil {
		return nil, nil, err
	}
	data, err := readContinuationBody(dir, *state.Latest)
	if err != nil {
		return nil, nil, err
	}
	latest := *state.Latest
	return &latest, data, nil
}

// continuationForResult takes the collection snapshot and returns what the
// result carries: the latest snapshot when the coordinator declared that it
// accepts one, and nothing otherwise, because an older coordinator rejects a
// result with an object it does not know. A checkpoint that cannot be read is
// logged and left out; it never fails the result.
func (d *LocalDriver) continuationForResult(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, turn string) *ContinuationSnapshot {
	if _, err := d.RecordContinuation(ctx, pkg, workspace, domain.ContinuationCollection, turn); err != nil {
		d.logger().Warn("continuation.md could not be checkpointed at collection", "attempt", pkg.Identity.AttemptID, "error", err)
	}
	if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint) {
		return nil
	}
	latest, data, err := d.LatestContinuation(pkg)
	if err != nil {
		d.logger().Warn("the continuation checkpoint is unreadable; the result goes without it", "attempt", pkg.Identity.AttemptID, "error", err)
		return nil
	}
	if latest == nil {
		return nil
	}
	return &ContinuationSnapshot{Checkpoint: *latest, Data: data}
}

func recordContinuation(dir, workspace, attemptID string, boundary domain.ContinuationBoundary, turn string, now time.Time) (*domain.ContinuationCheckpoint, error) {
	continuationLock.Lock()
	defer continuationLock.Unlock()
	state, err := loadContinuationState(dir)
	if err != nil {
		return nil, err
	}
	considered, err := continuationTurnConsidered(dir, state, turn)
	if err != nil {
		return nil, err
	}
	if considered {
		return cloneCheckpoint(state.Latest), nil
	}
	data, originalSize, found, err := readContinuationFile(workspace)
	if err != nil {
		return nil, err
	}
	if found {
		kept, truncated := domain.BoundContinuation(data, originalSize)
		digest := sha256Hex(kept)
		if state.Latest == nil || state.Latest.SHA256 != digest {
			sequence := int64(1)
			if state.Latest != nil {
				sequence = state.Latest.Sequence + 1
			}
			checkpoint := domain.ContinuationCheckpoint{
				AttemptID: attemptID, Sequence: sequence, Turn: turn, Boundary: boundary,
				SHA256: digest, Size: int64(len(kept)), OriginalSize: originalSize, Truncated: truncated, CapturedAt: now,
			}
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return nil, fmt.Errorf("continuation checkpoint: %w", err)
			}
			// The body is content-addressed and written before the state that
			// names it, so a crash between the two leaves the previous latest.
			if err := privateBytes(filepath.Join(dir, digest+".md"), kept); err != nil {
				return nil, fmt.Errorf("continuation checkpoint: %w", err)
			}
			state.Latest = &checkpoint
		}
	}
	state.Turns = append(state.Turns, turn)
	if evicted := len(state.Turns) - continuationTurnLimit; evicted > 0 {
		if err := markContinuationTurns(dir, state.Turns[:evicted]); err != nil {
			return nil, err
		}
		state.Turns = state.Turns[evicted:]
	}
	if err := saveContinuationState(dir, state); err != nil {
		return nil, err
	}
	return cloneCheckpoint(state.Latest), nil
}

// continuationTurnConsidered reports whether the store has considered turn:
// it is listed in the state or, once it left the list, marked under turns/.
func continuationTurnConsidered(dir string, state continuationState, turn string) (bool, error) {
	if slices.Contains(state.Turns, turn) {
		return true, nil
	}
	_, err := os.Lstat(continuationTurnMarker(dir, turn))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("continuation checkpoint turn: %w", err)
	}
	return true, nil
}

// continuationTurnMarker is the marker file of a turn key that has left the
// state's list, named by the key's digest so any key is a safe file name.
func continuationTurnMarker(dir, turn string) string {
	return filepath.Join(dir, "turns", sha256Hex([]byte(turn)))
}

// markContinuationTurns durably marks turn keys as considered; privateBytes
// syncs each marker and its directory.
func markContinuationTurns(dir string, turns []string) error {
	if err := os.MkdirAll(filepath.Join(dir, "turns"), 0o700); err != nil {
		return fmt.Errorf("continuation checkpoint turn: %w", err)
	}
	for _, turn := range turns {
		if err := privateBytes(continuationTurnMarker(dir, turn), nil); err != nil {
			return fmt.Errorf("continuation checkpoint turn: %w", err)
		}
	}
	return nil
}

// readContinuationFile reads the head of the task's continuation.md. Only a
// regular file counts: a symlink or anything else is the same as no file, so a
// task cannot make the worker read outside its workspace.
func readContinuationFile(workspace string) ([]byte, int64, bool, error) {
	if workspace == "" {
		return nil, 0, false, nil
	}
	path := filepath.Join(workspace, domain.ContinuationFileName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) || (err == nil && !info.Mode().IsRegular()) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf("continuation checkpoint: %w", err)
	}
	file, err := openRegular(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf("continuation checkpoint: %w", err)
	}
	defer file.Close()
	head, err := io.ReadAll(io.LimitReader(file, domain.ContinuationSnapshotLimit+1))
	if err != nil {
		return nil, 0, false, fmt.Errorf("continuation checkpoint: %w", err)
	}
	// The size is what was read whenever the whole file fit; a file that
	// changed size between the stat and the read is described as read.
	size := int64(len(head))
	if size > domain.ContinuationSnapshotLimit {
		size = max(size, info.Size())
	}
	return head, size, true, nil
}

func loadContinuationState(dir string) (continuationState, error) {
	var state continuationState
	file, err := openRegular(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("continuation checkpoint state: %w", err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 1<<20))
	if err != nil {
		return state, fmt.Errorf("continuation checkpoint state: %w", err)
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, fmt.Errorf("continuation checkpoint state: %w", err)
	}
	return state, nil
}

// saveContinuationState replaces the state file atomically and durably.
func saveContinuationState(dir string, state continuationState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("continuation checkpoint state: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".state-")
	if err != nil {
		return fmt.Errorf("continuation checkpoint state: %w", err)
	}
	defer os.Remove(temp.Name())
	_, writeErr := temp.Write(raw)
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("continuation checkpoint state: %w", err)
	}
	if err := os.Rename(temp.Name(), filepath.Join(dir, "state.json")); err != nil {
		return fmt.Errorf("continuation checkpoint state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("continuation checkpoint state: %w", err)
	}
	defer directory.Close()
	return directory.Sync()
}

func readContinuationBody(dir string, checkpoint domain.ContinuationCheckpoint) ([]byte, error) {
	if len(checkpoint.SHA256) != sha256.Size*2 || strings.ContainsAny(checkpoint.SHA256, `/\.`) {
		return nil, errors.New("continuation checkpoint: invalid digest")
	}
	file, err := openRegular(filepath.Join(dir, checkpoint.SHA256+".md"))
	if err != nil {
		return nil, fmt.Errorf("continuation checkpoint: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, domain.ContinuationSnapshotLimit+1))
	if err != nil {
		return nil, fmt.Errorf("continuation checkpoint: %w", err)
	}
	if int64(len(data)) != checkpoint.Size || sha256Hex(data) != checkpoint.SHA256 {
		return nil, errors.New("continuation checkpoint: stored snapshot does not match its digest")
	}
	return data, nil
}

func cloneCheckpoint(checkpoint *domain.ContinuationCheckpoint) *domain.ContinuationCheckpoint {
	if checkpoint == nil {
		return nil
	}
	copy := *checkpoint
	return &copy
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// PendingContinuation names the continuation.md snapshot a durable pause
// owes. Turn is the snapshot's turn key; with ObserveTurn the stopped turn's
// identity is the key when it can be observed, and Turn otherwise.
type PendingContinuation struct {
	Boundary    domain.ContinuationBoundary `json:"boundary"`
	Turn        string                      `json:"turn"`
	ObserveTurn bool                        `json:"observeTurn,omitempty"`
}

// settlePendingContinuation takes the pause snapshot the attempt owes, if
// any, and clears the obligation once it is taken. A snapshot that cannot be
// taken yet stays owed for the next reconcile pass.
func (r *Runtime) settlePendingContinuation(ctx context.Context, id string) {
	record, exists, err := r.currentRecord(id)
	if err != nil || !exists || record.PendingContinuation == nil {
		return
	}
	pending := *record.PendingContinuation
	turn := pending.Turn
	if pending.ObserveTurn {
		turn = r.pauseTurnKey(ctx, record.Package.Package, pending.Turn)
	}
	if !r.recordContinuation(ctx, id, pending.Boundary, turn) {
		return
	}
	if err := r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || current.PendingContinuation == nil || *current.PendingContinuation != pending {
			return nil
		}
		current.PendingContinuation = nil
		state.Attempts[id] = current
		return nil
	}); err != nil {
		r.log.Warn("the taken pause snapshot could not be journaled; it is taken again", "assignment", id, "error", err)
	}
}

// closePendingContinuation takes the pause snapshot the attempt owes, or,
// when it still cannot be taken, durably forgoes it, before a resume starts a
// new turn: taken later, it would be a snapshot of that turn passed off as
// the paused one. A resume is never held back for a snapshot.
func (r *Runtime) closePendingContinuation(ctx context.Context, id string) error {
	r.settlePendingContinuation(ctx, id)
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || current.PendingContinuation == nil {
			return nil
		}
		r.log.Warn("the pause snapshot could not be taken before the resume; it is forgone", "assignment", id, "turn", current.PendingContinuation.Turn)
		current.PendingContinuation = nil
		state.Attempts[id] = current
		return nil
	})
}

// recordContinuation snapshots the attempt's continuation.md at a boundary
// and keeps the latest checkpoint on the journal record. It never fails the
// caller: a checkpoint is evidence for a later attempt, and losing one must
// not change what happens to this one. It reports whether the boundary is
// settled, that is whether the snapshot was taken or there is none to take.
func (r *Runtime) recordContinuation(ctx context.Context, id string, boundary domain.ContinuationBoundary, turn string) bool {
	recorder, ok := r.driver.(continuationRecorder)
	if !ok {
		return true
	}
	record, exists, err := r.currentRecord(id)
	if err != nil {
		return false
	}
	if !exists || record.WorkspacePath == "" || record.Package.Package.IsActivation() {
		return true
	}
	checkpoint, err := recorder.RecordContinuation(ctx, record.Package.Package, record.WorkspacePath, boundary, turn)
	if err != nil {
		r.log.Warn("continuation.md could not be checkpointed", "assignment", id, "boundary", string(boundary), "error", err)
		return false
	}
	if checkpoint == nil {
		return true
	}
	// The worker sequence is left alone: the checkpoint is not part of what
	// the coordinator observes, and moving it would only make a command built
	// against the previous snapshot look stale.
	if err := r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || (current.Continuation != nil && current.Continuation.Sequence >= checkpoint.Sequence) {
			return nil
		}
		current.Continuation = checkpoint
		state.Attempts[id] = current
		return nil
	}); err != nil {
		r.log.Warn("continuation checkpoint could not be journaled", "assignment", id, "error", err)
		return false
	}
	return true
}

// continuationPromptSentence tells a new attempt, in one sentence, where the
// checkpoint an earlier attempt of its task left is. Static inputs are
// materialized under the workspace's .t3/ directory.
func continuationPromptSentence(input workerproto.ContinuationInput) string {
	return fmt.Sprintf("Earlier work on this task (attempt %s) left its continuation.md checkpoint, %d bytes captured %s, at `.t3/%s`: read it before you start, and keep your own continuation.md current.",
		input.AttemptID, input.Size, input.CapturedAt.UTC().Format(time.RFC3339), input.Path)
}

// throttlePauseContinuation is the snapshot an accepted drain or hard stop
// owes: of the turn it stopped, or keyed by the command when that turn
// cannot be observed.
func throttlePauseContinuation(command domain.ThrottleCommand) *PendingContinuation {
	return &PendingContinuation{Boundary: domain.ContinuationPause, Turn: "throttle:" + command.ID, ObserveTurn: true}
}

// pauseTurnKey names the turn a pause stopped, or fallback when the driver
// cannot say.
func (r *Runtime) pauseTurnKey(ctx context.Context, pkg workerproto.ExecutionPackage, fallback string) string {
	if observer, ok := r.driver.(turnObserver); ok {
		if _, turnID, err := observer.ObserveThreadTurn(ctx, pkg); err == nil && turnID != "" {
			return turnID
		}
	}
	return fallback
}
