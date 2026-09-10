package workerruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const journalVersion = 1

type Phase string

const (
	PhaseClaimed     Phase = "claimed"
	PhasePreparing   Phase = "preparing"
	PhasePrepared    Phase = "prepared"
	PhaseDispatching Phase = "dispatching"
	PhaseRunning     Phase = "running"
	PhaseStopping    Phase = "stopping"
	PhaseStopped     Phase = "stopped"
	PhaseCollecting  Phase = "collecting"
	PhaseCompleted   Phase = "completed"
	PhaseUnknown     Phase = "unknown"
)

type AttemptRecord struct {
	Assignment       domain.Assignment                         `json:"assignment"`
	Package          workerproto.ExecutionPackageManifest      `json:"package"`
	Phase            Phase                                     `json:"phase"`
	WorkspacePath    string                                    `json:"workspacePath,omitempty"`
	ThreadID         string                                    `json:"threadId,omitempty"`
	Failure          string                                    `json:"failure,omitempty"`
	CommandRequests  map[string]domain.WorkerCommand           `json:"commandRequests,omitempty"`
	CommandResults   map[string]domain.WorkerAcknowledgement   `json:"commandResults,omitempty"`
	ThrottleRequests map[string]domain.ThrottleCommand         `json:"throttleRequests,omitempty"`
	ThrottleResults  map[string]domain.ThrottleAcknowledgement `json:"throttleResults,omitempty"`
	PendingThrottle  *domain.ThrottleCommand                   `json:"pendingThrottle,omitempty"`
	UpdatedAt        time.Time                                 `json:"updatedAt"`
}

type journalState struct {
	Version          int                      `json:"version"`
	WorkerID         string                   `json:"workerId"`
	WorkerEpoch      string                   `json:"workerEpoch"`
	CoordinatorEpoch int64                    `json:"coordinatorEpoch"`
	Sequence         int64                    `json:"sequence"`
	Attempts         map[string]AttemptRecord `json:"attempts"`
}

type Journal struct {
	root     string
	path     string
	lockPath string
}

func OpenJournal(root, workerID, workerEpoch string, coordinatorEpoch int64) (*Journal, error) {
	if root == "" || workerID == "" || workerEpoch == "" || coordinatorEpoch < 1 {
		return nil, errors.New("worker journal: root, worker identity, and positive coordinator epoch are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("worker journal: resolve root: %w", err)
	}
	if filepath.Clean(absolute) == string(filepath.Separator) {
		return nil, errors.New("worker journal: filesystem root is not allowed")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("worker journal: create root: %w", err)
	}
	journal := &Journal{root: absolute, path: filepath.Join(absolute, "journal.json"), lockPath: filepath.Join(absolute, "journal.lock")}
	err = journal.update(func(state *journalState) error {
		if state.Version == 0 {
			*state = journalState{
				Version: journalVersion, WorkerID: workerID, WorkerEpoch: workerEpoch,
				CoordinatorEpoch: coordinatorEpoch, Attempts: make(map[string]AttemptRecord),
			}
			return nil
		}
		if state.Version != journalVersion {
			return fmt.Errorf("unsupported version %d", state.Version)
		}
		if state.WorkerID != workerID || state.WorkerEpoch != workerEpoch {
			return errors.New("worker identity or epoch does not match durable journal")
		}
		if state.CoordinatorEpoch != coordinatorEpoch {
			return errors.New("coordinator epoch does not match durable journal")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return journal, nil
}

func (j *Journal) snapshot() (journalState, error) {
	var result journalState
	err := j.withLock(func() error {
		state, err := j.read()
		if err != nil {
			return err
		}
		result = cloneState(state)
		return nil
	})
	return result, err
}

func (j *Journal) update(change func(*journalState) error) error {
	if change == nil {
		return errors.New("worker journal: update function is required")
	}
	return j.withLock(func() error {
		state, err := j.read()
		if err != nil {
			return err
		}
		before := cloneState(state)
		if err := change(&state); err != nil {
			return err
		}
		if state.Attempts == nil {
			state.Attempts = make(map[string]AttemptRecord)
		}
		if statesEqual(before, state) && state.Version != 0 {
			return nil
		}
		return j.write(state)
	})
}

func (j *Journal) withLock(run func() error) error {
	lock, err := os.OpenFile(j.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("worker journal: open lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("worker journal: lock: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return run()
}

func (j *Journal) read() (journalState, error) {
	data, err := os.ReadFile(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return journalState{}, nil
	}
	if err != nil {
		return journalState{}, fmt.Errorf("worker journal: read: %w", err)
	}
	var state journalState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return journalState{}, fmt.Errorf("worker journal: decode: %w", err)
	}
	if state.Version != journalVersion || state.WorkerID == "" || state.WorkerEpoch == "" || state.CoordinatorEpoch < 1 || state.Sequence < 0 {
		return journalState{}, errors.New("worker journal: invalid durable header")
	}
	if state.Attempts == nil {
		state.Attempts = make(map[string]AttemptRecord)
	}
	for id, record := range state.Attempts {
		if id == "" || record.Assignment.ID != id || record.Package.Package.Identity.AssignmentID != id {
			return journalState{}, fmt.Errorf("worker journal: invalid attempt record %q", id)
		}
	}
	return state, nil
}

func (j *Journal) write(state journalState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("worker journal: encode: %w", err)
	}
	data = append(data, '\n')
	stage, err := os.CreateTemp(j.root, ".journal-*.tmp")
	if err != nil {
		return fmt.Errorf("worker journal: create stage: %w", err)
	}
	stagePath := stage.Name()
	cleanup := func() {
		stage.Close()
		os.Remove(stagePath)
	}
	if err := stage.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("worker journal: protect stage: %w", err)
	}
	if _, err := stage.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("worker journal: write stage: %w", err)
	}
	if err := stage.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("worker journal: sync stage: %w", err)
	}
	if err := stage.Close(); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("worker journal: close stage: %w", err)
	}
	if err := os.Rename(stagePath, j.path); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("worker journal: publish: %w", err)
	}
	dir, err := os.Open(j.root)
	if err != nil {
		return fmt.Errorf("worker journal: open root for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("worker journal: sync root: %w", err)
	}
	return nil
}

func cloneState(source journalState) journalState {
	data, _ := json.Marshal(source)
	var target journalState
	_ = json.Unmarshal(data, &target)
	if target.Attempts == nil {
		target.Attempts = make(map[string]AttemptRecord)
	}
	return target
}

func statesEqual(left, right journalState) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}

func sortedAttemptIDs(attempts map[string]AttemptRecord) []string {
	ids := make([]string, 0, len(attempts))
	for id := range attempts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
