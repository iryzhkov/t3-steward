package workerruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const protocolReplayVersion = 1

type protocolReplayRecord struct {
	Digest   string `json:"digest"`
	Response []byte `json:"response,omitempty"`
	Ready    bool   `json:"ready"`
}

type protocolReplayState struct {
	Version          int                             `json:"version"`
	CoordinatorID    string                          `json:"coordinatorId"`
	WorkerID         string                          `json:"workerId"`
	CoordinatorEpoch int64                           `json:"coordinatorEpoch"`
	WorkerEpoch      string                          `json:"workerEpoch"`
	Sessions         map[string]int64                `json:"sessions"`
	Requests         map[string]protocolReplayRecord `json:"requests"`
	RequestOrder     []string                        `json:"requestOrder"`
}

// ProtocolReplayStore serializes authenticated control exchanges across worker
// processes and durably preserves sequence and exact-response replay state.
type ProtocolReplayStore struct {
	root             string
	path             string
	lockPath         string
	coordinatorID    string
	workerID         string
	coordinatorEpoch int64
	workerEpoch      string
}

func OpenProtocolReplayStore(root, coordinatorID, workerID string, coordinatorEpoch int64, workerEpoch string) (*ProtocolReplayStore, error) {
	if root == "" || coordinatorID == "" || workerID == "" || coordinatorEpoch < 1 || workerEpoch == "" {
		return nil, errors.New("protocol replay store: root, identities, and epochs are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("protocol replay store: resolve root: %w", err)
	}
	if filepath.Clean(absolute) == string(filepath.Separator) {
		return nil, errors.New("protocol replay store: filesystem root is not allowed")
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("protocol replay store: create root: %w", err)
	}
	store := &ProtocolReplayStore{
		root: absolute, path: filepath.Join(absolute, "protocol-replay.json"),
		lockPath:      filepath.Join(absolute, "protocol-replay.lock"),
		coordinatorID: coordinatorID, workerID: workerID,
		coordinatorEpoch: coordinatorEpoch, workerEpoch: workerEpoch,
	}
	lock, err := store.lock()
	if err != nil {
		return nil, err
	}
	defer releaseProtocolLock(lock)
	state, err := store.read()
	if err != nil {
		return nil, err
	}
	if state.Version == 0 {
		if err := store.write(store.emptyState()); err != nil {
			return nil, err
		}
		return store, nil
	}
	if err := store.validateHeader(state); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *ProtocolReplayStore) Begin(peerPrincipal string, envelope workerproto.Envelope, digest string, maxCachedRequests int) (workerproto.ReplayTransaction, error) {
	if peerPrincipal == "" || envelope.RequestID == "" || envelope.SessionID == "" || digest == "" || maxCachedRequests < 1 {
		return nil, errors.New("protocol replay store: invalid begin request")
	}
	lock, err := s.lock()
	if err != nil {
		return nil, err
	}
	fail := func(err error) (workerproto.ReplayTransaction, error) {
		releaseProtocolLock(lock)
		return nil, err
	}
	state, err := s.read()
	if err != nil {
		return fail(err)
	}
	if err := s.validateHeader(state); err != nil {
		return fail(err)
	}
	key := peerPrincipal + "/" + envelope.RequestID
	if record, ok := state.Requests[key]; ok {
		if record.Digest != digest {
			return fail(&workerproto.ProtocolError{Code: workerproto.ErrorReplay, Message: "request id was reused with different content", RequestID: envelope.RequestID})
		}
		var cached workerproto.Envelope
		if record.Ready {
			if err := decodeProtocolResponse(record.Response, &cached); err != nil {
				return fail(err)
			}
		}
		return &protocolReplayTransaction{store: s, lock: lock, state: state, key: key, cached: cached, ready: record.Ready, maxCachedRequests: maxCachedRequests}, nil
	}
	for _, record := range state.Requests {
		if !record.Ready {
			return fail(&workerproto.ProtocolError{
				Code: workerproto.ErrorBackpressure, Message: "another durable request must be resumed first",
				Retryable: true, RetryAfter: "1s", RequestID: envelope.RequestID,
			})
		}
	}
	if envelope.Sequence != state.Sessions[envelope.SessionID]+1 {
		return fail(&workerproto.ProtocolError{Code: workerproto.ErrorReordered, Message: "sequence is not the next durable session value", RequestID: envelope.RequestID})
	}
	state.Sessions[envelope.SessionID] = envelope.Sequence
	state.Requests[key] = protocolReplayRecord{Digest: digest}
	state.RequestOrder = append(state.RequestOrder, key)
	if err := s.write(state); err != nil {
		return fail(err)
	}
	return &protocolReplayTransaction{store: s, lock: lock, state: state, key: key, maxCachedRequests: maxCachedRequests}, nil
}

type protocolReplayTransaction struct {
	store             *ProtocolReplayStore
	lock              *os.File
	state             protocolReplayState
	key               string
	cached            workerproto.Envelope
	ready             bool
	maxCachedRequests int
}

func (t *protocolReplayTransaction) CachedResponse() (workerproto.Envelope, bool) {
	return t.cached, t.ready
}

func (t *protocolReplayTransaction) Complete(response workerproto.Envelope) error {
	if t.lock == nil {
		return errors.New("protocol replay store: transaction is closed")
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("protocol replay store: encode response: %w", err)
	}
	record := t.state.Requests[t.key]
	record.Response = encoded
	record.Ready = true
	t.state.Requests[t.key] = record
	for len(t.state.RequestOrder) > t.maxCachedRequests {
		evicted := t.state.RequestOrder[0]
		if !t.state.Requests[evicted].Ready {
			break
		}
		t.state.RequestOrder = t.state.RequestOrder[1:]
		delete(t.state.Requests, evicted)
	}
	if err := t.store.write(t.state); err != nil {
		return err
	}
	return t.Release()
}

func (t *protocolReplayTransaction) Release() error {
	if t.lock == nil {
		return nil
	}
	lock := t.lock
	t.lock = nil
	return releaseProtocolLock(lock)
}

func (s *ProtocolReplayStore) emptyState() protocolReplayState {
	return protocolReplayState{
		Version: protocolReplayVersion, CoordinatorID: s.coordinatorID, WorkerID: s.workerID,
		CoordinatorEpoch: s.coordinatorEpoch, WorkerEpoch: s.workerEpoch,
		Sessions: make(map[string]int64), Requests: make(map[string]protocolReplayRecord),
	}
}

func (s *ProtocolReplayStore) validateHeader(state protocolReplayState) error {
	if state.Version != protocolReplayVersion {
		return fmt.Errorf("protocol replay store: unsupported version %d", state.Version)
	}
	if state.CoordinatorID != s.coordinatorID || state.WorkerID != s.workerID ||
		state.CoordinatorEpoch != s.coordinatorEpoch || state.WorkerEpoch != s.workerEpoch {
		return errors.New("protocol replay store: durable identity or epoch mismatch")
	}
	if state.Sessions == nil || state.Requests == nil {
		return errors.New("protocol replay store: invalid durable state")
	}
	for sessionID, sequence := range state.Sessions {
		if sessionID == "" || sequence < 1 {
			return errors.New("protocol replay store: invalid durable session")
		}
	}
	for key, record := range state.Requests {
		if key == "" || record.Digest == "" || (record.Ready && len(record.Response) == 0) {
			return errors.New("protocol replay store: invalid durable request")
		}
	}
	if len(state.RequestOrder) != len(state.Requests) {
		return errors.New("protocol replay store: invalid durable request order")
	}
	seen := make(map[string]bool, len(state.RequestOrder))
	for _, key := range state.RequestOrder {
		if seen[key] {
			return errors.New("protocol replay store: duplicate durable request order")
		}
		if _, ok := state.Requests[key]; !ok {
			return errors.New("protocol replay store: missing durable request order")
		}
		seen[key] = true
	}
	return nil
}

func (s *ProtocolReplayStore) lock() (*os.File, error) {
	lock, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("protocol replay store: open lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		lock.Close()
		return nil, fmt.Errorf("protocol replay store: lock: %w", err)
	}
	return lock, nil
}

func releaseProtocolLock(lock *os.File) error {
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	closeErr := lock.Close()
	if unlockErr != nil {
		return fmt.Errorf("protocol replay store: unlock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("protocol replay store: close lock: %w", closeErr)
	}
	return nil
}

func decodeProtocolResponse(data []byte, response *workerproto.Envelope) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("protocol replay store: decode response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("protocol replay store: trailing response content")
	}
	return nil
}

func (s *ProtocolReplayStore) read() (protocolReplayState, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return protocolReplayState{}, nil
	}
	if err != nil {
		return protocolReplayState{}, fmt.Errorf("protocol replay store: read: %w", err)
	}
	var state protocolReplayState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return protocolReplayState{}, fmt.Errorf("protocol replay store: decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return protocolReplayState{}, errors.New("protocol replay store: trailing content")
	}
	return state, nil
}

func (s *ProtocolReplayStore) write(state protocolReplayState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("protocol replay store: encode: %w", err)
	}
	data = append(data, '\n')
	stage, err := os.CreateTemp(s.root, ".protocol-replay-*.tmp")
	if err != nil {
		return fmt.Errorf("protocol replay store: create stage: %w", err)
	}
	stagePath := stage.Name()
	cleanup := func() {
		stage.Close()
		os.Remove(stagePath)
	}
	if err := stage.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("protocol replay store: protect stage: %w", err)
	}
	if _, err := stage.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("protocol replay store: write stage: %w", err)
	}
	if err := stage.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("protocol replay store: sync stage: %w", err)
	}
	if err := stage.Close(); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("protocol replay store: close stage: %w", err)
	}
	if err := os.Rename(stagePath, s.path); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("protocol replay store: publish: %w", err)
	}
	directory, err := os.Open(s.root)
	if err != nil {
		return fmt.Errorf("protocol replay store: open root for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("protocol replay store: sync root: %w", err)
	}
	return nil
}
