package sqlite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type capabilitySpikeRecord struct {
	ID, AssignmentID, WorkerID, WorkerEpoch, ThreadID, Purpose, Digest string
	CoordinatorEpoch, AssignmentEpoch, ExpiresAtUnixNano               int64
	Revoked                                                            bool
}

func persistCapabilitySpike(path string) (string, error) {
	if raw, err := os.ReadFile(path); err == nil {
		decoded, decodeErr := hex.DecodeString(string(raw))
		if decodeErr != nil || len(decoded) != 32 {
			return "", errors.New("invalid capability journal")
		}
		return string(raw), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	tmp := path + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	if _, err = f.WriteString(token); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	if err = parent.Sync(); err == nil {
		err = parent.Close()
	} else {
		_ = parent.Close()
	}
	if err != nil {
		return "", err
	}
	return token, nil
}

func capabilitySpikeDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func installCapabilitySpikeSchema(t *testing.T, store *Store) {
	t.Helper()
	_, err := store.db.Exec(`CREATE TABLE capability_spike (
		id TEXT PRIMARY KEY, assignment_id TEXT NOT NULL, assignment_epoch INTEGER NOT NULL,
		coordinator_epoch INTEGER NOT NULL, worker_id TEXT NOT NULL, worker_epoch TEXT NOT NULL,
		thread_id TEXT NOT NULL, purpose TEXT NOT NULL, digest TEXT NOT NULL,
		expires_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		t.Fatal(err)
	}
}

func validCapabilitySpikePurpose(purpose string) bool {
	switch purpose {
	case "consultation.ask", "consultation.await", "consultation.inspect-own", "consultation.cancel-own":
		return true
	default:
		return false
	}
}

func validCapabilitySpikeHex(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == 32
}

// registerCapabilitySpike models an operation available only to the worker
// principal authenticated by the existing signed worker envelope. A task
// principal can use issued authority but can never mint or broaden it.
func registerCapabilitySpike(ctx context.Context, store *Store, record capabilitySpikeRecord, now time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current capabilitySpikeRecord
	err = tx.QueryRowContext(ctx, `SELECT id, assignment_id, assignment_epoch, coordinator_epoch, worker_id,
		worker_epoch, thread_id, purpose, digest, expires_at, revoked FROM capability_spike WHERE id=?`, record.ID).
		Scan(&current.ID, &current.AssignmentID, &current.AssignmentEpoch, &current.CoordinatorEpoch,
			&current.WorkerID, &current.WorkerEpoch, &current.ThreadID, &current.Purpose, &current.Digest,
			&current.ExpiresAtUnixNano, &current.Revoked)
	if err == nil {
		if current != record {
			return errors.New("capability replay changed registration")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !validCapabilitySpikeHex(record.ID) || !validCapabilitySpikeHex(record.Digest) ||
		!validCapabilitySpikePurpose(record.Purpose) || record.ExpiresAtUnixNano <= now.UnixNano() ||
		record.ExpiresAtUnixNano > now.Add(24*time.Hour).UnixNano() {
		return errors.New("capability registration has invalid identity, digest, purpose, or expiry")
	}
	assignment, err := loadAssignmentTx(ctx, tx, record.AssignmentID)
	if err != nil {
		return err
	}
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return err
	}
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, record.WorkerID)
	if err != nil {
		return err
	}
	if record.ExpiresAtUnixNano <= now.UnixNano() || !exists ||
		snapshot.CoordinatorEpoch != record.CoordinatorEpoch || snapshot.WorkerEpoch != record.WorkerEpoch ||
		assignment.WorkerID != record.WorkerID || assignment.WorkerEpoch != record.WorkerEpoch ||
		assignment.Epoch != record.AssignmentEpoch || assignment.ThreadID != record.ThreadID ||
		assignment.State != domain.AssignmentClaimed || attempt.AssignmentID != assignment.ID ||
		attempt.ThreadID != record.ThreadID || !attempt.TurnLive() {
		return errors.New("capability registration is outside the live assignment, attempt, epoch, or thread")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO capability_spike
		(id, assignment_id, assignment_epoch, coordinator_epoch, worker_id, worker_epoch, thread_id, purpose, digest, expires_at, revoked)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`, record.ID, record.AssignmentID, record.AssignmentEpoch,
		record.CoordinatorEpoch, record.WorkerID, record.WorkerEpoch, record.ThreadID, record.Purpose,
		record.Digest, record.ExpiresAtUnixNano)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func authorizeCapabilitySpike(ctx context.Context, store *Store, id, token, purpose string,
	assignmentEpoch int64, now time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var record capabilitySpikeRecord
	err = tx.QueryRowContext(ctx, `SELECT id, assignment_id, assignment_epoch, coordinator_epoch, worker_id,
		worker_epoch, thread_id, purpose, digest, expires_at, revoked FROM capability_spike WHERE id=?`, id).
		Scan(&record.ID, &record.AssignmentID, &record.AssignmentEpoch, &record.CoordinatorEpoch,
			&record.WorkerID, &record.WorkerEpoch, &record.ThreadID, &record.Purpose, &record.Digest,
			&record.ExpiresAtUnixNano, &record.Revoked)
	if err != nil {
		return errors.New("unknown capability")
	}
	if record.Revoked || record.Purpose != purpose || record.AssignmentEpoch != assignmentEpoch || record.ExpiresAtUnixNano <= now.UnixNano() {
		return errors.New("capability scope rejected")
	}
	got, err := hex.DecodeString(capabilitySpikeDigest(token))
	if err != nil {
		return err
	}
	want, err := hex.DecodeString(record.Digest)
	if err != nil || subtle.ConstantTimeCompare(got, want) != 1 {
		return errors.New("capability proof rejected")
	}
	assignment, err := loadAssignmentTx(ctx, tx, record.AssignmentID)
	if err != nil {
		return err
	}
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return err
	}
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, record.WorkerID)
	if err != nil {
		return err
	}
	if !exists || snapshot.CoordinatorEpoch != record.CoordinatorEpoch || snapshot.WorkerEpoch != record.WorkerEpoch ||
		assignment.WorkerID != record.WorkerID || assignment.WorkerEpoch != record.WorkerEpoch ||
		assignment.Epoch != record.AssignmentEpoch || assignment.ThreadID != record.ThreadID ||
		assignment.State != domain.AssignmentClaimed || attempt.AssignmentID != assignment.ID ||
		attempt.ThreadID != record.ThreadID || !attempt.TurnLive() {
		return errors.New("capability no longer names a live assignment and thread")
	}
	return tx.Commit()
}

func TestConsultationCapabilityAuthoritySpike(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	installCapabilitySpikeSchema(t, store)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assignment := records.Assignments[0]
	assignment.WorkerEpoch = "worker-epoch-1"
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}

	journal := filepath.Join(t.TempDir(), "consultation.cap")
	token, err := persistCapabilitySpike(journal)
	if err != nil {
		t.Fatal(err)
	}
	// The durable worker journal exists before the simulated external effect.
	if info, err := os.Stat(journal); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("private journal mode=%v err=%v", info, err)
	}
	replayedToken, err := persistCapabilitySpike(journal)
	if err != nil || replayedToken != token {
		t.Fatalf("durable token replay failed: %v", err)
	}
	record := capabilitySpikeRecord{
		ID: capabilitySpikeDigest(token + "identity"), AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		ThreadID: assignment.ThreadID, Purpose: "consultation.ask",
		Digest: capabilitySpikeDigest(token), ExpiresAtUnixNano: now.Add(time.Hour).UnixNano(),
	}
	if err := registerCapabilitySpike(ctx, store, record, now); err != nil {
		t.Fatal(err)
	}
	if err := registerCapabilitySpike(ctx, store, record, now); err != nil {
		t.Fatalf("exact registration replay failed: %v", err)
	}
	for name, mutate := range map[string]func(*capabilitySpikeRecord){
		"coordinator epoch": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("forged-coordinator")
			r.CoordinatorEpoch++
		},
		"expired": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("expired")
			r.ExpiresAtUnixNano = now.UnixNano()
		},
		"overlong expiry": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("overlong")
			r.ExpiresAtUnixNano = now.Add(24*time.Hour + time.Nanosecond).UnixNano()
		},
		"empty purpose": func(r *capabilitySpikeRecord) { r.ID = capabilitySpikeDigest("empty-purpose"); r.Purpose = "" },
		"unknown purpose": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("unknown-purpose")
			r.Purpose = "consultation.approve-gate"
		},
		"assignment": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("forged-assignment")
			r.AssignmentID = "assignment-forged"
		},
		"worker epoch": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("forged-worker")
			r.WorkerEpoch = "worker-epoch-forged"
		},
		"thread": func(r *capabilitySpikeRecord) {
			r.ID = capabilitySpikeDigest("forged-thread")
			r.ThreadID = "thread-forged"
		},
	} {
		t.Run("registration rejects forged "+name, func(t *testing.T) {
			forged := record
			mutate(&forged)
			if err := registerCapabilitySpike(ctx, store, forged, now); err == nil {
				t.Fatal("forged registration was accepted")
			}
		})
	}
	if err := authorizeCapabilitySpike(ctx, store, record.ID, token, record.Purpose, record.AssignmentEpoch, now); err != nil {
		t.Fatalf("exact capability replay failed: %v", err)
	}

	cases := []struct {
		name, id, token, purpose string
		epoch                    int64
	}{
		{"forged identifiers", "capability-forged", token, record.Purpose, record.AssignmentEpoch},
		{"forged token", record.ID, "00", record.Purpose, record.AssignmentEpoch},
		{"purpose mismatch", record.ID, token, "consultation.cancel", record.AssignmentEpoch},
		{"epoch mismatch", record.ID, token, record.Purpose, record.AssignmentEpoch + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := authorizeCapabilitySpike(ctx, store, tc.id, tc.token, tc.purpose, tc.epoch, now); err == nil {
				t.Fatal("forged or out-of-scope capability was accepted")
			}
		})
	}

	if err := authorizeCapabilitySpike(ctx, store, record.ID, token, record.Purpose,
		record.AssignmentEpoch, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired capability authorized a new use")
	}

	rotatedPath := filepath.Join(t.TempDir(), "rotated.cap")
	rotated, err := persistCapabilitySpike(rotatedPath)
	if err != nil {
		t.Fatal(err)
	}
	changed := record
	changed.Digest = capabilitySpikeDigest(rotated)
	if err := registerCapabilitySpike(ctx, store, changed, now); err == nil {
		t.Fatal("rotation under the same capability identity was accepted")
	}

	attempt.Progress = domain.ProgressSucceeded
	attempt.Control = domain.ControlUnassigned
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	if err := registerCapabilitySpike(ctx, store, record, now); err != nil {
		t.Fatalf("exact registration receipt replay failed after terminality: %v", err)
	}
	if err := authorizeCapabilitySpike(ctx, store, record.ID, token, record.Purpose, record.AssignmentEpoch, now); err == nil {
		t.Fatal("registration receipt replay authorized a new use after terminality")
	}
}
