package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var ErrCoordinatorOwned = errors.New("coordinator state is already owned")

// AcquireCoordinator obtains exclusive process ownership before durably advancing
// the coordinator epoch. A refused second owner cannot advance authority.
func (s *Store) AcquireCoordinator(ctx context.Context, identity string) (int64, error) {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return 0, errors.New("coordinator identity is required")
	}
	if s.path == "" || s.path == ":memory:" {
		return 0, errors.New("coordinator ownership requires a file-backed database")
	}
	if s.ownerLock != nil {
		return 0, errors.New("this store already owns coordinator state")
	}
	lock, err := os.OpenFile(s.path+".coordinator.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, fmt.Errorf("open coordinator ownership lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return 0, ErrCoordinatorOwned
		}
		return 0, fmt.Errorf("acquire coordinator ownership: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		_ = releaseCoordinatorLock(lock)
		return 0, fmt.Errorf("begin coordinator ownership: %w", err)
	}
	var epoch int64
	err = tx.QueryRowContext(ctx,
		`UPDATE coordinator_runtime SET epoch = epoch + 1 WHERE id = 1 RETURNING epoch`,
	).Scan(&epoch)
	if err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO kv(key, value) VALUES ('backlog_v2_coordinator_identity', ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`, identity)
	}
	if err == nil {
		_, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
			ID: fmt.Sprintf("coordinator-acquire-epoch:%d", epoch), Kind: "coordinator-authority-acquired",
			TargetType: domain.AuditTargetCoordinator, TargetID: identity,
			Actor: identity, Reason: "coordinator authority acquired", CreatedAt: s.now().UTC(),
			Detail: nativeAuditDetail{CoordinatorEpoch: epoch, Revision: epoch,
				IdempotencyIdentity: fmt.Sprintf("coordinator-acquire-epoch:%d", epoch), Outcome: "acquired"},
		})
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		_ = releaseCoordinatorLock(lock)
		return 0, fmt.Errorf("advance coordinator authority: %w", err)
	}
	s.ownerLock = lock
	return epoch, nil
}

func releaseCoordinatorLock(lock *os.File) error {
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	closeErr := lock.Close()
	if unlockErr != nil {
		return fmt.Errorf("release coordinator ownership: %w", unlockErr)
	}
	return closeErr
}
