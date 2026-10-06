package sqlite

import (
	"context"
	"database/sql"
)

type reviewEpochFenceKey struct{}

// WithCoordinatorEpochFence binds the coordinator epoch a review operation was
// authorized under to its context. Every review authority writer (freeze,
// checkpoint allocation, child materialization) and the read-only checkpoint
// and child replays then compare it with the durable epoch inside their own
// transaction, after the writer lock is held, and refuse with
// ErrStaleCoordinatorEpoch before writing or answering.
//
// A check made before a slow step, such as a remote probe, is not enough: a
// replacement coordinator can advance the epoch in between, and only a
// comparison inside the transaction that writes is atomic with the write.
// The fence travels in the context because the operation reaches these writers
// through the declared admission and child staging services, which are shared
// with callers that hold no epoch.
func WithCoordinatorEpochFence(ctx context.Context, epoch int64) context.Context {
	return context.WithValue(ctx, reviewEpochFenceKey{}, epoch)
}

// requireReviewEpochFenceTx compares the bound epoch, if any, with the durable
// one in tx. Callers without a fence are unaffected.
func requireReviewEpochFenceTx(ctx context.Context, tx *sql.Tx) error {
	expected, ok := ctx.Value(reviewEpochFenceKey{}).(int64)
	if !ok {
		return nil
	}
	return requireCoordinatorEpoch(ctx, tx, expected)
}
