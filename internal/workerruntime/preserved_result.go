package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// preservedResultName is the preserved result record in the attempt
// directory, beside collected-turn.json and with the same retention.
const preservedResultName = "preserved-result.json"

// preservedResultRecord binds a preserved result to the execution that
// recorded it.
type preservedResultRecord struct {
	Identity workerproto.ExecutionIdentity `json:"identity"`
	// Turn is the turn identity the collection bound to. A record of an
	// earlier turn describes work a later turn was entitled to change, so it
	// is replaced rather than compared.
	Turn   string                  `json:"turn"`
	Result backlog.PreservedResult `json:"result"`
}

// PreservedResultError is a collection retry that cannot reuse the work the
// turn left, because the workspace is gone or no longer matches the digest
// the first collection recorded. The attempt fails with it as an
// infrastructure failure, which the coordinator may retry with a fresh run of
// the agent; it never collects work it cannot prove is the turn's.
type PreservedResultError struct {
	Missing bool
	Detail  string
}

func (e *PreservedResultError) Error() string {
	if e.Missing {
		return "workspace is missing; outputs cannot be collected: " + e.Detail
	}
	return "preserved result digest mismatch: " + e.Detail
}

// verifyPreservedResult runs before a collection captures the workspace. On a
// first collection there is no record and it does nothing. On a collection
// retry, after an earlier one captured the result and then failed to publish
// it, it proves by digest that the declared outputs and commits are still the
// ones captured, so the retry reuses the turn's work instead of anything
// written since. A mismatch, an unreadable record or a vanished workspace is
// a PreservedResultError.
func (d *LocalDriver) verifyPreservedResult(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, turn string) error {
	if d.Config.RunsRoot == "" {
		return nil
	}
	path := filepath.Join(d.workspacePath(pkg), preservedResultName)
	recorded, found, err := readPreservedResult(path)
	if err != nil {
		return &PreservedResultError{Detail: "the preserved record cannot be read: " + err.Error()}
	}
	if !found {
		return nil
	}
	if recorded.Identity != pkg.Identity {
		return &PreservedResultError{Detail: "the preserved record belongs to another execution"}
	}
	if recorded.Turn != turn {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("replace the preserved result of an earlier turn: %w", err)
		}
		return nil
	}
	current, err := backlog.CapturePreservedResult(ctx, "", workspace, pkg.Outputs)
	if errors.Is(err, backlog.ErrPreservedWorkspaceMissing) {
		return &PreservedResultError{Missing: true, Detail: "the workspace a collection already captured was removed"}
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &PreservedResultError{Detail: "the workspace cannot be digested: " + err.Error()}
	}
	if difference := recorded.Result.Difference(current); difference != "" {
		return &PreservedResultError{Detail: difference}
	}
	d.logger().Info("collection retry reuses the preserved result; its digest matches",
		"attempt", pkg.Identity.AttemptID, "digest", current.Digest)
	return nil
}

// recordPreservedResult runs once a collection has captured the result and
// before it is published, and records what was captured. A record that
// already exists was verified by verifyPreservedResult and is kept. Failing
// to record is logged, not fatal: the collection goes on as it did before
// preserved results existed, and a retry then simply has nothing to verify.
func (d *LocalDriver) recordPreservedResult(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, turn string) {
	if d.Config.RunsRoot == "" {
		return
	}
	path := filepath.Join(d.workspacePath(pkg), preservedResultName)
	if _, found, err := readPreservedResult(path); err == nil && found {
		return
	}
	current, err := backlog.CapturePreservedResult(ctx, "", workspace, pkg.Outputs)
	if err == nil {
		err = privateJSON(path, preservedResultRecord{Identity: pkg.Identity, Turn: turn, Result: current})
	}
	if err != nil {
		d.logger().Warn("preserved result not recorded; a collection retry will not be able to prove the workspace unchanged",
			"attempt", pkg.Identity.AttemptID, "error", err)
	}
}

func readPreservedResult(path string) (preservedResultRecord, bool, error) {
	file, err := openRegular(path)
	if errors.Is(err, os.ErrNotExist) {
		return preservedResultRecord{}, false, nil
	}
	if err != nil {
		return preservedResultRecord{}, false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, 1<<20))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return preservedResultRecord{}, false, err
	}
	var record preservedResultRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return preservedResultRecord{}, false, fmt.Errorf("decode: %w", err)
	}
	return record, true, nil
}
