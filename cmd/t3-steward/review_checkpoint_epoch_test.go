package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// checkpointDurableState is everything a checkpoint operation can write: the
// coordinator records (runs, tasks, attempts), the frozen authority, the
// checkpoint allocation and its round, the child receipt, and the files under
// the staging root. Two equal states mean nothing was written in between.
type checkpointDurableState struct {
	Records    sqlite.CoordinatorRecords
	Authority  review.FrozenAuthority
	Frozen     bool
	Checkpoint review.CheckpointAuthority
	Allocated  bool
	Child      *sqlite.ReviewMaterialization
	Round      review.Round
	Files      map[string]string
}

func (h *reviewCheckpointHarness) durableState(t *testing.T) checkpointDurableState {
	t.Helper()
	ctx := context.Background()
	var state checkpointDurableState
	var err error
	if state.Records, err = h.db.LoadCoordinatorRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if state.Authority, state.Frozen, err = h.db.GetFrozenReviewAuthority(ctx, h.request.WorkflowRunID, h.request.TaskID); err != nil {
		t.Fatal(err)
	}
	if state.Frozen {
		if state.Checkpoint, state.Child, state.Allocated, err = h.db.ReviewCheckpointReplay(ctx, state.Authority, h.request.CheckpointID); err != nil {
			t.Fatal(err)
		}
	}
	if state.Allocated {
		if state.Round, err = h.db.GetReviewRound(ctx, state.Checkpoint.RoundID); err != nil {
			t.Fatal(err)
		}
	}
	state.Files = map[string]string{}
	root := h.cfg.Storage.Bundles
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		rel, err := filepath.Rel(root, path)
		state.Files[rel] = hex.EncodeToString(sum[:])
		return err
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return state
}

// advanceEpoch is what a replacement coordinator does when it starts. It may
// run inside the operation under test, so it reports rather than stops.
func (h *reviewCheckpointHarness) advanceEpoch(t *testing.T) int64 {
	t.Helper()
	epoch, err := h.db.AdvanceCoordinatorEpoch(context.Background(), h.epoch)
	if err != nil {
		t.Errorf("advance coordinator epoch: %v", err)
	}
	return epoch
}

// replacedBy installs the operation of the coordinator that now holds epoch.
func (h *reviewCheckpointHarness) replacedBy(t *testing.T, epoch int64) {
	t.Helper()
	h.refs.during = nil
	op, err := newCoordinatorReviewCheckpoint(h.cfg, h.db, epoch, h.refs)
	if err != nil {
		t.Fatal(err)
	}
	h.admin.SetReviewCheckpoint(op)
}

// A coordinator that passed its fence and then waited on the remote probe
// while a replacement advanced the epoch must not freeze, allocate or
// materialize anything with the answer.
func TestReviewCheckpointEpochMovesDuringProbe(t *testing.T) {
	h := reviewCheckpointFixture(t, true)
	h.refs.push(h.branch(), strings.Repeat("d", 40))
	var epoch int64
	h.refs.during = func() { epoch = h.advanceEpoch(t) }
	_, err := h.call(t, h.request)
	requireCheckpointRefusal(t, err, domain.ReviewCheckpointStaleCoordinator, true)
	h.noReviewRows(t)

	// The coordinator that holds the epoch now opens the round normally.
	h.replacedBy(t, epoch)
	round, err := h.call(t, h.request)
	if err != nil {
		t.Fatal(err)
	}
	if round.Replayed || round.Number != 1 || h.runCount(t) != 2 {
		t.Fatalf("current coordinator did not open the round: %+v runs=%d", round, h.runCount(t))
	}
}

// The epoch is compared by every owning writer, not only at the start: when it
// moves between two durable phases, the stale coordinator writes nothing more,
// and the current coordinator completes the same checkpoint from what is there.
func TestReviewCheckpointEpochMovesBetweenDurablePhases(t *testing.T) {
	for _, phase := range []string{"frozen", "allocated", "staged", "confirmed"} {
		t.Run(phase, func(t *testing.T) {
			h := reviewCheckpointFixture(t, true)
			h.refs.push(h.branch(), strings.Repeat("d", 40))
			var before checkpointDurableState
			var epoch int64
			moved := false
			h.op.fault = func(at string) error {
				if at == phase && !moved {
					before = h.durableState(t)
					epoch, moved = h.advanceEpoch(t), true
				}
				return nil
			}
			_, err := h.call(t, h.request)
			if !moved {
				t.Fatalf("the operation never reached the %s boundary (err=%v)", phase, err)
			}
			requireCheckpointRefusal(t, err, domain.ReviewCheckpointStaleCoordinator, true)
			if !before.Frozen || (phase != "frozen") != before.Allocated || before.Child != nil {
				t.Fatalf("unexpected state at %s: frozen=%v allocated=%v child=%v", phase, before.Frozen, before.Allocated, before.Child != nil)
			}
			if after := h.durableState(t); !reflect.DeepEqual(before, after) {
				t.Fatalf("stale coordinator wrote after the epoch moved at %s:\nbefore %+v\nafter  %+v", phase, before, after)
			}
			if h.runCount(t) != 1 {
				t.Fatal("stale coordinator materialized a child")
			}

			h.replacedBy(t, epoch)
			round, err := h.call(t, h.request)
			if err != nil {
				t.Fatal(err)
			}
			if round.Replayed || round.Number != 1 || h.runCount(t) != 2 {
				t.Fatalf("current coordinator did not complete the checkpoint: %+v runs=%d", round, h.runCount(t))
			}
		})
	}
}

// A replay is an answer with authority behind it, so a coordinator whose epoch
// moved while it probed must not return the existing round either.
func TestReviewCheckpointReplayRefusesStaleCoordinator(t *testing.T) {
	h := reviewCheckpointFixture(t, true)
	h.refs.push(h.branch(), strings.Repeat("d", 40))
	if _, err := h.call(t, h.request); err != nil {
		t.Fatal(err)
	}
	before := h.durableState(t)
	h.refs.during = func() { h.advanceEpoch(t) }
	_, err := h.call(t, h.request)
	requireCheckpointRefusal(t, err, domain.ReviewCheckpointStaleCoordinator, true)
	if after := h.durableState(t); !reflect.DeepEqual(before, after) {
		t.Fatal("a stale replay changed durable state")
	}
}
