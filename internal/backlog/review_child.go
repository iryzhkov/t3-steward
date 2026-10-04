package backlog

import (
	"context"
	"errors"
	"io"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// PrepareReviewChild verifies pre-staged immutable coordinator custody before
// SQL publication. The staging owner must retain these paths through commit and
// replay, and must never remove shared content on a losing concurrent call.
// The supplied artifact catalog is a read-only snapshot of staged metadata; it
// need not expose these artifacts in the database before the graph is committed.
func PrepareReviewChild(ctx context.Context, retained CoordinatorArtifactStore, f review.FrozenAuthority, c review.CheckpointAuthority, criteriaName string, deadline time.Time, artifacts []domain.Artifact) (review.ChildPreparation, error) {
	// No cleanup here: losing callers do not own retained paths.
	var files []review.RetainedFile
	if len(artifacts) > pinnedinput.MaxFiles+32 {
		return review.ChildPreparation{}, errors.New("too many child artifacts")
	}
	for _, a := range artifacts {
		if a.Size < 0 || a.Size > pinnedinput.MaxFileBytes {
			return review.ChildPreparation{}, errors.New("child artifact size exceeds bound")
		}
		opened, reader, err := retained.Open(ctx, a.ID)
		if err != nil {
			return review.ChildPreparation{}, err
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, pinnedinput.MaxFileBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return review.ChildPreparation{}, readErr
		}
		if closeErr != nil {
			return review.ChildPreparation{}, closeErr
		}
		if !reflect.DeepEqual(opened, a) {
			return review.ChildPreparation{}, errors.New("child retained metadata changed")
		}
		files = append(files, review.RetainedFile{Artifact: a, Bytes: raw})
	}
	return review.PrepareChild(f, c, criteriaName, deadline, files)
}
