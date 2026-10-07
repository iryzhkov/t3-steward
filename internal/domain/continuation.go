package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// A task's continuation.md is its own account of where it is: goal, checklist,
// current step, blockers. Steward treats it as a durable checkpoint. The worker
// snapshots the file at each turn end, at a quota or operator pause and at
// collection, keeps the latest snapshot beside the attempt (never in the task's
// tree), and hands it to the coordinator as soon as it is taken at a turn end
// or a pause, and again with the attempt's result. A later attempt of the same
// task receives the latest snapshot as an input.

// ContinuationFileName is the file a task keeps at its workspace root.
const ContinuationFileName = "continuation.md"

// ContinuationSnapshotLimit bounds the bytes a snapshot keeps, the truncation
// marker included. A larger file is cut, never refused: the head of a long
// continuation is still the best account of it there is.
const ContinuationSnapshotLimit = 64 << 10

// ContinuationArtifactName and ContinuationMetadataArtifactName are the names
// a result gives the snapshot and the metadata that describes it.
const (
	ContinuationArtifactName         = "continuation/snapshot.md"
	ContinuationMetadataArtifactName = "continuation/checkpoint.json"
)

// ContinuationArtifactID is the fixed identity of an attempt's snapshot, so a
// result can carry only its own attempt's snapshot.
func ContinuationArtifactID(attemptID string) string { return "continuation-" + attemptID }

// ContinuationMetadataArtifactID is the fixed identity of the snapshot's
// metadata object.
func ContinuationMetadataArtifactID(attemptID string) string {
	return "continuation-meta-" + attemptID
}

// ContinuationLiveArtifactID is the identity of a snapshot a running attempt
// hands to the coordinator before it has a result: the attempt's own
// identity, the assignment epoch of the dispatch that took it and the
// snapshot's sequence. Each distinct snapshot is its own immutable artifact;
// it never conflicts with the one the result carries, nor with a snapshot of
// another dispatch of the same attempt, whose sequence starts again at 1.
func ContinuationLiveArtifactID(attemptID string, assignmentEpoch, sequence int64) string {
	return fmt.Sprintf("continuation-%s-e%d-%d", attemptID, assignmentEpoch, sequence)
}

// ContinuationLiveMetadataArtifactID is the identity of a live snapshot's
// metadata object.
func ContinuationLiveMetadataArtifactID(attemptID string, assignmentEpoch, sequence int64) string {
	return fmt.Sprintf("continuation-meta-%s-e%d-%d", attemptID, assignmentEpoch, sequence)
}

// IsContinuationSnapshotID reports whether id is one of attemptID's snapshot
// identities: the one its result carries or a live one.
func IsContinuationSnapshotID(id, attemptID string) bool {
	if id == ContinuationArtifactID(attemptID) {
		return true
	}
	_, _, live := ContinuationLiveSequence(id, attemptID)
	return live
}

// ContinuationLiveSequence returns the assignment epoch and sequence of a
// live snapshot identity of attemptID, and false for any other identity, the
// result's fixed one included.
func ContinuationLiveSequence(id, attemptID string) (int64, int64, bool) {
	rest, ok := strings.CutPrefix(id, ContinuationArtifactID(attemptID)+"-e")
	if !ok {
		return 0, 0, false
	}
	epochText, sequenceText, ok := strings.Cut(rest, "-")
	if !ok {
		return 0, 0, false
	}
	epoch, epochErr := strconv.ParseInt(epochText, 10, 64)
	sequence, sequenceErr := strconv.ParseInt(sequenceText, 10, 64)
	if epochErr != nil || sequenceErr != nil || epoch < 1 || sequence < 1 ||
		id != ContinuationLiveArtifactID(attemptID, epoch, sequence) {
		return 0, 0, false
	}
	return epoch, sequence, true
}

// ContinuationBoundary names the moment a snapshot was taken.
type ContinuationBoundary string

const (
	ContinuationTurnEnd    ContinuationBoundary = "turn-end"
	ContinuationPause      ContinuationBoundary = "pause"
	ContinuationCollection ContinuationBoundary = "collection"
)

// ContinuationCheckpoint describes one snapshot. It carries the digest, size
// and time, never the content.
type ContinuationCheckpoint struct {
	AttemptID string `json:"attemptId"`
	// Sequence increases with every distinct snapshot of the attempt, so a
	// replay can never pass an older snapshot off as the latest.
	Sequence int64 `json:"sequence"`
	// Turn is the provider turn (or boundary key) the snapshot was taken for;
	// snapshots are idempotent per attempt and turn.
	Turn     string               `json:"turn"`
	Boundary ContinuationBoundary `json:"boundary"`
	SHA256   string               `json:"sha256"`
	// Size is the bytes kept; OriginalSize is the file's size when taken.
	Size         int64     `json:"size"`
	OriginalSize int64     `json:"originalSize"`
	Truncated    bool      `json:"truncated,omitempty"`
	CapturedAt   time.Time `json:"capturedAt"`
}

// ContinuationTruncationMarker ends a snapshot that was cut at the limit.
func ContinuationTruncationMarker(originalSize int64) string {
	return fmt.Sprintf("\n\n[t3-steward: continuation.md was truncated at the %d-byte checkpoint limit; the file was %d bytes]\n",
		ContinuationSnapshotLimit, originalSize)
}

// BoundContinuation returns the bytes a snapshot keeps from head, the first
// bytes of a file of originalSize bytes, and whether they were truncated.
// A truncated snapshot ends at a character boundary followed by the marker.
func BoundContinuation(head []byte, originalSize int64) ([]byte, bool) {
	if originalSize <= ContinuationSnapshotLimit && int64(len(head)) == originalSize {
		return append([]byte(nil), head...), false
	}
	marker := ContinuationTruncationMarker(originalSize)
	keep := min(len(head), ContinuationSnapshotLimit-len(marker))
	for keep > 0 && keep < len(head) && !utf8.RuneStart(head[keep]) {
		keep--
	}
	kept := make([]byte, 0, keep+len(marker))
	kept = append(kept, head[:keep]...)
	return append(kept, marker...), true
}
