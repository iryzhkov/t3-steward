package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// MaxRunCollectionIDBytes bounds the thread and run IDs of a collection. It is
// the bound a node wait already puts on its thread ID, so every thread that can
// own a run can also collect it.
const MaxRunCollectionIDBytes = 256

// RunCollection is a coordinating thread's durable acknowledgement that it has
// acted on one finished run. It is written only by an explicit "campaign
// collect": reading a result, showing a run or receiving its wake records
// nothing, because a session that read a result and then lost its context has
// not used it.
//
// A collection covers the run as it finished. Progress and CompletedAt are the
// run's own terminal facts, read by the coordinator when the collection was
// recorded, and the run counts as collected only while it still has them; a
// run that finishes again after a rerun is uncollected again.
type RunCollection struct {
	RunID       string        `json:"runId"`
	ThreadID    string        `json:"threadId"`
	Progress    ProgressState `json:"progress"`
	CompletedAt *time.Time    `json:"completedAt,omitempty"`
	CollectedAt time.Time     `json:"collectedAt"`
	Actor       string        `json:"actor,omitempty"`
}

// Covers reports whether this collection still covers run: the run is the one
// collected and it has the terminal progress and completion time that were
// recorded.
func (c RunCollection) Covers(run WorkflowRun) bool {
	if c.RunID != run.ID || c.Progress != run.Progress {
		return false
	}
	return SameCompletion(c.CompletedAt, run.CompletedAt)
}

// SameCompletion compares two optional completion times as instants.
func SameCompletion(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// ValidateRunCollectionID checks a thread or run ID that names a collection.
// The IDs are parts of a storage key whose separator is "/", so a slash is
// refused rather than escaped: no run or thread ID the coordinator issues
// contains one.
func ValidateRunCollectionID(kind, id string) error {
	switch {
	case id == "":
		return fmt.Errorf("the %s ID is empty", kind)
	case len(id) > MaxRunCollectionIDBytes:
		return fmt.Errorf("the %s ID is longer than %d bytes", kind, MaxRunCollectionIDBytes)
	case strings.Contains(id, "/"):
		return fmt.Errorf("the %s ID %q contains \"/\"", kind, id)
	case strings.TrimSpace(id) != id || strings.ContainsAny(id, "\x00\n\r\t"):
		return fmt.Errorf("the %s ID %q contains whitespace or a control character", kind, id)
	}
	return nil
}

// ErrRunCollectionRefused marks a collection the coordinator refused because
// of the run's own state: unknown, not finished, or not owned by the thread.
var ErrRunCollectionRefused = errors.New("run collection refused")
