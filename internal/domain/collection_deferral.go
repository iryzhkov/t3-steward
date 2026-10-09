package domain

import (
	"strings"
	"time"
)

// collectionDeferredNotePrefix opens the turn-end note a worker reports for an
// attempt whose turn ended and whose collection keeps deferring.
const collectionDeferredNotePrefix = "collection deferred since "

// CollectionDeferredNote is the journal excerpt's turn-end note for an
// attempt whose collection has deferred since since, most recently because of
// reason. It rides the TurnEnd field, which already says why a worker holds a
// turn that ended instead of collecting it, so that neither the worker
// protocol nor the admin read shape changes.
func CollectionDeferredNote(since time.Time, reason string) string {
	return collectionDeferredNotePrefix + since.UTC().Format(time.RFC3339) + ": " + reason
}

// ParseCollectionDeferredNote reads a note CollectionDeferredNote wrote.
func ParseCollectionDeferredNote(note string) (since time.Time, reason string, ok bool) {
	rest, found := strings.CutPrefix(note, collectionDeferredNotePrefix)
	if !found {
		return time.Time{}, "", false
	}
	stamp, reason, _ := strings.Cut(rest, ": ")
	since, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return time.Time{}, "", false
	}
	return since, reason, true
}
