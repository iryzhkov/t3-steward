package domain

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundContinuationKeepsAFileWithinTheCapWhole(t *testing.T) {
	raw := []byte("# Continuation\n\nstep 3 of 5\n")
	kept, truncated := BoundContinuation(raw, int64(len(raw)))
	if truncated || !bytes.Equal(kept, raw) {
		t.Fatalf("kept=%q truncated=%t", kept, truncated)
	}
}

func TestBoundContinuationTruncatesAboveTheCapWithAMarker(t *testing.T) {
	// A multi-byte rune straddles the cut, so the kept bytes must back off to a
	// rune boundary rather than end in half a character.
	raw := []byte(strings.Repeat("é", ContinuationSnapshotLimit))
	kept, truncated := BoundContinuation(raw[:ContinuationSnapshotLimit], int64(len(raw)))
	if !truncated || len(kept) > ContinuationSnapshotLimit {
		t.Fatalf("len=%d truncated=%t", len(kept), truncated)
	}
	if !utf8.Valid(kept) {
		t.Fatal("truncated snapshot is not valid UTF-8")
	}
	marker := ContinuationTruncationMarker(int64(len(raw)))
	if !strings.HasSuffix(string(kept), marker) || !strings.Contains(marker, "truncated") {
		t.Fatalf("snapshot does not end with the truncation marker %q", marker)
	}
}
