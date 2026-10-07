package sqlitetest

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The store no longer creates quarantine markers, so tests seed the rows that
// retired intake left behind. A seeded row must read back exactly as a
// historical one and leave only through the authenticated, audited release.
func TestHistoricQuarantineRowIsListedAndReleasedOnlyByTheAuditedPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)
	if err := SeedHistoricQuarantine(path, "legacy-abc", digest, "unmapped project", at); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListQuarantinedSubmissions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("quarantine = %+v", records)
	}
	got := records[0]
	if got.Key != "quarantine:legacy-abc" || got.Digest != digest || got.State != domain.SubmissionQuarantined ||
		got.Reason != "unmapped project" || !got.CreatedAt.Equal(at) || got.WorkflowID != "" || got.RunID != "" {
		t.Fatalf("historic row = %+v", got)
	}
	release, err := store.ReleaseQuarantinedSubmission(ctx, "legacy-abc", "operator", "mapped the project", at.Add(time.Hour))
	if err != nil || !release.Released || release.Digest != digest {
		t.Fatalf("release = %+v, %v", release, err)
	}
	if after, err := store.ListQuarantinedSubmissions(ctx); err != nil || len(after) != 0 {
		t.Fatalf("quarantine after release = %+v, %v", after, err)
	}
}

// A fixture that wrote a row the store refuses to decode would make every test
// built on it fail for the wrong reason, so malformed input is refused here.
func TestHistoricQuarantineFixtureRefusesRowsTheStoreCannotDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)
	for name, seed := range map[string]func() error{
		"empty key":     func() error { return SeedHistoricQuarantine(path, "", digest, "refused", at) },
		"untrimmed key": func() error { return SeedHistoricQuarantine(path, " legacy", digest, "refused", at) },
		"oversized key": func() error { return SeedHistoricQuarantine(path, strings.Repeat("k", 246), digest, "refused", at) },
		"short digest":  func() error { return SeedHistoricQuarantine(path, "legacy", "abc", "refused", at) },
		"no reason":     func() error { return SeedHistoricQuarantine(path, "legacy", digest, "", at) },
		"no time":       func() error { return SeedHistoricQuarantine(path, "legacy", digest, "refused", time.Time{}) },
	} {
		if err := seed(); err == nil {
			t.Errorf("%s: malformed historic row was seeded", name)
		}
	}
	if records, err := store.ListQuarantinedSubmissions(context.Background()); err != nil || len(records) != 0 {
		t.Fatalf("quarantine = %+v, %v", records, err)
	}
}
