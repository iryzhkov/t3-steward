package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// The drop directory is never drained, so an impossible file is re-read on every
// coordinator cycle. It must be reported once and then stay silent, across
// restarts, until its content changes.
func TestLegacyConflictIsReportedOnceAndSurvivesRestarts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	drop := filepath.Join(root, "drop")
	if err := os.MkdirAll(drop, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(drop, "impossible.md")
	writeLegacySubmission(t, file, "unmapped project", true, "first prompt")
	statePath := filepath.Join(root, "state.db")
	storage := filepath.Join(root, "bundles")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	aliases, err := LegacyProjectAliases(map[string]string{"steward": "t3-steward development"})
	if err != nil {
		t.Fatal(err)
	}
	// Each round opens the state afresh, the way a restarted coordinator does.
	round := func(t *testing.T) LegacySubmissionReport {
		t.Helper()
		store, err := sqlite.OpenMigrated(statePath)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		source := LegacySubmissionSource{
			Dir: drop, ProjectAliases: aliases,
			Submitter:  &SubmissionService{StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 10},
			MaxBytes:   1 << 20,
			MaxFiles:   10,
			AllowedUID: uint32(os.Getuid()),
			Quarantine: store,
		}
		return source.Tick(ctx)
	}

	first := round(t)
	if len(first.Errors) != 1 || !strings.Contains(first.Errors[0].Error(), "unmapped project") ||
		len(first.Quarantined) != 1 || first.Quarantined[0] != "impossible" {
		t.Fatalf("first report = %+v", first)
	}
	for cycle := 0; cycle < 4; cycle++ {
		later := round(t)
		if len(later.Errors) != 0 || len(later.Accepted) != 0 ||
			len(later.Quarantined) != 1 || later.Quarantined[0] != "impossible" {
			t.Fatalf("report after restart %d = %+v, want silence", cycle+1, later)
		}
	}

	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	key := LegacySubmissionKey("impossible")
	quarantined, err := store.ListQuarantinedSubmissions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantined) != 1 || quarantined[0].Key != sqlite.QuarantineKey(key) ||
		quarantined[0].State != domain.SubmissionQuarantined ||
		!strings.Contains(quarantined[0].Reason, "unmapped project") {
		t.Fatalf("quarantined submissions = %+v", quarantined)
	}
	events, err := store.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	reports := 0
	for _, event := range events {
		if event.Kind == "submission-quarantined" {
			reports++
			if event.TargetID != key || !strings.Contains(event.Reason, "unmapped project") {
				t.Fatalf("quarantine event = %+v", event)
			}
		}
	}
	if reports != 1 {
		t.Fatalf("durable quarantine reports = %d, want exactly one", reports)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// New content for the same key is a new decision and is tried again.
	writeLegacySubmission(t, file, "t3-steward development", true, "corrected prompt")
	corrected := round(t)
	if len(corrected.Errors) != 0 || len(corrected.Quarantined) != 0 || len(corrected.Accepted) != 1 {
		t.Fatalf("corrected report = %+v", corrected)
	}
	store, err = sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if quarantined, err := store.ListQuarantinedSubmissions(ctx); err != nil || len(quarantined) != 0 {
		t.Fatalf("quarantine after correction = %+v, %v", quarantined, err)
	}
}
