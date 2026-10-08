package backlogadmin

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// A quarantined submission is reported once and then silent, and its audit
// event names no workflow run, so before this view an operator who missed the
// one log line had nowhere to look. The view must name the key, the digest, the
// time and the reason, and explain deliberate historical marker cleanup.
func TestQuarantineQueryShowsWhatIntakeRefused(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	query := Query{Version: Version, Kind: QueryQuarantine, Principal: Principal{ID: "operator"}}
	// The digest is the content hash the retired intake source recorded, so it
	// is a real SHA-256 value here too: the store refuses anything else.
	const first = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"

	empty, err := service.Query(ctx, query)
	if err != nil || len(empty.Quarantine) != 0 {
		t.Fatalf("empty quarantine = %+v, %v", empty.Quarantine, err)
	}

	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	reason := "legacy submission references an unmapped project"
	if err := sqlitetest.SeedHistoricQuarantine(path, "legacy-abc", first, reason, at); err != nil {
		t.Fatal(err)
	}
	response, err := service.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Quarantine) != 1 {
		t.Fatalf("quarantine = %+v", response.Quarantine)
	}
	entry := response.Quarantine[0]
	if entry.Key != "legacy-abc" || entry.RecordKey != "quarantine:legacy-abc" ||
		entry.Digest != first || !entry.QuarantinedAt.Equal(at) || entry.Reason != reason {
		t.Fatalf("entry = %+v", entry)
	}
	if !strings.Contains(entry.Retry, "no longer scanned or retried") ||
		!strings.Contains(entry.Retry, "authenticated t3-steward backlog quarantine release") ||
		strings.Contains(entry.Retry, "explicitly enabled") || strings.Contains(entry.Retry, "change the file") {
		t.Fatalf("retry advice = %q", entry.Retry)
	}
}

// Retained historical markers require deliberate, authenticated cleanup.
// The release is audited with the operator's reason and is safe to repeat.
func TestQuarantineReleaseClearsAMarkerAndIsSafeToRepeat(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	service.SetClock(func() time.Time { return at.Add(time.Hour) })
	const digest = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
	const reason = "legacy submission references an unmapped project"
	if err := sqlitetest.SeedHistoricQuarantine(path, "legacy-abc", digest, reason, at); err != nil {
		t.Fatal(err)
	}

	principal := Principal{ID: "operator"}
	release, err := service.ReleaseQuarantine(ctx, principal, QuarantineReleaseRequest{
		Key: "legacy-abc", Reason: "mapped the project",
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !release.Released || release.Key != "legacy-abc" || release.Digest != digest || release.Reason != reason {
		t.Fatalf("release = %+v", release)
	}
	response, err := service.Query(ctx, Query{
		Version: Version, Kind: QueryQuarantine, Principal: principal,
	})
	if err != nil || len(response.Quarantine) != 0 {
		t.Fatalf("quarantine after release = %+v, %v", response.Quarantine, err)
	}

	// Repeating it says there was nothing to release rather than inventing a
	// failure, which is what makes an ambiguous response safe to retry.
	again, err := service.ReleaseQuarantine(ctx, principal, QuarantineReleaseRequest{
		Key: "legacy-abc", Reason: "mapped the project",
	})
	if err != nil || again.Released {
		t.Fatalf("second release = %+v, %v", again, err)
	}

	// The release is audited against the submission, with the operator and the
	// reason they gave.
	events, err := store.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Kind != "submission-quarantine-released" {
			continue
		}
		found = true
		if event.TargetType != domain.AuditTargetSubmission || event.TargetID != "legacy-abc" ||
			event.Actor != principal.ID || event.Reason != "mapped the project" {
			t.Fatalf("audit event = %+v", event)
		}
	}
	if !found {
		t.Fatal("the release was not audited")
	}
}

// A release needs a key and a reason, and is refused without them.
func TestQuarantineReleaseRefusesAnIncompleteRequest(t *testing.T) {
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]QuarantineReleaseRequest{
		"no key":        {Reason: "mapped the project"},
		"no reason":     {Key: "legacy-abc"},
		"untrimmed key": {Key: " legacy-abc", Reason: "mapped the project"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := service.ReleaseQuarantine(context.Background(), Principal{ID: "operator"}, request); err == nil {
				t.Fatal("an incomplete release was accepted")
			}
		})
	}
}

// The view is a read of the durable record and nothing else: it takes no
// target, it is one of the read kinds a verified remote client may perform, and
// it never releases a marker.
func TestQuarantineQueryIsAReadWithoutATarget(t *testing.T) {
	if !validQuery(Query{Kind: QueryQuarantine}) {
		t.Fatal("a quarantine query needs no target and must be valid without one")
	}
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	const digest = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
	if err := sqlitetest.SeedHistoricQuarantine(path, "legacy-abc", digest, "refused", at); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(ctx, Query{
		Version: Version, Kind: QueryQuarantine, Principal: Principal{ID: "operator"},
	}); err != nil {
		t.Fatal(err)
	}
	if records, err := store.ListQuarantinedSubmissions(ctx); err != nil || len(records) != 1 || records[0].Key != "quarantine:legacy-abc" {
		t.Fatalf("reading the view released the marker: records=%+v err=%v", records, err)
	}
}
