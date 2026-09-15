package backlogadmin

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A quarantined submission is reported once and then silent, and its audit
// event names no workflow run, so before this view an operator who missed the
// one log line had nowhere to look. The view must name the key, the digest, the
// time and the reason, and it must say that changed content is tried again.
func TestQuarantineQueryShowsWhatIntakeRefused(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	query := Query{Version: Version, Kind: QueryQuarantine, Principal: Principal{ID: "operator"}}
	// The digests are the content hashes the intake source records, so they are
	// real SHA-256 values here too: the store refuses anything else.
	const first = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
	const second = "3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3"

	empty, err := service.Query(ctx, query)
	if err != nil || len(empty.Quarantine) != 0 {
		t.Fatalf("empty quarantine = %+v, %v", empty.Quarantine, err)
	}

	at := time.Date(2026, 9, 14, 8, 30, 0, 0, time.UTC)
	reason := "legacy submission references an unmapped project"
	if _, _, err := store.QuarantineSubmission(ctx, "legacy-abc", first, reason, at); err != nil {
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
	if entry.Retry != QuarantineRetryAdvice || !strings.Contains(entry.Retry, "digest") {
		t.Fatalf("retry advice = %q", entry.Retry)
	}

	// Changed content is a new observation: the marker follows the digest.
	if _, _, err := store.QuarantineSubmission(ctx, "legacy-abc", second, reason, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	changed, err := service.Query(ctx, query)
	if err != nil || len(changed.Quarantine) != 1 || changed.Quarantine[0].Digest != second {
		t.Fatalf("quarantine after a digest change = %+v, %v", changed.Quarantine, err)
	}

	// When the content changes to something acceptable, the submission source
	// releases the marker and the entry disappears from the view.
	if err := store.ReleaseSubmissionQuarantine(ctx, "legacy-abc"); err != nil {
		t.Fatal(err)
	}
	released, err := service.Query(ctx, query)
	if err != nil || len(released.Quarantine) != 0 {
		t.Fatalf("quarantine after release = %+v, %v", released.Quarantine, err)
	}
}

// A quarantine caused by configuration cannot be cleared by editing the file:
// adding the missing project alias changes no byte of it, so the digest is
// unchanged and intake stays silent forever. The deliberate release is the way
// out, and it is audited with the operator's reason because the operator, not
// the content, is what changed.
func TestQuarantineReleaseClearsAMarkerAndIsSafeToRepeat(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
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
	if _, _, err := store.QuarantineSubmission(ctx, "legacy-abc", digest, reason, at); err != nil {
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
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
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
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
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
	if _, _, err := store.QuarantineSubmission(ctx, "legacy-abc", digest, "refused", at); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Query(ctx, Query{
		Version: Version, Kind: QueryQuarantine, Principal: Principal{ID: "operator"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadSubmissionQuarantine(ctx, "legacy-abc"); err != nil || !found {
		t.Fatalf("reading the view released the marker: found=%t err=%v", found, err)
	}
}
