package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSupervisionInboxAcknowledgementsAreRoleOwnedAcrossRestart(t *testing.T) {
	for _, tc := range []struct {
		name          string
		events        []string
		firstID       string
		firstPurpose  string
		secondID      string
		secondPurpose string
	}{
		{
			name:    "reviewer_then_repair",
			events:  []string{"review-1", "repair-1"},
			firstID: "review-1", firstPurpose: "",
			secondID: "repair-1", secondPurpose: string(domain.RecoveryActivationRepair),
		},
		{
			name:    "repair_then_reviewer",
			events:  []string{"repair-1", "review-1"},
			firstID: "repair-1", firstPurpose: string(domain.RecoveryActivationRepair),
			secondID: "review-1", secondPurpose: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			store := openSupervisionStore(t, path)
			seedSupervisedRun(t, store, nil)
			var rows []SupervisionInboxRow
			for _, id := range tc.events {
				rows = append(rows, SupervisionInboxRow{ID: id, Record: json.RawMessage(`{"id":"` + id + `"}`)})
			}
			if _, err := store.AppendSupervisionInbox(ctx, "run-1", rows); err != nil {
				t.Fatal(err)
			}
			ackOne := func(id, purpose string) {
				state, err := store.LoadSupervisionActivationRows(ctx, "run-1")
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CommitSupervisionActivationRows(ctx, SupervisionActivationRowCommit{
					RunID: "run-1", ExpectedRecordRevision: state.Record.Revision,
					Record: state.Record, CursorAdvanced: true,
					AcknowledgedEventIDs: []string{id}, AcknowledgementPurpose: purpose,
					CommittedAt: supervisionTestTime,
				}); err != nil {
					t.Fatal(err)
				}
			}
			ackOne(tc.firstID, tc.firstPurpose)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			store, err = OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			store.SetClock(func() time.Time { return supervisionTestTime })
			afterRestart, err := store.LoadSupervisionActivationRows(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			assertRoleAck(t, afterRestart.Inbox, tc.firstID, tc.firstPurpose, true)
			assertRoleAck(t, afterRestart.Inbox, tc.secondID, tc.secondPurpose, false)

			ackOne(tc.secondID, tc.secondPurpose)
			final, err := store.LoadSupervisionActivationRows(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			assertRoleAck(t, final.Inbox, tc.firstID, tc.firstPurpose, true)
			assertRoleAck(t, final.Inbox, tc.secondID, tc.secondPurpose, true)

			// Replaying either acknowledgement is idempotent and never assigns
			// the other role to that event.
			ackOne(tc.firstID, tc.firstPurpose)
			replayed, err := store.LoadSupervisionActivationRows(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			assertRoleAck(t, replayed.Inbox, tc.firstID, tc.secondPurpose, false)
		})
	}
}

func assertRoleAck(t *testing.T, rows []SupervisionInboxRow, id, purpose string, want bool) {
	t.Helper()
	for _, row := range rows {
		if row.ID != id {
			continue
		}
		for _, acknowledged := range row.AcknowledgedPurposes {
			if acknowledged == purpose {
				if !want {
					t.Fatalf("event %q unexpectedly acknowledged for purpose %q", id, purpose)
				}
				return
			}
		}
		if want {
			t.Fatalf("event %q missing acknowledgement for purpose %q: %#v", id, purpose, row)
		}
		return
	}
	t.Fatalf("event %q missing from inbox", id)
}
