package sqlite

import (
	"context"
	"testing"
)

func TestReviewCancellationEnumerationRefusesOrphanCustody(t *testing.T) {
	for _, missing := range []string{"checkpoint", "authority"} {
		t.Run(missing, func(t *testing.T) {
			s, f, cp, _ := parentWaitFixture(t)
			cancellationEndParent(t, s, f, "failed")
			query := "DELETE FROM coordinator_review_checkpoints WHERE id=?"
			id := cp.Key()
			if missing == "authority" {
				query = "DELETE FROM coordinator_review_authorities WHERE id=?"
				id = f.Key()
			}
			// Inject corruption in this disposable DB; normal writers cannot delete
			// the immutable authority/checkpoint, which is itself a retained guard.
			if _, err := s.db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("DROP TRIGGER immutable_review_" + missing + "_delete"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(query, id); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			if err := s.ReconcileMaterializedReviewChildren(context.Background()); err == nil {
				t.Fatal("orphan materialized custody skipped")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("corrupt scan wrote")
			}
		})
	}
}
