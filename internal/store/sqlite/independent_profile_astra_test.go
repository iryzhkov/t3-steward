package sqlite

import (
	"context"
	"testing"
)

func TestIndependentProfileReopenedCorruption(t *testing.T) {
	for _, kind := range []string{"extra-option", "missing-options", "graph-pool", "marker-profile"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, _ := executionChildFixture(t)
			ctx := context.Background()
			first, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			before := executionSQLSnapshot(t, s)
			switch kind {
			case "extra-option":
				_, err = s.db.Exec("UPDATE coordinator_tasks SET record=json_set(record,'$.routes[0].options.unsafe','yes') WHERE id=?", first.Graph.Tasks[0].ID)
			case "missing-options":
				_, err = s.db.Exec("UPDATE coordinator_tasks SET record=json_remove(record,'$.routes[0].options') WHERE id=?", first.Graph.Tasks[0].ID)
			case "graph-pool":
				q := "UPDATE coordinator_graph_revisions SET record=json_set(record,'$.tasks[0].routes[0].quotaPoolId','forged') WHERE run_id=?"
				if _, e := s.db.Exec(q, cp.RoundID); e == nil {
					t.Fatal("immutable graph writable")
				}
				if before != executionSQLSnapshot(t, s) {
					t.Fatal("trigger refusal mutated SQL")
				}
				if _, err = s.db.Exec("DROP TRIGGER immutable_graph_revision"); err != nil {
					t.Fatal(err)
				}
				_, err = s.db.Exec(q, cp.RoundID)
			case "marker-profile":
				q := "UPDATE coordinator_review_materializations SET record=json_set(record,'$.Authority.Requirements.Members[0].Execution.effort','high') WHERE checkpoint_id=?"
				if _, e := s.db.Exec(q, cp.Key()); e == nil {
					t.Fatal("immutable marker writable")
				}
				if before != executionSQLSnapshot(t, s) {
					t.Fatal("trigger refusal mutated SQL")
				}
				if _, err = s.db.Exec("DROP TRIGGER immutable_review_materialization_update"); err != nil {
					t.Fatal(err)
				}
				_, err = s.db.Exec(q, cp.Key())
			}
			if err != nil {
				t.Fatal(err)
			}
			corrupt := executionSQLSnapshot(t, s)
			if corrupt == before {
				t.Fatal("fixture did not corrupt")
			}
			path := s.path
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenReadOnly(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if corrupt != executionSQLSnapshot(t, reopened) {
				t.Fatal("reopen changed evidence")
			}
			if _, err = reopened.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("reopened corruption accepted")
			}
			if corrupt != executionSQLSnapshot(t, reopened) {
				t.Fatal("refusal changed logical SQL/native audit")
			}
			t.Logf("read-only reopened replay refused: %v", err)
		})
	}
}
