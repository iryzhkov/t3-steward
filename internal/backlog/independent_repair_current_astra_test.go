package backlog

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// Probe the interval AFTER saved-authority resolution, using the same real writer.
func TestIndependentRepairLateReplayAndProjection(t *testing.T) {
	ctx := context.Background()
	for _, replay := range []bool{false, true} {
		phase := "initial"
		if replay {
			phase = "stored-replay"
		}
		for _, kind := range []string{"healthy-advance", "duplicate-projection", "unrelated-duplicate", "empty-project-pair", "activation"} {
			t.Run(phase+"/"+kind, func(t *testing.T) {
				f := newDeclaredAdmissionFixture(t)
				original, err := f.service.ResolveDeclared(ctx, declaredRequest(f))
				if err != nil {
					t.Fatal(err)
				}
				if replay {
					got, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
					if e != nil || !reflect.DeepEqual(original, got) {
						t.Fatal("initial healthy freeze", e)
					}
				}
				db, err := sql.Open("sqlite", f.store.dbPath)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var originalRow string
				row := func() string {
					var raw string
					e := db.QueryRow("SELECT record FROM coordinator_review_authorities WHERE run_id=? AND task_id=?", f.request.RunID, f.request.TaskID).Scan(&raw)
					if e != nil && e != sql.ErrNoRows {
						t.Fatal(e)
					}
					return raw
				}
				originalRow = row()
				if replay {
					// Resolution must use the retained policy despite an unusable new catalog.
					f.catalog.catalog.Classifications = nil
					if _, err = db.Exec("UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?", f.request.AttemptID); err != nil {
						t.Fatal(err)
					}
				}
				fired := false
				var before string
				f.service.Store = independentDeclaredStore{f.store.Store, func() {
					fired = true
					var e error
					switch kind {
					case "duplicate-projection":
						// Unique SQL key/name and unique workflow.TaskIDs, but two projected
						// records have the exact selected JSON task ID and declaration digest.
						_, e = db.Exec("INSERT INTO coordinator_tasks(id,workflow_id,name,record) SELECT 'independent-duplicate-row',workflow_id,'independent-duplicate-name',record FROM coordinator_tasks WHERE id=?", f.request.TaskID)
					case "unrelated-duplicate":
						_, e = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json_array(?,'unrelated','unrelated')) WHERE id=?", f.request.TaskID, f.records.Workflows[0].ID)
					case "empty-project-pair":
						_, e = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.project','') WHERE id=?", f.records.Workflows[0].ID)
						if e == nil {
							_, e = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.project','') WHERE id=?", original.Authority.Parent.AssignmentID)
						}
					case "activation":
						_, e = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','late') WHERE id=?", original.Authority.Parent.AssignmentID)
					}
					if e != nil {
						t.Fatal(e)
					}
					before = independentDeclaredTables(t, db)
				}}
				got, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
				if !fired {
					t.Fatal("did not reach real writer after successful resolution")
				}
				if kind == "healthy-advance" {
					if err != nil || !reflect.DeepEqual(got, original) {
						t.Fatal("healthy original issue changed", err)
					}
					if replay && row() != originalRow {
						t.Fatal("healthy replay rewrote authority")
					}
				} else {
					if err == nil {
						t.Fatal("late malformed custody admitted")
					}
					if kind == "duplicate-projection" && !strings.Contains(err.Error(), "identity mismatch") {
						t.Fatal("did not reach selected-task count fence", err)
					}
					t.Logf("actual owning freeze refused after resolution: %v", err)
					if independentDeclaredTables(t, db) != before || row() != originalRow {
						t.Fatal("refusal changed full logical tables or exact original row")
					}
				}
				reopened, e := sqlitetest.OpenMigrated(f.store.dbPath)
				if e != nil {
					t.Fatal(e)
				}
				defer reopened.Close()
				f.service.Store = reopened
				if kind == "healthy-advance" {
					again, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
					if e != nil || !reflect.DeepEqual(again, original) {
						t.Fatal("healthy reopened replay", e)
					}
				} else {
					// Direct original snapshot cannot escape the writer even when the outer
					// resolver would reject the malformed projection earlier.
					if _, e = reopened.FreezeDeclaredReviewAuthority(ctx, original.Authority); e == nil {
						t.Fatal("reopened writer admitted")
					}
					if _, e = f.service.FreezeDeclared(ctx, declaredRequest(f)); e == nil {
						t.Fatal("reopened identity entry admitted")
					}
					if independentDeclaredTables(t, db) != before || row() != originalRow {
						t.Fatal("reopened refusal changed tables/row")
					}
				}
			})
		}
	}
}
