package backlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// Unlike the original portable probe's invalid initial allocation, these cases
// also freeze a VALID authority first, corrupt current custody, and reopen.
func TestReviewDeclaredCurrentCustodyInitialAndStoredReplay(t *testing.T) {
	ctx := context.Background()
	for _, graph := range []bool{false, true} {
		mode := "template"
		if graph {
			mode = "graph"
		}
		for _, replay := range []bool{false, true} {
			phase := "initial"
			if replay {
				phase = "stored-replay"
			}
			for _, kind := range []string{"healthy", "missing", "duplicate", "unrelated-duplicate", "empty-entry", "wrong-task", "wrong-task-workflow", "wrong-task-run", "wrong-workflow", "wrong-run", "project", "empty-project", "activation"} {
				t.Run(mode+"/"+phase+"/"+kind, func(t *testing.T) {
					f := newDeclaredAdmissionFixture(t)
					run := f.records.WorkflowRuns[0]
					task := f.records.Tasks[0]
					db, err := sql.Open("sqlite", f.store.dbPath)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					if graph {
						run.Graph = &domain.GraphDefinition{RunID: run.ID, Revision: run.GraphRevision, Tasks: []domain.Task{task}, Digest: domain.GraphDigest([]domain.Task{task})}
						raw, err := json.Marshal(run.Graph)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,'$.graph',json(?)) WHERE id=?", string(raw), run.ID); err != nil {
							t.Fatal(err)
						}
						// An amended graph is authoritative even when the template IDs differ.
						if _, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json('[\"template-only\"]')) WHERE id=?", run.WorkflowID); err != nil {
							t.Fatal(err)
						}
					}
					snapshot, err := f.service.ResolveDeclared(ctx, declaredRequest(f))
					if err != nil {
						t.Fatal("healthy resolve", err)
					}
					if replay {
						frozen, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
						if e != nil || !reflect.DeepEqual(snapshot, frozen) {
							t.Fatal("valid initial freeze", e)
						}
					}
					var originalRow string
					if replay {
						if err = db.QueryRow("SELECT record FROM coordinator_review_authorities WHERE run_id=? AND task_id=?", run.ID, task.ID).Scan(&originalRow); err != nil {
							t.Fatal(err)
						}
					}
					ids := []string{task.ID}
					switch kind {
					case "missing":
						ids = nil
					case "duplicate":
						ids = []string{task.ID, task.ID}
					case "unrelated-duplicate":
						ids = []string{task.ID, "other", "other"}
					case "empty-entry":
						ids = []string{task.ID, ""}
					}
					if kind == "missing" || kind == "duplicate" || kind == "unrelated-duplicate" || kind == "empty-entry" {
						if graph {
							var members []domain.Task
							for _, id := range ids {
								v := task
								v.ID = id
								members = append(members, v)
							}
							raw, e := json.Marshal(members)
							if e != nil {
								t.Fatal(e)
							}
							_, err = db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,'$.graph.tasks',json(?)) WHERE id=?", string(raw), run.ID)
						} else {
							raw, e := json.Marshal(ids)
							if e != nil {
								t.Fatal(e)
							}
							_, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json(?)) WHERE id=?", string(raw), run.WorkflowID)
						}
					} else {
						field, value := "", "foreign"
						switch kind {
						case "wrong-task":
							field = "id"
						case "wrong-task-workflow":
							field = "workflowId"
						case "wrong-task-run":
							field = "runId"
						}
						if field != "" {
							if graph {
								_, err = db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,?,?) WHERE id=?", "$.graph.tasks[0]."+field, value, run.ID)
							} else {
								_, err = db.Exec("UPDATE coordinator_tasks SET record=json_set(record,?,?) WHERE id=?", "$."+field, value, task.ID)
							}
						}
						switch kind {
						case "wrong-workflow":
							_, err = db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.id','foreign') WHERE id=?", run.WorkflowID)
						case "wrong-run":
							_, err = db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,'$.id','foreign') WHERE id=?", run.ID)
						case "project":
							_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.project','foreign') WHERE id=?", snapshot.Authority.Parent.AssignmentID)
						case "empty-project":
							_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.project','') WHERE id=?", snapshot.Authority.Parent.AssignmentID)
						case "activation":
							_, err = db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','foreign') WHERE id=?", snapshot.Authority.Parent.AssignmentID)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					before := independentDeclaredTables(t, db)
					// Send the original resolved bytes directly to the owning writer, even if
					// corruption would also cause the outer resolver to refuse earlier.
					got, err := f.store.FreezeDeclaredReviewAuthority(ctx, snapshot.Authority)
					if kind == "healthy" {
						if err != nil || !reflect.DeepEqual(got, snapshot.Authority) {
							t.Fatal("healthy writer", err)
						}
					} else {
						if err == nil {
							t.Fatal("owning writer accepted invalid current custody")
						}
						t.Logf("owning writer refused %s: %v", kind, err)
						if before != independentDeclaredTables(t, db) {
							t.Fatal("refusal changed full logical tables/native audit")
						}
					}
					// OpenMigrated runs startup graph-history backfill. Raw JSON run-ID
					// corruption would make that backfill add a foreign history row;
					// attach with Open for this case to isolate freeze rollback.
					open := sqlitetest.OpenMigrated
					if kind == "wrong-run" {
						open = sqlite.Open
					}
					reopened, e := open(f.store.dbPath)
					if e != nil {
						t.Fatal(e)
					}
					defer reopened.Close()
					f.service.Store = reopened
					again, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
					if kind == "healthy" {
						if e != nil || !reflect.DeepEqual(snapshot, again) {
							t.Fatal("healthy original-authority replay", e)
						}
					} else {
						if e == nil {
							t.Fatal("reopened identity-only freeze admitted corruption")
						}
						if before != independentDeclaredTables(t, db) {
							t.Fatalf("reopened refusal changed full logical tables/native audit:\nBEFORE %s\nAFTER %s", before, independentDeclaredTables(t, db))
						}
						_, found, e := reopened.GetFrozenReviewAuthority(ctx, run.ID, task.ID)
						if e != nil || found != replay {
							t.Fatal("original authority allocation/preservation changed", e, found)
						}
						if replay {
							var row string
							if e = db.QueryRow("SELECT record FROM coordinator_review_authorities WHERE run_id=? AND task_id=?", run.ID, task.ID).Scan(&row); e != nil || row != originalRow {
								t.Fatal("original immutable authority bytes changed", e)
							}
						}
					}
				})
			}
		}
	}
}
