package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
	"strings"
	"testing"
	"time"
)

func hiddenChildKey(t *testing.T, record any, key, shape string) string {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	needle := `"` + key + `":`
	switch shape {
	case "duplicate":
		return `{"` + key + `":"foreign",` + text[1:]
	case "reverse":
		return text[:len(text)-1] + `,"` + key + `":"foreign"}`
	case "case":
		return strings.Replace(text, needle, `"`+strings.ToUpper(key)+`":`, 1)
	case "mixed":
		return `{"` + strings.ToUpper(key) + `":"foreign",` + text[1:]
	case "escaped":
		return strings.Replace(text, needle, `"\u`+fmt.Sprintf("%04x", key[0])+key[1:]+`":`, 1)
	}
	t.Fatal("shape")
	return ""
}

func TestReviewChildAnalogousRuntimeOwnership(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, kind := range []string{"artifact", "workflow", "run", "task", "run-local task", "history", "assignment", "wait", "event"} {
			if bound && (kind == "assignment" || kind == "wait" || kind == "event") {
				continue
			} // Valid runtime execution records on replay are not an orphan fence.
			for _, shape := range []string{"duplicate", "case", "escaped", "mixed", "reverse"} {
				t.Run(fmt.Sprintf("%s/%s/bound=%v", kind, shape, bound), func(t *testing.T) {
					s, f, cp, p, _ := childFixture(t)
					ctx := context.Background()
					g, err := p.Build(f, cp, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					if bound {
						if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
							t.Fatal(err)
						}
					}
					var record any
					var key, query string
					var args []any
					switch kind {
					case "artifact":
						record = domain.Artifact{ID: "hidden-artifact", WorkflowRunID: g.Run.ID, TaskID: g.Tasks[0].ID, Kind: domain.ArtifactOutput}
						key = "workflowRunId"
						query = "INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?)"
						args = []any{"hidden-artifact", "foreign", "foreign", "", ""}
					case "workflow":
						w := g.Workflow
						record = w
						key = "id"
						query = "INSERT INTO coordinator_workflows(id,record) VALUES(?,?)"
						args = []any{"foreign-workflow"}
					case "run":
						r := g.Run
						r.ID = "hidden-run"
						record = r
						key = "workflowId"
						query = "INSERT INTO coordinator_workflow_runs(id,workflow_id,schedule_id,progress,revision,record) VALUES(?,?,?,?,?,?)"
						args = []any{"hidden-run", "foreign", "", r.Progress, r.Revision}
					case "task":
						task := g.Tasks[0]
						task.ID = "hidden-task"
						record = task
						key = "workflowId"
						query = "INSERT INTO coordinator_tasks(id,workflow_id,name,record) VALUES(?,?,?,?)"
						args = []any{"hidden-task", "foreign", "hidden"}
					case "run-local task":
						task := g.Tasks[0]
						task.ID = "hidden-task"
						task.WorkflowID = "foreign"
						task.RunID = g.Run.ID
						record = task
						key = "runId"
						query = "INSERT INTO coordinator_tasks(id,workflow_id,name,record) VALUES(?,?,?,?)"
						args = []any{"hidden-task", "foreign", "hidden"}
					case "history":
						record = domain.GraphDefinition{RunID: g.Run.ID, Revision: 1}
						key = "runId"
						query = "INSERT INTO coordinator_graph_revisions(run_id,revision,record) VALUES(?,?,?)"
						args = []any{"foreign-history", 1}
					case "assignment":
						record = domain.Assignment{ID: "hidden-assignment", AttemptID: g.Attempts[0].ID}
						key = "attemptId"
						query = "INSERT INTO coordinator_assignments(id,attempt_id,dispatch_token,record) VALUES(?,?,?,?)"
						args = []any{"hidden-assignment", "foreign", "hidden-token"}
					case "wait":
						record = domain.TaskWait{ID: "hidden-wait", WorkflowRunID: g.Run.ID, TaskID: g.Tasks[0].ID, AttemptID: g.Attempts[0].ID}
						key = "attemptId"
						query = "INSERT INTO coordinator_task_waits(id,request_id,attempt_id,thread_id,record) VALUES(?,?,?,?,?)"
						args = []any{"hidden-wait", "hidden-request", "foreign", ""}
					case "event":
						record = domain.TaskWaitReconciliation{ID: "hidden-event", AttemptID: g.Attempts[0].ID}
						key = "attemptId"
						query = "INSERT INTO coordinator_task_wait_events(id,attempt_id,record) VALUES(?,?,?)"
						args = []any{"hidden-event", "foreign"}
					}
					raw := hiddenChildKey(t, record, key, shape)
					// Decode into the actual domain record type, never a surrogate ownership schema.
					decoded := reflect.New(reflect.TypeOf(record)).Interface()
					if err = json.Unmarshal([]byte(raw), decoded); err != nil {
						t.Fatal(err)
					}
					if shape != "reverse" && !reflect.DeepEqual(reflect.ValueOf(decoded).Elem().Interface(), record) {
						t.Fatal("runtime type lost child ownership")
					}
					if _, err = s.db.Exec(query, append(args, raw)...); err != nil {
						t.Fatal(err)
					}
					if kind != "history" {
						if _, err = s.LoadCoordinatorRecords(ctx); err != nil {
							t.Fatal("runtime loader:", err)
						}
					} // History has a separate domain loader.
					before := childOwnershipSnapshot(t, s)
					if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
						t.Fatal("accepted analogous hidden ownership")
					}
					if before != childOwnershipSnapshot(t, s) {
						t.Fatal("refusal changed state")
					}
				})
			}
		}
	}
}

func TestReviewChildDecodeIdentityAndCompatibility(t *testing.T) {
	for _, key := range []string{"id", "number", "revision", "supervisionActivationId", "supervisionActivationEpoch"} {
		for _, shape := range []string{"duplicate", "case", "mixed", "escaped duplicate"} {
			t.Run(key+"/"+shape, func(t *testing.T) {
				s, f, cp, p, _ := childFixture(t)
				ctx := context.Background()
				receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
				if err != nil {
					t.Fatal(err)
				}
				a := receipt.Graph.Attempts[0]
				a.ID = "valid-retry"
				a.Number = 2
				raw, err := json.Marshal(a)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err = json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				value, ok := fields[key]
				if !ok {
					value = json.RawMessage("0")
					if key == "supervisionActivationId" {
						value = json.RawMessage(`""`)
					}
				}
				// Duplicate the same value too: ambiguity is rejected even when it happens to decode coherently.
				text := string(raw)
				if !ok {
					text = `{"` + key + `":` + string(value) + "," + text[1:]
				}
				alias := key
				if shape == "case" || shape == "mixed" {
					alias = strings.ToUpper(key)
				}
				if shape == "escaped duplicate" {
					alias = fmt.Sprintf("\\u%04x", key[0]) + key[1:]
				}
				text = `{"` + alias + `":` + string(value) + "," + text[1:]
				if shape == "case" {
					text = strings.Replace(text, `"`+key+`":`+string(value)+",", "", 1)
				}
				if _, err = s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", a.ID, a.WorkflowRunID, a.TaskID, a.Number, a.Revision, text); err != nil {
					t.Fatal(err)
				}
				before := childOwnershipSnapshot(t, s)
				if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
					t.Fatal("accepted ambiguous identity/counter")
				}
				if before != childOwnershipSnapshot(t, s) {
					t.Fatal("refusal wrote")
				}
			})
		}
	}
	t.Run("canonical unrelated and whitespace order expired", func(t *testing.T) {
		s, f, cp, p, _ := childFixture(t)
		ctx := context.Background()
		foreign := CoordinatorRecords{
			Workflows:    []domain.Workflow{{ID: "foreign-workflow"}},
			WorkflowRuns: []domain.WorkflowRun{{ID: "foreign-run", WorkflowID: "foreign-workflow", Revision: 1}},
			Tasks:        []domain.Task{{ID: "foreign-task", WorkflowID: "foreign-workflow", Name: "foreign"}},
			Attempts:     []domain.Attempt{{ID: "foreign-attempt", WorkflowRunID: "foreign-run", TaskID: "foreign-task", Number: 2, Revision: 1}},
			Artifacts:    []domain.Artifact{{ID: "foreign-artifact", WorkflowRunID: "foreign-run", TaskID: "foreign-task", Kind: domain.ArtifactOutput}},
			Assignments:  []domain.Assignment{{ID: "foreign-assignment", AttemptID: "foreign-attempt", DispatchToken: "foreign-token"}},
		}
		if err := s.SaveCoordinatorRecords(ctx, foreign); err != nil {
			t.Fatal(err)
		}
		first, err := s.MaterializeReviewChild(ctx, f, cp, p)
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"coordinator_workflows", "coordinator_workflow_runs", "coordinator_tasks", "coordinator_attempts", "coordinator_artifacts"} {
			rows, err := s.db.Query("SELECT id,record FROM " + table)
			if err != nil {
				t.Fatal(err)
			}
			type replacement struct{ id, raw string }
			var replacements []replacement
			for rows.Next() {
				var id string
				var raw []byte
				if err = rows.Scan(&id, &raw); err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err = json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				formatted, err := json.MarshalIndent(fields, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				replacements = append(replacements, replacement{id, string(formatted)})
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			for _, r := range replacements {
				if _, err = s.db.Exec("UPDATE "+table+" SET record=? WHERE id=?", r.raw, r.id); err != nil {
					t.Fatal(err)
				}
			}
		}
		s.now = func() time.Time { return p.Deadline().Add(time.Hour) }
		before := childOwnershipSnapshot(t, s)
		got, err := s.MaterializeReviewChild(ctx, f, cp, p)
		if err != nil || !reflect.DeepEqual(first, got) || before != childOwnershipSnapshot(t, s) {
			t.Fatalf("compatible replay: %v", err)
		}
	})
}

func TestReviewChildUnrelatedCorruptDecode(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, raw := range []string{"{", "null", "[]", "42", `{"id":"foreign","id":"foreign"}`, `{"ID":"foreign"}`} {
			t.Run(fmt.Sprintf("%s/bound=%v", raw, bound), func(t *testing.T) {
				s, f, cp, p, _ := childFixture(t)
				ctx := context.Background()
				if bound {
					if _, err := s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES('foreign','foreign','foreign',2,1,?)", raw); err != nil {
					t.Fatal(err)
				}
				before := childOwnershipSnapshot(t, s)
				if _, err := s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
					t.Fatal("discarded unrelated corruption before decode")
				}
				if before != childOwnershipSnapshot(t, s) {
					t.Fatal("refusal changed state")
				}
			})
		}
	}
}

func TestReviewChildAmbiguousParentEndedReadOnly(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	a := receipt.Graph.Attempts[0]
	a.ID = "hidden-retry"
	a.Number = 2
	raw := hiddenChildKey(t, a, "workflowRunId", "case")
	if _, err = s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", a.ID, "foreign", "foreign", a.Number, a.Revision, raw); err != nil {
		t.Fatal(err)
	}
	path := s.path
	s.Close()
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	records, err := ro.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, got := range records.Attempts {
		if got.ID == a.ID && got.WorkflowRunID == cp.RoundID {
			seen = true
		}
	}
	if !seen {
		t.Fatal("runtime ownership invisible")
	}
	before := childOwnershipSnapshot(t, ro)
	if _, err = ro.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Fatal("read-only corrupt replay accepted")
	}
	if before != childOwnershipSnapshot(t, ro) {
		t.Fatal("read-only refusal changed state")
	}
}

func TestReviewChildAmbiguousRuntimeOwnership(t *testing.T) {
	for _, bound := range []bool{false, true} {
		for _, shape := range []string{"duplicate", "case", "uppercase", "unicode case", "escaped", "mixed", "reverse"} {
			t.Run(fmt.Sprintf("%s/bound=%v", shape, bound), func(t *testing.T) {
				s, f, cp, p, _ := childFixture(t)
				ctx := context.Background()
				g, err := p.Build(f, cp, time.Time{})
				if err != nil {
					t.Fatal(err)
				}
				if bound {
					if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
						t.Fatal(err)
					}
				}
				a := g.Attempts[0]
				a.ID = "hidden-retry"
				a.Number = 2
				a.Progress = domain.ProgressSucceeded
				a.Control = domain.ControlStopped
				raw, err := json.Marshal(a)
				if err != nil {
					t.Fatal(err)
				}
				record := string(raw)
				switch shape {
				case "duplicate":
					record = `{"workflowRunId":"foreign-run","taskId":"foreign-task",` + record[1:]
				case "reverse":
					record = record[:len(record)-1] + `,"workflowRunId":"foreign-run","taskId":"foreign-task"}`
				case "case":
					record = strings.ReplaceAll(strings.ReplaceAll(record, `"workflowRunId":`, `"WorkflowRunId":`), `"taskId":`, `"TaskId":`)
				case "uppercase":
					record = strings.ReplaceAll(strings.ReplaceAll(record, `"workflowRunId":`, `"WORKFLOWRUNID":`), `"taskId":`, `"TASKID":`)
				case "unicode case":
					record = strings.ReplaceAll(record, `"taskId":`, `"taſkId":`)
				case "escaped":
					record = strings.ReplaceAll(strings.ReplaceAll(record, `"workflowRunId":`, `"workflow\u0052unId":`), `"taskId":`, `"task\u0049d":`)
				case "mixed":
					record = `{"WORKFLOWRUNID":"foreign-run","TaSkId":"foreign-task",` + record[1:]
				}
				var decoded domain.Attempt
				if err = json.Unmarshal([]byte(record), &decoded); err != nil {
					t.Fatal(err)
				}
				// Reversed duplicates demonstrate that a child claim may be overwritten.
				if shape != "reverse" && (decoded.WorkflowRunID != cp.RoundID || decoded.TaskID != a.TaskID) {
					t.Fatal("fixture ownership")
				}
				if _, err = s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", a.ID, "foreign-run", "foreign-task", 2, a.Revision, record); err != nil {
					t.Fatal(err)
				}
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				seen := false
				for _, got := range records.Attempts {
					if got.ID == a.ID && got.WorkflowRunID == decoded.WorkflowRunID && got.TaskID == decoded.TaskID {
						seen = true
					}
				}
				if !seen {
					t.Fatal("runtime loader visibility")
				}
				before := childOwnershipSnapshot(t, s)
				if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
					t.Error("accepted hidden/ambiguous retry")
				}
				if before != childOwnershipSnapshot(t, s) {
					t.Error("refusal changed SQL/JSON/round/marker")
				}
			})
		}
	}
}
