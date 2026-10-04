package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Snapshot SQL indexes and JSON, including rows hidden by a corrupt index.
func childOwnershipSnapshot(t *testing.T, s *Store) string {
	t.Helper()
	var all []any
	for _, table := range []string{"coordinator_workflows", "coordinator_workflow_runs", "coordinator_tasks", "coordinator_attempts", "coordinator_artifacts", "coordinator_assignments", "coordinator_task_waits", "coordinator_task_wait_events", "coordinator_graph_revisions", "coordinator_review_rounds", "coordinator_review_materializations"} {
		rows, err := s.db.Query("SELECT * FROM " + table + " ORDER BY 1,2")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			all = append(all, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	raw, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestReviewChildRefusesUnboundOwnership(t *testing.T) {
	for _, kind := range []string{"retry", "retry outputs", "output alone", "attempt indexed task", "attempt JSON task", "attempt JSON run", "artifact indexed task", "artifact JSON task", "artifact JSON run", "artifact initial attempt", "assignment initial attempt", "task JSON workflow", "run JSON workflow", "run JSON id", "task JSON id", "artifact JSON id", "malformed attempt"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, _ := childFixture(t)
			ctx := context.Background()
			g, err := p.Build(f, cp, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			a := g.Attempts[0]
			a.ID = "orphan-retry"
			a.Number = 2
			a.Progress = domain.ProgressSucceeded
			a.Control = domain.ControlStopped
			out := domain.Artifact{ID: "orphan-output", WorkflowRunID: cp.RoundID, TaskID: a.TaskID, AttemptID: a.ID, Kind: domain.ArtifactOutput, Name: "verdict.json", SHA256: childDigest([]byte("orphan evidence"))}
			switch kind {
			case "retry", "retry outputs", "attempt indexed task", "attempt JSON task", "attempt JSON run", "malformed attempt":
				if kind == "attempt indexed task" || kind == "attempt JSON task" {
					a.WorkflowRunID = "foreign-run"
				}
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "retry outputs":
					err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{out}})
				case "attempt JSON task":
					_, err = s.db.Exec("UPDATE coordinator_attempts SET task_id='foreign-task' WHERE id=?", a.ID)
				case "attempt JSON run":
					_, err = s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign-run',task_id='foreign-task',record=json_set(record,'$.taskId','foreign-task') WHERE id=?", a.ID)
				case "malformed attempt":
					_, err = s.db.Exec("UPDATE coordinator_attempts SET record='{' WHERE id=?", a.ID)
				}
			case "output alone", "artifact indexed task", "artifact JSON task", "artifact JSON run", "artifact initial attempt":
				if kind == "artifact indexed task" || kind == "artifact JSON task" {
					out.WorkflowRunID = "foreign-run"
				}
				if kind == "artifact initial attempt" {
					out.WorkflowRunID = "foreign-run"
					out.TaskID = "foreign-task"
					out.AttemptID = g.Attempts[0].ID
				}
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{out}}); err != nil {
					t.Fatal(err)
				}
				if kind == "artifact JSON task" {
					_, err = s.db.Exec("UPDATE coordinator_artifacts SET task_id='foreign-task' WHERE id=?", out.ID)
				}
				if kind == "artifact JSON run" {
					_, err = s.db.Exec("UPDATE coordinator_artifacts SET workflow_run_id='foreign-run',task_id='foreign-task',record=json_set(record,'$.taskId','foreign-task') WHERE id=?", out.ID)
				}
			case "run JSON id":
				run := g.Run
				run.WorkflowID = "foreign"
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
				if err == nil {
					_, err = s.db.Exec("UPDATE coordinator_workflow_runs SET id='foreign-run' WHERE id=?", run.ID)
				}
			case "task JSON id":
				task := g.Tasks[0]
				task.WorkflowID = "foreign"
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{task}})
				if err == nil {
					_, err = s.db.Exec("UPDATE coordinator_tasks SET id='foreign-task' WHERE id=?", task.ID)
				}
			case "artifact JSON id":
				artifact := g.Artifacts[0]
				artifact.WorkflowRunID = "foreign"
				artifact.TaskID = "foreign"
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{artifact}})
				if err == nil {
					_, err = s.db.Exec("UPDATE coordinator_artifacts SET id='foreign-artifact' WHERE id=?", artifact.ID)
				}
			case "assignment initial attempt":
				_, err = s.db.Exec("INSERT INTO coordinator_assignments(id,attempt_id,dispatch_token,record) VALUES('orphan-assignment',?,'orphan-token','{}')", g.Attempts[0].ID)
			case "task JSON workflow":
				task := g.Tasks[0]
				task.ID = "orphan-task"
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{task}})
				if err == nil {
					_, err = s.db.Exec("UPDATE coordinator_tasks SET workflow_id='foreign' WHERE id=?", task.ID)
				}
			case "run JSON workflow":
				run := g.Run
				run.ID = "orphan-run"
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
				if err == nil {
					_, err = s.db.Exec("UPDATE coordinator_workflow_runs SET workflow_id='foreign' WHERE id=?", run.ID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before := childOwnershipSnapshot(t, s)
			if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("creation adopted orphan ownership")
			}
			if before != childOwnershipSnapshot(t, s) {
				t.Fatal("refusal changed orphan rows, round, or graph")
			}
			round, err := s.GetReviewRound(ctx, cp.RoundID)
			if err != nil || !round.Deadline.IsZero() || round.Revision != 1 {
				t.Fatal("refusal froze deadline")
			}
			var count int
			if err = s.db.QueryRow("SELECT count(*) FROM coordinator_review_materializations").Scan(&count); err != nil || count != 0 {
				t.Fatal("new marker")
			}
		})
	}
}

func TestReviewChildAllRetryIdentities(t *testing.T) {
	mutations := map[string]string{
		"JSON task":              "record=json_set(record,'$.taskId','foreign-task')",
		"indexed run":            "workflow_run_id='foreign-run'",
		"JSON run":               "record=json_set(record,'$.workflowRunId','foreign-run')",
		"indexed task":           "task_id='foreign-task'",
		"JSON id":                "record=json_set(record,'$.id','foreign-id')",
		"indexed id":             "id='foreign-id'",
		"JSON number":            "record=json_set(record,'$.number',3)",
		"indexed number":         "number=3",
		"JSON revision":          "record=json_set(record,'$.revision',4)",
		"indexed revision":       "revision=4",
		"zero number":            "number=0,record=json_set(record,'$.number',0)",
		"negative number":        "number=-2,record=json_set(record,'$.number',-2)",
		"zero revision":          "revision=0,record=json_set(record,'$.revision',0)",
		"activation ID":          "record=json_set(record,'$.supervisionActivationId','foreign')",
		"activation epoch":       "record=json_set(record,'$.supervisionActivationEpoch',1)",
		"foreign run child task": "workflow_run_id='foreign',record=json_set(record,'$.workflowRunId','foreign')",
		"JSON task only":         "workflow_run_id='foreign',task_id='foreign',record=json_set(record,'$.workflowRunId','foreign')",
		"JSON run only":          "workflow_run_id='foreign',task_id='foreign',record=json_set(record,'$.taskId','foreign')",
		"indexed task only":      "workflow_run_id='foreign',record=json_set(record,'$.workflowRunId','foreign','$.taskId','foreign')",
		"duplicate JSON number":  "record=json_set(record,'$.number',1)",
		"malformed":              "record='{'",
		"null":                   "record='null'",
	}
	for kind, mutation := range mutations {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, _ := childFixture(t)
			ctx := context.Background()
			receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			a := receipt.Graph.Attempts[0]
			a.ID = "valid-retry"
			a.Number = 2
			a.Revision = 3
			a.Progress = domain.ProgressSucceeded
			a.Control = domain.ControlStopped
			a.Failure = "retained progress"
			a.ThreadID = "retained-thread"
			a.AssignmentID = "retained-assignment"
			a.FinalSummaryArtifactID = "retained-summary"
			a.AdminForceStart = true
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
				t.Fatal(err)
			}
			before := childOwnershipSnapshot(t, s)
			if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err != nil || before != childOwnershipSnapshot(t, s) {
				t.Fatalf("valid progressed retry refused or reset: %v", err)
			}
			if _, err = s.db.Exec("UPDATE coordinator_attempts SET "+mutation+" WHERE id=?", a.ID); err != nil {
				t.Fatal(err)
			}
			before = childOwnershipSnapshot(t, s)
			if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("corrupt later retry accepted")
			}
			if before != childOwnershipSnapshot(t, s) {
				t.Fatal("replay overwrote corrupt or mutable state")
			}
		})
	}
}

func TestReviewChildIndependentOrphanRetry(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	g, err := p.Build(f, cp, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	orphan := g.Attempts[0]
	orphan.ID += "-orphan-retry"
	orphan.Number = 2
	orphan.Progress = domain.ProgressSucceeded
	orphan.Control = domain.ControlStopped
	raw, err := json.Marshal(orphan)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)",
		orphan.ID, orphan.WorkflowRunID, orphan.TaskID, orphan.Number, orphan.Revision, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Fatal("creation adopted an orphan successful retry; collector will choose it instead of the new initial attempt")
	}
}

func TestIndependentChildRejectOrphanRetry(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	g, err := p.Build(f, cp, s.now())
	if err != nil {
		t.Fatal(err)
	}
	a := g.Attempts[0]
	a.ID, a.Number = "orphan-retry", 2
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Fatal("accepted pre-existing unbound runnable retry number=2; orphan adopted")
	}
}

func TestIndependentChildRejectCorruptRetryMembership(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	a := receipt.Graph.Attempts[0]
	a.ID, a.Number = "valid-retry", 2
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.taskId','foreign-task') WHERE id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Fatal("accepted corrupt later attempt JSON membership with unchanged indexed membership")
	}
}

func TestIndependentChildRejectCorruptRetryIndex(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	a := receipt.Graph.Attempts[0]
	a.ID, a.Number = "valid-retry", 2
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign-run' WHERE id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Fatal("accepted corrupt later attempt indexed run with unchanged child JSON")
	}
}
