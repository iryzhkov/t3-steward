package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Exercise the actual writer acquisition and validator, with no pretend
// successful concurrent write while SQLite's writer is held.
func TestReviewDeclaredWriterSerializesCurrentCustody(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"membership", "project", "activation"} {
		t.Run(kind, func(t *testing.T) {
			s, f := declaredAuthorityFixture(t)
			other, err := sql.Open("sqlite", s.path)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			other.SetMaxOpenConns(1)
			if _, err = other.Exec("PRAGMA busy_timeout=0"); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			tx, err := s.reviewAuthorityWriteTx(ctx, f.Parent)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err = validateDeclaredAuthorityTx(ctx, tx, f); err != nil {
				t.Fatal("valid owning snapshot", err)
			}
			query := "UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json('[]')) WHERE id='w'"
			args := []any{}
			switch kind {
			case "project":
				query = "UPDATE coordinator_assignments SET record=json_set(record,'$.project','foreign') WHERE id=?"
				args = append(args, f.Parent.AssignmentID)
			case "activation":
				query = "UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','foreign') WHERE id=?"
				args = append(args, f.Parent.AssignmentID)
			}
			_, err = other.ExecContext(ctx, query, args...)
			if err == nil || !strings.Contains(err.Error(), "locked") {
				t.Fatal("separate writer must be refused while owning transaction is held", err)
			}
			t.Logf("separate connection while owning writer held: %v", err)
			if err = validateDeclaredAuthorityTx(ctx, tx, f); err != nil {
				t.Fatal("blocked mutation altered owning snapshot", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("blocked writer/no-op transaction leaked logical tables/audit")
			}
			// After releasing the writer, that SAME mutation really commits. The next
			// real freeze must read it and refuse, without allocating an authority.
			if _, err = other.ExecContext(ctx, query, args...); err != nil {
				t.Fatal("mutation after release", err)
			}
			before = cancellationSnapshot(t, s)
			if _, err = s.FreezeDeclaredReviewAuthority(ctx, f); err == nil {
				t.Fatal("later writer ignored committed custody change")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("refusal leaked logical tables/audit")
			}
		})
	}
}

// Run/task identity must hold independently of a matching declaration digest.
func TestReviewDeclaredWriterExactTaskIdentity(t *testing.T) {
	ctx := context.Background()
	for _, field := range []string{"run", "workflow"} {
		t.Run(field, func(t *testing.T) {
			s, f := declaredAuthorityFixture(t)
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			task := records.Tasks[0]
			if field == "run" {
				task.RunID = ""
			} else {
				task.WorkflowID = "foreign"
			}
			// Use a graph so template selection cannot hide this malformed identity.
			run := records.WorkflowRuns[0]
			run.Graph = &domain.GraphDefinition{RunID: run.ID, Revision: run.GraphRevision, Tasks: []domain.Task{task}, Digest: domain.GraphDigest([]domain.Task{task})}
			f.DeclarationDigest = domain.TaskDigest(task)
			assignment := records.Assignments[0]
			assignment.TaskDigest = f.DeclarationDigest
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			tx, err := s.reviewAuthorityWriteTx(ctx, f.Parent)
			if err != nil {
				t.Fatal(err)
			}
			err = validateDeclaredAuthorityTx(ctx, tx, f)
			if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
				tx.Rollback()
				t.Fatal("matching digest hid malformed identity", err)
			}
			if e := tx.Rollback(); e != nil {
				t.Fatal(e)
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("identity refusal leaked logical tables/audit")
			}
		})
	}
}
