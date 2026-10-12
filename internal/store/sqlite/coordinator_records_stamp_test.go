package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

// Exercise raw SQL writes as well as store methods: invalidation cannot depend
// on a caller remembering to increment a Go-side generation.
func TestCoordinatorRecordsStampTracksEveryLoadedTable(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, snapshot := WithCoordinatorRecordsSnapshot(context.Background())
	defer snapshot.Finish()
	read := func() {
		t.Helper()
		if _, err := s.LoadCoordinatorRecords(ctx); err != nil {
			t.Fatal(err)
		}
	}
	read()
	tables := []struct{ name, columns, values string }{
		{"coordinator_workflows", "id,record", "'w','{}'"},
		{"coordinator_workflow_runs", "id,workflow_id,schedule_id,progress,revision,record", "'r','w','','active',0,'{}'"},
		{"coordinator_tasks", "id,workflow_id,name,record", "'t','w','t','{}'"},
		{"coordinator_attempts", "id,workflow_run_id,task_id,number,record", "'a','r','t',1,'{}'"},
		{"coordinator_assignments", "id,attempt_id,dispatch_token,record", "'as','a','token','{}'"},
		{"coordinator_schedules", "id,active_run_id,revision,record", "'s','',0,'{}'"},
		{"coordinator_schedule_templates", "schedule_id,version,workflow_id,record", "'s',1,'w','{}'"},
		{"coordinator_triggers", "id,schedule_id,occurrence_key,record", "'tr','s','occurrence','{}'"},
		{"coordinator_quota_pools", "id,record", "'q','{}'"},
		{"coordinator_artifacts", "id,workflow_run_id,task_id,attempt_id,sha256,record", "'ar','r','t','a','','{}'"},
		{"coordinator_admin_commands", "id,target_type,target_id,state,record", "'cmd','run','r','pending','{}'"},
	}
	want := 1
	for _, table := range tables {
		for _, statement := range []string{
			"INSERT INTO " + table.name + "(" + table.columns + ") VALUES(" + table.values + ")",
			"UPDATE " + table.name + " SET record = '{}'",
			"DELETE FROM " + table.name,
		} {
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
			read()
			want++
			if snapshot.loads != want {
				t.Fatalf("%s: loads=%d want=%d", statement, snapshot.loads, want)
			}
		}
	}
	// A write rolled back changes no visible records and needs no reload.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO coordinator_quota_pools(id,record) VALUES('rolled-back','{}')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	read()
	if snapshot.loads != want {
		t.Fatalf("rollback loads=%d want=%d", snapshot.loads, want)
	}
}
