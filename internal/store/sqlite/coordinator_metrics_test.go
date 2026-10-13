package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"testing"
	"time"
)

func TestMetricsSnapshotReadOnlyProjectionLatestAttemptAndUsageCoverage(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := store.db.Exec(q, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, a := range []struct {
		id, task, progress string
		n                  int
	}{{"old", "task", "failed", 1}, {"new", "task", "succeeded", 2}, {"activation", "", "active", 1}} {
		exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,record)VALUES(?,?,?,?,?)", a.id, "run", a.task, a.n, `{"progress":"`+a.progress+`","payload":"private task content"}`)
	}
	exec("INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,record)VALUES(?,?,?,?,?)", "supervisor", "run", "task", 3, `{"progress":"active","supervisionActivationId":"activation"}`)
	now := time.Now().UTC()
	w := domain.WorkerSnapshot{Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute)}
	raw, _ := json.Marshal(w)
	exec("INSERT INTO coordinator_worker_snapshots(worker_id,worker_epoch,coordinator_epoch,sequence,connected,valid_until,record)VALUES(?,?,?,?,?,?,?)", "private-worker", "epoch", 1, 1, 1, w.ValidUntil.Format(time.RFC3339Nano), string(raw))
	for _, u := range []struct {
		worker, event, kind string
		presence            int
	}{{"", "duplicate", "call", 15}, {"worker", "duplicate", "call", 15}, {"worker", "absent", "turn", 0}, {"worker", "partial", "call", 1}, {"worker", "diagnostic", "diagnostic", 0}} {
		exec("INSERT INTO usage_samples(worker_id,event_id,provider,thread_id,model,observed_at,input_tokens,cache_write_tokens,cache_read_tokens,output_tokens,cost_usd,kind,cumulative_tokens,field_presence)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", u.worker, u.event, "provider", "private-thread", "model", now.Format(time.RFC3339Nano), 100, 0, 0, 0, 0, u.kind, 0, u.presence)
	}
	var changesBefore, changesAfter int64
	if err := store.db.QueryRow("SELECT total_changes()").Scan(&changesBefore); err != nil {
		t.Fatal(err)
	}
	observer, err := OpenQueryOnly(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	if _, err := observer.db.Exec("DELETE FROM usage_samples"); err == nil {
		t.Fatal("observer connection permits writes")
	}
	s, err := observer.LoadMetricsSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT total_changes()").Scan(&changesAfter); err != nil {
		t.Fatal(err)
	}
	writerTx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, readErr := observer.LoadMetricsSnapshot(ctx)
	cancel()
	writerTx.Rollback()
	if readErr != nil {
		t.Fatal("observer queued behind occupied writer connection", readErr)
	}
	if changesAfter != changesBefore {
		t.Fatal("snapshot mutated durable state", changesBefore, changesAfter)
	}
	if len(s.Tasks) != 1 || s.Tasks["succeeded"] != 1 {
		t.Fatal(s.Tasks)
	}
	if len(s.Workers) != 1 || !s.Workers[0].Connected || s.Workers[0].WorkerID != "" || !s.Workers[0].ObservedAt.Equal(now) {
		t.Fatal(s.Workers)
	}
	if s.UsageEvidence["call"] != 2 || s.UsageEvidence["turn"] != 1 || s.UsageEvidence["diagnostic"] != 1 {
		t.Fatal(s.UsageEvidence)
	}
	if s.UsageFields["complete"] != 1 || s.UsageFields["absent"] != 1 || s.UsageFields["partial"] != 1 {
		t.Fatal(s.UsageFields)
	}
}
func TestMetricsSnapshotEvidenceBoundMarksPartialWindow(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Native SQL fixture generation avoids an expensive10001-call test loop.
	_, err = tx.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10001)
 INSERT INTO usage_samples(worker_id,event_id,provider,thread_id,model,observed_at,input_tokens,cache_write_tokens,cache_read_tokens,output_tokens,cost_usd,kind,cumulative_tokens,field_presence)
 SELECT 'worker','event-'||x,'provider','thread','model',printf('2026-10-%02dT00:00:00Z',1+(x%10)),0,0,0,0,0,'call',0,0 FROM n`)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	observer, err := OpenQueryOnly(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	s, err := observer.LoadMetricsSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !s.UsageEvidenceTruncated || s.UsageEvidence["call"] != 10000 || s.UsageFields["absent"] != 10000 || s.UsageEvidenceOldest.IsZero() {
		t.Fatal(s.UsageEvidenceTruncated, s.UsageEvidence, s.UsageFields, s.UsageEvidenceOldest)
	}
}
func TestMetricsSnapshotMalformedObservationFailsWholeRead(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.db.Exec("INSERT INTO buckets(key,state,updated_at) VALUES('bad','not-json','now')"); err != nil {
		t.Fatal(err)
	}
	observer, err := OpenQueryOnly(store.path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	if _, err = observer.LoadMetricsSnapshot(context.Background()); err == nil {
		t.Fatal("malformed observation accepted")
	}
}
