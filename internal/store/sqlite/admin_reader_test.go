package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func adminAssert(t *testing.T, err error, ok bool) {
	t.Helper()
	if err != nil || !ok {
		t.Fatalf("admin assertion: ok=%v err=%v", ok, err)
	}
}

type adminOutcome struct {
	token AdminReadToken
	err   error
}

func adminAwait[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("barrier deadline:", ctx.Err())
		var zero T
		return zero
	}
}

func adminStart(r *AdminReader, ctx context.Context, cb func(context.Context, *Store) error) <-chan adminOutcome {
	done := make(chan adminOutcome, 1)
	go func() {
		token, err := r.Assemble(ctx, cb)
		done <- adminOutcome{token, err}
	}()
	return done
}

func adminFixture(t *testing.T) (context.Context, *Store, *AdminReader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	w, err := OpenMigrated(copyFixtureSchema(t))
	adminAssert(t, err, w != nil)
	t.Cleanup(func() { adminAssert(t, w.Close(), true) })
	adminAssert(t, w.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow", Revision: 1, Progress: domain.ProgressSucceeded, Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded}}},
		Artifacts:    []domain.Artifact{{ID: "artifact", WorkflowRunID: "run"}},
	}), true)
	_, err = w.db.ExecContext(ctx, `INSERT INTO coordinator_worker_enrollments(worker_id,record) VALUES ('worker','{"request":{"workerId":"worker"},"revision":1,"snapshotSequence":1}')`)
	adminAssert(t, err, true)
	r, err := OpenAdminReader(ctx, w.path)
	adminAssert(t, err, r != nil)
	t.Cleanup(func() {
		closeErr := r.Close()
		// Only the explicitly verified intentional-loss cleanup is tolerated.
		if r.closeErr != nil && errors.Is(r.closeErr, sql.ErrConnDone) && r.closeErr.Error() == sql.ErrConnDone.Error() {
			closeErr = nil
		}
		adminAssert(t, closeErr, true)
	})
	return ctx, w, r
}

func adminFacts(ctx context.Context, s *Store, between func()) (facts [2]int64, err error) {
	records, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		return facts, err
	}
	if len(records.Workflows) != 1 || records.Workflows[0].ID != "workflow" || len(records.WorkflowRuns) != 1 || records.WorkflowRuns[0].ID != "run" || records.WorkflowRuns[0].WorkflowID != "workflow" || len(records.Artifacts) != 1 || records.Artifacts[0].ID != "artifact" || records.loadRunArtifacts != nil {
		return facts, errors.New("full-history fixture identity/lazy capability mismatch")
	}
	facts[0] = records.WorkflowRuns[0].Revision
	if between != nil {
		between()
	}
	enrollments, err := s.LoadWorkerEnrollments(ctx)
	if err == nil && (len(enrollments) != 1 || enrollments[0].Request.WorkerID != "worker" || enrollments[0].SnapshotSequence != enrollments[0].Revision) {
		err = errors.New("enrollment fixture identity/sequence mismatch")
	}
	if err == nil {
		facts[1] = enrollments[0].Revision
	}
	return
}

func adminStable(t *testing.T, ctx context.Context, r *AdminReader, want [2]int64) AdminReadToken {
	t.Helper()
	var facts [2]int64
	token, err := r.Assemble(ctx, func(ctx context.Context, s *Store) error {
		var err error
		facts, err = adminFacts(ctx, s, nil)
		return err
	})
	version, versionErr := adminDataVersion(ctx, r.conn)
	adminAssert(t, errors.Join(err, versionErr), facts == want && token.Generation == r.generation && token.Generation != 0 && token.DataVersion == version)
	return token
}

func adminChange(t *testing.T, ctx context.Context, w *Store, n int64) {
	t.Helper()
	tx, err := w.db.BeginTx(ctx, nil)
	adminAssert(t, err, true)
	defer tx.Rollback()
	for _, query := range []string{
		"UPDATE coordinator_workflow_runs SET revision=?,record=json_set(record,'$.revision',?) WHERE id='run'",
		"UPDATE coordinator_worker_enrollments SET record=json_set(record,'$.revision',?,'$.snapshotSequence',?) WHERE worker_id='worker'",
	} {
		_, err = tx.ExecContext(ctx, query, n, n)
		adminAssert(t, err, true)
	}
	adminAssert(t, tx.Commit(), true)
}

func TestAdminReaderWALIndependence(t *testing.T) {
	ctx, w, r := adminFixture(t)
	first := adminStable(t, ctx, r, [2]int64{1, 1})
	tx, err := w.db.BeginTx(ctx, nil) // owns the writer's sole connection
	adminAssert(t, err, true)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "UPDATE coordinator_workflow_runs SET revision=2,record=json_set(record,'$.revision',2)")
	adminAssert(t, err, true)
	var facts [2]int64
	result := adminAwait(t, ctx, adminStart(r, ctx, func(ctx context.Context, s *Store) error {
		var err error
		facts, err = adminFacts(ctx, s, nil)
		return err
	}))
	adminAssert(t, result.err, facts == [2]int64{1, 1} && result.token == first)
	_, err = tx.ExecContext(ctx, "UPDATE coordinator_worker_enrollments SET record=json_set(record,'$.revision',2,'$.snapshotSequence',2)")
	adminAssert(t, err, true) // writer continues AFTER independent callback completed
	adminAssert(t, tx.Commit(), true)
	next := adminStable(t, ctx, r, [2]int64{2, 2})
	adminAssert(t, nil, next.Generation == first.Generation && next.DataVersion > first.DataVersion)
}

func TestAdminReaderLivePolicy(t *testing.T) {
	ctx, w, r := adminFixture(t)
	schema := func() string {
		var signature string
		err := w.db.QueryRowContext(ctx, "SELECT (SELECT version FROM schema_version)||':'||group_concat(name||':'||coalesce(sql,''),'|') FROM (SELECT name,sql FROM sqlite_master ORDER BY name)").Scan(&signature)
		adminAssert(t, err, true)
		return signature
	}
	before := schema()
	opened, openErr := OpenAdminReader(ctx, w.path)
	adminAssert(t, openErr, opened != nil)
	adminAssert(t, opened.Close(), schema() == before)
	adminAssert(t, nil, r.load.db != r.observer.db && r.load.db.Stats().MaxOpenConnections == 1 && r.observer.db.Stats().MaxOpenConnections == 1 && r.adapter.ownerLock == nil)
	_, err := r.conn.ExecContext(ctx, "DELETE FROM kv")
	adminAssert(t, nil, err != nil)
	_, err = r.load.db.ExecContext(ctx, "DELETE FROM kv")
	adminAssert(t, nil, err != nil)
	adminStable(t, ctx, r, [2]int64{1, 1})
	adminAssert(t, r.Close(), schema() == before)
	missing := filepath.Join(t.TempDir(), "missing.db")
	for _, path := range []string{"", ":memory:", missing} {
		reader, err := OpenAdminReader(ctx, path)
		adminAssert(t, nil, err != nil && reader == nil)
	}
	_, err = os.Stat(missing)
	adminAssert(t, nil, errors.Is(err, os.ErrNotExist))
}

func TestAdminReaderFullHistory(t *testing.T) {
	ctx, w, r := adminFixture(t)
	type key struct{}
	base, cancel := context.WithCancel(context.WithValue(ctx, key{}, "preserved"))
	defer cancel()
	tick, snapshot := WithCoordinatorRecordsSnapshot(base)
	defer snapshot.Finish()
	active, err := w.LoadCoordinatorRecords(tick)
	adminAssert(t, err, len(active.Artifacts) == 0 && len(active.WorkflowRuns) == 1)
	for _, input := range []context.Context{base, tick, WithActiveCoordinatorRecords(tick)} {
		token, err := r.Assemble(input, func(normal context.Context, s *Store) error {
			deadline, ok := normal.Deadline()
			want, _ := input.Deadline()
			if normal.Value(key{}) != "preserved" || !ok || deadline != want {
				return errors.New("context values/deadline lost")
			}
			_, err := adminFacts(normal, s, nil)
			return err
		})
		adminAssert(t, err, token.Generation != 0)
	}
	cancel()
	token, err := r.Assemble(WithActiveCoordinatorRecords(tick), func(context.Context, *Store) error { return errors.New("canceled callback ran") })
	adminAssert(t, nil, errors.Is(err, context.Canceled) && token == (AdminReadToken{}))
}

func adminRace(t *testing.T, exhaust bool) {
	ctx, w, r := adminFixture(t)
	loaded, resume := make(chan struct{}), make(chan struct{})
	var attempts [][2]int64
	published := [2]int64{}
	done := adminStart(r, ctx, func(ctx context.Context, s *Store) error {
		facts, err := adminFacts(ctx, s, func() {
			if exhaust || len(attempts) == 0 {
				loaded <- struct{}{}
				<-resume
			}
		})
		attempts = append(attempts, facts) // replace staged state; publish only below
		return err
	})
	commits := 1
	if exhaust {
		commits = 3
	}
	for n := 1; n <= commits; n++ {
		adminAwait(t, ctx, loaded)
		adminChange(t, ctx, w, int64(n+1)) // one correlated COMMIT before second load
		resume <- struct{}{}
	}
	result := adminAwait(t, ctx, done)
	if result.err == nil {
		published = attempts[len(attempts)-1]
	}
	if exhaust {
		adminAssert(t, nil, errors.Is(result.err, ErrAdminCoherence) && result.token == (AdminReadToken{}) && published == [2]int64{} && reflect.DeepEqual(attempts, [][2]int64{{1, 2}, {2, 3}, {3, 4}}))
		adminStable(t, ctx, r, [2]int64{4, 4})
	} else {
		adminAssert(t, result.err, reflect.DeepEqual(attempts, [][2]int64{{1, 2}, {2, 2}}) && published == [2]int64{2, 2})
		adminAssert(t, nil, result.token == adminStable(t, ctx, r, published))
	}
}

func TestAdminReaderCommitRace(t *testing.T)      { adminRace(t, false) }
func TestAdminReaderRetryExhaustion(t *testing.T) { adminRace(t, true) }

func TestAdminReaderActiveCancellation(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		ctx, _, r := adminFixture(t)
		canceled, cancel := context.WithCancel(ctx)
		entered := make(chan struct{})
		done := adminStart(r, canceled, func(ctx context.Context, s *Store) error {
			close(entered)
			<-ctx.Done()
			_, err := s.LoadCoordinatorRecords(ctx)
			if ignore {
				return nil
			}
			return err
		})
		adminAwait(t, ctx, entered)
		cancel()
		result := adminAwait(t, ctx, done)
		adminAssert(t, nil, errors.Is(result.err, context.Canceled) && result.token == (AdminReadToken{}))
		adminStable(t, ctx, r, [2]int64{1, 1})
	}
}

func TestAdminReaderCallbackError(t *testing.T) {
	ctx, _, r := adminFixture(t)
	sentinel, calls := errors.New("callback failed"), 0
	token, err := r.Assemble(ctx, func(context.Context, *Store) error {
		calls++
		return sentinel
	})
	adminAssert(t, nil, errors.Is(err, sentinel) && token == (AdminReadToken{}) && calls == 1)
	adminStable(t, ctx, r, [2]int64{1, 1})
}

func adminLease(t *testing.T, closing bool) {
	ctx, _, r := adminFixture(t)
	entered, release, waiting := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r.waiting = func() { close(waiting) }
	active := adminStart(r, ctx, func(ctx context.Context, s *Store) error {
		close(entered)
		<-release
		_, err := adminFacts(ctx, s, nil) // usable after Close starts, before active fence
		return err
	})
	adminAwait(t, ctx, entered)
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	waiter := adminStart(r, waitCtx, func(context.Context, *Store) error { return errors.New("waiter callback ran") })
	adminAwait(t, ctx, waiting) // already took the contended lease path
	closes := make(chan error, 2)
	want := context.Canceled
	if closing {
		want = ErrAdminReaderClosed
		go func() { closes <- r.Close() }()
		go func() { closes <- r.Close() }()
	} else {
		cancel()
	}
	rejected := adminAwait(t, ctx, waiter) // MUST finish before active release
	adminAssert(t, nil, errors.Is(rejected.err, want) && rejected.token == (AdminReadToken{}))
	select {
	case err := <-closes:
		t.Fatalf("Close finished under active adapter: %v", err)
	default:
	}
	close(release)
	completed := adminAwait(t, ctx, active)
	adminAssert(t, completed.err, completed.token.Generation != 0) // active fence completed
	if closing {
		adminAssert(t, adminAwait(t, ctx, closes), true)
		adminAssert(t, adminAwait(t, ctx, closes), true)
		adminAssert(t, r.Close(), r.conn.PingContext(ctx) == sql.ErrConnDone && r.load.db.PingContext(ctx) != nil && r.observer.db.PingContext(ctx) != nil)
		token, err := r.Assemble(ctx, func(context.Context, *Store) error { return errors.New("closed callback ran") })
		adminAssert(t, nil, errors.Is(err, ErrAdminReaderClosed) && token == (AdminReadToken{}))
	} else {
		adminStable(t, ctx, r, [2]int64{1, 1})
	}
}

func TestAdminReaderCancellationObserverOverlap(t *testing.T) {
	for _, mode := range []string{"contended-invalid", "available-invalid", "contended-healthy"} {
		t.Run(mode, func(t *testing.T) {
			ctx, _, r := adminFixture(t)
			conn, generation := r.conn, r.generation
			lost := mode != "contended-healthy"
			canceled, cancel := context.WithCancel(ctx)
			defer cancel()
			invoked := false
			callback := func(context.Context, *Store) error {
				invoked = true
				return errors.New("canceled callback must not run")
			}
			var waiter <-chan adminOutcome
			if mode == "available-invalid" {
				adminAssert(t, conn.Close(), true)
				rejected := adminAwait(t, ctx, adminStart(r, ctx, callback))
				adminAssert(t, nil, errors.Is(rejected.err, ErrAdminObserver) && rejected.token == (AdminReadToken{}) && !invoked)
				r.admitted = cancel // cancel AFTER the fast lease acquisition
				waiter = adminStart(r, canceled, callback)
			} else {
				entered, release := make(chan struct{}), make(chan struct{})
				active := adminStart(r, ctx, func(context.Context, *Store) error {
					close(entered)
					<-release
					return nil
				})
				adminAwait(t, ctx, entered)
				waiting, admit := make(chan struct{}), make(chan struct{})
				r.waiting = func() { close(waiting); <-admit }
				r.admitted = cancel // force cancellation before the invalid-reader branch
				waiter = adminStart(r, canceled, callback)
				adminAwait(t, ctx, waiting)
				if lost {
					adminAssert(t, conn.Close(), true)
				}
				close(release)
				completed := adminAwait(t, ctx, active) // invalidation and lease release precede admission
				adminAssert(t, nil, errors.Is(completed.err, ErrAdminObserver) == lost && (completed.token == (AdminReadToken{})) == lost)
				close(admit) // only lease is ready; cancellation is forced by admitted
			}
			result := adminAwait(t, ctx, waiter)
			adminAssert(t, nil, errors.Is(result.err, context.Canceled) && errors.Is(result.err, ErrAdminObserver) == lost && result.token == (AdminReadToken{}) && !invoked)
			adminAssert(t, nil, r.conn == conn && r.generation == generation && r.invalid == lost)
			r.admitted, r.waiting = nil, nil // all assembly goroutines have joined
			select {
			case <-r.lease:
				r.lease <- struct{}{}
			default:
				t.Fatal("canceled admission leaked its lease")
			}
			if lost {
				rejected := adminAwait(t, ctx, adminStart(r, ctx, callback))
				adminAssert(t, nil, errors.Is(rejected.err, ErrAdminObserver) && rejected.token == (AdminReadToken{}) && !invoked && r.conn == conn)
			} else {
				adminStable(t, ctx, r, [2]int64{1, 1})
			}
			closeErr := r.Close()
			if lost {
				adminAssert(t, nil, errors.Is(closeErr, sql.ErrConnDone) && closeErr.Error() == sql.ErrConnDone.Error())
			} else {
				adminAssert(t, closeErr, true)
			}
			adminAssert(t, nil, conn.PingContext(ctx) == sql.ErrConnDone && r.load.db.PingContext(ctx) != nil && r.observer.db.PingContext(ctx) != nil)
		})
	}
}

func TestAdminReaderWaitingCancellation(t *testing.T) { adminLease(t, false) }
func TestAdminReaderCloseCoordination(t *testing.T)   { adminLease(t, true) }

func TestAdminReaderObserverIdentityAndLoss(t *testing.T) {
	ctx, w, r := adminFixture(t)
	old := adminStable(t, ctx, r, [2]int64{1, 1})
	conn := r.conn
	adminAssert(t, conn.Close(), true)
	for n := 0; n < 2; n++ {
		token, err := r.Assemble(ctx, func(context.Context, *Store) error { return errors.New("invalid callback ran") })
		adminAssert(t, nil, errors.Is(err, ErrAdminObserver) && token == (AdminReadToken{}) && r.conn == conn)
	}
	closeErr := r.Close()
	adminAssert(t, nil, errors.Is(closeErr, sql.ErrConnDone) && closeErr.Error() == sql.ErrConnDone.Error() && r.load.db.PingContext(ctx) != nil && r.observer.db.PingContext(ctx) != nil)
	reopened, err := OpenAdminReader(ctx, w.path)
	adminAssert(t, err, true)
	defer func() { adminAssert(t, reopened.Close(), true) }()
	next := adminStable(t, ctx, reopened, [2]int64{1, 1})
	adminAssert(t, nil, next.Generation != old.Generation && next.DataVersion == old.DataVersion)
}

func TestAdminReaderObserverCancellation(t *testing.T) {
	for _, boundary := range []int{1, 2} {
		for _, lost := range []bool{false, true} {
			ctx, _, r := adminFixture(t)
			canceled, cancel := context.WithCancel(ctx)
			calls := 0
			r.fence = func(ctx context.Context, conn *sql.Conn) (int64, error) {
				calls++
				if calls == boundary {
					cancel()
					if lost {
						adminAssert(t, conn.Close(), true)
					}
				}
				return adminDataVersion(ctx, conn)
			}
			token, err := r.Assemble(canceled, func(ctx context.Context, s *Store) error { _, err := adminFacts(ctx, s, nil); return err })
			adminAssert(t, nil, errors.Is(err, context.Canceled) && token == (AdminReadToken{}) && errors.Is(err, ErrAdminObserver) == lost)
			r.fence = adminDataVersion
			if lost {
				_, err = r.Assemble(ctx, func(context.Context, *Store) error { return errors.New("lost callback ran") })
				adminAssert(t, nil, errors.Is(err, ErrAdminObserver))
			} else {
				adminStable(t, ctx, r, [2]int64{1, 1})
			}
		}
	}
}

func TestAdminReaderAcquisitionCleanup(t *testing.T) {
	ctx, w, _ := adminFixture(t)
	for stage := 0; stage <= 3; stage++ {
		for _, cancellation := range []bool{false, true} {
			canceled, cancel := context.WithCancel(ctx)
			sentinel := fmt.Errorf("acquisition stage %d", stage)
			if stage == 0 {
				cancel()
				sentinel = context.Canceled
			}
			var acquired *AdminReader
			calls := 0
			reader, err := openAdminReader(canceled, w.path, func(r *AdminReader) error {
				acquired = r
				calls++
				if calls == stage {
					if cancellation {
						cancel()
						sentinel = context.Canceled
						return nil
					}
					return sentinel
				}
				return nil
			})
			cancel()
			adminAssert(t, nil, reader == nil && errors.Is(err, sentinel) && calls == stage)
			if acquired != nil {
				for _, pool := range []*QueryOnlyStore{acquired.load, acquired.observer} {
					adminAssert(t, nil, pool == nil || pool.db.PingContext(ctx) != nil)
				}
				adminAssert(t, nil, acquired.conn == nil || acquired.conn.PingContext(ctx) == sql.ErrConnDone)
			}
		}
	}
}
