package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type collectionInterruptedPublisher struct {
	*CustodyStore
	failures   int
	afterWrite bool
	calls      int
}

func (p *collectionInterruptedPublisher) PublishResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	p.calls++
	if permanentCollectionIntent(result.Finalized.Completion.Failure) && p.failures > 0 {
		p.failures--
		if p.afterWrite {
			if err := p.CustodyStore.PublishResult(ctx, pkg, result); err != nil {
				return err
			}
		}
		return errors.New("simulated transport interrupted")
	}
	return p.CustodyStore.PublishResult(ctx, pkg, result)
}

func TestPermanentCollectionFailureInterruptedFallbackReplay(t *testing.T) {
	for _, afterWrite := range []bool{false, true} {
		t.Run(fmt.Sprint(afterWrite), func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 1400, 2000)
			publisher := &collectionInterruptedPublisher{CustodyStore: f.custody, failures: 1, afterWrite: afterWrite}
			f.driver.Publisher = publisher
			_ = f.runtime.collect(context.Background(), "assignment-1")
			failure := f.record(t).Failure
			if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
				t.Fatal("interruption not surfaced")
			}
			if record := f.record(t); record.Phase != PhaseFailed || record.Failure != failure {
				t.Fatalf("intent lost: %+v", record)
			}
			f.reopen(t)
			for i := 0; i < 3; i++ {
				if err := f.runtime.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if record := f.record(t); record.Phase != PhaseCompleted || f.process.calls != 1 {
				t.Fatalf("phase=%s verification=%d", record.Phase, f.process.calls)
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil || len(pending.Manifest.Objects) != 2 {
				t.Fatalf("failed envelope missing: %v", err)
			}
			f.preserved(t)
		})
	}
}

func TestPermanentCollectionFailureTinyBudgetFailsClosed(t *testing.T) {
	f := newCollectionFixture(t, 64, 128, 1400, 2000)
	_ = f.runtime.collect(context.Background(), "assignment-1")
	failure := f.record(t).Failure
	for i := 0; i < 3; i++ {
		f.reopen(t)
		if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
			t.Fatal("unsafe envelope accepted")
		}
		record := f.record(t)
		observation := observation(record, runtimeTestNow, true)
		if record.Phase != PhaseFailed || record.Failure != failure || f.process.calls != 1 ||
			observation.State != domain.AssignmentClaimed || !strings.Contains(observation.Journal.Failure, "retained") {
			t.Fatalf("not fail closed: record=%+v observation=%+v verification=%d", record, observation, f.process.calls)
		}
	}
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending != nil {
		t.Fatalf("false failed custody: %v %+v", err, pending)
	}
	f.preserved(t)
}

type collectionTransientPublisher struct {
	*CustodyStore
	failures int
}

func (p *collectionTransientPublisher) PublishResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	if p.failures > 0 {
		p.failures--
		return errors.New("I/O total size exceeds limit (untyped)")
	}
	return p.CustodyStore.PublishResult(ctx, pkg, result)
}

func TestPermanentCollectionFailureTransientPublicationRecovers(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	f.driver.Publisher = &collectionTransientPublisher{CustodyStore: f.custody, failures: 1}
	if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
		t.Fatal("transient failure missing")
	}
	if record := f.record(t); record.Phase != PhaseCollecting || record.Failure != "" {
		t.Fatalf("transient reclassified: %+v", record)
	}
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted || record.Failure != "" || f.process.calls != 2 {
		t.Fatalf("transient did not retry original collection: %+v verification=%d", record, f.process.calls)
	}
}

type collectionUnsettledControl struct {
	*recordingT3
	fail bool
}

func (c *collectionUnsettledControl) SettleThread(ctx context.Context, id, token string) error {
	if c.fail {
		return errors.New("settlement unavailable")
	}
	return c.recordingT3.SettleThread(ctx, id, token)
}
func TestPermanentCollectionFailureAlreadyDurableSuccess(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprint(acknowledged), func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			control := &collectionUnsettledControl{recordingT3: f.control, fail: true}
			f.driver.T3 = control
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			record := f.record(t)
			if record.Phase != PhaseCompleted || !record.SettlePending || record.Failure != "" {
				t.Fatalf("success reclassified: %+v", record)
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(pending)
			if err != nil {
				t.Fatal(err)
			}
			if acknowledged {
				if err := f.custody.AcknowledgeUpload(pending.Manifest.ID); err != nil {
					t.Fatal(err)
				}
			}
			// A later read would now exceed limits; it cannot overwrite durable success.
			f.control.archive = append(f.control.archive, bytes.Repeat([]byte("x"), 32000)...)
			f.reopen(t)
			if err := f.driver.Collect(context.Background(), f.pkg, f.workspace); !errors.Is(err, ErrSettleUnproven) {
				t.Fatalf("durable replay=%v", err)
			}
			if f.process.calls != 1 {
				t.Fatalf("durable result reverified %d times", f.process.calls)
			}
			location := "outbox"
			if acknowledged {
				location = "acknowledged"
			}
			raw, err := os.ReadFile(filepath.Join(f.custody.config.Root, location, pending.Manifest.ID+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var after PendingUpload
			if err := json.Unmarshal(raw, &after); err != nil {
				t.Fatal(err)
			}
			afterRaw, _ := json.Marshal(after)
			if !bytes.Equal(before, afterRaw) {
				t.Fatal("durable success manifest replaced")
			}
			control.fail = false
			if err := f.runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if record := f.record(t); record.SettlePending || record.Failure != "" {
				t.Fatalf("settlement retry failed: %+v", record)
			}
		})
	}
}

func TestPermanentCollectionFailureStaleFlightCannotDecide(t *testing.T) {
	for _, mutate := range []string{"cancelled", "superseded", "paused", "stop-requested"} {
		t.Run(mutate, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 1400, 100)
			before := f.record(t)
			flight := &collectionFlight{done: make(chan struct{}), err: &permanentCollectionFailure{size: workerproto.NewArtifactSizeError("object", 1024, 1400, testArtifact("output", "results/output", "x"))}}
			close(flight.done)
			key := f.runtime.collectionFlightKey(before)
			collectionFlights.Lock()
			collectionFlights.running[key] = flight
			collectionFlights.Unlock()
			if err := f.runtime.journal.update(func(state *journalState) error {
				current := state.Attempts["assignment-1"]
				switch mutate {
				case "cancelled":
					current.Phase, current.StopConfirmed = PhaseStopped, true
				case "superseded":
					current.Assignment.Epoch++
				case "paused":
					current.LocalThrottle = &LocalThrottleRequest{Reason: "quota"}
				case "stop-requested":
					current.CommandRequests = map[string]domain.WorkerCommand{"stop": {Kind: domain.WorkerCommandStop}}
				}
				state.Attempts["assignment-1"] = current
				state.Sequence++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			_ = f.runtime.finishCollection("assignment-1", before, flight)
			if record := f.record(t); record.Phase == PhaseFailed || record.Failure != "" {
				t.Fatalf("stale failure decided current: %+v", record)
			}
			if f.runtime.collectionRegistered(before) {
				t.Fatal("discarded flight retained")
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending != nil {
				t.Fatalf("stale failed custody: %v", err)
			}
		})
	}
}

type collectionGateProcess struct {
	process *collectionCountingProcess
	started chan struct{}
	release chan struct{}
}

func (p *collectionGateProcess) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	close(p.started)
	select {
	case <-p.release:
		return p.process.Run(ctx, request)
	case <-ctx.Done():
		return backlog.ProcessResult{}, ctx.Err()
	}
}
func (*collectionGateProcess) Kill(string) error { return nil }

func TestPermanentCollectionFailureRealAsyncStaleFlight(t *testing.T) {
	for _, change := range []string{"cancelled", "superseded", "stop-requested"} {
		t.Run(change, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 1400, 100)
			gate := &collectionGateProcess{process: f.process, started: make(chan struct{}), release: make(chan struct{})}
			f.driver.Finalizer.Processes = gate
			before := f.record(t)
			ctx := withCollectionPass(context.Background(), nil)
			if err := f.runtime.collect(ctx, "assignment-1"); !errors.Is(err, errCollectionRunning) {
				t.Fatalf("async collection=%v", err)
			}
			select {
			case <-gate.started:
			case <-time.After(5 * time.Second):
				t.Fatal("verification did not start")
			}
			if err := f.runtime.journal.update(func(state *journalState) error {
				current := state.Attempts["assignment-1"]
				switch change {
				case "cancelled":
					current.Phase, current.StopConfirmed = PhaseStopped, true
				case "superseded":
					current.Assignment.Epoch++
				case "stop-requested":
					current.CommandRequests = map[string]domain.WorkerCommand{"stop": {Kind: domain.WorkerCommandStop}}
				}
				state.Attempts["assignment-1"] = current
				state.Sequence++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			key := f.runtime.collectionFlightKey(before)
			collectionFlights.Lock()
			flight := collectionFlights.running[key]
			collectionFlights.Unlock()
			close(gate.release)
			select {
			case <-flight.done:
			case <-time.After(5 * time.Second):
				t.Fatal("collection did not finish")
			}
			var permanent *permanentCollectionFailure
			if !errors.As(flight.err, &permanent) {
				t.Fatalf("not actual permanent rejection: %v", flight.err)
			}
			_ = f.runtime.finishCollection("assignment-1", before, flight)
			if current := f.record(t); current.Phase == PhaseFailed || current.Failure != "" {
				t.Fatalf("stale outcome applied: %+v", current)
			}
			if f.runtime.collectionRegistered(before) || f.process.calls != 1 {
				t.Fatal("stale flight leaked or verification repeated")
			}
			f.preserved(t)
		})
	}
}

type collectionQuiescenceDriver struct {
	*LocalDriver
	fail bool
}

func (d *collectionQuiescenceDriver) StopPreparation(context.Context, workerproto.ExecutionPackage) error {
	if d.fail {
		return errors.New("quiescence unproven")
	}
	return nil
}
func TestPermanentCollectionFailureWaitsForQuiescence(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 1400, 100)
	driver := &collectionQuiescenceDriver{LocalDriver: f.driver, fail: true}
	f.runtime.driver = driver
	for i := 0; i < 2; i++ {
		if err := f.runtime.collect(context.Background(), "assignment-1"); err == nil {
			t.Fatal("quiescence refusal hidden")
		}
		record := f.record(t)
		if record.Phase != PhaseCollecting || !f.runtime.collectionRegistered(record) || f.process.calls != 1 {
			t.Fatalf("quiescence bypass: %+v", record)
		}
	}
	driver.fail = false
	_ = f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed || f.process.calls != 1 {
		t.Fatal("failed intent not durable")
	}
}

func TestPermanentCollectionFailureFailedCustodySettlementRetry(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 1400, 100)
	control := &collectionUnsettledControl{recordingT3: f.control, fail: true}
	f.driver.T3 = control
	_ = f.runtime.collect(context.Background(), "assignment-1")
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted || !record.SettlePending || !permanentCollectionIntent(record.Failure) {
		t.Fatalf("failed custody lost: %+v", record)
	}
	f.reopen(t)
	control.fail = false
	if err := f.runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := f.record(t); record.SettlePending || f.process.calls != 1 {
		t.Fatalf("failure reverified: %+v", record)
	}
}

func TestPermanentCollectionFailureAmbiguousCustodyIsTransient(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	// The retained success receipt no longer fits the configured bounds. This is
	// ambiguous existing custody, not a new oversized output rejection.
	f.custody.config.MaxArtifactBytes = 1
	f.custody.config.MaxTotalBytes = 2
	if err := f.driver.Collect(context.Background(), f.pkg, f.workspace); err == nil {
		t.Fatal("ambiguous receipt was ignored")
	} else {
		var permanent *permanentCollectionFailure
		if errors.As(err, &permanent) {
			t.Fatalf("durable result reclassified: %v", err)
		}
	}
	if f.process.calls != 1 || f.record(t).Failure != "" {
		t.Fatal("ambiguous result reverified or reclassified")
	}
}

type collectionCountingProcess struct{ calls int }

func (p *collectionCountingProcess) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	p.calls++
	return successfulProcessRunner{}.Run(ctx, request)
}
func (*collectionCountingProcess) Kill(string) error { return nil }

type collectionUploadOpener struct{ store *CustodyStore }

func (o collectionUploadOpener) OpenWorkerUpload(ctx context.Context, object workerproto.ArtifactObject) (io.ReadCloser, error) {
	return o.store.OpenArtifact(ctx, object)
}

type collectionFixture struct {
	runtime   *Runtime
	driver    *LocalDriver
	custody   *CustodyStore
	process   *collectionCountingProcess
	control   *recordingT3
	pkg       workerproto.ExecutionPackage
	workspace string
	raw       []byte
	root      string
}

func newCollectionFixture(t *testing.T, maxObject, maxTotal int64, outputBytes, archiveBytes int) *collectionFixture {
	t.Helper()
	return newCollectionFixtureWith(t, maxObject, maxTotal, outputBytes, archiveBytes, nil)
}

// newCollectionFixtureWith lets a test shape the package and the workspace
// before the offer is accepted, which is when the journal fixes the package.
func newCollectionFixtureWith(t *testing.T, maxObject, maxTotal int64, outputBytes, archiveBytes int, edit func(pkg *workerproto.ExecutionPackage, driver *LocalDriver, workspace string)) *collectionFixture {
	t.Helper()
	root := t.TempDir()
	t.Cleanup(func() {
		// Finalizer seals its capture. Unseal only this disposable test fixture.
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	custody.config.MaxArtifactBytes, custody.config.MaxTotalBytes = maxObject, maxTotal
	pkg := testPackage()
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
	pkg.Verification = []string{"verify-once"}
	process := &collectionCountingProcess{}
	// The padding sits inside the latest turn, which a completion decision
	// reads, so an archive over the limits cannot be compacted and reaches
	// the real size boundary. thread_archive_bound_test.go covers the
	// archives that can be compacted.
	control := &recordingT3{
		thread:  &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"},
		message: "BACKLOG STATUS: done",
		archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z","padding":"` + strings.Repeat("a", archiveBytes) + `"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
	}
	driver := &LocalDriver{
		Config:    LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Finalizer: backlog.AttemptFinalizer{StorageRoot: filepath.Join(root, "artifacts"), Processes: process, Now: func() time.Time { return runtimeTestNow }},
		Publisher: custody, T3: control, Now: func() time.Time { return runtimeTestNow }, scoped: true,
	}
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte("x"), outputBytes)
	if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(&pkg, driver, workspace)
	}
	journal, err := OpenJournal(filepath.Join(root, "journal"), "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	offer := testOffer(t)
	offer.Package, err = workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseCollecting, "", workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return &collectionFixture{runtime, driver, custody, process, control, pkg, workspace, raw, root}
}

func (f *collectionFixture) reopen(t *testing.T) {
	t.Helper()
	journal, err := OpenJournal(f.runtime.journal.root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	f.runtime, err = New(testConfig(func() time.Time { return runtimeTestNow }), journal, f.driver)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *collectionFixture) record(t *testing.T) AttemptRecord {
	t.Helper()
	state, err := f.runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return state.Attempts["assignment-1"]
}
func (f *collectionFixture) preserved(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.workspace, "answer.txt"))
	if err != nil || !bytes.Equal(raw, f.raw) {
		t.Fatalf("original output changed: %v", err)
	}
	capture := filepath.Join(f.driver.Config.ArtifactRoot, "runs", "run-1", "task-1", "attempt-1", "artifacts", "outputs", "answer.txt")
	raw, err = os.ReadFile(capture)
	if err != nil || !bytes.Equal(raw, f.raw) {
		t.Fatalf("capture changed: %v", err)
	}
	raw, err = os.ReadFile(filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json"))
	var turn collectedTurn
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &turn); err != nil || !bytes.Equal(turn.Archive, f.control.archive) {
		t.Fatalf("raw archive changed: %v", err)
	}
}

func TestPermanentCollectionFailureRealCustodyImport(t *testing.T) {
	for _, test := range []struct {
		name            string
		object, total   int64
		output, archive int
	}{
		{"aggregate", 2048, 2500, 1800, 800},
		{"object", 1024, 4096, 1400, 100},
		{"full-thread-archive", 1024, 4096, 100, 2000},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCollectionFixture(t, test.object, test.total, test.output, test.archive)
			err := f.runtime.collect(context.Background(), "assignment-1")
			record := f.record(t)
			t.Logf("actual first collect err=%v phase=%s verification=%d", err, record.Phase, f.process.calls)
			if record.Phase != PhaseFailed || !strings.Contains(record.Failure, "size") {
				t.Fatalf("want durable permanent failed intent, got phase=%s failure=%q err=%v", record.Phase, record.Failure, err)
			}
			f.reopen(t)
			for i := 0; i < 3; i++ {
				if err := f.runtime.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			record = f.record(t)
			if record.Phase != PhaseCompleted || f.process.calls != 1 {
				t.Fatalf("phase=%s original verification=%d", record.Phase, f.process.calls)
			}
			f.preserved(t)
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatalf("failure custody absent: %v", err)
			}
			if len(pending.Manifest.Objects) != 2 || pending.Manifest.TotalBytes > test.total {
				t.Fatalf("unbounded fallback: %+v", pending.Manifest)
			}
			for _, object := range pending.Manifest.Objects {
				if object.Size > test.object {
					t.Fatalf("oversized fallback: %+v", object)
				}
			}
			ctx := context.Background()
			db, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for epoch := int64(1); epoch < 9; epoch++ {
				if _, err := db.AdvanceCoordinatorEpoch(ctx, epoch); err != nil {
					t.Fatal(err)
				}
			}
			task, attempt := packageRecords(f.pkg, runtimeTestNow)
			attempt.Progress, attempt.Control, attempt.Revision, attempt.AssignmentID = domain.ProgressVerifying, domain.ControlStopped, 3, "assignment-1"
			assignment := record.Assignment
			assignment.State = domain.AssignmentCompleted
			assignment.WorkerEpoch = "worker-1"
			dependent := domain.Task{ID: "dependent", Name: "dependent", WorkflowID: task.WorkflowID, Needs: []string{task.ID}}
			blocked := domain.Attempt{ID: "dependent-attempt", TaskID: dependent.ID, WorkflowRunID: "run-1", Number: 1,
				Revision: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: runtimeTestNow}
			run, err := domain.BindRunSink(domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1",
				Revision: 1, Progress: domain.ProgressActive, CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}, []domain.Task{task, dependent})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
				Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "collection-failure", Class: domain.TaskClassRequired, CreatedAt: runtimeTestNow}},
				WorkflowRuns: []domain.WorkflowRun{run},
				Tasks:        []domain.Task{task, dependent}, Attempts: []domain.Attempt{attempt, blocked}, Assignments: []domain.Assignment{assignment},
			}); err != nil {
				t.Fatal(err)
			}
			importer := backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 9, Store: db,
				Artifacts:        backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "imported"), Catalog: db},
				MaxArtifactBytes: test.object, MaxTotalBytes: test.total, Now: func() time.Time { return runtimeTestNow },
			}
			response := workerproto.ArtifactUploadResponse{Manifest: pending.Manifest, Custody: pending.Custody}
			report, err := importer.Import(ctx, response, collectionUploadOpener{f.custody})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
				!strings.Contains(report.Transition[0].Attempt.Failure, "retained") {
				t.Fatalf("failed import: %+v", report)
			}
			replay, err := importer.Import(ctx, response, collectionUploadOpener{f.custody})
			if err != nil || len(replay.Transition) != 0 {
				t.Fatalf("import replay %+v %v", replay, err)
			}
			if _, err := backlog.ProjectWorkflowRuns(ctx, db, runtimeTestNow.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			records, err := db.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sink := records.WorkflowRuns[0].Sink
			if sink.Progress != domain.ProgressFailed || len(sink.Result.FailedTaskIDs) != 1 ||
				sink.Result.FailedTaskIDs[0] != task.ID || len(sink.Result.SkippedTaskIDs) != 1 ||
				sink.Result.SkippedTaskIDs[0] != dependent.ID || records.Assignments[0].State != domain.AssignmentCompleted {
				t.Fatalf("ordinary lifecycle did not settle failure/dependency/sink: %+v", records)
			}
			f.runtime.config.Now = func() time.Time { return runtimeTestNow.Add(2 * DefaultRetention) }
			if err := f.runtime.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			f.preserved(t)
			t.Logf("AFTER failed import reason=%s; failed sink and skipped dependent; original verification=1, raw output/capture/archive preserved beyond retention, envelope=%d bytes",
				report.Transition[0].Attempt.Failure, pending.Manifest.TotalBytes)
		})
	}
}
