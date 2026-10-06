package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Review round 1 of M16-6 found four ways the checkpoint contract could lose
// or regress a snapshot. Each test below is one of them.

// R1: a turn-end snapshot reaches the coordinator while the attempt still
// runs, so an attempt that is superseded before it publishes any result has
// already handed its latest checkpoint on. The upload rides the checkpoint
// channel under the attempt's own identity and the snapshot's sequence.
func TestTurnEndCheckpointIsHandedToTheCoordinatorWhileTheAttemptRuns(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	custody := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return runtimeTestNow })
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Publisher: custody, Now: func() time.Time { return runtimeTestNow }}
	pkg := testPackage()
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityContinuationCheckpoint}
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	writeContinuation(t, workspace, "step 2 of 4\n")

	checkpoint, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-1")
	if err != nil || checkpoint == nil {
		t.Fatalf("turn-end snapshot = %+v, %v", checkpoint, err)
	}
	upload, err := custody.PendingUploadByPurpose("checkpoint")
	if err != nil || upload == nil {
		t.Fatalf("the turn-end snapshot was not handed to the coordinator: %+v, %v", upload, err)
	}
	objects := upload.Manifest.Objects
	if len(objects) != 2 || objects[0].ID != "continuation-attempt-1-1" || objects[1].ID != "continuation-meta-attempt-1-1" ||
		objects[0].SHA256 != checkpoint.SHA256 || objects[0].Size != checkpoint.Size || objects[0].Kind != string(domain.ArtifactCheckpoint) {
		t.Fatalf("live checkpoint upload = %+v", objects)
	}
	if upload.Manifest.AssignmentID != pkg.Identity.AssignmentID || upload.Manifest.AssignmentEpoch != pkg.Identity.AssignmentEpoch {
		t.Fatalf("live checkpoint upload is not fenced to the assignment: %+v", upload.Manifest)
	}
	metadata, err := os.ReadFile(custody.objectPath(objects[1].SHA256))
	if err != nil {
		t.Fatal(err)
	}
	var described domain.ContinuationCheckpoint
	if err := json.Unmarshal(metadata, &described); err != nil || described != *checkpoint {
		t.Fatalf("live checkpoint metadata = %s (%v)", metadata, err)
	}

	// Once imported, a replay of the turn after a restart hands nothing on again.
	if err := custody.AcknowledgeUpload(upload.Manifest.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-1"); err != nil {
		t.Fatal(err)
	}
	if again, err := custody.PendingUploadByPurpose("checkpoint"); err != nil || again != nil {
		t.Fatalf("a replayed turn uploaded its snapshot again: %+v, %v", again, err)
	}

	// A newer snapshot is handed on under its own sequence.
	writeContinuation(t, workspace, "step 3 of 4\n")
	if _, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationPause, "turn-2"); err != nil {
		t.Fatal(err)
	}
	newer, err := custody.PendingUploadByPurpose("checkpoint")
	if err != nil || newer == nil || newer.Manifest.Objects[0].ID != "continuation-attempt-1-2" {
		t.Fatalf("the newer snapshot was not handed on: %+v, %v", newer, err)
	}

	// A coordinator that did not declare the contract is never sent one.
	plainRoot := t.TempDir()
	plainCustody := testCustodyStore(t, filepath.Join(plainRoot, "custody"), func() time.Time { return runtimeTestNow })
	plain := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(plainRoot, "runs")}, Publisher: plainCustody, Now: func() time.Time { return runtimeTestNow }}
	plainPkg := testPackage()
	plainWorkspace := filepath.Join(plain.workspacePath(plainPkg), "workspace")
	writeContinuation(t, plainWorkspace, "step 1\n")
	if _, err := plain.RecordContinuation(ctx, plainPkg, plainWorkspace, domain.ContinuationTurnEnd, "turn-1"); err != nil {
		t.Fatal(err)
	}
	if sent, err := plainCustody.PendingUploadByPurpose("checkpoint"); err != nil || sent != nil {
		t.Fatalf("a coordinator without the contract was sent a live checkpoint: %+v, %v", sent, err)
	}
}

// flakyContinuationDriver fails the first snapshots it is asked for, as a
// worker that dies or hits an I/O error right after a pause became durable.
type flakyContinuationDriver struct {
	*continuationTurnDriver
	failures int
}

func (d *flakyContinuationDriver) RecordContinuation(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string, boundary domain.ContinuationBoundary, turn string) (*domain.ContinuationCheckpoint, error) {
	if d.failures > 0 {
		d.failures--
		return nil, errors.New("no space left on device")
	}
	return d.continuationTurnDriver.RecordContinuation(ctx, pkg, workspace, boundary, turn)
}

// R2, coordinator or operator pause: the stop is durable (the acknowledgement
// is journaled and the attempt is stopped) but the worker dies before the
// pause snapshot is taken. Reconciling the restarted worker takes it.
func TestThrottlePauseSnapshotIsTakenAfterARestartAtTheDurableStop(t *testing.T) {
	now := runtimeTestNow
	root := t.TempDir()
	base := &fakeDriver{workspace: root, workspaceReady: true}
	r := runningRuntime(t, base, nil, &now)
	local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, Now: func() time.Time { return now }}
	r.driver = &continuationTurnDriver{fakeDriver: base, local: local, turnID: "paused-turn"}
	writeContinuation(t, root, "checkpoint at pause")
	record := journalRecord(t, r)
	command := r.localThrottleCommand(record, LocalThrottleRequest{Kind: domain.ThrottleCommandHardStop, RequestedAt: now})
	// The exact durable boundary in executeThrottle: finishThrottle has
	// succeeded and the snapshot has not been taken yet.
	if _, err := r.finishThrottle(command, true, domain.ThrottleResultStopped, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkpoint, _, err := local.LatestContinuation(record.Package.Package)
	if err != nil || checkpoint == nil || checkpoint.Boundary != domain.ContinuationPause || checkpoint.Turn != "paused-turn" {
		t.Fatalf("the paused attempt was reconciled without its pause snapshot: %+v, %v", checkpoint, err)
	}
	if record := journalRecord(t, r); record.Continuation == nil || record.Continuation.Sequence != checkpoint.Sequence {
		t.Fatalf("the repaired snapshot is not journaled: %+v", record.Continuation)
	}
}

// R2, the worker's own quota pause: the stopped pause is durable, the snapshot
// fails once, and the next reconcile pass takes it before anything resumes.
func TestQuotaPauseSnapshotIsRetriedAfterItFailedAtTheDurableStop(t *testing.T) {
	now := runtimeTestNow
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	base := &fakeDriver{workspace: workspace, workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Now: func() time.Time { return now }}
	driver := &flakyContinuationDriver{continuationTurnDriver: &continuationTurnDriver{fakeDriver: base, local: local, turnID: "turn-drained"}, failures: 1}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, base, guard, &now)
	runtime.driver = driver
	writeContinuation(t, workspace, "checkpoint before the pause\n")
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if record.LocalThrottle == nil || record.LocalThrottle.StoppedAt == nil || record.Continuation != nil {
		t.Fatalf("setup: the pause should be durable without its snapshot: %+v", record)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record = journalRecord(t, runtime)
	if record.Continuation == nil || record.Continuation.Boundary != domain.ContinuationPause || record.Continuation.Turn != "turn-drained" {
		t.Fatalf("the pause snapshot was never taken: %+v", record.Continuation)
	}
	if base.collectCalls != 0 {
		t.Fatalf("a paused attempt was collected: %d", base.collectCalls)
	}
}

// A pause snapshot that still cannot be taken when the attempt resumes is
// forgone durably before the resume, never taken later from the next turn
// and passed off as the paused one. The resume itself is not held back.
func TestAResumeNeverLeavesAPauseSnapshotOwedToTheNextTurn(t *testing.T) {
	ctx := context.Background()
	t.Run("throttle", func(t *testing.T) {
		now := runtimeTestNow
		root := t.TempDir()
		base := &fakeDriver{workspace: root, workspaceReady: true}
		r := runningRuntime(t, base, nil, &now)
		local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, Now: func() time.Time { return now }}
		turns := &continuationTurnDriver{fakeDriver: base, local: local, turnID: "paused-turn"}
		driver := &flakyContinuationDriver{continuationTurnDriver: turns, failures: 2}
		r.driver = driver
		writeContinuation(t, root, "at the pause")
		record := journalRecord(t, r)
		stop := r.localThrottleCommand(record, LocalThrottleRequest{Kind: domain.ThrottleCommandHardStop, RequestedAt: now})
		if ack, err := r.deliverThrottle(ctx, stop); err != nil || !ack.Accepted {
			t.Fatalf("stop = %+v, %v", ack, err)
		}
		if record := journalRecord(t, r); record.PendingContinuation == nil {
			t.Fatal("setup: the failed pause snapshot is not owed")
		}
		resume := r.localThrottleCommand(journalRecord(t, r), LocalThrottleRequest{Kind: domain.ThrottleCommandResume, RequestedAt: now.Add(time.Minute)})
		if ack, err := r.deliverThrottle(ctx, resume); err != nil || !ack.Accepted || base.resumeCalls != 1 {
			t.Fatalf("resume = %+v, %v (resumes %d)", ack, err, base.resumeCalls)
		}
		if record := journalRecord(t, r); record.PendingContinuation != nil {
			t.Fatalf("the pause snapshot is still owed in the next turn: %+v", record.PendingContinuation)
		}
		turns.turnID = "next-turn"
		writeContinuation(t, root, "in the middle of the next turn")
		if err := r.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if latest, _, err := local.LatestContinuation(record.Package.Package); err != nil || (latest != nil && latest.Boundary == domain.ContinuationPause) {
			t.Fatalf("the next turn was passed off as the pause: %+v, %v", latest, err)
		}
	})
	t.Run("quota", func(t *testing.T) {
		now := runtimeTestNow
		root := t.TempDir()
		workspace := filepath.Join(root, "workspace")
		base := &fakeDriver{workspace: workspace, workspaceReady: true,
			observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
		local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Now: func() time.Time { return now }}
		driver := &flakyContinuationDriver{continuationTurnDriver: &continuationTurnDriver{fakeDriver: base, local: local, turnID: "turn-drained"}, failures: 1 << 10}
		guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
		runtime := runningRuntime(t, base, guard, &now)
		runtime.driver = driver
		writeContinuation(t, workspace, "checkpoint before the pause\n")
		if err := runtime.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if record := journalRecord(t, runtime); record.LocalThrottle == nil || record.PendingContinuation == nil {
			t.Fatalf("setup: the pause should owe its snapshot: %+v", record)
		}
		guard.pauseNeeded, guard.resumeOK, guard.resumeWhy = false, true, "window reset"
		if err := runtime.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		record := journalRecord(t, runtime)
		if base.resumeCalls != 1 || record.LocalThrottle != nil {
			t.Fatalf("a failing snapshot held the resume back: resumes %d, pause %+v", base.resumeCalls, record.LocalThrottle)
		}
		if record.PendingContinuation != nil {
			t.Fatalf("the pause snapshot is still owed in the next turn: %+v", record.PendingContinuation)
		}
	})
}

// R3: a replay of a turn the store considered long ago, after many later
// turns, never takes a new snapshot or moves the latest back to that turn.
func TestContinuationReplayOfALongForgottenTurnNeverRegresses(t *testing.T) {
	ctx := context.Background()
	driver, pkg, workspace := continuationDriver(t)
	writeContinuation(t, workspace, "step 255\n")
	if _, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-255"); err != nil {
		t.Fatal(err)
	}
	// The store has considered 256 turns, as a long task's would; seeding
	// them keeps the test fast.
	dir := driver.continuationDir(pkg)
	state, err := loadContinuationState(dir)
	if err != nil {
		t.Fatal(err)
	}
	state.Turns = state.Turns[:0]
	for turn := range 256 {
		state.Turns = append(state.Turns, fmt.Sprintf("turn-%d", turn))
	}
	if err := saveContinuationState(dir, state); err != nil {
		t.Fatal(err)
	}
	writeContinuation(t, workspace, "step 256\n")
	before, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-256")
	if err != nil || before == nil || before.Turn != "turn-256" {
		t.Fatalf("turn 256 = %+v, %v", before, err)
	}

	writeContinuation(t, workspace, "replayed old content\n")
	// The replay arrives after a worker restart: a new driver over the same store.
	restarted := &LocalDriver{Config: driver.Config, Now: driver.Now}
	after, err := restarted.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-0")
	if err != nil || after == nil || *after != *before {
		t.Fatalf("the replay of turn 0 changed the latest: before %+v after %+v (%v)", before, after, err)
	}
	if latest, data, err := restarted.LatestContinuation(pkg); err != nil || *latest != *before || string(data) != "step 256\n" {
		t.Fatalf("latest after the replay = %+v %q %v", latest, data, err)
	}
}

// limitedPublisher holds a result to the package's limits exactly as the
// production upload validation does.
type limitedPublisher struct{ recordingPublisher }

func (p *limitedPublisher) PublishResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	planned, err := resultObjects(pkg, result)
	if err != nil {
		return err
	}
	objects := make([]workerproto.ArtifactObject, 0, len(planned))
	for _, entry := range planned {
		objects = append(objects, entry.object)
	}
	if _, err := workerproto.ValidateUploadObjects(objects, pkg.Limits.MaxArtifactBytes, pkg.Limits.MaxTotalBytes); err != nil {
		return err
	}
	return p.recordingPublisher.PublishResult(ctx, pkg, result)
}

// R4: a checkpoint that does not fit the failed result's upload, by itself or
// in total, is left out; the failure is still published with its reason.
func TestFailedResultIsPublishedWithoutACheckpointThatDoesNotFit(t *testing.T) {
	for _, limits := range []struct {
		name          string
		object, total int64
		size          int
	}{
		{name: "object", object: 1024, total: 4096, size: 2048},
		{name: "total", object: 4096, total: 4096, size: 3900},
	} {
		t.Run(limits.name, func(t *testing.T) {
			publisher := &limitedPublisher{}
			control := &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"}, archive: []byte(finishedTurnArchive)}
			driver, workspace := collectingDriver(t, &publisher.recordingPublisher, control)
			driver.Publisher = publisher
			pkg := testPackage()
			pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityContinuationCheckpoint}
			pkg.Limits.MaxArtifactBytes, pkg.Limits.MaxTotalBytes = limits.object, limits.total
			writeContinuation(t, workspace, strings.Repeat("x", limits.size))
			if err := driver.CollectFailure(context.Background(), pkg, workspace, "provider thread vanished"); err != nil {
				t.Fatalf("an optional checkpoint prevented the failure's publication: %v", err)
			}
			if len(publisher.results) != 1 || publisher.results[0].Continuation != nil ||
				!strings.Contains(publisher.results[0].FinalMessage, "provider thread vanished") {
				t.Fatalf("failed result = %+v", publisher.results)
			}
			// The snapshot itself is still kept beside the attempt.
			if latest, _, err := driver.LatestContinuation(pkg); err != nil || latest == nil || latest.Size != int64(limits.size) {
				t.Fatalf("latest = %+v, %v", latest, err)
			}
		})
	}
}
