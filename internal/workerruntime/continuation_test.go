package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// continuationTurnDriver is a scripted driver whose stopped turns have an
// identity and whose continuation snapshots go to a real attempt store.
type continuationTurnDriver struct {
	*fakeDriver
	local  *LocalDriver
	turnID string
}

func (d *continuationTurnDriver) ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error) {
	return backlog.DispatchThreadStopped, d.turnID, nil
}

func (d *continuationTurnDriver) RecordContinuation(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string, boundary domain.ContinuationBoundary, turn string) (*domain.ContinuationCheckpoint, error) {
	return d.local.RecordContinuation(ctx, pkg, workspace, boundary, turn)
}

func writeContinuation(t *testing.T, workspace, content string) {
	t.Helper()
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, domain.ContinuationFileName), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func continuationDriver(t *testing.T) (*LocalDriver, workerproto.ExecutionPackage, string) {
	t.Helper()
	pkg := testPackage()
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, Now: func() time.Time { return runtimeTestNow }}
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return driver, pkg, workspace
}

// An absent continuation.md is not an error: there is simply no checkpoint.
func TestContinuationSnapshotOfAMissingFileIsNoCheckpoint(t *testing.T) {
	driver, pkg, workspace := continuationDriver(t)
	checkpoint, err := driver.RecordContinuation(context.Background(), pkg, workspace, domain.ContinuationTurnEnd, "turn-1")
	if err != nil || checkpoint != nil {
		t.Fatalf("missing file: checkpoint=%+v err=%v", checkpoint, err)
	}
	latest, data, err := driver.LatestContinuation(pkg)
	if err != nil || latest != nil || data != nil {
		t.Fatalf("latest after a missing file: %+v %q %v", latest, data, err)
	}
	// A symlink is not the task's file and is treated as absent.
	outside := filepath.Join(t.TempDir(), "secret.md")
	if err := os.WriteFile(outside, []byte("not yours"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, domain.ContinuationFileName)); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := driver.RecordContinuation(context.Background(), pkg, workspace, domain.ContinuationTurnEnd, "turn-2"); err != nil || checkpoint != nil {
		t.Fatalf("symlinked file: checkpoint=%+v err=%v", checkpoint, err)
	}
}

// Snapshots are idempotent per (attempt, turn): a replay of a turn already
// recorded, before or after a restart, neither duplicates the snapshot nor
// moves the latest one back to an older turn.
func TestContinuationSnapshotIsIdempotentPerTurnAndNeverRegresses(t *testing.T) {
	ctx := context.Background()
	driver, pkg, workspace := continuationDriver(t)
	writeContinuation(t, workspace, "step 1\n")
	first, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-1")
	if err != nil || first == nil || first.Sequence != 1 || first.Turn != "turn-1" || first.Boundary != domain.ContinuationTurnEnd ||
		first.Size != int64(len("step 1\n")) || first.OriginalSize != first.Size || first.Truncated || first.SHA256 == "" ||
		!first.CapturedAt.Equal(runtimeTestNow) || first.AttemptID != pkg.Identity.AttemptID {
		t.Fatalf("first snapshot = %+v err=%v", first, err)
	}
	writeContinuation(t, workspace, "step 2\n")
	replayed, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationCollection, "turn-1")
	if err != nil || replayed == nil || *replayed != *first {
		t.Fatalf("replay of turn-1 = %+v err=%v, want %+v", replayed, err, first)
	}
	second, err := driver.RecordContinuation(ctx, pkg, workspace, domain.ContinuationPause, "turn-2")
	if err != nil || second == nil || second.Sequence != 2 || second.Turn != "turn-2" || second.Boundary != domain.ContinuationPause {
		t.Fatalf("second snapshot = %+v err=%v", second, err)
	}
	// A worker restart builds a new driver over the same attempt directory and
	// replays the older turn: the latest snapshot stays the newer one.
	restarted := &LocalDriver{Config: driver.Config, Now: func() time.Time { return runtimeTestNow.Add(time.Hour) }}
	writeContinuation(t, workspace, "step 3\n")
	if replay, err := restarted.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-1"); err != nil || replay == nil || *replay != *second {
		t.Fatalf("restart replay of turn-1 = %+v err=%v, want %+v", replay, err, second)
	}
	latest, data, err := restarted.LatestContinuation(pkg)
	if err != nil || latest == nil || *latest != *second || string(data) != "step 2\n" {
		t.Fatalf("latest after restart = %+v %q %v", latest, data, err)
	}
	// Unchanged content at a new turn is not a new snapshot.
	writeContinuation(t, workspace, "step 2\n")
	if same, err := restarted.RecordContinuation(ctx, pkg, workspace, domain.ContinuationTurnEnd, "turn-3"); err != nil || same == nil || *same != *second {
		t.Fatalf("unchanged content at turn-3 = %+v err=%v", same, err)
	}
	entries, err := os.ReadDir(filepath.Join(driver.workspacePath(pkg), "continuation"))
	if err != nil {
		t.Fatal(err)
	}
	snapshots := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".md") {
			snapshots++
		}
	}
	if snapshots != 2 {
		t.Fatalf("stored %d snapshot bodies, want 2 (one per distinct content)", snapshots)
	}
}

func TestContinuationSnapshotTruncatesAnOversizedFile(t *testing.T) {
	driver, pkg, workspace := continuationDriver(t)
	original := strings.Repeat("x", domain.ContinuationSnapshotLimit+4096)
	writeContinuation(t, workspace, original)
	checkpoint, err := driver.RecordContinuation(context.Background(), pkg, workspace, domain.ContinuationTurnEnd, "turn-1")
	if err != nil || checkpoint == nil {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	if !checkpoint.Truncated || checkpoint.OriginalSize != int64(len(original)) || checkpoint.Size > domain.ContinuationSnapshotLimit {
		t.Fatalf("oversized checkpoint = %+v", checkpoint)
	}
	_, data, err := driver.LatestContinuation(pkg)
	if err != nil || int64(len(data)) != checkpoint.Size || !strings.HasSuffix(string(data), domain.ContinuationTruncationMarker(int64(len(original)))) {
		t.Fatalf("stored snapshot len=%d err=%v", len(data), err)
	}
}

// The snapshot is kept beside the workspace, never inside the task's tree, so
// a task that commits everything it sees cannot commit a snapshot.
func TestContinuationSnapshotIsNeverWrittenIntoTheTaskGitTree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	driver, pkg, workspace := continuationDriver(t)
	if output, err := exec.Command("git", "-C", workspace, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, output)
	}
	writeContinuation(t, workspace, "step 1\n")
	if _, err := driver.RecordContinuation(context.Background(), pkg, workspace, domain.ContinuationCollection, "turn-1"); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("git", "-C", workspace, "status", "--porcelain", "--untracked-files=all", "--ignored").CombinedOutput()
	if err != nil {
		t.Fatalf("git status: %v %s", err, output)
	}
	if strings.TrimSpace(string(output)) != "?? "+domain.ContinuationFileName {
		t.Fatalf("the snapshot reached the task's tree:\n%s", output)
	}
	store := filepath.Join(driver.workspacePath(pkg), "continuation")
	if relative, err := filepath.Rel(workspace, store); err != nil || !strings.HasPrefix(relative, "..") {
		t.Fatalf("snapshot store %s is inside the workspace %s", store, workspace)
	}
}

// Turn end: a turn that parks on a task-bound wait is snapshotted and the
// journal names the checkpoint; a restart replaying the same turn adds nothing.
func TestRuntimeSnapshotsContinuationAtTurnEnd(t *testing.T) {
	root := t.TempDir()
	live := true
	workspace := filepath.Join(root, "workspace")
	base := &fakeDriver{workspace: workspace, workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Now: func() time.Time { return runtimeTestNow }}
	driver := &continuationTurnDriver{fakeDriver: base, local: local, turnID: "turn-1"}
	runtime := waitingRuntime(t, root, base, &live, nil, func() time.Time { return runtimeTestNow })
	runtime.driver = driver
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{testOffer(t)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	writeContinuation(t, workspace, "parked at step 2\n")
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := mustRecord(t, runtime, "assignment-1")
	if record.Phase != PhaseWaiting || record.Continuation == nil || record.Continuation.Boundary != domain.ContinuationTurnEnd ||
		record.Continuation.Turn != "turn-1" || record.Continuation.Sequence != 1 {
		t.Fatalf("turn-end record = phase %q continuation %+v", record.Phase, record.Continuation)
	}
	// The journal a restarted worker reopens still names the checkpoint, and
	// the replayed turn does not take a second snapshot.
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(runtime.config, journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	writeContinuation(t, workspace, "edited after the turn\n")
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := mustRecord(t, restarted, "assignment-1"); record.Continuation == nil || record.Continuation.Sequence != 1 || record.Continuation.Turn != "turn-1" {
		t.Fatalf("replayed turn changed the checkpoint: %+v", record.Continuation)
	}
	encoded, err := json.Marshal(mustRecord(t, restarted, "assignment-1"))
	if err != nil || !bytes.Contains(encoded, []byte(`"continuation"`)) {
		t.Fatalf("journal record does not carry the checkpoint: %s %v", encoded, err)
	}
}

// Pause: the worker's own quota pause snapshots continuation.md once the
// drained turn has stopped.
func TestRuntimeSnapshotsContinuationAtQuotaPause(t *testing.T) {
	now := runtimeTestNow
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	base := &fakeDriver{workspace: workspace, workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	local := &LocalDriver{Config: LocalDriverConfig{RunsRoot: filepath.Join(root, "runs")}, Now: func() time.Time { return now }}
	driver := &continuationTurnDriver{fakeDriver: base, local: local, turnID: "turn-drained"}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, base, guard, &now)
	runtime.driver = driver
	writeContinuation(t, workspace, "checkpoint before the pause\n")
	// The drain notice is asynchronous: the first pass sends it, the second
	// observes the stopped turn and records the pause.
	for range 2 {
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	record := journalRecord(t, runtime)
	if record.LocalThrottle == nil || record.LocalThrottle.StoppedAt == nil {
		t.Fatalf("the pause did not take effect: %+v", record)
	}
	if record.Continuation == nil || record.Continuation.Boundary != domain.ContinuationPause || record.Continuation.Turn != "turn-drained" {
		t.Fatalf("pause checkpoint = %+v", record.Continuation)
	}
	if base.collectCalls != 0 {
		t.Fatalf("a paused attempt was collected: %d", base.collectCalls)
	}
}

func collectingDriver(t *testing.T, publisher *recordingPublisher, control *recordingT3) (*LocalDriver, string) {
	t.Helper()
	root := t.TempDir()
	// Finalized artifacts are read-only; make them removable again.
	t.Cleanup(func() { _ = removeReadOnlyTree(root) })
	driver := &LocalDriver{
		Config:    LocalDriverConfig{RunsRoot: filepath.Join(root, "runs"), ArtifactRoot: filepath.Join(root, "artifacts")},
		Finalizer: backlog.AttemptFinalizer{StorageRoot: filepath.Join(root, "artifacts"), Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
		Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
	}
	pkg := testPackage()
	workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return driver, workspace
}

const finishedTurnArchive = `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`

// Collection: the result carries the snapshot when the coordinator declared it
// accepts one, and never otherwise, because an older coordinator rejects a
// result with an object it does not know.
func TestCollectionSnapshotsContinuationAndPublishesItWhenAccepted(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		publisher := &recordingPublisher{}
		control := &recordingT3{
			thread:  &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"},
			message: "done\nBACKLOG STATUS: done", archive: []byte(finishedTurnArchive),
		}
		driver, workspace := collectingDriver(t, publisher, control)
		pkg := testPackage()
		if accepted {
			pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityContinuationCheckpoint}
		}
		writeContinuation(t, workspace, "finished; nothing left\n")
		if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
			t.Fatal(err)
		}
		if len(publisher.results) != 1 {
			t.Fatalf("results = %d", len(publisher.results))
		}
		latest, _, err := driver.LatestContinuation(pkg)
		if err != nil || latest == nil || latest.Boundary != domain.ContinuationCollection || latest.Turn != "turn-1" {
			t.Fatalf("collection snapshot = %+v err=%v", latest, err)
		}
		published := publisher.results[0].Continuation
		if !accepted {
			if published != nil {
				t.Fatalf("a coordinator that did not accept the snapshot was sent one: %+v", published.Checkpoint)
			}
			continue
		}
		if published == nil || published.Checkpoint != *latest || string(published.Data) != "finished; nothing left\n" {
			t.Fatalf("published continuation = %+v", published)
		}
	}
}

// A failed attempt still hands its latest checkpoint on, including one taken at
// an earlier turn end.
func TestFailedCollectionPublishesTheLatestCheckpoint(t *testing.T) {
	publisher := &recordingPublisher{}
	control := &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnID: "turn-1", TurnState: "completed"}, archive: []byte(finishedTurnArchive)}
	driver, workspace := collectingDriver(t, publisher, control)
	pkg := testPackage()
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityContinuationCheckpoint}
	writeContinuation(t, workspace, "half done\n")
	if _, err := driver.RecordContinuation(context.Background(), pkg, workspace, domain.ContinuationTurnEnd, "turn-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(workspace); err != nil {
		t.Fatal(err)
	}
	if err := driver.CollectFailure(context.Background(), pkg, workspace, "workspace is missing; outputs cannot be collected"); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 || publisher.results[0].Continuation == nil ||
		publisher.results[0].Continuation.Checkpoint.Boundary != domain.ContinuationTurnEnd || string(publisher.results[0].Continuation.Data) != "half done\n" {
		t.Fatalf("failed result = %+v", publisher.results)
	}
}

func TestResultUploadCarriesTheContinuationSnapshotAndItsMetadata(t *testing.T) {
	pkg := testPackage()
	data := []byte("next: run the gate\n")
	checkpoint := domain.ContinuationCheckpoint{AttemptID: pkg.Identity.AttemptID, Sequence: 3, Turn: "turn-3", Boundary: domain.ContinuationCollection,
		SHA256: sha256Hex(data), Size: int64(len(data)), OriginalSize: int64(len(data)), CapturedAt: runtimeTestNow}
	objects, err := resultObjects(pkg, PublishedResult{FinalMessage: "done", ThreadArchive: []byte("{}"), Continuation: &ContinuationSnapshot{Checkpoint: checkpoint, Data: data}})
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]resultObject{}
	for _, object := range objects {
		byPath[object.object.Path] = object
	}
	snapshot, ok := byPath["results/"+domain.ContinuationArtifactName]
	if !ok || snapshot.object.ID != domain.ContinuationArtifactID(pkg.Identity.AttemptID) || snapshot.object.Kind != string(domain.ArtifactCheckpoint) ||
		snapshot.object.MediaType != "text/markdown" || snapshot.object.SHA256 != checkpoint.SHA256 {
		t.Fatalf("snapshot object = %+v", snapshot.object)
	}
	metadata, ok := byPath["results/"+domain.ContinuationMetadataArtifactName]
	if !ok || metadata.object.ID != domain.ContinuationMetadataArtifactID(pkg.Identity.AttemptID) || metadata.object.MediaType != "application/json" {
		t.Fatalf("metadata object = %+v", metadata.object)
	}
	var decoded domain.ContinuationCheckpoint
	if err := json.Unmarshal(metadata.data, &decoded); err != nil || decoded != checkpoint {
		t.Fatalf("metadata = %+v err=%v", decoded, err)
	}
}

// The next attempt is told, in one sentence, where the previous checkpoint is.
func TestRetryPromptNamesThePreviousCheckpoint(t *testing.T) {
	pkg := testPackage()
	pkg.Continuation = &workerproto.ContinuationInput{
		Path: workerproto.ContinuationInputPath, AttemptID: "attempt-0", Size: 42, CapturedAt: runtimeTestNow,
	}
	root := t.TempDir()
	control := &recordingT3{projectID: "project-uuid"}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root}, T3: control}
	cachePath := filepath.Join(root, "objects", pkg.Prompt.SHA256)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := driver.CreateThread(context.Background(), pkg, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	prompt := control.created[0].Prompt
	if !strings.Contains(prompt, ".t3/inputs/continuation/previous.md") || !strings.Contains(prompt, "continuation.md") {
		t.Fatalf("prompt does not name the previous checkpoint:\n%s", prompt)
	}
	pkg.Continuation = nil
	control.created = nil
	if err := driver.CreateThread(context.Background(), pkg, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(control.created[0].Prompt, "previous.md") {
		t.Fatal("a first attempt was told about a checkpoint it does not have")
	}
}
