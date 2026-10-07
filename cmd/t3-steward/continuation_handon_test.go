package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// handOnWorker is a failed attempt whose assignment on worker normandy was
// released, with the uploads that worker can hold for it.
type handOnWorker struct {
	store      *sqlite.Store
	now        time.Time
	task       domain.Task
	attempt    domain.Attempt
	assignment domain.Assignment
}

func newHandOnWorker(t *testing.T) handOnWorker {
	t.Helper()
	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "task"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1", State: domain.AssignmentReleased, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	return handOnWorker{store: store, now: now, task: task, attempt: attempt, assignment: assignment}
}

func (w handOnWorker) upload(t *testing.T, id string, objects []workerproto.ArtifactObject, raw []byte) *pendingUpload {
	t.Helper()
	var total int64
	for _, object := range objects {
		total += object.Size
	}
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: id, Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: w.assignment.ID, AssignmentEpoch: w.assignment.Epoch, Objects: objects, TotalBytes: total, CreatedAt: w.now.Add(time.Minute), ExpiresAt: w.now.Add(time.Hour)}
	return &pendingUpload{upload: workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: coordinatorResultCustody(t, manifest)}, raw: raw}
}

// blocker is a throttle checkpoint upload, which the hand-on leaves pending.
func (w handOnWorker) blocker(t *testing.T, n int) *pendingUpload {
	t.Helper()
	raw := []byte("resume here\n")
	id := fmt.Sprintf("checkpoint-attempt-1-%08x", n)
	return w.upload(t, fmt.Sprintf("upload-assignment-1-checkpoint-%08x", n), []workerproto.ArtifactObject{
		coordinatorResultObject(id, "checkpoints/"+id+".md", "checkpoint", "text/markdown", raw),
	}, raw)
}

// snapshot is the attempt's live continuation.md snapshot of one sequence.
func (w handOnWorker) snapshot(t *testing.T, sequence int64) (*pendingUpload, string) {
	t.Helper()
	body := []byte(fmt.Sprintf("step %d\n", sequence))
	snapshotID, metadataID := domain.ContinuationLiveArtifactID(w.attempt.ID, 1, sequence), domain.ContinuationLiveMetadataArtifactID(w.attempt.ID, 1, sequence)
	object := coordinatorResultObject(snapshotID, "checkpoints/"+snapshotID+".md", "checkpoint", "text/markdown", body)
	metadata, err := json.Marshal(domain.ContinuationCheckpoint{AttemptID: w.attempt.ID, Sequence: sequence, Turn: fmt.Sprintf("turn-%d", sequence), Boundary: domain.ContinuationTurnEnd,
		SHA256: object.SHA256, Size: object.Size, OriginalSize: object.Size, CapturedAt: w.now})
	if err != nil {
		t.Fatal(err)
	}
	return w.upload(t, fmt.Sprintf("upload-assignment-1-checkpoint-continuation-%020d-%020d", 1, sequence), []workerproto.ArtifactObject{
		object,
		coordinatorResultObject(metadataID, "checkpoints/"+metadataID+".json", "checkpoint", "application/json", metadata),
	}, bytes.Join([][]byte{body, metadata}, nil)), snapshotID
}

func (w handOnWorker) session(t *testing.T, control *queuedUploadControl) coordinatorWorkerSession {
	t.Helper()
	return coordinatorWorkerSession{
		Client:             coordinatorResultClient(t, w.now, "exchange-control", control),
		ArtifactClient:     coordinatorResultClient(t, w.now, "exchange-artifact", queuedUploadArtifacts{control: control}),
		CheckpointImporter: backlog.CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: w.store, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: w.store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return w.now.Add(3 * time.Minute) }},
	}
}

func (w handOnWorker) latest(t *testing.T) *domain.Artifact {
	t.Helper()
	records, err := w.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return backlog.LatestContinuationArtifact(records.Artifacts, records.Attempts, w.attempt.WorkflowRunID, w.task.ID)
}

// Review round 4, R6: the replacement is offered on a worker polled before
// the reachable worker that holds its predecessor's snapshot. Tick imports
// every worker's snapshots before any exchange builds an offer.
func TestTheTickHandsOnEveryWorkersSnapshotsBeforeAnyOffer(t *testing.T) {
	ctx := context.Background()
	w := newHandOnWorker(t)
	continuation, snapshotID := w.snapshot(t, 1)
	control := &queuedUploadControl{uploads: []*pendingUpload{w.blocker(t, 0), continuation}}
	session := w.session(t, control)
	var order []string
	sessions := &coordinatorWorkerSessions{
		// The destination sorts first.
		workerIDs: []string{"anvil", "normandy"},
		handOn: func(ctx context.Context, workerID string) (backlog.WorkerExchangeReport, error) {
			order = append(order, "hand-on "+workerID)
			if workerID == "anvil" {
				return backlog.WorkerExchangeReport{}, nil
			}
			return handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, 1024)
		},
		exchange: func(ctx context.Context, workerID string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			order = append(order, "exchange "+workerID)
			if workerID == "anvil" {
				// The destination builds the replacement's first offer.
				if latest := w.latest(t); latest == nil || latest.ID != snapshotID {
					t.Errorf("the destination offered before the source's snapshot was imported: %+v", latest)
				}
				return backlog.WorkerExchangeReport{}, nil
			}
			return exchangeCoordinatorWorker(ctx, session, 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
				return backlog.WorkerExchangeReport{}, nil
			})
		},
	}
	report := sessions.Tick(ctx, backlog.QuotaBridgeReport{})
	for _, result := range report.Results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if want := []string{"hand-on anvil", "hand-on normandy", "exchange anvil", "exchange normandy"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if !continuation.acked || len(report.Results) != 2 || len(report.Results[1].Report.Checkpoints) != 1 || report.Results[1].Report.Checkpoints[0].ID != snapshotID {
		t.Fatalf("results = %+v, acknowledged %t", report.Results, continuation.acked)
	}
}

// countingResolver fails every credential resolution and counts them.
type countingResolver struct{ calls map[string]int }

func (r *countingResolver) ResolveProtocol(_ context.Context, reference string) (workerruntime.ProtocolCredentials, error) {
	r.calls[reference]++
	return workerruntime.ProtocolCredentials{}, errors.New("host unreachable")
}

// Self-review of round 4: the hand-on phase opens each worker's session
// before the exchanges, so a worker that cannot be opened must not be opened
// a second time for its exchange in the same pass, and its exchange reports
// the error.
func TestAWorkerThatCannotBeOpenedIsTriedOncePerPass(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolver := &countingResolver{calls: map[string]int{}}
	sessions, err := newCoordinatorWorkerSessions(cfg.BacklogV2, store, 7, resolver, nil, backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 2; pass++ {
		report := sessions.Tick(context.Background(), backlog.QuotaBridgeReport{})
		if len(report.Results) == 0 {
			t.Fatal("no workers configured")
		}
		for _, result := range report.Results {
			if result.Err == nil || !strings.Contains(result.Err.Error(), "host unreachable") {
				t.Fatalf("pass %d, worker %s: %v", pass, result.WorkerID, result.Err)
			}
		}
		total := 0
		for _, calls := range resolver.calls {
			total += calls
		}
		if total != pass*len(report.Results) {
			t.Fatalf("pass %d: %d session opens for %d workers", pass, total, len(report.Results))
		}
	}
}

// Review round 4, R7, under the flat contract: the scan bound and a snapshot
// that cannot be imported yet never hold anything. A snapshot behind 32 other
// uploads, and the latest of more than 32 snapshots, are imported before
// offers; a snapshot past the bound, or one that cannot be fetched, is left in
// the worker's custody, the pass goes on, and a later pass imports it and
// makes it the task's latest.
func TestTheHandOnIsNeverSettledByTheScanBound(t *testing.T) {
	ctx := context.Background()
	t.Run("snapshot behind 32 other uploads", func(t *testing.T) {
		w := newHandOnWorker(t)
		control := &queuedUploadControl{}
		for n := 0; n < 32; n++ {
			control.uploads = append(control.uploads, w.blocker(t, n))
		}
		continuation, snapshotID := w.snapshot(t, 1)
		control.uploads = append(control.uploads, continuation)
		_, err := exchangeCoordinatorWorker(ctx, w.session(t, control), 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
			if latest := w.latest(t); latest == nil || latest.ID != snapshotID {
				t.Errorf("offers were built before the queued snapshot was imported: %+v", latest)
			}
			return backlog.WorkerExchangeReport{}, nil
		})
		if err != nil || !continuation.acked {
			t.Fatalf("exchange = %v, acknowledged %t", err, continuation.acked)
		}
	})
	t.Run("more than 32 live snapshots", func(t *testing.T) {
		w := newHandOnWorker(t)
		control := &queuedUploadControl{}
		var latestID string
		for sequence := int64(1); sequence <= 34; sequence++ {
			var continuation *pendingUpload
			continuation, latestID = w.snapshot(t, sequence)
			control.uploads = append(control.uploads, continuation)
		}
		_, err := exchangeCoordinatorWorker(ctx, w.session(t, control), 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
			if latest := w.latest(t); latest == nil || latest.ID != latestID {
				t.Errorf("offers were built from an older snapshot: %+v, want %s", latest, latestID)
			}
			return backlog.WorkerExchangeReport{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	// passes runs Tick over one worker until the pass with the given number
	// and reports the task's latest snapshot after each pass. Every exchange
	// runs, and none reports an error; nothing is held.
	passes := func(t *testing.T, w handOnWorker, control *queuedUploadControl, count int) []string {
		t.Helper()
		session := w.session(t, control)
		exchanged := 0
		sessions := &coordinatorWorkerSessions{
			workerIDs: []string{"normandy"},
			handOn: func(ctx context.Context, _ string) (backlog.WorkerExchangeReport, error) {
				return handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, 1024)
			},
			exchange: func(ctx context.Context, _ string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
				exchanged++
				return exchangeCoordinatorWorker(ctx, session, 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
					return backlog.WorkerExchangeReport{}, nil
				})
			},
		}
		var latest []string
		for pass := 1; pass <= count; pass++ {
			if report := sessions.Tick(ctx, backlog.QuotaBridgeReport{}); len(report.Results) != 1 || report.Results[0].Err != nil {
				t.Fatalf("pass %d: report = %+v", pass, report)
			}
			if exchanged != pass {
				t.Fatalf("pass %d: %d exchanges; the pass waited", pass, exchanged)
			}
			id := ""
			if artifact := w.latest(t); artifact != nil {
				id = artifact.ID
			}
			latest = append(latest, id)
		}
		return latest
	}
	t.Run("snapshot past the scan bound", func(t *testing.T) {
		w := newHandOnWorker(t)
		earlier, earlierID := w.snapshot(t, 1)
		control := &queuedUploadControl{uploads: []*pendingUpload{earlier}}
		for n := 0; n < continuationHandOnRounds; n++ {
			control.uploads = append(control.uploads, w.blocker(t, n))
		}
		continuation, snapshotID := w.snapshot(t, 2)
		control.uploads = append(control.uploads, continuation)
		latest := passes(t, w, control, 2)
		// The first pass imports the earlier snapshot and leaves the one past
		// the bound in custody; the ordinary pass after reconcile takes one
		// throttle checkpoint off the queue, so the next hand-on reaches it.
		if latest[0] != earlierID {
			t.Fatalf("after pass 1 latest = %q, want %q; the fixture no longer reaches the bound", latest[0], earlierID)
		}
		if latest[1] != snapshotID || !continuation.acked {
			t.Fatalf("after pass 2 latest = %q, want %q (acknowledged %t)", latest[1], snapshotID, continuation.acked)
		}
	})
	// Self-review of round 4: a queue of exactly the bound is drained in one
	// scan, not cut short.
	t.Run("queue of exactly the scan bound", func(t *testing.T) {
		w := newHandOnWorker(t)
		control := &queuedUploadControl{}
		for n := 0; n < continuationHandOnRounds-1; n++ {
			control.uploads = append(control.uploads, w.blocker(t, n))
		}
		continuation, snapshotID := w.snapshot(t, 1)
		control.uploads = append(control.uploads, continuation)
		report, err := handOnCoordinatorWorkerContinuations(ctx, w.session(t, control), backlog.WorkerExchangeReport{}, 1024)
		if err != nil || len(report.Checkpoints) != 1 || report.Checkpoints[0].ID != snapshotID || !continuation.acked {
			t.Fatalf("a queue of %d uploads: %+v, %v", continuationHandOnRounds, report.Checkpoints, err)
		}
	})
	t.Run("snapshot that cannot be fetched yet", func(t *testing.T) {
		w := newHandOnWorker(t)
		broken, snapshotID := w.snapshot(t, 1)
		body := broken.raw
		broken.raw = []byte("not the announced bytes")
		control := &queuedUploadControl{uploads: []*pendingUpload{broken}}
		if latest := passes(t, w, control, 1); latest[0] != "" || broken.acked {
			t.Fatalf("a snapshot that could not be fetched was imported: %q", latest[0])
		}
		broken.raw = body
		if latest := passes(t, w, control, 1); latest[0] != snapshotID || !broken.acked {
			t.Fatalf("the repaired snapshot was not imported by the next pass: %q", latest[0])
		}
	})
}

// Review of the simplification, R1: uploads at the head of a worker's
// custody that cannot be fetched never stop the passes from reaching what lies
// behind them. Each pass takes at least one upload it can handle off the first
// continuationHandOnRounds, if there is one, so a valid snapshot past the scan bound is
// imported within a bounded number of passes, and no exchange waits for it.
func TestAnUnfetchableHeadNeverStarvesASnapshotPastTheScanBound(t *testing.T) {
	ctx := context.Background()
	for _, unavailable := range []int{1, 33} {
		t.Run(fmt.Sprintf("%d unavailable at the head", unavailable), func(t *testing.T) {
			w := newHandOnWorker(t)
			control := &queuedUploadControl{}
			for n := 0; n < continuationHandOnRounds; n++ {
				blocker := w.blocker(t, n)
				if n < unavailable {
					// The announced bytes are gone from the worker's custody.
					blocker.raw = []byte("unavailable")
				}
				control.uploads = append(control.uploads, blocker)
			}
			continuation, snapshotID := w.snapshot(t, 1)
			control.uploads = append(control.uploads, continuation)
			session := w.session(t, control)
			sessions := &coordinatorWorkerSessions{
				workerIDs: []string{"normandy"},
				handOn: func(ctx context.Context, _ string) (backlog.WorkerExchangeReport, error) {
					return handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, 1024)
				},
				exchange: func(ctx context.Context, _ string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					return exchangeCoordinatorWorker(ctx, session, 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
						return backlog.WorkerExchangeReport{}, nil
					})
				},
			}
			for pass := 1; pass <= 3; pass++ {
				report := sessions.Tick(ctx, backlog.QuotaBridgeReport{})
				if len(report.Results) != 1 || report.Results[0].Err != nil {
					t.Fatalf("pass %d: report = %+v", pass, report)
				}
			}
			if latest := w.latest(t); latest == nil || latest.ID != snapshotID || !continuation.acked {
				t.Fatalf("the snapshot behind %d unavailable uploads was not imported after 3 passes: latest = %+v, acknowledged %t", unavailable, latest, continuation.acked)
			}
			for n, upload := range control.uploads[:unavailable] {
				if upload.acked {
					t.Fatalf("unavailable upload %d was acknowledged; it must stay in custody", n)
				}
			}
		})
	}
}

// brokenStreamArtifacts fails the artifact transport itself, as a worker
// whose send of an upload's body dies partway does, for the named uploads.
type brokenStreamArtifacts struct {
	queuedUploadArtifacts
	broken map[string]bool
}

func (b brokenStreamArtifacts) RoundTripArtifactWithRetry(ctx context.Context, request workerproto.Envelope, policy workerproto.RetryPolicy, limit int64) (workerproto.Envelope, []byte, error) {
	var download workerproto.ArtifactDownloadRequest
	if err := json.Unmarshal(request.Payload, &download); err != nil {
		return workerproto.Envelope{}, nil, err
	}
	if b.broken[download.ManifestID] {
		return workerproto.Envelope{}, nil, errors.New("ssh transport: remote exchange: exit status 1: artifact send: short object")
	}
	return b.queuedUploadArtifacts.RoundTripArtifactWithRetry(ctx, request, policy, limit)
}

// Self-review of the R1 fix: a fetch that fails in the artifact transport
// leaves that session's artifact client unusable. The scan must report it,
// so the exchange fails and the session is replaced, and the next sessions
// must step over that upload, so a valid snapshot behind an upload whose
// body the worker can never send is still imported.
func TestATransportFailureReplacesTheSessionAndIsSteppedOver(t *testing.T) {
	ctx := context.Background()
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "transient", true: "permanent"}[permanent], func(t *testing.T) {
			w := newHandOnWorker(t)
			// Two uploads at the head whose bodies the worker cannot send.
			first, _ := w.snapshot(t, 1)
			second := w.blocker(t, 0)
			continuation, snapshotID := w.snapshot(t, 2)
			control := &queuedUploadControl{uploads: []*pendingUpload{first, second, continuation}}
			broken := map[string]bool{first.upload.Manifest.ID: true, second.upload.Manifest.ID: true}
			sessions, failures := brokenTransportSessions(t, w, control, broken)
			for pass := 1; pass <= 3; pass++ {
				sessions.Tick(ctx, backlog.QuotaBridgeReport{})
				if pass == 1 && !permanent {
					clear(broken)
				}
			}
			if *failures == 0 {
				t.Fatal("a transport failure was never reported by the exchange, so its session was never replaced")
			}
			if latest := w.latest(t); latest == nil || latest.ID != snapshotID || !continuation.acked {
				t.Fatalf("the snapshot behind uploads the worker cannot send was not imported after 3 passes: latest = %+v, acknowledged %t", latest, continuation.acked)
			}
			if permanent && (first.acked || second.acked) {
				t.Fatal("an upload that was never fetched was acknowledged")
			}
			if !permanent && (!first.acked || !second.acked) {
				t.Fatalf("uploads whose transport recovered were not imported: %t %t", first.acked, second.acked)
			}
		})
	}
	// Self-review of the fix above: the hand-on steps over every other
	// upload, so it must not clear the list the ordinary pass needs to get past
	// a broken head; a throttle checkpoint behind one is imported.
	for _, snapshotHead := range []bool{false, true} {
		t.Run(map[bool]string{false: "checkpoint behind a broken checkpoint", true: "checkpoint behind a broken snapshot"}[snapshotHead], func(t *testing.T) {
			w := newHandOnWorker(t)
			head := w.blocker(t, 9)
			if snapshotHead {
				head, _ = w.snapshot(t, 1)
			}
			behind := w.blocker(t, 0)
			control := &queuedUploadControl{uploads: []*pendingUpload{head, behind}}
			sessions, failures := brokenTransportSessions(t, w, control, map[string]bool{head.upload.Manifest.ID: true})
			for pass := 1; pass <= 3; pass++ {
				sessions.Tick(ctx, backlog.QuotaBridgeReport{})
			}
			if !behind.acked || head.acked {
				t.Fatalf("checkpoint behind the broken head acknowledged %t after 3 passes, %d failed exchanges", behind.acked, *failures)
			}
		})
	}
}

// brokenTransportSessions runs one worker as production does: one cached
// session, replaced after any hand-on or exchange error, whose artifact
// transport fails for the named uploads. It counts the failed exchanges.
func brokenTransportSessions(t *testing.T, w handOnWorker, control *queuedUploadControl, broken map[string]bool) (*coordinatorWorkerSessions, *int) {
	t.Helper()
	skips := &checkpointScanSkips{}
	opened := 0
	open := func() coordinatorWorkerSession {
		opened++
		session := w.session(t, control)
		session.ArtifactClient = coordinatorResultClient(t, w.now, fmt.Sprintf("exchange-artifact-%d", opened), brokenStreamArtifacts{queuedUploadArtifacts: queuedUploadArtifacts{control: control}, broken: broken})
		session.CheckpointSkips = skips
		return session
	}
	session := open()
	failures := 0
	return &coordinatorWorkerSessions{
		workerIDs: []string{"normandy"},
		handOn: func(ctx context.Context, _ string) (backlog.WorkerExchangeReport, error) {
			report, err := handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, 1024)
			if err != nil {
				session = open()
			}
			return report, err
		},
		exchange: func(ctx context.Context, _ string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			report, err := exchangeCoordinatorWorker(ctx, session, 1024, func(context.Context) (backlog.WorkerExchangeReport, error) {
				return backlog.WorkerExchangeReport{}, nil
			})
			if err != nil {
				failures++
				session = open()
			}
			return report, err
		},
	}, &failures
}

// Review of the simplification, R2: only a worker removed from the
// configuration is no longer polled. A worker whose persistent connection is
// emptied stays in every pass on the slot-only SSH route.
func TestAWorkerWithAnEmptiedConnectionIsStillPolledOverSSH(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	kept := ""
	for id := range cfg.BacklogV2.Workers {
		if kept == "" || id < kept {
			kept = id
		}
	}
	if kept == "" {
		t.Fatal("no workers configured")
	}
	for id, worker := range cfg.BacklogV2.Workers {
		if id != kept {
			delete(cfg.BacklogV2.Workers, id)
			continue
		}
		worker.Connection = ""
		cfg.BacklogV2.Workers[id] = worker
	}
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolver := &countingResolver{calls: map[string]int{}}
	sessions, err := newCoordinatorWorkerSessions(cfg.BacklogV2, store, 7, resolver, nil, backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store})
	if err != nil {
		t.Fatal(err)
	}
	report := sessions.Tick(context.Background(), backlog.QuotaBridgeReport{})
	if len(report.Results) != 1 || report.Results[0].WorkerID != kept || report.Results[0].Err == nil {
		t.Fatalf("the worker with an emptied connection was not polled: %+v", report.Results)
	}
	total := 0
	for _, calls := range resolver.calls {
		total += calls
	}
	if total != 1 {
		t.Fatalf("session opens = %v, want one for %s", resolver.calls, kept)
	}
}
