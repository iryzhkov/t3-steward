package main

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type exchangeCountingConnector struct {
	snapshotCountingConnector
	decoded *atomic.Int64
}

func (c exchangeCountingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.snapshotCountingConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &exchangeCountingConn{snapshotCountingConn: conn.(*snapshotCountingConn), decoded: c.decoded}, nil
}

type exchangeCountingConn struct {
	*snapshotCountingConn
	decoded *atomic.Int64
}

func (c *exchangeCountingConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.snapshotCountingConn.QueryContext(ctx, q, args)
	if err == nil && len(args) > 0 && args[0].Value == "worker-0" &&
		(strings.HasPrefix(q, "SELECT record FROM coordinator_assignments") || strings.HasPrefix(q, "SELECT record FROM coordinator_attempts")) {
		rows = exchangeCountingRows{Rows: rows, decoded: c.decoded}
	}
	return rows, err
}

type exchangeCountingRows struct {
	driver.Rows
	decoded *atomic.Int64
}

func (r exchangeCountingRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil {
		r.decoded.Add(1)
	}
	return err
}

type exchangePollingControl struct {
	queuedUploadControl
	polls []string
}

func (c *exchangePollingControl) RoundTripWithRetry(ctx context.Context, req workerproto.Envelope, retry workerproto.RetryPolicy) (workerproto.Envelope, error) {
	if req.Type == workerproto.MessageArtifactPoll {
		var p workerproto.ArtifactPollRequest
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			return workerproto.Envelope{}, err
		}
		c.polls = append(c.polls, p.Purpose)
	}
	return c.queuedUploadControl.RoundTripWithRetry(ctx, req, retry)
}

type eventfulExchangeControl struct {
	queuedUploadControl
	result *pendingUpload
	order  *[]string
}

func (c *eventfulExchangeControl) RoundTripWithRetry(ctx context.Context, req workerproto.Envelope, retry workerproto.RetryPolicy) (workerproto.Envelope, error) {
	if req.Type == workerproto.MessageArtifactPoll {
		var p workerproto.ArtifactPollRequest
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			return workerproto.Envelope{}, err
		}
		if p.Purpose == "result" {
			var upload *workerproto.ArtifactUploadResponse
			if !c.result.acked {
				upload = &c.result.upload
			}
			return coordinatorResultResponse(req, workerproto.MessageArtifactAnnouncement, workerproto.ArtifactAnnouncement{Upload: upload})
		}
	}
	if req.Type == workerproto.MessageArtifactAcknowledge {
		var a workerproto.ArtifactAcknowledgeRequest
		if err := json.Unmarshal(req.Payload, &a); err != nil {
			return workerproto.Envelope{}, err
		}
		*c.order = append(*c.order, "ack:"+a.ManifestID)
		if a.ManifestID == c.result.upload.Manifest.ID {
			c.result.acked = true
			return coordinatorResultResponse(req, workerproto.MessageArtifactAcknowledged, workerproto.ArtifactAcknowledgement(a))
		}
	}
	return c.queuedUploadControl.RoundTripWithRetry(ctx, req, retry)
}

type eventfulExchangeArtifacts struct {
	queuedUploadArtifacts
	fail  string
	order *[]string
}

func (a eventfulExchangeArtifacts) RoundTripArtifactWithRetry(ctx context.Context, req workerproto.Envelope, retry workerproto.RetryPolicy, limit int64) (workerproto.Envelope, []byte, error) {
	var d workerproto.ArtifactDownloadRequest
	if err := json.Unmarshal(req.Payload, &d); err != nil {
		return workerproto.Envelope{}, nil, err
	}
	*a.order = append(*a.order, "fetch:"+d.ManifestID)
	if d.ManifestID == a.fail {
		return workerproto.Envelope{}, nil, fmt.Errorf("injected fetch failure: %s", a.fail)
	}
	return a.queuedUploadArtifacts.RoundTripArtifactWithRetry(ctx, req, retry, limit)
}

type eventfulExchangeTransport struct {
	snapshotIdleTransport
	usage    []domain.UsageSample
	requests *[]workerproto.SnapshotRequest
	order    *[]string
}

func (s eventfulExchangeTransport) SnapshotObservations(_ context.Context, req workerproto.SnapshotRequest) (workerproto.Observations, error) {
	*s.requests = append(*s.requests, req)
	*s.order = append(*s.order, "snapshot")
	return workerproto.Observations{Snapshot: s.snapshot, Usage: s.usage}, nil
}

func (s eventfulExchangeTransport) DeliverWorkerCommands(_ context.Context, snapshot domain.WorkerSnapshot, commands []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	var acks []domain.WorkerAcknowledgement
	for _, c := range commands {
		acks = append(acks, domain.WorkerAcknowledgement{CommandID: c.ID, WorkerID: c.WorkerID, WorkerEpoch: c.WorkerEpoch, CoordinatorEpoch: c.CoordinatorEpoch, AssignmentID: c.AssignmentID, AssignmentEpoch: c.AssignmentEpoch, WorkerSequence: snapshot.Sequence + 1, Accepted: true, AcknowledgedAt: snapshot.ObservedAt})
	}
	return acks, nil
}

func TestEventfulScopedExchangeAndRetry(t *testing.T) {
	for _, mode := range []string{"idle-normal", "idle-retry", "wait-normal", "wait-retry"} {
		t.Run(mode, func(t *testing.T) {
			waiting, retry := strings.HasPrefix(mode, "wait"), strings.HasSuffix(mode, "retry")
			ctx := context.Background()
			w := newHandOnWorker(t)
			check := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			var err error
			var wait domain.TaskWait
			if waiting {
				w.attempt.Progress, w.attempt.Control, w.attempt.ThreadID = domain.ProgressActive, domain.ControlRunning, "thread-wait"
				w.assignment.State, w.assignment.ThreadID = domain.AssignmentClaimed, w.attempt.ThreadID
				w.assignment.LeaseExpiresAt = w.now.Add(time.Hour)
				check(w.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{w.attempt}, Assignments: []domain.Assignment{w.assignment}}))
				wait, err = w.store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{RequestID: "wait", WorkflowRunID: w.attempt.WorkflowRunID, TaskID: w.task.ID, AttemptID: w.attempt.ID, IssuedRevision: w.attempt.Revision, ThreadID: w.attempt.ThreadID, Wake: domain.WakeEach, MaxDuration: time.Hour, Condition: "true"}, w.now)
				check(err)
			}
			continuation, continuationID := w.snapshot(t, 1)
			checkpoint := w.blocker(t, 1)
			cpObject := checkpoint.upload.Manifest.Objects[0]
			cpAttempt := w.attempt
			cpAttempt.ID, cpAttempt.TaskID, cpAttempt.AssignmentID = "attempt-cp", "task-cp", "assignment-cp"
			cpAttempt.Progress, cpAttempt.Control, cpAttempt.CheckpointArtifactID = domain.ProgressSucceeded, domain.ControlStopped, cpObject.ID
			cpAssignment := w.assignment
			cpAssignment.ID, cpAssignment.AttemptID, cpAssignment.State, cpAssignment.DispatchToken = cpAttempt.AssignmentID, cpAttempt.ID, domain.AssignmentCompleted, "dispatch-cp"
			checkpoint.upload.Manifest.ID, checkpoint.upload.Manifest.AssignmentID = "upload-assignment-cp-checkpoint", cpAssignment.ID
			checkpoint.upload.Custody = coordinatorResultCustody(t, checkpoint.upload.Manifest)
			resultAttempt, resultAssignment := cpAttempt, cpAssignment
			resultAttempt.ID, resultAttempt.TaskID, resultAttempt.AssignmentID, resultAttempt.ThreadID = "attempt-result", "task-result", "assignment-result", "thread-result"
			resultAttempt.Progress, resultAttempt.CheckpointArtifactID = domain.ProgressVerifying, ""
			resultAssignment.ID, resultAssignment.AttemptID, resultAssignment.ThreadID, resultAssignment.DispatchToken = resultAttempt.AssignmentID, resultAttempt.ID, resultAttempt.ThreadID, "dispatch-result"
			check(w.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
				Tasks:    []domain.Task{{ID: cpAttempt.TaskID, WorkflowID: w.task.WorkflowID, Name: "checkpoint"}, {ID: resultAttempt.TaskID, WorkflowID: w.task.WorkflowID, Name: "result", Outputs: []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}}},
				Attempts: []domain.Attempt{cpAttempt, resultAttempt}, Assignments: []domain.Assignment{cpAssignment, resultAssignment},
			}))
			evidence := domain.ThrottleAttemptRecord{DirectiveID: "directive", AttemptID: cpAttempt.ID, Revision: 1, Delivery: domain.ThrottleDeliveryPending, Control: domain.ControlDraining, Command: domain.ThrottleCommand{ID: "drain", DirectiveID: "directive", AttemptID: cpAttempt.ID, Kind: domain.ThrottleCommandDrain, WorkerID: "normandy", ThreadID: "thread-cp", WorkspacePath: "/workspace", QuotaPoolID: "pool"}, UpdatedAt: w.now}
			check(w.store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 0, Record: evidence}}))
			evidence.Revision, evidence.Delivery, evidence.Control, evidence.Result = 2, domain.ThrottleDeliveryAcknowledged, domain.ControlPaused, domain.ThrottleResultCheckpointed
			evidence.Checkpoint = &domain.CheckpointMetadata{ArtifactID: cpObject.ID, Path: ".t3/checkpoint.md", SHA256: cpObject.SHA256, Size: cpObject.Size, CapturedAt: w.now}
			check(w.store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 1, Record: evidence}}))
			contents := [][]byte{[]byte("answer\n"), []byte("BACKLOG STATUS: done\n"), []byte(`{"thread":{"id":"thread-result","latestTurn":{"turnId":"turn","state":"completed","startedAt":"2026-09-10T16:00:00Z","completedAt":"2026-09-10T16:01:00Z"},"session":{"threadId":"thread-result","status":"ready","activeTurnId":null,"lastError":null}}}`)}
			objects := []workerproto.ArtifactObject{
				coordinatorResultObject("output-result", "results/answer.txt", "output", "text/plain", contents[0]),
				coordinatorResultObject("final-message-attempt-result", "results/final-message.md", "summary", "text/markdown", contents[1]),
				coordinatorResultObject("thread-archive-attempt-result", "results/thread.json", "log", "application/json", contents[2]),
			}
			resultWorker := w
			resultWorker.assignment = resultAssignment
			result := resultWorker.upload(t, "upload-assignment-result-result", objects, bytes.Join(contents, nil))
			var order []string
			var requests []workerproto.SnapshotRequest
			control := &eventfulExchangeControl{queuedUploadControl: queuedUploadControl{uploads: []*pendingUpload{checkpoint, continuation}}, result: result, order: &order}
			all := &queuedUploadControl{uploads: []*pendingUpload{checkpoint, continuation, result}}
			fail := continuation.upload.Manifest.ID
			pass, unaffected := 0, 0
			sessionFor := func() coordinatorWorkerSession {
				s := w.session(t, &control.queuedUploadControl)
				s.Client = coordinatorResultClient(t, w.now, "events-control", control)
				s.ArtifactClient = coordinatorResultClient(t, w.now, "events-artifacts", eventfulExchangeArtifacts{queuedUploadArtifacts{all}, fail, &order})
				s.Records = w.store
				s.Importer = backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: w.store, Artifacts: s.CheckpointImporter.Artifacts, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return w.now.Add(3 * time.Minute) }}
				return s
			}
			sessions := &coordinatorWorkerSessions{workerIDs: []string{"normandy", "unaffected"}}
			sessions.handOn = func(ctx context.Context, id string) (backlog.WorkerExchangeReport, error) {
				if id != "normandy" {
					return backlog.WorkerExchangeReport{}, nil
				}
				return handOnCoordinatorWorkerContinuations(ctx, sessionFor(), backlog.WorkerExchangeReport{}, 1024)
			}
			sessions.exchange = func(ctx context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
				if id != "normandy" {
					unaffected++
					return backlog.WorkerExchangeReport{}, nil
				}
				pass++
				now := w.now.Add(time.Duration(pass) * time.Second)
				snapshot := domain.WorkerSnapshot{WorkerID: id, WorkerEpoch: "worker-1", CoordinatorEpoch: 1, Sequence: int64(pass * 10), QuotaObservations: []domain.WorkerQuotaObservation{{Key: domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: domain.WindowPrimary}, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: float64(pass), ObservedAt: now}}, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now, Capabilities: []string{workerproto.CapabilityQuotaObservations}}}
				transport := eventfulExchangeTransport{snapshotIdleTransport{snapshot: snapshot}, []domain.UsageSample{{ProviderInstanceID: "codex", ThreadID: "thread-result", Model: "model", ObservedAt: now, SourceEventID: fmt.Sprint(pass), Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: int64(pass)}}, &requests, &order}
				return exchangeCoordinatorWorker(ctx, sessionFor(), 1024, func(ctx context.Context) (backlog.WorkerExchangeReport, error) {
					return (backlog.FleetCoordinator{Store: w.store, Now: func() time.Time { return now }}).ReconcileWorker(ctx, transport, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicy{}, nil, nil, time.Minute, time.Hour)
				})
			}
			reconnect := &coordinatorReconnect{workers: sessions}
			var logs bytes.Buffer
			cycle := coordinatorBoundaryCycle{workers: reconnect, logger: slog.New(slog.NewTextHandler(&logs, nil))}
			replay := cycle.retryWorkers
			if !retry {
				replay = cycle.tickWorkersOnly
			}
			cycle.tickWorkersOnly(ctx, backlog.QuotaBridgeReport{})
			if !reconnect.pending || continuation.acked || result.acked || checkpoint.acked {
				t.Fatalf("failed normal pass lost custody: %+v", control)
			}
			fail = result.upload.Manifest.ID
			order = nil
			replay(ctx, backlog.QuotaBridgeReport{})
			if !reconnect.pending || !continuation.acked || result.acked || checkpoint.acked {
				t.Fatal("failed result retry lost custody")
			}
			if got := strings.Join(order, ","); !strings.Contains(got, "ack:"+continuation.upload.Manifest.ID+",snapshot,fetch:"+result.upload.Manifest.ID) {
				t.Fatalf("hand-on/reconcile/result order: %s", got)
			}
			if waiting {
				_, err = w.store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet, Output: "ready"}, w.now.Add(time.Minute))
				check(err)
				wakes, err := w.store.WakeTaskWaits(ctx, w.now.Add(time.Minute))
				check(err)
				if len(wakes) != 1 || wakes[0].AttemptID != w.attempt.ID || !strings.Contains(wakes[0].Prompt(), "ready") {
					t.Fatalf("wake=%+v", wakes)
				}
			}
			fail = checkpoint.upload.Manifest.ID
			order = nil
			replay(ctx, backlog.QuotaBridgeReport{})
			if !reconnect.pending || !result.acked || checkpoint.acked {
				t.Fatalf("failed checkpoint retry: pending=%t result=%t checkpoint=%t logs=%s", reconnect.pending, result.acked, checkpoint.acked, &logs)
			}
			if got := strings.Join(order, ","); !strings.Contains(got, "snapshot,fetch:"+result.upload.Manifest.ID+",ack:"+result.upload.Manifest.ID+",fetch:"+checkpoint.upload.Manifest.ID) {
				t.Fatalf("result/checkpoint order: %s", got)
			}
			fail = ""
			replay(ctx, backlog.QuotaBridgeReport{})
			if reconnect.pending || !checkpoint.acked || pass != 4 || retry && unaffected != 1 || !retry && unaffected != pass {
				t.Fatalf("retry pending=%t passes=%d unaffected=%d", reconnect.pending, pass, unaffected)
			}
			records, err := w.store.LoadCoordinatorRecords(ctx)
			check(err)
			for _, id := range []string{continuationID, cpObject.ID, "output-result"} {
				if !slices.ContainsFunc(records.Artifacts, func(a domain.Artifact) bool { return a.ID == id }) {
					t.Fatalf("artifact %s was acknowledged without publication", id)
				}
			}
			for _, a := range records.Attempts {
				if a.ID == resultAttempt.ID && a.Progress != domain.ProgressSucceeded {
					t.Fatalf("result attempt=%+v", a)
				}
			}
			snapshot, found, err := w.store.LoadWorkerSnapshot(ctx, "normandy")
			check(err)
			if !found || len(snapshot.QuotaObservations) != 1 || snapshot.QuotaObservations[0].UsedPercent != float64(pass) {
				t.Fatalf("durable quota=%+v", snapshot)
			}
			usage, err := w.store.UsageSamples(ctx, w.now, w.now.Add(time.Hour))
			check(err)
			if len(usage) != pass {
				t.Fatalf("usage=%+v", usage)
			}
			for i, req := range requests {
				if !req.ParkedReported || waiting && (len(req.Parked) != 1 || req.Parked[0].WaitID != wait.ID) || !waiting && len(req.Parked) != 0 {
					t.Fatalf("pass %d wait fence=%+v", i, req)
				}
				if i > 0 && (len(req.UsageAcknowledgements) == 0 || !req.QuotaObservationsWanted) {
					t.Fatalf("pass %d lost usage receipt", i)
				}
			}
			if waiting {
				waits, err := w.store.ListTaskWaits(ctx)
				check(err)
				if len(waits) != 1 || !waits[0].Woken() || waits[0].Result.Outcome != domain.TaskWaitMet {
					t.Fatalf("durable wake=%+v", waits)
				}
			}
		})
	}
}

func TestIdleExchangeAddsNoFullLoadsWithPollingAndHistory(t *testing.T) {
	var smallDecode int64
	for _, history := range []int{1, 60} {
		for _, workers := range []int{1, 6} {
			t.Run(fmt.Sprintf("%d-workers-%d-history", workers, history), func(t *testing.T) {
				ctx := context.Background()
				now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
				path := t.TempDir() + "/state.db"
				if err := sqlitetest.Seed(path); err != nil {
					t.Fatal(err)
				}
				loads, decoded := &atomic.Int64{}, &atomic.Int64{}
				store, err := sqlite.OpenMigratedInstrumented(path, func(c driver.Connector) driver.Connector {
					return exchangeCountingConnector{snapshotCountingConnector{Connector: c, loads: loads}, decoded}
				})
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				var records sqlite.CoordinatorRecords
				for i := 0; i <= history; i++ {
					id, worker := fmt.Sprint(i), "unrelated"
					if i == 0 {
						worker = "worker-0"
					}
					records.Assignments = append(records.Assignments, domain.Assignment{ID: id, AttemptID: id, DispatchToken: id, WorkerID: worker, WorkerEpoch: "epoch", Epoch: 1, State: domain.AssignmentReleased, UpdatedAt: now})
					records.Attempts = append(records.Attempts, domain.Attempt{ID: id, TaskID: id, WorkflowRunID: id, Revision: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped})
				}
				if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&logs, nil))
				polls, calls, enrollment := map[string]*exchangePollingControl{}, 0, 0
				sessions := &coordinatorWorkerSessions{enroll: func(context.Context) error { enrollment++; return nil }}
				for i := 0; i < workers; i++ {
					id := fmt.Sprintf("worker-%d", i)
					sessions.workerIDs = append(sessions.workerIDs, id)
					polls[id] = &exchangePollingControl{}
				}
				sessionFor := func(id string) coordinatorWorkerSession {
					session := (handOnWorker{store: store, now: now}).session(t, &polls[id].queuedUploadControl)
					session.Client = coordinatorResultClient(t, now, "exchange-control", polls[id])
					return session
				}
				sessions.handOn = func(ctx context.Context, id string) (backlog.WorkerExchangeReport, error) {
					return handOnCoordinatorWorkerContinuations(ctx, sessionFor(id), backlog.WorkerExchangeReport{}, 1024)
				}
				sessions.exchange = func(ctx context.Context, id string, q backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					calls++
					observed := now.Add(time.Duration(calls) * time.Second)
					snapshot := domain.WorkerSnapshot{WorkerID: id, WorkerEpoch: "epoch", CoordinatorEpoch: 1, Sequence: int64(calls), Connected: true, ObservedAt: observed, ValidUntil: observed.Add(time.Hour), Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: observed, Capabilities: []string{workerproto.CapabilitySessionTitles, workerproto.CapabilityTaskWaitCollectionFence}}}
					return exchangeCoordinatorWorker(ctx, sessionFor(id), 1024, func(ctx context.Context) (backlog.WorkerExchangeReport, error) {
						return (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).ReconcileWorker(ctx, snapshotIdleTransport{snapshot: snapshot}, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicyFromQuotaReport(q), nil, q.Pools, time.Minute, time.Hour)
					})
				}
				quota := backlog.QuotaBridgeReport{Pools: []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: 1}}}
				schedules := 0
				cycle := coordinatorBoundaryCycle{quota: fixedCoordinatorQuotaTicker{quota}, schedules: recordingCoordinatorScheduleTicker{calls: &schedules}, planning: snapshotReadingPlanner{store}, admin: fairnessAdmin{}, workers: sessions, logger: logger}
				// Match state boundaries; only the worker exchange phase differs.
				before := loads.Load()
				cycle.Tick(ctx)
				baseline := loads.Load() - before
				before = loads.Load()
				cycle.TickWithWorkers(ctx)
				if added := loads.Load() - before - baseline; added != 0 {
					t.Fatalf("idle exchange added %d full loads", added)
				}
				// No tick snapshot can mask calls on this independent worker-only pass.
				decoded.Store(0)
				before = loads.Load()
				cycle.tickWorkersOnly(ctx, quota)
				if loads.Load() != before || calls != 2*workers || enrollment != 2 || strings.Contains(logs.String(), "level=ERROR") {
					t.Fatalf("loads=%d calls=%d enrollment=%d logs=%s", loads.Load()-before, calls, enrollment, &logs)
				}
				if history == 1 && workers == 1 {
					smallDecode = decoded.Load()
				}
				if decoded.Load() != smallDecode || smallDecode == 0 {
					t.Fatalf("target decode=%d small=%d", decoded.Load(), smallDecode)
				}
				for id, control := range polls {
					if got := strings.Join(control.polls, ","); got != "checkpoint,checkpoint,result,checkpoint,checkpoint,checkpoint,result,checkpoint" {
						t.Fatalf("%s polling order=%s", id, got)
					}
				}
			})
		}
	}
}
