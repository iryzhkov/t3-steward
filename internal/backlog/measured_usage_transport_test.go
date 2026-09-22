package backlog_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var causalUsageNow = time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)

type causalAuthorizer struct{}

func (causalAuthorizer) Authorize(context.Context, backlogadmin.Principal, backlogadmin.Action) error {
	return nil
}

type causalOfferBuilder struct{}

func (causalOfferBuilder) BuildAssignmentOffer(_ context.Context, assignment domain.Assignment, expiresAt time.Time) (workerproto.AssignmentOffer, error) {
	object := workerproto.ArtifactObject{
		ID: "prompt-" + assignment.ID, Path: "prompt/task.md", Kind: "prompt", MediaType: "text/markdown",
		Size: 1, SHA256: strings.Repeat("a", 64),
	}
	pkg := workerproto.ExecutionPackage{
		Version: workerproto.ExecutionPackageVersion, ID: "package-" + assignment.ID,
		CoordinatorID: "coordinator", CoordinatorEpoch: 1,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: "workflow", WorkflowRunID: "run", TaskID: "task",
			AttemptID: assignment.AttemptID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			DispatchToken: assignment.DispatchToken, ThreadID: assignment.ThreadID,
		},
		Class: domain.TaskClassRequired, Prompt: object, Route: assignment.Route,
		Environment: workerproto.EnvironmentReference{
			CatalogRevision: "catalog", Project: "project", Repository: "ssh://git/repository",
			Ref: "main", Scope: "task", SetupProfile: "setup", T3Project: "project",
		},
		Verification: []string{"true"},
		Limits: workerproto.ExecutionLimits{
			MaxTurns: 2, PrepareTimeout: time.Minute, VerificationTimeout: time.Minute,
			MaxArtifactBytes: 1024, MaxTotalBytes: 2048,
		},
		CreatedAt: causalUsageNow,
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	return workerproto.AssignmentOffer{Assignment: assignment, Package: manifest, ExpiresAt: expiresAt}, err
}

type signedUsageWorker struct {
	store        *sqlite.Store
	server       *workerproto.Server
	snapshot     domain.WorkerSnapshot
	claimed      map[string]domain.Assignment
	spoof        func(*domain.WorkerSnapshot)
	snapshotSeq  int64
	snapshotHits int
}

func newSignedUsageWorker(t *testing.T, workerID, workerEpoch string, store *sqlite.Store) *signedUsageWorker {
	t.Helper()
	worker := &signedUsageWorker{
		store: store,
		snapshot: domain.WorkerSnapshot{
			WorkerID: workerID, WorkerEpoch: workerEpoch, CoordinatorEpoch: 1, Connected: true,
			Inventory: domain.WorkerInventory{
				ID: workerID, Epoch: workerEpoch, AcceptBacklog: true, Health: domain.WorkerHealthReady,
				Projects: []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: causalUsageNow}},
				Providers: []domain.WorkerProviderInventory{
					{InstanceID: "claude-agent", Models: []string{"model"}, Available: true},
					{InstanceID: "codex-primary", Models: []string{"model"}, Available: true},
				},
				ObservedAt: causalUsageNow,
			},
			ObservedAt: causalUsageNow, ValidUntil: causalUsageNow.Add(time.Hour),
		},
		claimed: make(map[string]domain.Assignment),
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: workerID, CoordinatorEpoch: 1, WorkerEpoch: workerEpoch,
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: []byte("coordinator-secret"),
		SignerPrincipal: "ssh:" + workerID, SignerKeyID: "worker-key", SignerSecret: []byte("worker-response-secret"),
		Allowed: map[workerproto.MessageType]bool{
			workerproto.MessageSnapshot: true, workerproto.MessageOffers: true,
			workerproto.MessageCommands: true, workerproto.MessageLeaseRenewals: true,
		},
		Now: func() time.Time { return causalUsageNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.server = server
	return worker
}

func (w *signedUsageWorker) handle(ctx context.Context, envelope workerproto.Envelope) (workerproto.MessageType, any, error) {
	switch envelope.Type {
	case workerproto.MessageSnapshot:
		var request workerproto.SnapshotRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageSnapshot, &request); err != nil {
			return "", nil, err
		}
		usage, err := w.store.WorkerUsageBatch(ctx, request.UsageAcknowledgements, workerproto.MaxUsageDelivery)
		if err != nil {
			return "", nil, err
		}
		w.snapshotSeq++
		w.snapshotHits++
		snapshot := w.snapshot
		snapshot.Sequence = w.snapshotSeq
		snapshot.ObservedAt = causalUsageNow.Add(time.Duration(w.snapshotSeq) * time.Second)
		snapshot.ValidUntil = snapshot.ObservedAt.Add(time.Hour)
		snapshot.Inventory.Sequence = snapshot.Sequence
		snapshot.Inventory.ObservedAt = snapshot.ObservedAt
		for _, assignment := range w.claimed {
			snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{
				AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
				State: domain.AssignmentClaimed, Control: domain.ControlPreparing,
				ThreadID: assignment.ThreadID, ObservedAt: snapshot.ObservedAt,
			})
		}
		if w.spoof != nil {
			w.spoof(&snapshot)
		}
		return workerproto.MessageObservations, workerproto.Observations{
			Snapshot: snapshot, Usage: usage,
			AcknowledgedUsageEventIDs: append([]string(nil), request.UsageAcknowledgements...),
		}, nil
	case workerproto.MessageOffers:
		var offered workerproto.AssignmentOffers
		if err := workerproto.DecodePayload(envelope, workerproto.MessageOffers, &offered); err != nil {
			return "", nil, err
		}
		claims := make([]domain.AssignmentClaimRequest, 0, len(offered.Offers))
		for _, offer := range offered.Offers {
			assignment := offer.Assignment
			w.claimed[assignment.ID] = assignment
			claims = append(claims, domain.AssignmentClaimRequest{
				CoordinatorEpoch: 1, WorkerID: w.snapshot.WorkerID, WorkerEpoch: w.snapshot.WorkerEpoch,
				AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken,
				ClaimedAt: causalUsageNow, LeaseExpiresAt: assignment.LeaseExpiresAt,
			})
		}
		return workerproto.MessageClaims, workerproto.AssignmentClaims{Claims: claims}, nil
	case workerproto.MessageCommands:
		var delivery workerproto.CommandDelivery
		if err := workerproto.DecodePayload(envelope, workerproto.MessageCommands, &delivery); err != nil {
			return "", nil, err
		}
		acks := make([]domain.WorkerAcknowledgement, 0, len(delivery.Commands))
		for _, command := range delivery.Commands {
			acks = append(acks, domain.WorkerAcknowledgement{
				CommandID: command.ID, WorkerID: command.WorkerID, WorkerEpoch: command.WorkerEpoch,
				CoordinatorEpoch: command.CoordinatorEpoch, AssignmentID: command.AssignmentID,
				AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: w.snapshotSeq,
				Accepted: true, AcknowledgedAt: causalUsageNow,
			})
		}
		return workerproto.MessageAcknowledgements, workerproto.Acknowledgements{Acknowledgements: acks}, nil
	case workerproto.MessageLeaseRenewals:
		return workerproto.MessageObservations, workerproto.Observations{Snapshot: w.snapshot}, nil
	default:
		return "", nil, errors.New("unexpected signed worker message")
	}
}

type signedLoopback struct {
	worker   *signedUsageWorker
	loseNext bool
}

func (l *signedLoopback) RoundTripWithRetry(ctx context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	response, err := l.worker.server.Handle(ctx, request, l.worker.handle)
	if err != nil {
		return workerproto.Envelope{}, err
	}
	if l.loseNext {
		l.loseNext = false
		return workerproto.Envelope{}, errors.New("simulated lost signed response")
	}
	return response, nil
}

func signedUsageClient(t *testing.T, worker *signedUsageWorker, session string, transport workerproto.RoundTripper) *workerproto.Client {
	t.Helper()
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: "coordinator", WorkerID: worker.snapshot.WorkerID,
		CoordinatorEpoch: 1, WorkerEpoch: worker.snapshot.WorkerEpoch, SessionID: session,
		RequestTimeout: time.Minute, SignerPrincipal: "ssh:coordinator",
		SignerKeyID: "coordinator-key", SignerSecret: []byte("coordinator-secret"),
		RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 1}, Transport: transport,
		Now: func() time.Time { return causalUsageNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestMeasuredUsageSignedReconcileClaimReplayAndPublicQuery(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	workerPath := filepath.Join(root, "worker.db")
	workerStore, err := sqlite.OpenMigrated(workerPath)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorPath := filepath.Join(root, "coordinator.db")
	coordinatorStore, err := sqlite.OpenMigrated(coordinatorPath)
	if err != nil {
		t.Fatal(err)
	}

	parsed := parseUsageFixtures(t)
	roles := []struct {
		name, thread string
		role         domain.ExecutionRole
		activation   *domain.Activation
		incident     *domain.ReviewIncident
	}{
		{name: "executor", thread: parsed[0].ThreadID, role: domain.ExecutionRoleExecutor},
		{name: "repair", thread: "thread-repair", role: domain.ExecutionRoleRepairExecutor,
			activation: &domain.Activation{ID: "activation-repair", RunID: "run", Epoch: 1, Purpose: domain.RecoveryActivationRepair, State: domain.ActivationPendingDispatch, DispatchIdentity: "repair"}},
		{name: "supervisor", thread: "thread-supervisor", role: domain.ExecutionRoleSupervisorActivation,
			activation: &domain.Activation{ID: "activation-supervisor", RunID: "run", Epoch: 1, State: domain.ActivationPendingDispatch, DispatchIdentity: "supervisor"}},
		{name: "gate", thread: parsed[1].ThreadID, role: domain.ExecutionRoleGateReviewer,
			activation: &domain.Activation{ID: "activation-gate", RunID: "run", Epoch: 1, IncidentID: "incident-gate", State: domain.ActivationPendingDispatch, DispatchIdentity: "gate"},
			incident:   &domain.ReviewIncident{ID: "incident-gate", RunID: "run", GateID: "gate", State: domain.IncidentOpen, Revision: 1}},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow", Progress: domain.ProgressActive, Revision: 1}},
	}
	for i, item := range roles {
		attemptID := "attempt-" + item.name
		assignmentID := "assignment-" + item.name
		attempt := domain.Attempt{
			ID: attemptID, WorkflowRunID: "run", TaskID: "task-" + item.name, Number: 1,
			AssignmentID: assignmentID, Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: causalUsageNow,
		}
		if item.activation != nil {
			attempt.SupervisionActivationID = item.activation.ID
			attempt.SupervisionActivationEpoch = item.activation.Epoch
			records.Activations = append(records.Activations, *item.activation)
		}
		if item.incident != nil {
			records.Incidents = append(records.Incidents, *item.incident)
		}
		provider := parsed[i%len(parsed)].ProviderInstanceID
		records.Attempts = append(records.Attempts, attempt)
		records.Assignments = append(records.Assignments, domain.Assignment{
			ID: assignmentID, AttemptID: attemptID, WorkerID: "worker-a", WorkerEpoch: "epoch-a",
			Route: domain.ProviderRoute{WorkerID: "worker-a", ProviderInstanceID: provider, Model: "model", QuotaPoolID: "pool"},
			State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-" + item.name,
			DispatchToken: "dispatch-" + item.name, ThreadID: item.thread,
			LeaseExpiresAt: causalUsageNow.Add(time.Hour), CreatedAt: causalUsageNow, UpdatedAt: causalUsageNow,
		})
	}
	if err := coordinatorStore.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}

	samples := []domain.UsageSample{parsed[0], parsed[1]}
	diagnostic, err := providerlog.ParseUsageEvidenceLine(`[2026-09-22T18:00:00Z] CANON: {"type":"thread.token-usage.updated","eventId":"bad","providerInstanceId":"claude-agent","threadId":"thread-claude","createdAt":"2026-09-22T18:00:00Z","raw":{"method":"future/usage","payload":{"secret":"must-not-survive"}}}`)
	if err != nil || len(diagnostic) != 1 {
		t.Fatalf("diagnostic fixture = %#v, %v", diagnostic, err)
	}
	samples = append(samples, diagnostic[0])
	for i := 1; i < 3; i++ {
		sample := parsed[i%len(parsed)]
		sample.ThreadID = roles[i].thread
		sample.SourceEventID += "-" + roles[i].name
		samples = append(samples, sample)
	}
	unknown := parsed[0]
	unknown.ThreadID, unknown.SourceEventID = "thread-unbound", "event-unbound"
	samples = append(samples, unknown)
	for _, sample := range samples {
		if err := workerStore.RecordUsage(ctx, sample); err != nil {
			t.Fatal(err)
		}
	}

	worker := newSignedUsageWorker(t, "worker-a", "epoch-a", workerStore)
	lost := &signedLoopback{worker: worker, loseNext: true}
	firstClient := signedUsageClient(t, worker, "lost-session", lost)
	coordinator := backlog.FleetCoordinator{Store: coordinatorStore, Now: func() time.Time { return causalUsageNow }}
	_, err = coordinator.ReconcileWorker(ctx, firstClient, causalOfferBuilder{},
		backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
	if err == nil {
		t.Fatal("lost signed response unexpectedly reconciled")
	}
	if err := workerStore.Close(); err != nil {
		t.Fatal(err)
	}
	workerStore, err = sqlite.OpenMigrated(workerPath)
	if err != nil {
		t.Fatal(err)
	}
	worker.store = workerStore
	t.Cleanup(func() { _ = workerStore.Close() })

	client := signedUsageClient(t, worker, "replay-session", &signedLoopback{worker: worker})
	report, err := coordinator.ReconcileWorker(ctx, client, causalOfferBuilder{},
		backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Claimed) != len(roles) {
		t.Fatalf("real claims = %d, want %d: %#v", len(report.Claimed), len(roles), report)
	}
	for _, claimed := range report.Claimed {
		var want domain.ExecutionRole
		for _, item := range roles {
			if claimed.ID == "assignment-"+item.name {
				want = item.role
			}
		}
		if want == "" || claimed.ExecutionRole != want {
			t.Fatalf("claimed role for %s = %q, want %q", claimed.ID, claimed.ExecutionRole, want)
		}
	}
	pending, err := workerStore.WorkerUsageBatch(ctx, nil, workerproto.MaxUsageDelivery)
	if err != nil || len(pending) != 0 {
		t.Fatalf("worker replay settlement pending=%#v err=%v", pending, err)
	}

	otherPath := filepath.Join(root, "worker-b.db")
	otherStore, err := sqlite.OpenMigrated(otherPath)
	if err != nil {
		t.Fatal(err)
	}
	defer otherStore.Close()
	collision := parsed[0]
	if err := otherStore.RecordUsage(ctx, collision); err != nil {
		t.Fatal(err)
	}
	other := newSignedUsageWorker(t, "worker-b", "epoch-b", otherStore)
	otherClient := signedUsageClient(t, other, "other-session", &signedLoopback{worker: other})
	if _, err := coordinator.ReconcileWorker(ctx, otherClient, causalOfferBuilder{},
		backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour); err != nil {
		t.Fatal(err)
	}

	if err := coordinatorStore.Close(); err != nil {
		t.Fatal(err)
	}
	coordinatorStore, err = sqlite.OpenMigrated(coordinatorPath)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinatorStore.Close()
	admin, err := backlogadmin.New(coordinatorStore, causalAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	admin.SetClock(func() time.Time { return causalUsageNow.Add(2 * time.Minute) })
	response, err := admin.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryUsage, WorkflowRunID: "run", UsageRaw: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Usage) != len(roles)+1 {
		t.Fatalf("public attributed usage = %#v", response.Usage)
	}
	gotRoles := make(map[domain.ExecutionRole]bool)
	for _, sample := range response.Usage {
		if sample.WorkerID != "worker-a" || sample.Attribution.WorkerID != "worker-a" {
			t.Fatalf("worker provenance = %#v", sample)
		}
		gotRoles[sample.Attribution.Role] = true
		if strings.Contains(sample.SourceEventID, "must-not-survive") || strings.Contains(sample.DiagnosticCode, "must-not-survive") {
			t.Fatalf("diagnostic leaked content: %#v", sample)
		}
	}
	for _, item := range roles {
		if !gotRoles[item.role] {
			t.Fatalf("public query omitted role %q: %#v", item.role, response.Usage)
		}
	}
	if response.UsageCoverage == nil || response.UsageCoverage.UnscopedUnattributedCount != 2 ||
		response.UsageCoverage.DiagnosticCount != 1 || response.UsageCoverage.UnsupportedCount != 1 ||
		response.UsageCoverage.State != domain.UsageCoveragePartial || response.UsageCoverage.Reason == "" {
		t.Fatalf("unscoped coverage = %#v", response.UsageCoverage)
	}
}

func TestMeasuredUsageRejectsSpoofedSnapshotBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name  string
		spoof func(*domain.WorkerSnapshot)
	}{
		{name: "worker id", spoof: func(s *domain.WorkerSnapshot) { s.WorkerID = "payload-worker" }},
		{name: "worker epoch", spoof: func(s *domain.WorkerSnapshot) { s.WorkerEpoch = "payload-epoch" }},
		{name: "coordinator epoch", spoof: func(s *domain.WorkerSnapshot) { s.CoordinatorEpoch = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			workerStore, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "worker.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer workerStore.Close()
			sample := parseUsageFixtures(t)[0]
			if err := workerStore.RecordUsage(ctx, sample); err != nil {
				t.Fatal(err)
			}
			coordinatorStore, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer coordinatorStore.Close()
			worker := newSignedUsageWorker(t, "worker-auth", "epoch-auth", workerStore)
			worker.spoof = test.spoof
			client := signedUsageClient(t, worker, "spoof-session", &signedLoopback{worker: worker})
			_, err = (backlog.FleetCoordinator{Store: coordinatorStore, Now: func() time.Time { return causalUsageNow }}).
				ReconcileWorker(ctx, client, causalOfferBuilder{}, backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true},
					nil, nil, time.Minute, time.Hour)
			if err == nil {
				t.Fatal("spoofed signed payload was accepted")
			}
			coverage, err := coordinatorStore.AttributedUsage(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			if coverage.Coverage.UnscopedUnattributedCount != 0 {
				t.Fatalf("spoof mutated usage: %#v", coverage)
			}
			acks, err := coordinatorStore.WorkerUsageAcknowledgements(ctx, "worker-auth")
			if err != nil || len(acks) != 0 {
				t.Fatalf("spoof mutated receipts: %#v err=%v", acks, err)
			}
			snapshots, err := coordinatorStore.LoadWorkerSnapshots(ctx)
			if err != nil || len(snapshots) != 0 {
				t.Fatalf("spoof mutated snapshots: %#v err=%v", snapshots, err)
			}
		})
	}
}

func parseUsageFixtures(t *testing.T) []domain.UsageSample {
	t.Helper()
	var samples []domain.UsageSample
	for _, name := range []string{"measured-usage-claude.json", "measured-usage-codex.json"} {
		body, err := os.ReadFile(filepath.Join("..", "source", "providerlog", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := providerlog.ParseUsageJSON(body, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		samples = append(samples, parsed...)
	}
	if len(samples) != 2 {
		t.Fatalf("fixture samples = %#v", samples)
	}
	return samples
}
