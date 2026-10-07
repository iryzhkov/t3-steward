package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	coordinatorWorkerRemoteCommand = "worker-exchange"
	// The coordinator still names the operation it wants. A forced command
	// that pinned one accepts only that one, and a forced command that pinned
	// none ignores the client's argument entirely and reads the operation from
	// the signed envelope, so naming it is correct either way.
	coordinatorWorkerControlOperation         = workerruntime.OperationControl
	coordinatorWorkerArtifactSendOperation    = workerruntime.OperationArtifactSend
	coordinatorWorkerArtifactReceiveOperation = workerruntime.OperationArtifactReceive
)

type coordinatorWorkerSession struct {
	Close              func() error
	Client             *workerproto.Client
	ArtifactClient     *workerproto.Client
	Builder            backlog.AssignmentOfferBuilder
	Importer           backlog.CoordinatorResultImporter
	CheckpointImporter backlog.CoordinatorCheckpointImporter
	Binding            workerruntime.WorkerBinding
	Records            backlog.ExecutionPackageRecordStore
	// CheckpointSkips outlives the session: it is the worker's, so the
	// sessions that replace this one step over what broke it.
	CheckpointSkips *checkpointScanSkips
}

// checkpointScanSkips holds the uploads of one worker whose fetch failed in
// the artifact transport, which leaves the session that tried it unusable.
// Every scan of that worker's custody steps over them, so one body the worker
// cannot send costs one session and never hides what lies behind it. A scan
// that ends without such a failure has reached everything it could, and
// clears the list so the next scan tries them again. Nothing waits on it.
type checkpointScanSkips struct {
	mu  sync.Mutex
	ids []string
}

func (s *checkpointScanSkips) excluded() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ids)
}

func (s *checkpointScanSkips) add(id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ids) >= continuationHandOnRounds {
		// Never a longer poll than one scan can build; start over instead.
		s.ids = nil
	}
	if !slices.Contains(s.ids, id) {
		s.ids = append(s.ids, id)
	}
}

func (s *checkpointScanSkips) reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = nil
}

type coordinatorWorkerTickResult struct {
	WorkerID string
	Report   backlog.WorkerExchangeReport
	Err      error
}

type coordinatorWorkerTickReport struct {
	Results []coordinatorWorkerTickResult
}

// coordinatorWorkerSessions delays credential resolution and transport
// construction until a scheduled post-startup cycle. Each pass uses a fresh
// protocol session, so an ambiguous exchange cannot poison a later pass.
type coordinatorWorkerSessions struct {
	close     func() error
	workerIDs []string
	// handOn imports one worker's continuation.md snapshots; Tick runs it for
	// every worker before any exchange. Nothing waits for its outcome.
	handOn   func(context.Context, string) (backlog.WorkerExchangeReport, error)
	exchange func(context.Context, string, backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error)
}

// coordinatorWorkerUnopened is a hand-on failure to open a worker's session.
// Tick reports it as that worker's exchange error instead of dialling the
// worker a second time in the same pass, so an unreachable host costs one
// connection timeout per pass.
type coordinatorWorkerUnopened struct{ err error }

func (e coordinatorWorkerUnopened) Error() string { return e.err.Error() }

func (e coordinatorWorkerUnopened) Unwrap() error { return e.err }

func newCoordinatorWorkerSessions(
	settings config.BacklogV2,
	store *sqlite.Store,
	coordinatorEpoch int64,
	resolver workerruntime.ProtocolCredentialResolver,
	commandFactory workerproto.CommandFactory,
	artifacts backlog.CoordinatorArtifactStore,
) (*coordinatorWorkerSessions, error) {
	if store == nil || coordinatorEpoch < 1 || resolver == nil {
		return nil, fmt.Errorf("coordinator worker sessions require store, authority, and credential resolver")
	}
	var requirements []domain.WorkerRequirement
	for id, w := range settings.Workers {
		if w.Connection != "" {
			b, err := workerruntime.BuildWorkerBinding(settings, id, time.Now())
			if err != nil {
				return nil, err
			}
			requirements = append(requirements, domain.WorkerRequirement{Draining: !w.AcceptBacklog, WorkerID: id, WorkerEpoch: w.Epoch, CatalogRevision: b.CatalogRevision, CredentialRef: w.Credential, Connection: w.Connection})
		}
	}
	if err := store.SaveWorkerRequirements(context.Background(), requirements); err != nil {
		return nil, err
	}
	workerIDs := make([]string, 0, len(settings.Workers))
	for workerID := range settings.Workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	coordinator := backlog.FleetCoordinator{Store: store}
	cached := map[string]coordinatorWorkerSession{}
	skips := make(map[string]*checkpointScanSkips, len(workerIDs))
	for _, workerID := range workerIDs {
		skips[workerID] = &checkpointScanSkips{}
	}
	open := func(ctx context.Context, workerID string) (coordinatorWorkerSession, error) {
		if session, ok := cached[workerID]; ok {
			return session, nil
		}
		sessionID, err := newCoordinatorWorkerSessionID(settings.Coordinator.ID, workerID)
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		session, err := newCoordinatorWorkerSession(ctx, settings, store, workerID, coordinatorEpoch, sessionID, resolver, time.Now().UTC(), commandFactory, artifacts)
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		session.CheckpointSkips = skips[workerID]
		if settings.Workers[workerID].Connection != "" {
			cached[workerID] = session
		}
		return session, nil
	}
	drop := func(workerID string, session coordinatorWorkerSession) {
		if session.Close != nil {
			_ = session.Close()
			delete(cached, workerID)
		}
	}
	return &coordinatorWorkerSessions{
		workerIDs: workerIDs,
		handOn: func(ctx context.Context, workerID string) (backlog.WorkerExchangeReport, error) {
			session, err := open(ctx, workerID)
			if err != nil {
				return backlog.WorkerExchangeReport{}, coordinatorWorkerUnopened{err: err}
			}
			report, err := handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, settings.MessageLimits.MaxArtifactBytes)
			if err != nil {
				drop(workerID, session)
			}
			return report, err
		},
		close: func() error {
			var errs []error
			for id, session := range cached {
				if session.Close != nil {
					errs = append(errs, session.Close())
				}
				delete(cached, id)
			}
			return errors.Join(errs...)
		},
		exchange: func(ctx context.Context, workerID string, quota backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			session, err := open(ctx, workerID)
			if err != nil {
				return backlog.WorkerExchangeReport{}, err
			}
			report, err := exchangeCoordinatorWorker(ctx, session, settings.MessageLimits.MaxArtifactBytes, func(ctx context.Context) (backlog.WorkerExchangeReport, error) {
				return coordinator.ReconcileWorker(
					ctx, session.Client, session.Builder, backlog.WorkerAdmissionPolicyFromQuotaReport(quota), quota.Directives, quota.Pools,
					settings.Leases.RenewInterval.D(), settings.Leases.Duration.D(),
				)
			})
			if err != nil {
				drop(workerID, session)
				return report, err
			}
			return report, nil
		},
	}, nil
}

// exchangeCoordinatorWorker runs one exchange with a worker. The
// continuation.md snapshots the worker holds are imported first, before
// reconcile expires leases and freezes the continuation decision of any
// offer: a snapshot an attempt queued before its lease ended then reaches the
// replacement's first package whenever this worker is reachable. A failure
// of that first pass never holds back the reconcile; the snapshots stay in
// the worker's custody and the pass after reconcile tries them again.
//
// Tick has already run the same import for every worker before the first of
// these exchanges, so a replacement offered on another worker is covered too;
// this pass picks up what arrived since.
func exchangeCoordinatorWorker(ctx context.Context, session coordinatorWorkerSession, maxArtifactBytes int64, reconcile func(context.Context) (backlog.WorkerExchangeReport, error)) (backlog.WorkerExchangeReport, error) {
	handedOn, handOnErr := handOnCoordinatorWorkerContinuations(ctx, session, backlog.WorkerExchangeReport{}, maxArtifactBytes)
	if handOnErr != nil {
		slog.Warn("worker continuation snapshots not imported before offers", "error", handOnErr)
	}
	report, reconcileErr := reconcile(ctx)
	report.Checkpoints = append(handedOn.Checkpoints, report.Checkpoints...)
	report, importErr := importCoordinatorWorkerArtifacts(ctx, session, report, maxArtifactBytes)
	return report, errors.Join(reconcileErr, importErr)
}

func importCoordinatorWorkerArtifacts(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64) (backlog.WorkerExchangeReport, error) {
	report, err := importCoordinatorWorkerResult(ctx, session, report, maxArtifactBytes)
	if err != nil {
		return report, err
	}
	return importCoordinatorWorkerCheckpoint(ctx, session, report, maxArtifactBytes)
}

// resultImportDisposition decides what to do with an announced result before
// its bytes are fetched: import it, wait for the coordinator projection to
// catch up, or discard an upload that belongs to a superseded execution.
type resultImportDisposition int

const (
	resultImportNow resultImportDisposition = iota
	resultImportDefer
	resultImportDiscard
)

func classifyResultImport(records sqlite.CoordinatorRecords, manifest workerproto.ArtifactTransferManifest) (resultImportDisposition, string) {
	for _, assignment := range records.Assignments {
		if assignment.ID != manifest.AssignmentID {
			continue
		}
		if assignment.Epoch != manifest.AssignmentEpoch {
			return resultImportDiscard, fmt.Sprintf("assignment epoch %d superseded by %d", manifest.AssignmentEpoch, assignment.Epoch)
		}
		switch assignment.State {
		case domain.AssignmentCompleted:
			return resultImportNow, ""
		case domain.AssignmentReleased:
			return resultImportDiscard, "assignment was released before its result was imported"
		default:
			return resultImportDefer, fmt.Sprintf("assignment is %q; waiting for the completed projection", assignment.State)
		}
	}
	return resultImportDiscard, "assignment is unknown to the coordinator"
}

func importCoordinatorWorkerResult(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64) (backlog.WorkerExchangeReport, error) {
	if session.Client == nil || session.ArtifactClient == nil || maxArtifactBytes < 1 {
		return report, fmt.Errorf("coordinator worker result import requires control, artifact transport, and a positive limit")
	}
	// Uploads are discovered one at a time. A result that cannot be imported
	// yet is skipped for the rest of this pass so it does not hide the results
	// behind it; discarded uploads are acknowledged and the poll continues.
	var upload *workerproto.ArtifactUploadResponse
	var deferred []string
	for round := 0; round < 32; round++ {
		candidate, err := session.Client.PollArtifact(ctx, "result", deferred...)
		if err != nil || candidate == nil {
			return report, err
		}
		if session.Records == nil {
			upload = candidate
			break
		}
		records, err := session.Records.LoadCoordinatorRecords(ctx)
		if err != nil {
			return report, err
		}
		disposition, reason := classifyResultImport(records, candidate.Manifest)
		if disposition == resultImportDefer {
			slog.Debug("worker result import deferred", "manifest", candidate.Manifest.ID, "reason", reason)
			deferred = append(deferred, candidate.Manifest.ID)
			continue
		}
		if disposition == resultImportDiscard {
			slog.Warn("worker result discarded", "manifest", candidate.Manifest.ID, "reason", reason)
			if err := session.Client.AcknowledgeArtifact(ctx, candidate.Manifest.ID); err != nil {
				return report, err
			}
			continue
		}
		upload = candidate
		break
	}
	if upload == nil {
		return report, nil
	}
	fetched, err := session.ArtifactClient.FetchArtifact(ctx, *upload, maxArtifactBytes, maxArtifactBytes)
	if err != nil {
		return report, err
	}
	imported, err := session.Importer.Import(ctx, fetched.Response, fetched)
	if errors.Is(err, backlog.ErrResultImportSuperseded) {
		slog.Warn("worker result discarded", "manifest", upload.Manifest.ID, "reason", err)
		return report, session.Client.AcknowledgeArtifact(ctx, upload.Manifest.ID)
	}
	if errors.Is(err, backlog.ErrResultImportRejected) {
		// Retrying cannot help, and leaving it unacknowledged blocks every other
		// result this worker holds. It is discarded loudly and once; the attempt
		// itself stays visible as unsettled rather than being quietly completed.
		slog.Error("worker result rejected and discarded", "manifest", upload.Manifest.ID,
			"worker", upload.Manifest.WorkerID, "reason", err)
		return report, session.Client.AcknowledgeArtifact(ctx, upload.Manifest.ID)
	}
	if err != nil {
		return report, err
	}
	report.Imports = append(report.Imports, imported)
	if err := session.Client.AcknowledgeArtifact(ctx, upload.Manifest.ID); err != nil {
		return report, err
	}
	return report, nil
}

// continuationHandOnRounds bounds one scan of a worker's checkpoint custody,
// counted in uploads handled, for the hand-on and the ordinary pass alike. It
// is a fairness bound only: whatever lies past it is imported by a later pass,
// and nothing waits for it.
const continuationHandOnRounds = 256

// handOnCoordinatorWorkerContinuations imports the continuation.md snapshots
// a worker holds and leaves every other upload pending, untouched, for the
// ordinary pass. A snapshot that cannot be fetched or imported yet stays in
// the worker's custody for a later pass. An error means the worker could not
// be polled or acknowledged.
func handOnCoordinatorWorkerContinuations(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64) (backlog.WorkerExchangeReport, error) {
	return scanCoordinatorWorkerCheckpoints(ctx, session, report, maxArtifactBytes, true)
}

// importCoordinatorWorkerCheckpoint imports the worker's pending checkpoint
// uploads after reconcile.
func importCoordinatorWorkerCheckpoint(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64) (backlog.WorkerExchangeReport, error) {
	return scanCoordinatorWorkerCheckpoints(ctx, session, report, maxArtifactBytes, false)
}

// scanCoordinatorWorkerCheckpoints is the one scan of a worker's checkpoint
// custody. Checkpoints are discovered one at a time, like results. One that
// cannot be fetched or imported yet is skipped for the rest of this scan and
// stays in the worker's custody; it never hides what lies behind it. It used
// to fail this worker's whole exchange on every boundary, so its results,
// commands and offers stopped too (S14), and an upload that could not be
// fetched at the head of the queue kept every snapshot past the scan bound
// out of reach for good.
//
// A fetch that fails in the artifact transport is the exception: it leaves
// the session's artifact client unusable, so the scan ends with that error,
// the exchange fails and the session is replaced, and the upload joins the
// worker's CheckpointSkips for the scans after it.
//
// The hand-on (continuationsOnly) imports every continuation.md snapshot it
// meets and steps over every other upload. The ordinary pass imports what it
// meets, going on past continuation.md snapshots, which arrive at every turn
// end of every running attempt, and ending after one other checkpoint. So
// each pass takes off the queue at least one upload it can handle among the
// first continuationHandOnRounds, if there is one, and a snapshot further back
// is reached within a bounded number of passes. Only a head of that many
// uploads that all keep failing hides what lies behind it.
func scanCoordinatorWorkerCheckpoints(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64, continuationsOnly bool) (backlog.WorkerExchangeReport, error) {
	if session.Client == nil || session.ArtifactClient == nil || maxArtifactBytes < 1 {
		return report, fmt.Errorf("coordinator worker checkpoint import requires control, artifact transport, and a positive limit")
	}
	excluded := session.CheckpointSkips.excluded()
	seen := map[string]struct{}{}
	for round := len(excluded); round < continuationHandOnRounds; round++ {
		if !session.ArtifactClient.Usable() {
			return report, errors.New("coordinator worker checkpoint import: the artifact session is unusable after an earlier transport failure")
		}
		upload, err := session.Client.PollArtifact(ctx, "checkpoint", excluded...)
		if err != nil {
			return report, err
		}
		if upload == nil {
			session.CheckpointSkips.reset()
			return report, nil
		}
		id := upload.Manifest.ID
		if _, again := seen[id]; again {
			// The worker announced an upload it was told to skip or had
			// acknowledged; going on would never end.
			session.CheckpointSkips.reset()
			return report, nil
		}
		seen[id] = struct{}{}
		if continuationsOnly && !backlog.IsContinuationUpload(upload.Manifest) {
			excluded = append(excluded, id)
			continue
		}
		fetched, err := session.ArtifactClient.FetchArtifact(ctx, *upload, maxArtifactBytes, maxArtifactBytes)
		if err != nil && !session.ArtifactClient.Usable() {
			session.CheckpointSkips.add(id)
			return report, err
		}
		if err != nil {
			slog.Warn("worker checkpoint not fetched", "manifest", id, "worker", upload.Manifest.WorkerID, "reason", err)
			excluded = append(excluded, id)
			continue
		}
		imported, err := session.CheckpointImporter.Import(ctx, fetched.Response, fetched)
		if errors.Is(err, backlog.ErrCheckpointImportRejected) {
			// Retrying cannot help; the bytes stay in the worker's custody.
			slog.Error("worker checkpoint rejected and discarded", "manifest", id, "worker", upload.Manifest.WorkerID, "reason", err)
			if err := session.Client.AcknowledgeArtifact(ctx, id); err != nil {
				return report, err
			}
			continue
		}
		if err != nil {
			slog.Warn("worker checkpoint import deferred", "manifest", id, "worker", upload.Manifest.WorkerID, "reason", err)
			excluded = append(excluded, id)
			continue
		}
		report.Checkpoints = append(report.Checkpoints, imported)
		if err := session.Client.AcknowledgeArtifact(ctx, id); err != nil {
			return report, err
		}
		if !continuationsOnly && imported.Name != domain.ContinuationArtifactName {
			session.CheckpointSkips.reset()
			return report, nil
		}
	}
	session.CheckpointSkips.reset()
	return report, nil
}

func (s *coordinatorWorkerSessions) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	if s == nil || s.exchange == nil {
		return coordinatorWorkerTickReport{}
	}
	// The hand-on phase: every worker's continuation.md snapshots are imported
	// before any worker's exchange expires a lease or freezes an offer, so a
	// replacement offered on one worker carries what its predecessor left on
	// another, whichever order the workers are polled in. Nothing waits for
	// it: a snapshot this phase cannot import yet is imported by a later pass
	// and counts toward the task's latest from then on.
	handedOn := map[string]backlog.WorkerExchangeReport{}
	unopened := map[string]error{}
	if s.handOn != nil {
		for _, workerID := range s.workerIDs {
			report, err := s.handOn(ctx, workerID)
			handedOn[workerID] = report
			if err != nil {
				slog.Warn("worker continuation snapshots not imported before offers", "worker", workerID, "error", err)
			}
			if failed := (coordinatorWorkerUnopened{}); errors.As(err, &failed) {
				unopened[workerID] = failed.err
			}
		}
	}
	report := coordinatorWorkerTickReport{Results: make([]coordinatorWorkerTickResult, 0, len(s.workerIDs))}
	for _, workerID := range s.workerIDs {
		var exchange backlog.WorkerExchangeReport
		err, failed := unopened[workerID]
		if !failed {
			exchange, err = s.exchange(ctx, workerID, quota)
		}
		exchange.Checkpoints = append(handedOn[workerID].Checkpoints, exchange.Checkpoints...)
		report.Results = append(report.Results, coordinatorWorkerTickResult{
			WorkerID: workerID, Report: exchange, Err: err,
		})
	}
	return report
}

func newCoordinatorWorkerSession(
	ctx context.Context,
	settings config.BacklogV2,
	store *sqlite.Store,
	workerID string,
	coordinatorEpoch int64,
	sessionID string,
	resolver workerruntime.ProtocolCredentialResolver,
	now time.Time,
	commandFactory workerproto.CommandFactory,
	artifacts backlog.CoordinatorArtifactStore,
) (coordinatorWorkerSession, error) {
	if store == nil || resolver == nil || coordinatorEpoch < 1 || sessionID == "" || now.IsZero() {
		return coordinatorWorkerSession{}, fmt.Errorf("coordinator worker session requires store, authority, identity, resolver, and time")
	}
	worker, ok := settings.Workers[workerID]
	if !ok || worker.Epoch == "" {
		return coordinatorWorkerSession{}, fmt.Errorf("coordinator worker session: worker %q has no configured epoch", workerID)
	}
	binding, err := workerruntime.BuildWorkerBinding(settings, workerID, now)
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	credentials, err := resolver.ResolveProtocol(ctx, binding.CredentialRef)
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	requestTimeout := settings.Transport.RequestTimeout.D()
	connectTimeout := min(requestTimeout, 10*time.Second)
	transport, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
		Address: worker.Address, RemoteCommand: coordinatorWorkerRemoteCommand,
		RemoteArguments: []string{coordinatorWorkerControlOperation},
		RequestTimeout:  requestTimeout, ConnectTimeout: connectTimeout,
		MaxMessageBytes:   settings.MessageLimits.MaxBytes,
		MaxStderrBytes:    settings.MessageLimits.MaxBytes,
		ResponsePrincipal: credentials.WorkerPrincipal,
		ResponseKeyID:     credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret,
		Factory: commandFactory,
	})
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	artifactTransport, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
		Address: worker.Address, RemoteCommand: coordinatorWorkerRemoteCommand,
		RemoteArguments: []string{coordinatorWorkerArtifactSendOperation},
		RequestTimeout:  requestTimeout, ConnectTimeout: connectTimeout,
		MaxMessageBytes:   settings.MessageLimits.MaxBytes,
		MaxStderrBytes:    settings.MessageLimits.MaxBytes,
		ResponsePrincipal: credentials.WorkerPrincipal,
		ResponseKeyID:     credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret,
		Factory: commandFactory,
	})
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	downloadTransport, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
		Address: worker.Address, RemoteCommand: coordinatorWorkerRemoteCommand,
		RemoteArguments: []string{coordinatorWorkerArtifactReceiveOperation},
		RequestTimeout:  requestTimeout, ConnectTimeout: connectTimeout,
		MaxMessageBytes:   settings.MessageLimits.MaxBytes,
		MaxStderrBytes:    settings.MessageLimits.MaxBytes,
		ResponsePrincipal: credentials.WorkerPrincipal,
		ResponseKeyID:     credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret,
		Factory: commandFactory,
	})
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	var controlTransport workerproto.RoundTripper = transport
	var resultTransport workerproto.RoundTripper = artifactTransport
	var inputTransport coordinatorArtifactDownloadTransport = downloadTransport
	var closeTransport func() error
	if worker.Connection != "" {
		stream, err := newPersistentWorkerTransport(worker, credentials, settings, commandFactory)
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		controlTransport = stream
		resultTransport = stream
		inputTransport = stream
		closeTransport = stream.Close
	}
	success := false
	defer func() {
		if !success && closeTransport != nil {
			_ = closeTransport()
		}
	}()
	// The supervisor principal is resolved here rather than passed in so that
	// the offer builder and the coordinator's activation boundary read the one
	// configuration entry. An ambiguous or absent supervisor leaves it empty,
	// which refuses an activation offer instead of guessing an identity.
	supervisorPrincipal, supervisorCredential, err := coordinatorSupervisorClient(settings.Coordinator.AdminClients)
	if err != nil {
		supervisorPrincipal, supervisorCredential = "", ""
	}
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: settings.Coordinator.ID, WorkerID: workerID,
		CoordinatorEpoch: coordinatorEpoch, WorkerEpoch: worker.Epoch, SessionID: sessionID,
		RequestTimeout:  requestTimeout,
		SignerPrincipal: credentials.CoordinatorPrincipal,
		SignerKeyID:     credentials.CoordinatorKeyID, SignerSecret: credentials.CoordinatorSecret,
		RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second},
		Transport:   controlTransport,
	})
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	artifactClient, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: settings.Coordinator.ID, WorkerID: workerID,
		CoordinatorEpoch: coordinatorEpoch, WorkerEpoch: worker.Epoch, SessionID: sessionID + "-artifact",
		RequestTimeout:  requestTimeout,
		SignerPrincipal: credentials.CoordinatorPrincipal,
		SignerKeyID:     credentials.CoordinatorKeyID, SignerSecret: credentials.CoordinatorSecret,
		RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second},
		Transport:   resultTransport,
	})
	if err != nil {
		return coordinatorWorkerSession{}, err
	}
	if worker.Connection != "" {
		projection, err := workerruntime.BuildCatalogProjection(settings, workerID)
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		catalogClient, err := workerproto.NewClient(workerproto.ClientConfig{
			CoordinatorID: settings.Coordinator.ID, WorkerID: workerID,
			CoordinatorEpoch: coordinatorEpoch, WorkerEpoch: worker.Epoch, SessionID: sessionID + "-catalog",
			RequestTimeout: requestTimeout, SignerPrincipal: credentials.CoordinatorPrincipal,
			SignerKeyID: credentials.CoordinatorKeyID, SignerSecret: credentials.CoordinatorSecret,
			RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second},
			Transport:   controlTransport,
		})
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		expectedRevision := projection.Revision
		snapshots, err := store.LoadWorkerSnapshots(ctx)
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		for _, snapshot := range snapshots {
			if snapshot.WorkerID == workerID && snapshot.Inventory.CatalogRevision != "" {
				expectedRevision = snapshot.Inventory.CatalogRevision
			}
		}
		accepted, err := catalogClient.Catalog(ctx, workerruntime.CatalogRequest{Projection: projection, ExpectedRevision: expectedRevision})
		if err != nil {
			return coordinatorWorkerSession{}, err
		}
		if accepted["revision"] != binding.CatalogRevision || accepted["workerId"] != workerID {
			return coordinatorWorkerSession{}, errors.New("worker accepted wrong catalog")
		}
	}
	success = true
	if artifacts.Catalog == nil {
		artifacts.Catalog = store
	}
	return coordinatorWorkerSession{
		Client: client, ArtifactClient: artifactClient, Binding: binding, Records: store, Close: closeTransport,
		Importer: backlog.CoordinatorResultImporter{
			CoordinatorID: settings.Coordinator.ID, CoordinatorEpoch: coordinatorEpoch,
			Store: store, Artifacts: artifacts,
			MaxArtifactBytes: settings.MessageLimits.MaxArtifactBytes,
			MaxTotalBytes:    settings.MessageLimits.MaxArtifactBytes,
		},
		CheckpointImporter: backlog.CoordinatorCheckpointImporter{
			CoordinatorID: settings.Coordinator.ID, CoordinatorEpoch: coordinatorEpoch,
			Store: store, Artifacts: artifacts,
			MaxArtifactBytes: settings.MessageLimits.MaxArtifactBytes,
			MaxTotalBytes:    settings.MessageLimits.MaxArtifactBytes,
		},
		Builder: coordinatorDeliveringOfferBuilder{
			Base: backlog.CoordinatorOfferBuilder{
				Authorization: &binding.Inventory,
				Store:         store, Catalog: binding.Catalog, CatalogRevision: binding.CatalogRevision,
				ActivationEvidence: &artifacts,
				CoordinatorID:      settings.Coordinator.ID, CoordinatorEpoch: coordinatorEpoch,
				VerificationTimeout: settings.Verification.CommandTimeout.D(),
				// An overseer activation's preparation is a protocol-sized step;
				// it keeps the request timeout it always had.
				ActivationPrepareTimeout: requestTimeout,
				MaxArtifactBytes:         settings.MessageLimits.MaxArtifactBytes,
				MaxTotalBytes:            settings.MessageLimits.MaxArtifactBytes,
				// WorkerCapabilities stays nil on purpose: the builder then reads
				// what the worker reported about itself, which is the only source
				// that knows which build is running on that host.
				//
				// Supervision lets this builder also render an overseer
				// activation, so one offer path serves both kinds of work. The
				// supervisor principal is read from configuration, because the
				// coordinator has to be told which admin client it will see.
				Supervision:                   backlog.CoordinatorSupervisionStore{Store: store},
				SupervisorPrincipal:           supervisorPrincipal,
				SupervisorCredentialReference: supervisorCredential,
			},
			Transport: inputTransport, Artifacts: artifacts,
			CoordinatorID: settings.Coordinator.ID, CoordinatorEpoch: coordinatorEpoch,
			WorkerID: workerID, WorkerEpoch: worker.Epoch,
			SignerPrincipal: credentials.CoordinatorPrincipal,
			SignerKeyID:     credentials.CoordinatorKeyID, SignerSecret: credentials.CoordinatorSecret,
			RequestTimeout:   requestTimeout,
			RetryPolicy:      workerproto.RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second},
			MaxArtifactBytes: settings.MessageLimits.MaxArtifactBytes,
			MaxTotalBytes:    settings.MessageLimits.MaxArtifactBytes,
			Now:              time.Now,
		},
	}, nil
}

func newCoordinatorWorkerSessionID(coordinatorID, workerID string) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("create worker session id: %w", err)
	}
	return coordinatorID + "-" + workerID + "-" + hex.EncodeToString(nonce[:]), nil
}
