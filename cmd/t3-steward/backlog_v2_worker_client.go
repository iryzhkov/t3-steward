package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	coordinatorWorkerRemoteCommand            = "worker-exchange"
	coordinatorWorkerControlOperation         = "control"
	coordinatorWorkerArtifactSendOperation    = "artifact-send"
	coordinatorWorkerArtifactReceiveOperation = "artifact-receive"
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
	exchange  func(context.Context, string, backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error)
}

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
	return &coordinatorWorkerSessions{
		workerIDs: workerIDs,
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
			sessionID, err := newCoordinatorWorkerSessionID(settings.Coordinator.ID, workerID)
			if err != nil {
				return backlog.WorkerExchangeReport{}, err
			}
			session, ok := cached[workerID]
			if !ok {
				session, err = newCoordinatorWorkerSession(ctx, settings, store, workerID, coordinatorEpoch, sessionID, resolver, time.Now().UTC(), commandFactory, artifacts)
				if err != nil {
					return backlog.WorkerExchangeReport{}, err
				}
				if settings.Workers[workerID].Connection != "" {
					cached[workerID] = session
				}
			}
			report, reconcileErr := coordinator.ReconcileWorker(
				ctx, session.Client, session.Builder, backlog.WorkerAdmissionPolicyFromQuotaReport(quota), quota.Directives, quota.Pools,
				settings.Leases.RenewInterval.D(), settings.Leases.Duration.D(),
			)
			report, importErr := importCoordinatorWorkerArtifacts(ctx, session, report, settings.MessageLimits.MaxArtifactBytes)
			if reconcileErr != nil || importErr != nil {
				if session.Close != nil {
					_ = session.Close()
					delete(cached, workerID)
				}
				return report, errors.Join(reconcileErr, importErr)
			}
			return report, nil
		},
	}, nil
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
	if err != nil {
		return report, err
	}
	report.Imports = append(report.Imports, imported)
	if err := session.Client.AcknowledgeArtifact(ctx, upload.Manifest.ID); err != nil {
		return report, err
	}
	return report, nil
}

func importCoordinatorWorkerCheckpoint(ctx context.Context, session coordinatorWorkerSession, report backlog.WorkerExchangeReport, maxArtifactBytes int64) (backlog.WorkerExchangeReport, error) {
	if session.Client == nil || session.ArtifactClient == nil || maxArtifactBytes < 1 {
		return report, fmt.Errorf("coordinator worker checkpoint import requires control, artifact transport, and a positive limit")
	}
	upload, err := session.Client.PollArtifact(ctx, "checkpoint")
	if err != nil || upload == nil {
		return report, err
	}
	fetched, err := session.ArtifactClient.FetchArtifact(ctx, *upload, maxArtifactBytes, maxArtifactBytes)
	if err != nil {
		return report, err
	}
	imported, err := session.CheckpointImporter.Import(ctx, fetched.Response, fetched)
	if err != nil {
		return report, err
	}
	report.Checkpoints = append(report.Checkpoints, imported)
	if err := session.Client.AcknowledgeArtifact(ctx, upload.Manifest.ID); err != nil {
		return report, err
	}
	return report, nil
}

func (s *coordinatorWorkerSessions) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	if s == nil || s.exchange == nil {
		return coordinatorWorkerTickReport{}
	}
	report := coordinatorWorkerTickReport{Results: make([]coordinatorWorkerTickResult, 0, len(s.workerIDs))}
	for _, workerID := range s.workerIDs {
		exchange, err := s.exchange(ctx, workerID, quota)
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
				Store: store, Catalog: binding.Catalog, CatalogRevision: binding.CatalogRevision,
				CoordinatorID: settings.Coordinator.ID, CoordinatorEpoch: coordinatorEpoch,
				VerificationTimeout: requestTimeout,
				MaxArtifactBytes:    settings.MessageLimits.MaxArtifactBytes,
				MaxTotalBytes:       settings.MessageLimits.MaxArtifactBytes,
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
