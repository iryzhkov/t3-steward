package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// WorkerServiceOptions composes the fixed restricted-command runtime for one
// configured worker identity. Construction performs no T3 or fleet operation.
type WorkerServiceOptions struct {
	Settings            config.BacklogV2
	WorkerID            string
	WorkerEpoch         string
	CoordinatorEpoch    int64
	ProtocolCredentials ProtocolCredentialResolver
	ProjectCredentials  CredentialChecker
	T3                  T3Control
	DryRun              bool
	Now                 func() time.Time
	Logger              *slog.Logger
}

// WorkerService owns the bounded codec and authenticated exchange used by the
// restricted SSH command.
type WorkerService struct {
	Codec    workerproto.Codec
	Exchange Exchange
}

// NewWorkerService binds configuration, credential identity, local persistence,
// containment, artifact custody, and the T3 adapter into one worker runtime.
func NewWorkerService(ctx context.Context, options WorkerServiceOptions) (*WorkerService, error) {
	if options.ProtocolCredentials == nil {
		return nil, errors.New("worker service: protocol credential resolver is required")
	}
	if options.WorkerEpoch == "" || options.CoordinatorEpoch < 1 {
		return nil, errors.New("worker service: worker and coordinator epochs are required")
	}
	if options.Settings.Freshness.WorkerMaxAge.D() >= protocolReplayMaxAge {
		return nil, errors.New("worker service: protocol clock skew must be shorter than replay retention")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	binding, err := BuildWorkerBinding(options.Settings, options.WorkerID, options.Now())
	if err != nil {
		return nil, err
	}
	credentials, err := options.ProtocolCredentials.ResolveProtocol(ctx, binding.CredentialRef)
	if err != nil {
		return nil, err
	}
	wantCoordinator := "ssh:" + options.Settings.Coordinator.ID
	wantWorker := "ssh:" + options.WorkerID
	if credentials.CoordinatorPrincipal != wantCoordinator || credentials.WorkerPrincipal != wantWorker {
		return nil, errors.New("worker service: protocol credential principal does not match configured identity")
	}

	artifactRoot, runsRoot, journalRoot := WorkerRoots(options.Settings, options.WorkerID)
	journal, err := OpenJournal(journalRoot, options.WorkerID, options.WorkerEpoch, options.CoordinatorEpoch)
	if err != nil {
		return nil, fmt.Errorf("worker service: %w", err)
	}
	replayStore, err := OpenProtocolReplayStore(
		filepath.Join(journalRoot, "protocol"),
		options.Settings.Coordinator.ID, options.WorkerID,
		options.CoordinatorEpoch, options.WorkerEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("worker service: %w", err)
	}
	custody, err := OpenCustodyStore(CustodyConfig{
		Root:             artifactRoot,
		CoordinatorID:    options.Settings.Coordinator.ID,
		CoordinatorEpoch: options.CoordinatorEpoch,
		WorkerID:         options.WorkerID,
		WorkerEpoch:      options.WorkerEpoch,
		MaxArtifactBytes: options.Settings.MessageLimits.MaxArtifactBytes,
		MaxTotalBytes:    options.Settings.MessageLimits.MaxArtifactBytes,
		Now:              options.Now,
	})
	if err != nil {
		return nil, err
	}
	processes := backlog.SystemdScopeRunner{}
	t3 := options.T3
	if t3 != nil {
		t3 = NewCachedT3(t3)
	}
	driver, err := NewLocalDriver(LocalDriver{
		Config: LocalDriverConfig{
			CatalogRevision:  binding.CatalogRevision,
			ArtifactRoot:     artifactRoot,
			RunsRoot:         runsRoot,
			StopTimeout:      options.Settings.Transport.RequestTimeout.D(),
			RetainWorkspaces: true,
			DryRun:           options.DryRun,
		},
		Catalog: binding.Catalog,
		Workspace: backlog.WorkspacePreparer{
			Cache:     backlog.LocalRepositoryCache{Root: filepath.Join(options.Settings.Storage.Workspaces, "repository-cache")},
			Processes: processes,
		},
		Finalizer:   backlog.AttemptFinalizer{Processes: processes, Now: options.Now},
		Source:      custody,
		Publisher:   custody,
		Credentials: options.ProjectCredentials,
		T3:          t3,
		Now:         options.Now,
	})
	if err != nil {
		return nil, err
	}
	runtime, err := New(Config{
		WorkerID:         options.WorkerID,
		WorkerEpoch:      options.WorkerEpoch,
		CoordinatorID:    options.Settings.Coordinator.ID,
		CoordinatorEpoch: options.CoordinatorEpoch,
		SnapshotTTL:      options.Settings.Freshness.WorkerMaxAge.D(),
		LeaseDuration:    options.Settings.Leases.Duration.D(),
		MaxPackageBytes:  options.Settings.MessageLimits.MaxBytes,
		Inventory:        binding.Inventory,
		Retention:        options.Settings.Storage.Retention.D(),
		Now:              options.Now,
		Logger:           options.Logger,
	}, journal, driver)
	if err != nil {
		return nil, err
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID:    options.Settings.Coordinator.ID,
		WorkerID:         options.WorkerID,
		CoordinatorEpoch: options.CoordinatorEpoch,
		WorkerEpoch:      options.WorkerEpoch,
		PeerPrincipal:    credentials.CoordinatorPrincipal,
		PeerKeyID:        credentials.CoordinatorKeyID,
		PeerSecret:       credentials.CoordinatorSecret,
		SignerPrincipal:  credentials.WorkerPrincipal,
		SignerKeyID:      credentials.WorkerKeyID,
		SignerSecret:     credentials.WorkerSecret,
		Allowed: map[workerproto.MessageType]bool{
			workerproto.MessageCapabilities:        true,
			workerproto.MessageSnapshot:            true,
			workerproto.MessageOffers:              true,
			workerproto.MessageLeaseRenewals:       true,
			workerproto.MessageCommands:            true,
			workerproto.MessageThrottleCommands:    true,
			workerproto.MessageArtifactDownload:    true,
			workerproto.MessageArtifactUpload:      true,
			workerproto.MessageArtifactPoll:        true,
			workerproto.MessageArtifactAcknowledge: true,
		},
		SupportedVersions: []int{workerproto.Version},
		MaxClockSkew:      options.Settings.Freshness.WorkerMaxAge.D(),
		MaxInFlight:       1,
		MaxCachedRequests: 4096,
		ReplayStore:       replayStore,
		Now:               options.Now,
	})
	if err != nil {
		return nil, err
	}
	return &WorkerService{
		Codec: workerproto.Codec{MaxBytes: options.Settings.MessageLimits.MaxBytes},
		Exchange: Exchange{
			Runtime: runtime,
			Server:  server,
			Custody: custody,
		},
	}, nil
}

// Serve reconciles durable local work, processes exactly one authenticated
// envelope, emits one response, and exits.
func (s *WorkerService) Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	if s == nil || s.Exchange.Runtime == nil {
		return errors.New("worker service: service is not initialized")
	}
	var request workerproto.Envelope
	if err := s.Codec.Decode(input, &request); err != nil {
		return err
	}
	return s.ServeEnvelope(ctx, request, output)
}

// ServeEnvelope is Serve for an envelope the caller already read, for example
// to adopt its coordinator epoch before constructing the service.
func (s *WorkerService) ServeEnvelope(ctx context.Context, request workerproto.Envelope, output io.Writer) error {
	if s == nil || s.Exchange.Runtime == nil {
		return errors.New("worker service: service is not initialized")
	}
	// Reconciliation runs before every request but must leave time to answer
	// it: a slow provider call is cut off and retried on the next exchange
	// instead of making the whole exchange time out.
	reconcileCtx, cancel := context.WithCancel(ctx)
	if !request.Deadline.IsZero() {
		budget := time.Until(request.Deadline) * 2 / 3
		if budget < time.Second {
			budget = time.Second
		}
		reconcileCtx, cancel = context.WithTimeout(ctx, budget)
	}
	err := s.Exchange.Runtime.Reconcile(reconcileCtx)
	cancel()
	if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		s.Exchange.Runtime.log.Warn("reconciliation exceeded its time budget; remaining attempts are retried next exchange")
	} else if err != nil {
		return err
	}
	response, err := s.Exchange.Handle(ctx, request)
	if err != nil {
		return err
	}
	return s.Codec.Encode(output, response)
}

// WorkerRoots returns the worker-scoped artifact, runs, and journal roots.
func WorkerRoots(settings config.BacklogV2, workerID string) (artifactRoot, runsRoot, journalRoot string) {
	storageKey := shortDigest([]byte(workerID))
	artifactRoot = filepath.Join(settings.Storage.Artifacts, "workers", storageKey)
	runsRoot = filepath.Join(settings.Storage.Workspaces, "workers", storageKey)
	journalRoot = filepath.Join(runsRoot, "journal")
	return artifactRoot, runsRoot, journalRoot
}

// AdoptCoordinatorEpoch chooses the coordinator epoch a worker exchange runs
// under: the highest of the configured floor, the durable journal, and the
// epoch of an envelope that verifies against the coordinator credential. A
// forged or unsigned envelope cannot move the epoch.
func AdoptCoordinatorEpoch(ctx context.Context, options WorkerServiceOptions, envelope workerproto.Envelope) (int64, error) {
	if options.ProtocolCredentials == nil {
		return 0, errors.New("worker service: protocol credential resolver is required")
	}
	binding, err := BuildWorkerBinding(options.Settings, options.WorkerID, time.Now())
	if err != nil {
		return 0, err
	}
	credentials, err := options.ProtocolCredentials.ResolveProtocol(ctx, binding.CredentialRef)
	if err != nil {
		return 0, err
	}
	_, _, journalRoot := WorkerRoots(options.Settings, options.WorkerID)
	journalEpoch, err := JournalCoordinatorEpoch(journalRoot)
	if err != nil {
		return 0, err
	}
	epoch := max(options.CoordinatorEpoch, journalEpoch)
	if envelope.Sender == options.Settings.Coordinator.ID && envelope.Recipient == options.WorkerID &&
		envelope.WorkerEpoch == options.WorkerEpoch &&
		envelope.Authentication.Principal == credentials.CoordinatorPrincipal &&
		envelope.Authentication.KeyID == credentials.CoordinatorKeyID &&
		workerproto.VerifyEnvelopeSignature(envelope, credentials.CoordinatorSecret) == nil &&
		envelope.CoordinatorEpoch > epoch {
		epoch = envelope.CoordinatorEpoch
	}
	if epoch < 1 {
		return 0, errors.New("worker service: no coordinator epoch is known yet; set local_worker.coordinator_epoch or wait for an authenticated coordinator envelope")
	}
	return epoch, nil
}
