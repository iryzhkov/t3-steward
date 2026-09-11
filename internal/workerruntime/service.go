package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	storageKey := shortDigest([]byte(options.WorkerID))
	artifactRoot := filepath.Join(options.Settings.Storage.Artifacts, "workers", storageKey)
	runsRoot := filepath.Join(options.Settings.Storage.Workspaces, "workers", storageKey)
	journalRoot := filepath.Join(runsRoot, "journal")
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
	driver, err := NewLocalDriver(LocalDriver{
		Config: LocalDriverConfig{
			CatalogRevision: binding.CatalogRevision,
			ArtifactRoot:    artifactRoot,
			RunsRoot:        runsRoot,
			StopTimeout:     options.Settings.Transport.RequestTimeout.D(),
			DryRun:          options.DryRun,
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
		T3:          options.T3,
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
		Now:              options.Now,
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
	if err := s.Exchange.Runtime.Reconcile(ctx); err != nil {
		return err
	}
	return ServeOne(ctx, input, output, s.Codec, s.Exchange)
}
