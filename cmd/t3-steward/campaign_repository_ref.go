package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// errRepositoryRefUnsupportedByWorker is returned when the addressed worker's
// observed build does not advertise exact-ref resolution. The message is never
// sent to such a worker.
var errRepositoryRefUnsupportedByWorker = errors.New("repository ref resolution is unsupported by worker")

// repositoryRefClient is the one exchange a ref resolution needs.
type repositoryRefClient interface {
	ResolveRepositoryRef(context.Context, workerproto.RepositoryRefRequest) (workerproto.RepositoryRefResolution, error)
}

// workerSnapshotSource is where the coordinator reads what each worker last
// reported about itself, including the capabilities its build advertises.
type workerSnapshotSource interface {
	LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error)
}

// repositoryRefQuery names the worker to ask and the exact ref to resolve.
type repositoryRefQuery struct {
	WorkerID       string
	Repository     string
	Ref            string
	CredentialRefs []string
}

// repositoryRefAnswer is one worker's resolution of one ref. ObjectID is set
// only when Status is resolved. ObservedAt is the worker's clock.
type repositoryRefAnswer struct {
	WorkerID   string
	Ref        string
	Status     workerproto.RefResolutionStatus
	ObjectID   string
	Class      backlog.RepositoryReachability
	Detail     string
	ObservedAt time.Time
}

// coordinatorRepositoryRefResolver asks one named worker which object one exact
// ref names on a project's remote.
//
// It is deliberately separate from coordinatorRepositoryObserver. The observer
// answers "can this worker read this repository" and retains its answer for
// ten minutes; this answers "what does this ref point at now", and retains
// nothing, because a retained head is a stale head. Every call opens a fresh
// session to the named worker and observes the remote through that worker's
// own network position and credentials; the coordinator never answers from its
// own.
type coordinatorRepositoryRefResolver struct {
	settings  config.BacklogV2
	resolver  workerruntime.ProtocolCredentialResolver
	epoch     int64
	factory   workerproto.CommandFactory
	snapshots workerSnapshotSource
	// dial is the transport seam, as for the observer.
	dial func(context.Context, string) (repositoryRefClient, func() error, error)
}

func newCoordinatorRepositoryRefResolver(
	settings config.BacklogV2,
	resolver workerruntime.ProtocolCredentialResolver,
	epoch int64,
	factory workerproto.CommandFactory,
	snapshots workerSnapshotSource,
) *coordinatorRepositoryRefResolver {
	resolverValue := &coordinatorRepositoryRefResolver{
		settings: settings, resolver: resolver, epoch: epoch, factory: factory, snapshots: snapshots,
	}
	resolverValue.dial = resolverValue.dialWorker
	return resolverValue
}

// ResolveRef asks query.WorkerID to resolve query.Ref.
//
// An invalid ref name is answered here as invalid-ref without opening a
// session. A worker whose observed build does not advertise the capability
// gets errRepositoryRefUnsupportedByWorker and is never sent the message. A
// worker that cannot be reached, or whose capabilities have not been observed
// for its current enrolment, is an error and never an answer.
func (r *coordinatorRepositoryRefResolver) ResolveRef(ctx context.Context, query repositoryRefQuery) (repositoryRefAnswer, error) {
	if r == nil {
		return repositoryRefAnswer{}, errors.New("repository ref resolution: no resolver is configured")
	}
	if query.WorkerID == "" {
		return repositoryRefAnswer{}, errors.New("repository ref resolution: a worker is required")
	}
	if err := backlog.ValidateExactRef(query.Ref); err != nil {
		return repositoryRefAnswer{
			WorkerID: query.WorkerID, Ref: query.Ref,
			Status: workerproto.RefResolutionInvalidRef, Detail: err.Error(),
		}, nil
	}
	if err := backlog.ValidateRepositoryProbeArguments(query.Repository, query.Ref); err != nil {
		return repositoryRefAnswer{}, err
	}
	request := workerproto.RepositoryRefRequest{
		Repository:     query.Repository,
		Ref:            query.Ref,
		CredentialRefs: append([]string(nil), query.CredentialRefs...),
		TimeoutSeconds: repositoryProbeTimeoutSeconds(r.settings),
	}
	if err := workerproto.ValidateRepositoryRefRequest(request); err != nil {
		return repositoryRefAnswer{}, err
	}
	if err := r.requireCapability(ctx, query.WorkerID); err != nil {
		return repositoryRefAnswer{}, err
	}
	client, closer, err := r.dial(ctx, query.WorkerID)
	if err != nil {
		return repositoryRefAnswer{}, err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}
	resolution, err := client.ResolveRepositoryRef(ctx, request)
	if err != nil {
		return repositoryRefAnswer{}, fmt.Errorf("repository ref resolution: worker %q: %w", query.WorkerID, err)
	}
	// The client has already checked the answer against the request; this is
	// the coordinator's own check, so a different client cannot skip it.
	if err := workerproto.ValidateRepositoryRefResolution(request, resolution); err != nil {
		return repositoryRefAnswer{}, err
	}
	answer := repositoryRefAnswer{
		WorkerID: query.WorkerID, Ref: resolution.Ref, Status: resolution.Status,
		ObjectID: resolution.ObjectID, ObservedAt: resolution.ObservedAt,
	}
	if resolution.Class != "" {
		class, known := backlog.ParseRepositoryReachability(resolution.Class)
		if !known {
			return repositoryRefAnswer{}, fmt.Errorf(
				"repository ref resolution: worker %q reported an unknown classification %q", query.WorkerID, resolution.Class)
		}
		answer.Class = class
	}
	detail, _ := backlog.DefaultRedactor().Redact(resolution.Detail)
	answer.Detail = workerproto.BoundRepositoryProbeDetail(detail)
	return answer, nil
}

// requireCapability reads the worker's last stored snapshot and requires that
// it belongs to the worker's configured enrolment and advertises exact-ref
// resolution.
func (r *coordinatorRepositoryRefResolver) requireCapability(ctx context.Context, workerID string) error {
	if r.snapshots == nil {
		return errors.New("repository ref resolution: worker capabilities are unavailable")
	}
	snapshots, err := r.snapshots.LoadWorkerSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("repository ref resolution: load worker capabilities: %w", err)
	}
	epoch := r.settings.Workers[workerID].Epoch
	var latest *domain.WorkerSnapshot
	for index := range snapshots {
		snapshot := &snapshots[index]
		if snapshot.WorkerID != workerID || (epoch != "" && snapshot.WorkerEpoch != epoch) {
			continue
		}
		if latest == nil || snapshot.Sequence > latest.Sequence {
			latest = snapshot
		}
	}
	if latest == nil {
		return fmt.Errorf("repository ref resolution: worker %q has no observed capabilities for its current enrolment", workerID)
	}
	if !slices.Contains(latest.Inventory.Capabilities, workerproto.CapabilityRepositoryRefResolve) {
		return fmt.Errorf("%w %q: its build does not advertise %s",
			errRepositoryRefUnsupportedByWorker, workerID, workerproto.CapabilityRepositoryRefResolve)
	}
	return nil
}

func (r *coordinatorRepositoryRefResolver) dialWorker(ctx context.Context, workerID string) (repositoryRefClient, func() error, error) {
	client, closer, err := dialRepositoryProbeSession(ctx, r.settings, r.resolver, r.epoch, r.factory, workerID, coordinatorWorkerControlOperation, "-ref")
	if err != nil {
		return nil, nil, err
	}
	return client, closer, nil
}
