package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// repositoryProbeClient is the one exchange a reachability observation needs. It
// is an interface so that a test can answer the probe without a transport, and
// so that nothing else about a worker session leaks into this path.
type repositoryProbeClient interface {
	ObserveRepository(context.Context, workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error)
}

// coordinatorRepositoryObserver dispatches the built-in repository probe to the
// candidate worker and returns its classified answer to the viability matrix.
//
// Every probe uses a fresh, short-lived protocol session that is closed again
// when the exchange ends. That is deliberate: the coordinator's long-lived
// worker sessions belong to the boundary cycle, which runs on its own goroutine,
// and a read-only query arriving on the admin socket must not reach into them.
//
// The observer never answers from the coordinator's own network or credentials.
// A worker that cannot be reached produces an error, which the matrix reports as
// an unobserved reachability: temporary, never a passed check and never a
// permanent refusal.
type coordinatorRepositoryObserver struct {
	settings config.BacklogV2
	resolver workerruntime.ProtocolCredentialResolver
	epoch    int64
	factory  workerproto.CommandFactory
	cache    *backlog.RepositoryProbeCache
	// dial is the transport seam. It returns one client and the closer that
	// releases whatever the client holds.
	dial func(context.Context, string) (repositoryProbeClient, func() error, error)
	// now is the one clock the observation and its retention share, so evidence
	// cannot be stamped on one clock and expired against another.
	now func() time.Time

	// mu serializes probes. Several candidates in one matrix are observed one
	// after another, which keeps the number of concurrent worker connections a
	// single read-only query can open at one.
	mu sync.Mutex
}

func newCoordinatorRepositoryObserver(
	settings config.BacklogV2,
	resolver workerruntime.ProtocolCredentialResolver,
	epoch int64,
	factory workerproto.CommandFactory,
) *coordinatorRepositoryObserver {
	observer := &coordinatorRepositoryObserver{
		settings: settings, resolver: resolver, epoch: epoch, factory: factory,
		now: func() time.Time { return time.Now().UTC() },
	}
	observer.cache = &backlog.RepositoryProbeCache{
		TTL: backlog.RepositoryProbeEvidenceTTL,
		Now: func() time.Time { return observer.now() },
	}
	observer.dial = observer.dialWorker
	return observer
}

// ObserveRepository satisfies the coordinator's RepositoryObserver seam.
//
// The retained observation is keyed on worker, catalog digest, repository, ref
// and credential references, so rotating a credential reference or re-enrolling
// a worker produces a different key rather than a stale answer.
func (o *coordinatorRepositoryObserver) ObserveRepository(ctx context.Context, key backlog.RepositoryProbeKey) (backlog.RepositoryProbeObservation, error) {
	if o == nil {
		return backlog.RepositoryProbeObservation{}, errors.New("repository probe: no observer is configured")
	}
	if observation, found := o.cache.Lookup(key); found {
		return observation, nil
	}
	// The argument vector is refused here as well as on the worker. A value that
	// could be read as an option must never reach a transport, let alone a
	// process, and the coordinator is where the campaign's own values are first
	// turned into a request.
	if err := backlog.ValidateRepositoryProbeArguments(key.Repository, key.Ref); err != nil {
		return backlog.RepositoryProbeObservation{}, err
	}
	request := workerproto.RepositoryProbeRequest{
		Repository:     key.Repository,
		Ref:            key.Ref,
		CredentialRefs: append([]string(nil), key.CredentialRefs...),
		MaxOutputBytes: workerproto.MaxRepositoryProbeOutputBytes,
		TimeoutSeconds: o.probeTimeoutSeconds(),
	}
	if err := workerproto.ValidateRepositoryProbeRequest(request); err != nil {
		return backlog.RepositoryProbeObservation{}, err
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	// A second candidate may have filled the entry while this one waited.
	if observation, found := o.cache.Lookup(key); found {
		return observation, nil
	}
	client, closer, err := o.dial(ctx, key.WorkerID)
	if err != nil {
		return backlog.RepositoryProbeObservation{}, err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}
	answer, err := client.ObserveRepository(ctx, request)
	if err != nil {
		return backlog.RepositoryProbeObservation{}, err
	}
	class, known := backlog.ParseRepositoryReachability(answer.Class)
	if !known {
		return backlog.RepositoryProbeObservation{}, fmt.Errorf(
			"repository probe: worker %q reported an unknown classification %q", key.WorkerID, answer.Class)
	}
	// The worker redacts before it answers. Redacting again here costs nothing
	// and means a worker build that forgot to cannot put a credential-shaped
	// span into this coordinator's evidence or diagnostics.
	detail, _ := backlog.DefaultRedactor().Redact(answer.Detail)
	observation := backlog.RepositoryProbeObservation{
		Key:        key,
		Class:      class,
		ExitCode:   answer.ExitCode,
		Detail:     workerproto.BoundRepositoryProbeDetail(detail),
		ObservedAt: o.now(),
	}
	o.cache.Store(observation)
	return observation, nil
}

// probeTimeoutSeconds keeps one probe inside the transport's own request
// timeout, so a probe cannot outlive the exchange that carries it.
func (o *coordinatorRepositoryObserver) probeTimeoutSeconds() int {
	timeout := o.settings.Transport.RequestTimeout.D()
	if timeout <= 0 || timeout > workerproto.MaxRepositoryProbeTimeout {
		timeout = workerproto.MaxRepositoryProbeTimeout
	}
	seconds := int(timeout / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

// dialWorker builds one short-lived control session to the named worker, using
// the same transports, credential reference and epochs the reconcile cycle uses.
// The probe therefore runs under the identity the real task would run under,
// which is the whole point of asking the worker rather than answering here.
func (o *coordinatorRepositoryObserver) dialWorker(ctx context.Context, workerID string) (repositoryProbeClient, func() error, error) {
	if o.resolver == nil || o.epoch < 1 {
		return nil, nil, errors.New("repository probe: coordinator authority and credential resolver are required")
	}
	worker, configured := o.settings.Workers[workerID]
	if !configured || worker.Epoch == "" {
		return nil, nil, fmt.Errorf("repository probe: worker %q has no configured epoch", workerID)
	}
	binding, err := workerruntime.BuildWorkerBinding(o.settings, workerID, time.Now().UTC())
	if err != nil {
		return nil, nil, err
	}
	credentials, err := o.resolver.ResolveProtocol(ctx, binding.CredentialRef)
	if err != nil {
		return nil, nil, err
	}
	requestTimeout := o.settings.Transport.RequestTimeout.D()
	if requestTimeout <= 0 {
		return nil, nil, errors.New("repository probe: a positive transport request timeout is required")
	}
	var transport workerproto.RoundTripper
	var closer func() error
	if worker.Connection != "" {
		stream, err := newPersistentWorkerTransport(worker, credentials, o.settings, o.factory)
		if err != nil {
			return nil, nil, err
		}
		transport, closer = stream, stream.Close
	} else {
		ssh, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
			Address: worker.Address, RemoteCommand: coordinatorWorkerRemoteCommand,
			RemoteArguments:   []string{coordinatorWorkerControlOperation},
			RequestTimeout:    requestTimeout,
			ConnectTimeout:    min(requestTimeout, 10*time.Second),
			MaxMessageBytes:   o.settings.MessageLimits.MaxBytes,
			MaxStderrBytes:    o.settings.MessageLimits.MaxBytes,
			ResponsePrincipal: credentials.WorkerPrincipal,
			ResponseKeyID:     credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret,
			Factory: o.factory,
		})
		if err != nil {
			return nil, nil, err
		}
		transport = ssh
	}
	sessionID, err := newCoordinatorWorkerSessionID(o.settings.Coordinator.ID, workerID+"-probe")
	if err != nil {
		if closer != nil {
			_ = closer()
		}
		return nil, nil, err
	}
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: o.settings.Coordinator.ID, WorkerID: workerID,
		CoordinatorEpoch: o.epoch, WorkerEpoch: worker.Epoch, SessionID: sessionID,
		RequestTimeout:  requestTimeout,
		SignerPrincipal: credentials.CoordinatorPrincipal,
		SignerKeyID:     credentials.CoordinatorKeyID, SignerSecret: credentials.CoordinatorSecret,
		RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 2, BaseDelay: 250 * time.Millisecond, MaxDelay: time.Second},
		Transport:   transport,
	})
	if err != nil {
		if closer != nil {
			_ = closer()
		}
		return nil, nil, err
	}
	return client, closer, nil
}
