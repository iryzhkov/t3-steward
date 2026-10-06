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

	// budget bounds one probe, dial included. A readiness check runs inside an
	// admin request with a 30 s deadline and observes every task on every
	// eligible worker one after another, so a single worker that accepts a
	// connection and never answers must not consume the transport's whole
	// request timeout.
	budget time.Duration
	// failures remembers a probe that could not be answered, by key digest, for
	// repositoryProbeFailureTTL. Without it every task of a campaign re-dialled
	// the same unreachable worker over a fresh connection. Guarded by mu.
	failures map[string]failedRepositoryProbe
}

// repositoryProbeBudget is the default bound on one probe, dial included.
const repositoryProbeBudget = 10 * time.Second

// repositoryProbeFailureTTL is how long an unanswered probe is remembered. It is
// short because an unreachable worker is a temporary finding: long enough to
// cover the remaining tasks of one readiness check and an immediate retry,
// short enough that a worker that comes back is probed again within minutes.
const repositoryProbeFailureTTL = 2 * time.Minute

type failedRepositoryProbe struct {
	err error
	at  time.Time
}

// rememberedFailure returns the remembered error for key while it is fresh.
// The caller holds mu.
func (o *coordinatorRepositoryObserver) rememberedFailure(key backlog.RepositoryProbeKey) error {
	failure, found := o.failures[key.Digest()]
	if !found {
		return nil
	}
	if o.now().Sub(failure.at) > repositoryProbeFailureTTL {
		delete(o.failures, key.Digest())
		return nil
	}
	return failure.err
}

// rememberFailure records err for key. The caller holds mu.
func (o *coordinatorRepositoryObserver) rememberFailure(key backlog.RepositoryProbeKey, err error) error {
	if o.failures == nil {
		o.failures = map[string]failedRepositoryProbe{}
	}
	err = fmt.Errorf("%w (remembered for %s)", err, repositoryProbeFailureTTL)
	o.failures[key.Digest()] = failedRepositoryProbe{err: err, at: o.now()}
	return err
}

func newCoordinatorRepositoryObserver(
	settings config.BacklogV2,
	resolver workerruntime.ProtocolCredentialResolver,
	epoch int64,
	factory workerproto.CommandFactory,
) *coordinatorRepositoryObserver {
	observer := &coordinatorRepositoryObserver{
		settings: settings, resolver: resolver, epoch: epoch, factory: factory,
		// time.Now rather than time.Now().UTC(): calling UTC strips the
		// monotonic reading, and the retention window is an elapsed time.
		now:    time.Now,
		budget: repositoryProbeBudget,
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
	if err := o.rememberedFailure(key); err != nil {
		return backlog.RepositoryProbeObservation{}, err
	}
	caller := ctx
	if o.budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.budget)
		defer cancel()
		if seconds := int(o.budget / time.Second); seconds >= 1 && seconds < request.TimeoutSeconds {
			request.TimeoutSeconds = seconds
		}
	}
	// The caller giving up is not evidence about the worker, so only a
	// failure the probe itself reached is remembered.
	fail := func(err error) error {
		if caller.Err() != nil {
			return err
		}
		return o.rememberFailure(key, err)
	}
	client, closer, err := o.dial(ctx, key.WorkerID)
	if err != nil {
		return backlog.RepositoryProbeObservation{}, fail(err)
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}
	answer, err := client.ObserveRepository(ctx, request)
	if err != nil {
		return backlog.RepositoryProbeObservation{}, fail(err)
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
	return repositoryProbeTimeoutSeconds(o.settings)
}

// repositoryProbeTimeoutSeconds is the worker-side bound every repository
// question sent from this coordinator carries.
func repositoryProbeTimeoutSeconds(settings config.BacklogV2) int {
	timeout := settings.Transport.RequestTimeout.D()
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
	return o.dialWorkerOperation(ctx, workerID, coordinatorWorkerControlOperation)
}

// dialWorkerOperation opens the same session as dialWorker but names the
// worker operation the SSH transport starts, which commit export needs to
// reach the artifact send operation rather than control.
func (o *coordinatorRepositoryObserver) dialWorkerOperation(ctx context.Context, workerID, operation string) (repositoryProbeClient, func() error, error) {
	client, closer, err := dialRepositoryProbeSession(ctx, o.settings, o.resolver, o.epoch, o.factory, workerID, operation, "-probe")
	if err != nil {
		return nil, nil, err
	}
	return client, closer, nil
}

// dialRepositoryProbeSession opens the short-lived session that every
// repository question uses: the reachability probe, exact-ref resolution and
// commit export alike. The operation selects the worker entry point an SSH
// transport starts; the suffix keeps their session identities apart.
func dialRepositoryProbeSession(
	ctx context.Context,
	settings config.BacklogV2,
	resolver workerruntime.ProtocolCredentialResolver,
	epoch int64,
	factory workerproto.CommandFactory,
	workerID, operation, sessionSuffix string,
) (*workerproto.Client, func() error, error) {
	if resolver == nil || epoch < 1 {
		return nil, nil, errors.New("repository probe: coordinator authority and credential resolver are required")
	}
	worker, configured := settings.Workers[workerID]
	if !configured || worker.Epoch == "" {
		return nil, nil, fmt.Errorf("repository probe: worker %q has no configured epoch", workerID)
	}
	binding, err := workerruntime.BuildWorkerBinding(settings, workerID, time.Now().UTC())
	if err != nil {
		return nil, nil, err
	}
	credentials, err := resolver.ResolveProtocol(ctx, binding.CredentialRef)
	if err != nil {
		return nil, nil, err
	}
	requestTimeout := settings.Transport.RequestTimeout.D()
	if requestTimeout <= 0 {
		return nil, nil, errors.New("repository probe: a positive transport request timeout is required")
	}
	var transport workerproto.RoundTripper
	var closer func() error
	if worker.Connection != "" {
		stream, err := newPersistentWorkerTransport(worker, credentials, settings, factory)
		if err != nil {
			return nil, nil, err
		}
		transport, closer = stream, stream.Close
	} else {
		ssh, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
			Address: worker.Address, RemoteCommand: coordinatorWorkerRemoteCommand,
			RemoteArguments:   []string{operation},
			RequestTimeout:    requestTimeout,
			ConnectTimeout:    min(requestTimeout, 10*time.Second),
			MaxMessageBytes:   settings.MessageLimits.MaxBytes,
			MaxStderrBytes:    settings.MessageLimits.MaxBytes,
			ResponsePrincipal: credentials.WorkerPrincipal,
			ResponseKeyID:     credentials.WorkerKeyID, ResponseSecret: credentials.WorkerSecret,
			Factory: factory,
		})
		if err != nil {
			return nil, nil, err
		}
		transport = ssh
	}
	sessionID, err := newCoordinatorWorkerSessionID(settings.Coordinator.ID, workerID+sessionSuffix)
	if err != nil {
		if closer != nil {
			_ = closer()
		}
		return nil, nil, err
	}
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: settings.Coordinator.ID, WorkerID: workerID,
		CoordinatorEpoch: epoch, WorkerEpoch: worker.Epoch, SessionID: sessionID,
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
