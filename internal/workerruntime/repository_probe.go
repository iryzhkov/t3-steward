package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// RepositoryProber is the optional driver capability behind the built-in
// repository-reachability probe.
//
// It is optional rather than part of Driver because a driver that cannot run a
// process on this host cannot answer the question, and a worker that answers
// "unsupported" is honest in a way a worker that answers "reachable" from an
// empty implementation would not be. The coordinator treats a refusal here as
// an unobserved reachability, never as a passed check.
type RepositoryProber interface {
	ObserveRepository(context.Context, workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error)
}

// ObserveRepository answers one repository-reachability probe.
//
// The runtime holds no reachability state of its own: the observation is made by
// the driver, under the same execution identity and credential references the
// driver would use to prepare the real workspace.
func (r *Runtime) ObserveRepository(ctx context.Context, request workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	if r == nil || r.driver == nil {
		return workerproto.RepositoryObservation{}, errors.New("worker runtime: no driver can observe repository reachability")
	}
	prober, supported := r.driver.(RepositoryProber)
	if !supported {
		return workerproto.RepositoryObservation{}, errors.New("worker runtime: this worker does not observe repository reachability")
	}
	return prober.ObserveRepository(ctx, request)
}

// ObserveRepository runs the built-in git_ls_remote probe on this worker.
//
// Three properties make the answer mean what it claims.
//
// It runs through the same process runner preflight and verification use, so the
// execution identity is the one the real task would have. It requires the
// project's credential references to be available here before it reports a
// reachability at all, so a probe cannot succeed on credentials only the
// coordinator holds. And it reports that a reference resolved rather than what
// it resolved to: no credential value reaches the result, the detail, or any
// log, and the detail is redacted and bounded before it leaves this host.
func (d *LocalDriver) ObserveRepository(ctx context.Context, request workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	if err := workerproto.ValidateRepositoryProbeRequest(request); err != nil {
		return workerproto.RepositoryObservation{}, err
	}
	// The catalog's own validators run here as well as on the coordinator. They
	// are what keeps a repository or ref that begins with a dash from reaching an
	// argument vector, and the worker must not depend on a caller having applied
	// them.
	if err := backlog.ValidateRepositoryProbeArguments(request.Repository, request.Ref); err != nil {
		return workerproto.RepositoryObservation{}, err
	}
	runner := d.preflightRunner()
	if runner == nil {
		return workerproto.RepositoryObservation{}, errors.New("observe repository: this worker has no process runner")
	}
	if len(request.CredentialRefs) > 0 {
		if d.Credentials == nil {
			return workerproto.RepositoryObservation{}, errors.New("observe repository: required credential resolver is unavailable")
		}
		// A reference that is unavailable here is reported as an unobserved
		// reachability rather than as an authentication failure. The coordinator
		// already states an absent credential reference as its own permanent
		// finding, and manufacturing a second, different permanent verdict out of
		// the same fact would refuse a campaign twice for one cause, once with
		// the wrong name.
		if err := d.Credentials.Require(ctx, request.CredentialRefs); err != nil {
			return workerproto.RepositoryObservation{}, fmt.Errorf("observe repository: %w", err)
		}
	}
	// The evidence key is the coordinator's concern: this side runs the probe
	// once and retains nothing, so the key here carries only what the argument
	// vector needs.
	observation, err := backlog.ObserveRepositoryWith(ctx, nil, runner, d.Config.RunsRoot, backlog.RepositoryProbeKey{
		Repository:     request.Repository,
		Ref:            request.Ref,
		CredentialRefs: append([]string(nil), request.CredentialRefs...),
	}, backlog.RepositoryProbeOptions{
		MaxOutputBytes: repositoryProbeOutputBound(request.MaxOutputBytes),
		Timeout:        repositoryProbeTimeout(request.TimeoutSeconds),
	})
	if err != nil {
		return workerproto.RepositoryObservation{}, err
	}
	return workerproto.RepositoryObservation{
		Class:               string(observation.Class),
		ExitCode:            observation.ExitCode,
		Detail:              workerproto.BoundRepositoryProbeDetail(observation.Detail),
		CredentialsResolved: true,
		ObservedAt:          d.observedAt(),
	}, nil
}

// observedAt reads the driver's clock, and falls back to the wall clock for a
// driver built directly rather than through NewLocalDriver.
func (d *LocalDriver) observedAt() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

// repositoryProbeOutputBound and repositoryProbeTimeout clamp what the
// coordinator asked for to what this worker is willing to spend. A request that
// declared nothing gets the maximum, which is a bound rather than an absence of
// one.
func repositoryProbeOutputBound(requested int) int {
	if requested <= 0 || requested > workerproto.MaxRepositoryProbeOutputBytes {
		return workerproto.MaxRepositoryProbeOutputBytes
	}
	return requested
}

func repositoryProbeTimeout(seconds int) time.Duration {
	requested := time.Duration(seconds) * time.Second
	if requested <= 0 || requested > workerproto.MaxRepositoryProbeTimeout {
		return workerproto.MaxRepositoryProbeTimeout
	}
	return requested
}
