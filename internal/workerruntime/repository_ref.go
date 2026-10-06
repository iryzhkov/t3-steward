package workerruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// RepositoryRefResolver is the optional driver capability behind exact-ref
// resolution. It is optional for the same reason RepositoryProber is: a driver
// that cannot run a process here must say so rather than answer.
type RepositoryRefResolver interface {
	ResolveRepositoryRef(context.Context, workerproto.RepositoryRefRequest) (workerproto.RepositoryRefResolution, error)
}

// ResolveRepositoryRef answers one exact-ref resolution through the driver.
func (r *Runtime) ResolveRepositoryRef(ctx context.Context, request workerproto.RepositoryRefRequest) (workerproto.RepositoryRefResolution, error) {
	if r == nil || r.driver == nil {
		return workerproto.RepositoryRefResolution{}, errors.New("worker runtime: no driver can resolve repository refs")
	}
	resolver, supported := r.driver.(RepositoryRefResolver)
	if !supported {
		return workerproto.RepositoryRefResolution{}, errors.New("worker runtime: this worker does not resolve repository refs")
	}
	return resolver.ResolveRepositoryRef(ctx, request)
}

// ResolveRepositoryRef resolves one exact ref on this worker.
//
// It takes the repository probe's path on purpose: the same process runner and
// runs root, the same requirement that the project's credential references be
// available here before anything runs, and the same fixed argument vector. What
// differs is the answer, which is the object the exact ref names, and that
// nothing is retained: every request observes the remote again.
func (d *LocalDriver) ResolveRepositoryRef(ctx context.Context, request workerproto.RepositoryRefRequest) (workerproto.RepositoryRefResolution, error) {
	if err := workerproto.ValidateRepositoryRefRequest(request); err != nil {
		return workerproto.RepositoryRefResolution{}, err
	}
	if err := backlog.ValidateExactRef(request.Ref); err != nil {
		// An invalid name is an answer, not a failure: the caller asked a
		// question that has none, and nothing was run to find that out.
		return workerproto.RepositoryRefResolution{
			Ref:    request.Ref,
			Status: workerproto.RefResolutionInvalidRef,
			Detail: workerproto.BoundRepositoryProbeDetail(err.Error()),
			// No credential reference was consulted for a name that cannot be
			// resolved, so none is reported as resolved.
			ObservedAt: d.observedAt(),
		}, nil
	}
	if err := backlog.ValidateRepositoryProbeArguments(request.Repository, request.Ref); err != nil {
		return workerproto.RepositoryRefResolution{}, err
	}
	runner := d.preflightRunner()
	if runner == nil {
		return workerproto.RepositoryRefResolution{}, errors.New("resolve repository ref: this worker has no process runner")
	}
	if len(request.CredentialRefs) > 0 {
		if d.Credentials == nil {
			return workerproto.RepositoryRefResolution{}, errors.New("resolve repository ref: required credential resolver is unavailable")
		}
		if err := d.Credentials.Require(ctx, request.CredentialRefs); err != nil {
			return workerproto.RepositoryRefResolution{}, fmt.Errorf("resolve repository ref: %w", err)
		}
	}
	resolved, err := backlog.ResolveExactRef(ctx, runner, d.Config.RunsRoot, backlog.ExactRefRequest{
		Repository:     request.Repository,
		Ref:            request.Ref,
		CredentialRefs: append([]string(nil), request.CredentialRefs...),
		MaxOutputBytes: workerproto.MaxRepositoryProbeOutputBytes,
		Timeout:        repositoryProbeTimeout(request.TimeoutSeconds),
	})
	if err != nil {
		return workerproto.RepositoryRefResolution{}, err
	}
	resolution := workerproto.RepositoryRefResolution{
		Ref:                 request.Ref,
		Class:               string(resolved.Class),
		ExitCode:            resolved.ExitCode,
		Detail:              workerproto.BoundRepositoryProbeDetail(resolved.Detail),
		CredentialsResolved: true,
		ObservedAt:          d.observedAt(),
	}
	switch {
	case resolved.Found:
		resolution.Status, resolution.ObjectID = workerproto.RefResolutionResolved, resolved.ObjectID
	case resolved.Class == backlog.RepositoryRefNotFound:
		resolution.Status = workerproto.RefResolutionNotFound
	default:
		resolution.Status = workerproto.RefResolutionUnreachable
	}
	return resolution, nil
}
