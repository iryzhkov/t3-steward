package workerproto

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MessageRepositoryRefResolve and MessageRepositoryRefResolution are the
// request/response pair that asks one worker which object one exact ref names on
// a project's remote.
//
// It extends the repository probe rather than adding a coordinator-side lookup,
// for the same reason the probe exists: the coordinator's network position and
// credentials are not the worker's, and the head that matters is the one the
// worker that pushed it can see. Like the probe it carries a repository, one ref
// and credential references, never an argument vector, and the worker builds one
// fixed command from them.
//
// Unlike the probe the answer is never retained: the question is "what does this
// ref point at now", and a cached answer to that is a stale head.
const (
	MessageRepositoryRefResolve    MessageType = "repository-ref-resolve"
	MessageRepositoryRefResolution MessageType = "repository-ref-resolution"
)

// CapabilityRepositoryRefResolve advertises that this worker build understands
// MessageRepositoryRefResolve. The coordinator sends the message only to a worker
// whose observed inventory carries it; an older worker never meets the message,
// and one that did would refuse it through its allowlist.
const CapabilityRepositoryRefResolve = "repository-ref-resolve-v1"

// RefResolutionStatus is the closed set of answers a ref resolution can give.
type RefResolutionStatus string

const (
	// RefResolutionResolved means the remote answered and advertises exactly
	// the requested ref name; ObjectID is what it points at.
	RefResolutionResolved RefResolutionStatus = "resolved"
	// RefResolutionNotFound means the remote answered and has no ref with
	// exactly that name. A ref whose name only ends with it does not count.
	RefResolutionNotFound RefResolutionStatus = "not-found"
	// RefResolutionUnreachable means the remote did not answer the question:
	// timeout, network, authentication or a missing repository. Class carries
	// the reachability classification.
	RefResolutionUnreachable RefResolutionStatus = "unreachable"
	// RefResolutionInvalidRef means the ref name is not an exact, well-formed
	// full ref name, so nothing was run.
	RefResolutionInvalidRef RefResolutionStatus = "invalid-ref"
)

// RepositoryRefRequest asks one worker to resolve one exact ref name. The
// credential references are references and never values.
type RepositoryRefRequest struct {
	Repository     string   `json:"repository"`
	Ref            string   `json:"ref"`
	CredentialRefs []string `json:"credentialRefs,omitempty"`
	TimeoutSeconds int      `json:"timeoutSeconds,omitempty"`
}

// RepositoryRefResolution is one worker's answer. ObjectID is present only when
// Status is resolved, and is then a full lowercase SHA-1 or SHA-256 object ID.
type RepositoryRefResolution struct {
	Ref                 string              `json:"ref"`
	Status              RefResolutionStatus `json:"status"`
	ObjectID            string              `json:"objectId,omitempty"`
	Class               string              `json:"class,omitempty"`
	ExitCode            int                 `json:"exitCode"`
	Detail              string              `json:"detail,omitempty"`
	CredentialsResolved bool                `json:"credentialsResolved"`
	ObservedAt          time.Time           `json:"observedAt"`
}

// ValidateRepositoryRefRequest enforces the transport bounds on one request. It
// shares the probe's bounds and its refusal of option-shaped values; the ref
// syntax itself is judged by the worker, which answers invalid-ref.
func ValidateRepositoryRefRequest(request RepositoryRefRequest) error {
	return ValidateRepositoryProbeRequest(RepositoryProbeRequest{
		Repository:     request.Repository,
		Ref:            request.Ref,
		CredentialRefs: request.CredentialRefs,
		TimeoutSeconds: request.TimeoutSeconds,
	})
}

// ValidateRepositoryRefResolution checks an answer against the request it
// answers before a coordinator acts on it. A reply that names another ref,
// carries an object ID it should not, or carries a malformed one is refused
// whole.
func ValidateRepositoryRefResolution(request RepositoryRefRequest, resolution RepositoryRefResolution) error {
	if resolution.Ref != request.Ref {
		return errors.New("worker protocol: ref resolution answers a different ref")
	}
	if resolution.ObservedAt.IsZero() {
		return errors.New("worker protocol: ref resolution carries no observation time")
	}
	if len(resolution.Class) > MaxRepositoryProbeValueBytes {
		return errors.New("worker protocol: ref resolution classification is oversized")
	}
	if len(resolution.Detail) > MaxRepositoryProbeDetailBytes {
		return fmt.Errorf("worker protocol: ref resolution detail exceeds %d bytes", MaxRepositoryProbeDetailBytes)
	}
	switch resolution.Status {
	case RefResolutionResolved:
		if !IsFullObjectID(resolution.ObjectID) {
			return errors.New("worker protocol: resolved ref carries no full object ID")
		}
	case RefResolutionNotFound, RefResolutionUnreachable, RefResolutionInvalidRef:
		if resolution.ObjectID != "" {
			return fmt.Errorf("worker protocol: %s ref resolution carries an object ID", resolution.Status)
		}
	default:
		return fmt.Errorf("worker protocol: unknown ref resolution status %q", resolution.Status)
	}
	return nil
}

// IsFullObjectID reports whether value is a full lowercase hexadecimal SHA-1
// (40) or SHA-256 (64) object ID. An abbreviation is never accepted as a head.
func IsFullObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// ResolveRepositoryRef asks one worker which object one exact ref names on the
// remote, using the worker's own configured remote access and credentials.
//
// Like ObserveRepository it carries no snapshot requirement. The caller is
// responsible for sending it only to a worker that advertises
// CapabilityRepositoryRefResolve.
func (c *Client) ResolveRepositoryRef(ctx context.Context, request RepositoryRefRequest) (RepositoryRefResolution, error) {
	if err := ValidateRepositoryRefRequest(request); err != nil {
		return RepositoryRefResolution{}, err
	}
	var resolution RepositoryRefResolution
	if err := c.exchange(ctx, MessageRepositoryRefResolve, MessageRepositoryRefResolution, request, &resolution); err != nil {
		return RepositoryRefResolution{}, err
	}
	if err := ValidateRepositoryRefResolution(request, resolution); err != nil {
		return RepositoryRefResolution{}, err
	}
	return resolution, nil
}
