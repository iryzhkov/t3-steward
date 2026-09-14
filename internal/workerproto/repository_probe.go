package workerproto

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// MessageRepositoryProbe and MessageRepositoryObservation are the bounded
// request/response pair behind the built-in repository-reachability probe.
//
// The probe belongs on the worker because the coordinator's credentials and
// network position are not the worker's, and a coordinator-side answer would
// report success for a repository the worker cannot reach. The pair is
// deliberately narrow: the request names a repository, a ref and the credential
// references the real task would use, and the worker turns them into exactly one
// fixed argument vector. There is no caller-supplied argument vector, no command
// name and no shell, so this message grants the coordinator no authority beyond
// asking one question about one repository the catalog already names.
const (
	MessageRepositoryProbe       MessageType = "repository-probe"
	MessageRepositoryObservation MessageType = "repository-observation"
)

// The bounds below are applied by the coordinator before it sends, by the worker
// before it runs anything, and again by the coordinator when it reads the reply,
// so neither side depends on the other having been well behaved.
const (
	// MaxRepositoryProbeOutputBytes bounds what one probe run may accumulate. A
	// repository with very many refs would otherwise be buffered in full before
	// anything truncated it.
	MaxRepositoryProbeOutputBytes = 64 << 10
	// MaxRepositoryProbeTimeout bounds one probe run on the worker.
	MaxRepositoryProbeTimeout = time.Minute
	// MaxRepositoryProbeCredentialRefs bounds how many references one probe may
	// be asked to resolve for availability.
	MaxRepositoryProbeCredentialRefs = 16
	// MaxRepositoryProbeValueBytes bounds the repository, the ref, and one
	// credential reference.
	MaxRepositoryProbeValueBytes = 2048
	// MaxRepositoryProbeDetailBytes bounds the human-readable detail the worker
	// returns. The detail exists to explain a refusal to an operator, not to
	// carry a repository's output back to the coordinator.
	MaxRepositoryProbeDetailBytes = 512
)

// RepositoryProbeRequest asks one worker whether it can read one repository and
// ref under the credential references the real task would use.
//
// CredentialRefs are references and never values. The worker resolves them for
// availability only, and reports that a reference resolved rather than what it
// resolved to.
type RepositoryProbeRequest struct {
	Repository     string   `json:"repository"`
	Ref            string   `json:"ref"`
	CredentialRefs []string `json:"credentialRefs,omitempty"`
	MaxOutputBytes int      `json:"maxOutputBytes,omitempty"`
	TimeoutSeconds int      `json:"timeoutSeconds,omitempty"`
}

// RepositoryObservation is one worker's classified answer.
//
// Class is the frozen classification the probe assigns; this package keeps it a
// string rather than importing the classifier, because the classifier lives
// alongside the probe and that package already depends on this one. The
// coordinator parses it back into the closed set and refuses a class it does not
// know, so an unrecognised value cannot become a verdict.
type RepositoryObservation struct {
	Class               string    `json:"class"`
	ExitCode            int       `json:"exitCode"`
	Detail              string    `json:"detail,omitempty"`
	CredentialsResolved bool      `json:"credentialsResolved"`
	ObservedAt          time.Time `json:"observedAt"`
}

// ValidateRepositoryProbeRequest enforces the transport bounds on one probe
// request.
//
// It deliberately does not restate Git's repository and ref syntax rules. Those
// belong to the catalog validators, which the probe itself applies before it
// builds an argument vector; restating them here would create a second, drifting
// copy. What this function owns is the property no syntax rule can give: the
// message is bounded, and a value that could be read as an option is refused as
// a value rather than passed on.
func ValidateRepositoryProbeRequest(request RepositoryProbeRequest) error {
	if err := validateRepositoryProbeValue("repository", request.Repository); err != nil {
		return err
	}
	if err := validateRepositoryProbeValue("ref", request.Ref); err != nil {
		return err
	}
	if len(request.CredentialRefs) > MaxRepositoryProbeCredentialRefs {
		return fmt.Errorf("worker protocol: %d credential references exceed the limit of %d",
			len(request.CredentialRefs), MaxRepositoryProbeCredentialRefs)
	}
	for _, reference := range request.CredentialRefs {
		if err := validateRepositoryProbeValue("credential reference", reference); err != nil {
			return err
		}
	}
	if request.MaxOutputBytes < 0 || request.MaxOutputBytes > MaxRepositoryProbeOutputBytes {
		return fmt.Errorf("worker protocol: repository probe output bound must be between 0 and %d bytes",
			MaxRepositoryProbeOutputBytes)
	}
	if request.TimeoutSeconds < 0 || time.Duration(request.TimeoutSeconds)*time.Second > MaxRepositoryProbeTimeout {
		return fmt.Errorf("worker protocol: repository probe timeout must be between 0 and %s",
			MaxRepositoryProbeTimeout)
	}
	return nil
}

func validateRepositoryProbeValue(name, value string) error {
	if value == "" {
		return fmt.Errorf("worker protocol: repository probe %s is required", name)
	}
	if len(value) > MaxRepositoryProbeValueBytes {
		return fmt.Errorf("worker protocol: repository probe %s exceeds %d bytes", name, MaxRepositoryProbeValueBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("worker protocol: repository probe %s is not valid UTF-8", name)
	}
	if strings.HasPrefix(value, "-") {
		return fmt.Errorf("worker protocol: repository probe %s beginning with %q is refused as a value, never passed as a flag", name, "-")
	}
	if strings.ContainsAny(value, "\x00\n\r") {
		return fmt.Errorf("worker protocol: repository probe %s contains a control character", name)
	}
	return nil
}

// ValidateRepositoryObservation checks what came back before a coordinator acts
// on it. A reply whose detail is unbounded is refused whole rather than
// truncated, because a worker that ignored the bound is not one whose
// classification should be trusted either.
func ValidateRepositoryObservation(observation RepositoryObservation) error {
	if observation.Class == "" {
		return errors.New("worker protocol: repository observation carries no classification")
	}
	if len(observation.Class) > MaxRepositoryProbeValueBytes {
		return errors.New("worker protocol: repository observation classification is oversized")
	}
	if len(observation.Detail) > MaxRepositoryProbeDetailBytes {
		return fmt.Errorf("worker protocol: repository observation detail exceeds %d bytes", MaxRepositoryProbeDetailBytes)
	}
	return nil
}

// BoundRepositoryProbeDetail truncates one detail to the transport bound without
// splitting a rune.
func BoundRepositoryProbeDetail(detail string) string {
	if len(detail) <= MaxRepositoryProbeDetailBytes {
		return detail
	}
	cut := detail[:MaxRepositoryProbeDetailBytes]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
